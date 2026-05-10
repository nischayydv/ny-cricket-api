"""
Cricket Score API v8.0
======================

ROOT CAUSE OF OSCILLATION (was in v7):
  The problem was NOT the CDN. The problem was in the PARSER:

  1. _find_json_object() only searched for `"miniscore":{` (no space).
     Cricbuzz often outputs `"miniscore": {` WITH a space → always fell to
     the broken regex fallback path which scanned ms_substr[0:3000] and
     still matched `"overs"` from a COMPLETED innings block that happened
     to appear before the real miniscore in that 3000-char window.

  2. The `__next_f` chunks are pushed in RANDOM ORDER by Next.js streaming.
     v7 joined raw_texts in arrival order, so "full" text had miniscore
     data interleaved with scorecard data. When chunk order changed between
     requests, the regex fallback matched DIFFERENT "overs" values →
     oscillation between current and past over number.

  3. recentOvsStats in v5 was taken from the LAST pipe-segment which IS
     the current over — that part was correct. But v7 changed it to use
     overSummary as primary and discarded the last segment → broke it
     for matches where overSummary is absent (some match types).

FIXES IN v8:
  A. _find_json_object() now handles both `"key":{` AND `"key": {` and
     also `"key" : {` (all spacing variants) — no more fallback needed
     for the miniscore block in normal cases.

  B. Miniscore is extracted by POSITION: we find the miniscore key, then
     brace-count the EXACT JSON object. We never regex-scan outside it.

  C. current_over_balls: PRIMARY = overSummary (current over only).
     FALLBACK = last pipe-segment of recentOvsStats (v5 behavior).
     Both work; whichever is present wins.

  D. current_over_number: read ONLY from inside the extracted miniscore
     object — never from a global regex on the full text.

  E. HTTP: new client per live request (v7 approach kept — this IS correct
     for CDN busting). No local cache for live endpoints.

  F. Chunk deduplication: raw_texts are deduplicated before joining so
     repeated chunks don't corrupt regex counts.
"""

import re
import json
import html as html_lib
import time
import random
import asyncio
from typing import List, Optional, Dict, Any, Tuple
from contextlib import asynccontextmanager

import httpx
from bs4 import BeautifulSoup, Tag
from fastapi import FastAPI, Query, Request, Path
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from pydantic import BaseModel, field_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

NF = "not found"
CB = "https://www.cricbuzz.com"

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
]

_REFERERS = [
    "https://www.google.com/search?q=cricket+live+score",
    "https://www.google.com/",
    "https://www.cricbuzz.com/",
]


def _fresh_headers() -> Dict[str, str]:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": random.choice(["en-US,en;q=0.9", "en-GB,en;q=0.8", "en-IN,en;q=0.7"]),
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Referer": random.choice(_REFERERS),
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "cross-site",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Static cache (non-live pages only — 30s TTL)
# ─────────────────────────────────────────────────────────────────────────────

_STATIC_CACHE: Dict[str, Tuple[float, str]] = {}
_STATIC_CACHE_TTL = 30.0


def _static_cache_get(url: str) -> Optional[str]:
    entry = _STATIC_CACHE.get(url)
    if entry and (time.monotonic() - entry[0]) < _STATIC_CACHE_TTL:
        return entry[1]
    return None


def _static_cache_set(url: str, html: str) -> None:
    _STATIC_CACHE[url] = (time.monotonic(), html)


# ─────────────────────────────────────────────────────────────────────────────
# HTTP — new client per live request (CDN bypass)
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch_live(url: str, retries: int = 3) -> Optional[str]:
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(12.0, connect=6.0),
                follow_redirects=True,
                http2=False,
                limits=httpx.Limits(max_connections=1, max_keepalive_connections=0),
            ) as client:
                r = await client.get(url, headers=_fresh_headers())
                if r.status_code == 200:
                    text = r.text
                    if "__next_f" in text:
                        return text
                    if attempt < retries - 1:
                        await asyncio.sleep(0.5)
                        continue
                    return text
                if r.status_code in (429, 503):
                    await asyncio.sleep(1.0 * (attempt + 1))
                elif r.status_code == 404:
                    return None
        except (httpx.TimeoutException, httpx.ConnectError, httpx.RemoteProtocolError):
            if attempt < retries - 1:
                await asyncio.sleep(0.4 * (attempt + 1))
        except Exception:
            if attempt < retries - 1:
                await asyncio.sleep(0.3)
    return None


async def _fetch_static(url: str) -> Optional[str]:
    cached = _static_cache_get(url)
    if cached:
        return cached
    html = await _fetch_live(url)
    if html:
        _static_cache_set(url, html)
    return html


async def _fetch_many_live(*urls: str) -> List[Optional[str]]:
    return list(await asyncio.gather(*(_fetch_live(u) for u in urls)))


@asynccontextmanager
async def _lifespan(app: "FastAPI"):
    yield


# ─────────────────────────────────────────────────────────────────────────────
# Pydantic Models
# ─────────────────────────────────────────────────────────────────────────────

class APIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message


class RecentBall(BaseModel):
    label: str
    runs: int = 0
    is_dot: bool = False
    is_four: bool = False
    is_six: bool = False
    is_wicket: bool = False
    is_wide: bool = False
    is_no_ball: bool = False


class ScorecardBatsman(BaseModel):
    name: str = NF
    runs: str = NF
    balls: str = NF
    fours: str = NF
    sixes: str = NF
    strike_rate: str = NF
    is_striker: bool = False


class ScorecardBowler(BaseModel):
    name: str = NF
    overs: str = NF
    maidens: str = NF
    runs: str = NF
    wickets: str = NF
    economy: str = NF


class InningsScore(BaseModel):
    team: str = NF
    runs: str = NF
    wickets: str = NF
    overs: str = NF
    display: str = NF


class LiveScoreResponse(BaseModel):
    status: str
    match_id: str = NF
    innings_id: str = NF
    title: str = NF
    match_type: str = NF
    venue: str = NF
    match_status: str = NF
    toss: str = NF
    innings: List[InningsScore] = []
    score: str = NF
    current_batsmen: List[ScorecardBatsman] = []
    current_bowler: ScorecardBowler = ScorecardBowler()
    last_wicket: str = NF
    partnership: str = NF
    current_run_rate: str = NF
    required_run_rate: str = NF
    target: str = NF
    current_over_balls: List[RecentBall] = []
    current_over_number: str = NF
    balls_in_current_over: int = 0
    recent_overs_summary: List[str] = []
    day_number: Optional[int] = None
    match_state: str = NF
    fetched_at: float = 0.0


class BattingEntry(BaseModel):
    name: str = NF
    dismissal: str = NF
    runs: str = NF
    balls: str = NF
    fours: str = NF
    sixes: str = NF
    strike_rate: str = NF


class BowlingEntry(BaseModel):
    name: str = NF
    overs: str = NF
    maidens: str = NF
    runs: str = NF
    wickets: str = NF
    no_balls: str = NF
    wides: str = NF
    economy: str = NF


class FowEntry(BaseModel):
    batsman: str = NF
    score: str = NF
    over: str = NF


class PartnershipEntry(BaseModel):
    batsman1: str = NF
    batsman1_runs: str = NF
    batsman1_balls: str = NF
    batsman2: str = NF
    batsman2_runs: str = NF
    batsman2_balls: str = NF
    partnership_runs: str = NF
    partnership_balls: str = NF


class PowerplayEntry(BaseModel):
    type: str = NF
    overs: str = NF
    runs: str = NF


class InningsScorecard(BaseModel):
    team: str = NF
    score: str = NF
    overs: str = NF
    batting: List[BattingEntry] = []
    bowling: List[BowlingEntry] = []
    extras: str = NF
    fall_of_wickets: List[FowEntry] = []
    powerplays: List[PowerplayEntry] = []
    partnerships: List[PartnershipEntry] = []
    yet_to_bat: List[str] = []


class ScorecardResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    result: str = NF
    innings: List[InningsScorecard] = []


class MatchInfo(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    series: str = NF
    match_type: str = NF
    match_number: str = NF
    venue: str = NF
    city: str = NF
    date: str = NF
    day_night: str = NF
    toss: str = NF
    umpires: List[str] = []
    third_umpire: str = NF
    match_referee: str = NF
    result: str = NF
    state: str = NF


class PlayerEntry(BaseModel):
    name: str = NF
    role: str = NF
    is_captain: bool = False
    is_keeper: bool = False


class TeamSquad(BaseModel):
    team: str = NF
    playing_xi: List[PlayerEntry] = []
    bench: List[PlayerEntry] = []


class SquadsResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    squads: List[TeamSquad] = []


class BallEvent(BaseModel):
    ball_number: int
    ball_label: str
    runs: int = 0
    is_dot: bool = False
    is_wide: bool = False
    is_no_ball: bool = False
    is_wicket: bool = False
    is_four: bool = False
    is_six: bool = False
    extras: int = 0
    commentary: str = NF


class OverDetail(BaseModel):
    over_number: int
    innings_number: int = 1
    bowler: str = NF
    batsmen: List[str] = []
    runs_in_over: int = 0
    wickets_in_over: int = 0
    balls: List[BallEvent] = []
    over_summary: str = NF
    is_current: bool = False


class OversResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    total_overs: int = 0
    current_over: Optional[int] = None
    overs: List[OverDetail] = []


class PreviewResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    fetched_pages: List[str] = []
    fetch_time_ms: int = 0
    ai_summary: str = NF
    score: Optional[LiveScoreResponse] = None
    scorecard: Optional[ScorecardResponse] = None
    info: Optional[MatchInfo] = None
    recent_over: Optional[OverDetail] = None


class MatchCard(BaseModel):
    match_id: str = NF
    series: str = NF
    title: str = NF
    teams: List[Dict[str, str]] = []
    venue: str = NF
    date: str = NF
    time: str = NF
    match_type: str = NF
    status: str = NF
    overview: str = NF


class MatchListResponse(BaseModel):
    status: str
    type: str = NF
    total: int = 0
    matches: List[MatchCard] = []


class MatchValidator(BaseModel):
    match_id: str

    @field_validator("match_id")
    @classmethod
    def validate(cls, v: str) -> str:
        v = v.strip()
        if not v or not v.isdigit():
            raise ValueError("match_id must be digits only")
        if len(v) < 4 or len(v) > 20:
            raise ValueError("match_id length must be 4-20 digits")
        return v


# ─────────────────────────────────────────────────────────────────────────────
# FastAPI App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Cricket Score API",
    version="8.0.0",
    description="Stable current over. No oscillation. Correct miniscore extraction.",
    docs_url=None,
    redoc_url=None,
    lifespan=_lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.middleware("http")
async def _security(request: Request, call_next):
    resp = await call_next(request)
    resp.headers.update({
        "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
        "Pragma": "no-cache",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
    })
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# THE CORE FIX: Robust JSON object extractor
#
# v7 bug: only searched for `"key":{` — missed `"key": {` and `"key" : {`
# This caused _find_json_object("miniscore") to ALWAYS return None when
# Cricbuzz used spaced syntax → fell to broken regex fallback → oscillation.
# ─────────────────────────────────────────────────────────────────────────────

def _find_json_object(text: str, key: str) -> Optional[Dict]:
    """
    Find and parse a JSON object by key. Handles ALL spacing variants:
      "key":{    "key": {    "key" : {
    Returns parsed dict or None.
    """
    # Try all spacing variants of the key
    search_patterns = [
        f'"{key}"' + ':{',
        f'"{key}"' + ': {',
        f'"{key}"' + ' : {',
        f'"{key}"' + ':  {',
    ]

    for pattern in search_patterns:
        idx = text.find(pattern)
        if idx == -1:
            continue
        # Start at the opening brace
        brace_start = text.index('{', idx + len(f'"{key}"'))
        obj_str = _extract_json_object_at(text, brace_start)
        if obj_str:
            try:
                return json.loads(obj_str)
            except json.JSONDecodeError:
                # Try with some cleanup
                try:
                    clean = re.sub(r':\s*\$undefined\b', ': null', obj_str)
                    return json.loads(clean)
                except Exception:
                    pass
    return None


def _extract_json_object_at(text: str, start: int) -> Optional[str]:
    """
    Extract a complete JSON object starting at index `start` (which must be '{').
    Properly handles nested objects, arrays, and strings.
    Returns the raw JSON string or None.
    """
    if start >= len(text) or text[start] != '{':
        return None

    depth = 0
    i = start
    n = len(text)

    while i < n:
        c = text[i]
        if c == '{':
            depth += 1
            i += 1
        elif c == '}':
            depth -= 1
            i += 1
            if depth == 0:
                return text[start:i]
        elif c == '"':
            # Skip string — handle escape sequences
            i += 1
            while i < n:
                sc = text[i]
                if sc == '\\':
                    i += 2  # skip escaped char
                elif sc == '"':
                    i += 1
                    break
                else:
                    i += 1
        elif c == '[':
            # Skip array by tracking depth separately won't work;
            # we just let depth tracking handle it via braces inside arrays
            i += 1
        else:
            i += 1

    return None


# ─────────────────────────────────────────────────────────────────────────────
# Next.js chunk extractor
# Deduplicates chunks so repeated payloads don't corrupt parsing
# ─────────────────────────────────────────────────────────────────────────────

def _extract_nextjs_chunks(html: str) -> str:
    """
    Extract and join all __next_f type-1 payloads.
    Deduplicates chunks by content hash to prevent repeated chunks
    from making the same key appear twice (causing wrong regex matches).
    Returns a single joined string.
    """
    pattern = re.compile(
        r'self\.__next_f\.push\(\[(\d+),\s*"((?:[^"\\]|\\.)*)"\]\)',
        re.DOTALL
    )

    seen_chunks: set = set()
    chunks: List[str] = []

    for m in pattern.finditer(html):
        if int(m.group(1)) != 1:
            continue
        try:
            raw = m.group(2).encode("utf-8").decode("unicode_escape")
        except Exception:
            raw = m.group(2)

        # Deduplicate by content
        chunk_hash = hash(raw)
        if chunk_hash in seen_chunks:
            continue
        seen_chunks.add(chunk_hash)
        chunks.append(raw)

    return "\n".join(chunks)


# ─────────────────────────────────────────────────────────────────────────────
# THE CORE EXTRACTION PIPELINE
#
# Step 1: Extract miniscore JSON object using robust brace-counting extractor
# Step 2: Read overs/overSummary/recentOvsStats ONLY from inside that object
# Step 3: Never regex-scan the full text for overs — always use miniscore obj
# ─────────────────────────────────────────────────────────────────────────────

def _extract_nextjs_json(html: str) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "miniscore": {},
        "matchHeader": {},
        "raw_texts": [],
        "innings_id": NF,
        "current_over_balls": [],
        "current_over_number": NF,
        "balls_in_current_over": 0,
        "recent_overs_summary": [],
    }

    full = _extract_nextjs_chunks(html)
    result["raw_texts"] = [full]  # keep as list for compatibility

    # ── Step 1: Extract miniscore JSON object ──────────────────────────────
    ms_obj = _find_json_object(full, "miniscore")
    if ms_obj is not None:
        result["miniscore"] = ms_obj
        iid = ms_obj.get("inningsId")
        if iid is not None:
            result["innings_id"] = str(iid)
    else:
        # Fallback: find the miniscore substring by brace position
        ms_obj = _find_miniscore_fallback(full)
        if ms_obj:
            result["miniscore"] = ms_obj
            iid = ms_obj.get("inningsId")
            if iid is not None:
                result["innings_id"] = str(iid)

    # ── Step 2: Extract matchHeader ────────────────────────────────────────
    mh_obj = _find_json_object(full, "matchHeader")
    if mh_obj is not None:
        result["matchHeader"] = mh_obj
    else:
        result["matchHeader"] = _extract_match_header_regex(full)

    # ── Step 3: Current over data — ONLY from miniscore ───────────────────
    _extract_current_over(result)

    return result


def _find_miniscore_fallback(full_text: str) -> Optional[Dict]:
    """
    Fallback: locate the miniscore key, find its opening brace,
    extract the object using brace counting.
    Handles all spacing variants.
    """
    for needle in ['"miniscore":{', '"miniscore": {', '"miniscore" : {']:
        idx = full_text.find(needle)
        if idx == -1:
            continue
        brace_idx = full_text.index('{', idx)
        obj_str = _extract_json_object_at(full_text, brace_idx)
        if obj_str:
            try:
                clean = re.sub(r':\s*\$undefined\b', ': null', obj_str)
                return json.loads(clean)
            except Exception:
                pass
        # If full JSON parse fails, extract fields via regex from just this substring
        end_idx = brace_idx + 5000  # miniscore is never > 5000 chars
        substr = full_text[brace_idx:end_idx]
        return _parse_miniscore_fields_from_substr(substr)

    return None


def _parse_miniscore_fields_from_substr(substr: str) -> Dict:
    """Parse key fields from a miniscore substring using targeted regex."""
    result: Dict = {}
    field_patterns = [
        ("inningsId",       r'"inningsId"\s*:\s*(\d+)'),
        ("overs",           r'"overs"\s*:\s*([\d.]+)'),
        ("overSummary",     r'"overSummary"\s*:\s*"([^"]*)"'),
        ("recentOvsStats",  r'"recentOvsStats"\s*:\s*"([^"]*)"'),
        ("score",           r'"score"\s*:\s*(\d+)'),
        ("wickets",         r'"wickets"\s*:\s*(\d+)'),
        ("currentRunRate",  r'"currentRunRate"\s*:\s*([\d.]+)'),
        ("requiredRunRate", r'"requiredRunRate"\s*:\s*([\d.]+)'),
        ("target",          r'"target"\s*:\s*(\d+)'),
        ("customStatus",    r'"customStatus"\s*:\s*"([^"]*)"'),
        ("state",           r'"state"\s*:\s*"([^"]*)"'),
        ("lastWicket",      r'"lastWicket"\s*:\s*"([^"]{5,120})"'),
    ]
    for key, pat in field_patterns:
        m = re.search(pat, substr)
        if m:
            result[key] = m.group(1)
    return result


def _extract_match_header_regex(full_text: str) -> Dict:
    mh_data: Dict = {}
    for key, pat in [
        ("status",           r'"status"\s*:\s*"([^"]{5,120})"'),
        ("tossWinnerName",   r'"tossWinnerName"\s*:\s*"([^"]+)"'),
        ("decision",         r'"decision"\s*:\s*"([^"]+)"'),
        ("seriesDesc",       r'"seriesDesc"\s*:\s*"([^"]+)"'),
        ("matchDescription", r'"matchDescription"\s*:\s*"([^"]+)"'),
        ("matchFormat",      r'"matchFormat"\s*:\s*"([^"]+)"'),
    ]:
        m = re.search(pat, full_text)
        if m:
            mh_data[key] = m.group(1)
    return mh_data


# ─────────────────────────────────────────────────────────────────────────────
# Current over extraction — stable, no oscillation
#
# Data sources (in priority order):
#
# current_over_number → miniscore["overs"]  (e.g. "23.4")
#   Only read from the parsed miniscore dict — never from full text regex.
#
# current_over_balls → PRIMARY: miniscore["overSummary"]
#                      FALLBACK: last segment of miniscore["recentOvsStats"]
#   Both are inside the miniscore object so both are safe.
#
# recent_overs_summary → all segments of recentOvsStats EXCEPT the last
#   (last segment = current over in progress, not a completed over)
# ─────────────────────────────────────────────────────────────────────────────

def _extract_current_over(result: Dict) -> None:
    ms = result.get("miniscore", {})

    # ── Over number ────────────────────────────────────────────────────────
    overs_val = ms.get("overs", "") or ms.get("overs_str", "")
    # overs might be an int/float or string depending on JSON parse
    overs_str = str(overs_val).strip() if overs_val is not None else ""

    if overs_str and overs_str not in ("", "0", "null", "$undefined", "None"):
        result["current_over_number"] = overs_str
        try:
            ov_f = float(overs_str)
            decimal_part = round((ov_f - int(ov_f)) * 10)
            result["balls_in_current_over"] = decimal_part
        except (ValueError, TypeError):
            result["balls_in_current_over"] = 0
    else:
        result["current_over_number"] = "0.0"
        result["balls_in_current_over"] = 0

    # ── Current over balls ─────────────────────────────────────────────────
    over_summary = ms.get("overSummary", "")
    rov = ms.get("recentOvsStats", "")

    # Normalize
    over_summary = str(over_summary).strip() if over_summary else ""
    rov = str(rov).strip() if rov else ""

    current_balls: List[RecentBall] = []
    completed_segments: List[str] = []

    if rov and rov not in ("$undefined", "null", "None", ""):
        # recentOvsStats format: "0 1 4 | W 0 1 6 0 | 1 0"
        # Pipe separates overs. Last segment = current over in progress.
        segments = [s.strip() for s in rov.split("|")]
        if segments:
            last_seg = segments[-1].strip()
            completed_segments = [s for s in segments[:-1] if s.strip()]
            # Last segment is current over
            if last_seg:
                current_balls = _parse_over_balls_from_str(last_seg)

    # If overSummary is present AND non-empty, it is more authoritative
    # (it's exclusively the current over, whereas recentOvsStats last segment
    # might be the last COMPLETED over briefly at over boundary)
    if over_summary and over_summary not in ("$undefined", "null", "None"):
        os_balls = _parse_over_balls_from_str(over_summary)
        if os_balls:
            current_balls = os_balls
        elif not current_balls:
            # overSummary is empty string = over just started, no balls yet
            current_balls = []

    result["current_over_balls"] = current_balls

    # ── Recent completed overs ─────────────────────────────────────────────
    result["recent_overs_summary"] = completed_segments[-3:] if completed_segments else []


def _parse_over_balls_from_str(s: str) -> List[RecentBall]:
    """Parse balls from string like '0 1 W 4 0 6' or '• 1 W 4 • 6'."""
    if not s or not s.strip():
        return []
    balls: List[RecentBall] = []
    tokens = re.findall(r'[A-Za-z]+\d*|\d+|[•·]', s.strip())
    for tok in tokens:
        tok = tok.strip()
        if not tok or tok in ('|', '-', 'undefined', 'null', 'None'):
            continue

        is_dot = tok in ('0', '•', '·', 'dot')
        is_wide = (
            tok.upper() in ('WD', 'WIDE') or
            (tok.upper().startswith('WD') and len(tok) <= 5)
        )
        is_nb = tok.upper().startswith('NB') or tok.upper() in ('NO', 'NOBALL')
        is_wicket = tok.upper() == 'W' and not is_wide
        is_four = tok == '4'
        is_six = tok == '6'

        runs = 0
        if is_wide:
            num_m = re.search(r'\d+', tok)
            runs = int(num_m.group()) if num_m else 1
        elif is_nb:
            num_m = re.search(r'\d+', tok)
            runs = int(num_m.group()) if num_m else 1
        elif is_four:
            runs = 4
        elif is_six:
            runs = 6
        elif is_dot or is_wicket:
            runs = 0
        else:
            try:
                runs = int(tok)
            except ValueError:
                runs = 0

        label = (
            '•' if is_dot else
            'W' if is_wicket else
            '4' if is_four else
            '6' if is_six else
            'Wd' if is_wide else
            'Nb' if is_nb else
            str(runs)
        )
        balls.append(RecentBall(
            label=label, runs=runs,
            is_dot=is_dot, is_four=is_four, is_six=is_six,
            is_wicket=is_wicket, is_wide=is_wide, is_no_ball=is_nb,
        ))
    return balls


def _parse_page_html(html: str, page_type: str) -> Tuple[Dict, BeautifulSoup]:
    nj_data = _extract_nextjs_json(html)
    soup = BeautifulSoup(html, "lxml")
    return nj_data, soup


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _s(v: Any, default: str = NF) -> str:
    if v is None or v == "" or v == "$undefined":
        return default
    return str(v).strip() or default


def _soup_text(el: Any) -> str:
    if el is None:
        return NF
    raw = el.get_text(" ", strip=True) if isinstance(el, Tag) else str(el)
    out = html_lib.unescape(" ".join(raw.split())).strip()
    return out or NF


def _match_type_from_str(text: str) -> str:
    t = (text or "").upper()
    for tag in ("TEST", "T20I", "T20", "T10", "ODI", "THE HUNDRED", "LIST A", "FIRST-CLASS"):
        if tag in t:
            return tag
    return NF


def _og(soup: BeautifulSoup, prop: str) -> str:
    el = soup.find("meta", property=prop)
    return (el.get("content", "") if el else "") or ""


def _mid_from_href(href: str) -> str:
    m = re.search(r"/(\d{4,})", href or "")
    return m.group(1) if m else NF


# ─────────────────────────────────────────────────────────────────────────────
# Live Score Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_live_score_from_nj(nj: Dict, mid: str, soup: BeautifulSoup) -> LiveScoreResponse:
    ms = nj.get("miniscore", {})
    mh = nj.get("matchHeader", {})
    full = nj.get("raw_texts", [""])[0]  # single deduplicated string

    # ── Title ──────────────────────────────────────────────────────────────
    og_title = _og(soup, "og:title")
    og_desc  = _og(soup, "og:description")
    title_tag = soup.title.get_text(strip=True) if soup.title else ""

    def _clean_title(raw: str) -> str:
        raw = re.sub(r'^[\d/.()\s]+\([^)]*\)\s*\|\s*', '', raw).strip()
        parts = raw.split(' | ')
        clean_parts = []
        boilerplate = re.compile(
            r'live scores|ball.by.ball|highlights|videos|news|cricbuzz|cricket stream',
            re.IGNORECASE
        )
        for p in parts:
            if boilerplate.search(p):
                break
            clean_parts.append(p.strip())
        return ' | '.join(clean_parts).strip() or raw.strip()

    title = _clean_title(title_tag)
    title = re.sub(r"^Cricket\s*(?:commentary\s*)?\|\s*", "", title, flags=re.IGNORECASE).strip()
    if not title:
        title = _clean_title(og_title) if og_title else NF

    # ── Match status ───────────────────────────────────────────────────────
    match_status = _s(ms.get("customStatus") or ms.get("status")) or _s(mh.get("status")) or NF
    if match_status == NF:
        for cls in ["cb-text-complete", "cb-text-inprogress", "cb-game-status",
                    "cb-text-stumps", "cb-text-lunch", "cb-text-tea"]:
            el = soup.find(class_=cls)
            if el:
                match_status = _soup_text(el)
                break

    match_state = _s(ms.get("state") or mh.get("state"), "unknown")

    day_number = None
    dm = re.search(r'Day\s+(\d+)', match_status, re.IGNORECASE)
    if dm:
        day_number = int(dm.group(1))

    # ── Innings scores ─────────────────────────────────────────────────────
    innings: List[InningsScore] = []
    for pat in [
        r'([A-Z]{2,5})\s+(\d+)/(\d+)\s*\(([\d.]+)\)',
        r'([A-Z]{2,5})\s+(\d+)\s*\(([\d.]+)\)',
    ]:
        for team, *nums in re.findall(pat, og_title + " " + og_desc):
            if len(nums) == 3:
                r, w, o = nums
                innings.append(InningsScore(team=team, runs=r, wickets=w, overs=o,
                    display=f"{team} {r}/{w} ({o})"))
            elif len(nums) == 2:
                r, o = nums
                innings.append(InningsScore(team=team, runs=r, wickets="10", overs=o,
                    display=f"{team} {r} ({o})"))

    if not innings:
        innings = _extract_innings_from_nj_text(full)

    seen_teams: set = set()
    innings = [i for i in innings if i.team not in seen_teams and not seen_teams.add(i.team)]  # type: ignore

    score_str = "  |  ".join(i.display for i in innings) if innings else NF

    # ── Batsmen ────────────────────────────────────────────────────────────
    batsmen: List[ScorecardBatsman] = []
    seen_bat: set = set()
    for m in re.finditer(r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(\d+)\*?\((\d+)\)(\*)?', og_title):
        name = m.group(1).strip()
        if name not in seen_bat and len(name) > 3:
            seen_bat.add(name)
            batsmen.append(ScorecardBatsman(
                name=name, runs=m.group(2), balls=m.group(3),
                is_striker=bool(m.group(4))
            ))
    _enrich_batsmen_from_nj(batsmen, full, ms)

    # ── Bowler ─────────────────────────────────────────────────────────────
    bowler = _extract_bowler_from_nj(full, ms)

    # ── Partnership ────────────────────────────────────────────────────────
    partnership = NF
    ps = ms.get("partnerShip") or ms.get("partnership", {})
    if isinstance(ps, dict):
        p_runs = ps.get("runs", "")
        p_balls = ps.get("balls", "")
        if p_runs:
            partnership = f"{p_runs}({p_balls})" if p_balls else str(p_runs)
    if partnership == NF:
        pm = re.search(r'"partnerShip"\s*:\s*\{"balls"\s*:\s*(\d+)\s*,\s*"runs"\s*:\s*(\d+)\}', full)
        if pm:
            partnership = f"{pm.group(2)}({pm.group(1)})"

    # ── Last wicket ────────────────────────────────────────────────────────
    last_wicket = _s(ms.get("lastWicket", ""))
    if last_wicket == NF:
        lw_m = re.search(r'"lastWicket"\s*:\s*"([^"]{5,120})"', full)
        if lw_m:
            last_wicket = lw_m.group(1)

    # ── Run rates ──────────────────────────────────────────────────────────
    crr = _s(ms.get("currentRunRate", ""))
    rrr = _s(ms.get("requiredRunRate", ""))
    target = _s(ms.get("target", ""))
    if crr == NF:
        m2 = re.search(r'"currentRunRate"\s*:\s*([\d.]+)', full)
        if m2: crr = m2.group(1)
    if rrr == NF:
        m2 = re.search(r'"requiredRunRate"\s*:\s*([\d.]+)', full)
        if m2: rrr = m2.group(1)
    if target == NF:
        m2 = re.search(r'"target"\s*:\s*(\d+)', full)
        if m2: target = m2.group(1)

    # ── Toss ───────────────────────────────────────────────────────────────
    toss_results = mh.get("tossResults", {}) if isinstance(mh.get("tossResults"), dict) else {}
    toss_winner = _s(toss_results.get("tossWinnerName", ""))
    toss_decision = _s(toss_results.get("decision", ""))
    toss = f"{toss_winner} ({toss_decision})" if toss_winner != NF else NF
    if toss == NF:
        t_m = re.search(r'"tossWinnerName"\s*:\s*"([^"]+)".*?"decision"\s*:\s*"([^"]+)"', full, re.DOTALL)
        if t_m:
            toss = f"{t_m.group(1)} ({t_m.group(2)})"

    # ── Venue ──────────────────────────────────────────────────────────────
    venue = NF
    vm = re.search(r'"ground"\s*:\s*"([^"]+)".*?"city"\s*:\s*"([^"]+)"', full, re.DOTALL)
    if vm:
        venue = f"{vm.group(1)}, {vm.group(2)}"

    # ── Match type ─────────────────────────────────────────────────────────
    match_format = _s(mh.get("matchFormat", ""))
    match_type = match_format if match_format != NF else _match_type_from_str(title)

    return LiveScoreResponse(
        status="success",
        match_id=mid,
        innings_id=nj.get("innings_id", NF),
        title=title,
        match_type=match_type,
        venue=venue,
        match_status=match_status,
        toss=toss,
        innings=innings,
        score=score_str,
        current_batsmen=batsmen,
        current_bowler=bowler,
        last_wicket=last_wicket,
        partnership=partnership,
        current_run_rate=crr,
        required_run_rate=rrr,
        target=target,
        current_over_balls=nj.get("current_over_balls", []),
        current_over_number=nj.get("current_over_number", NF),
        balls_in_current_over=nj.get("balls_in_current_over", 0),
        recent_overs_summary=nj.get("recent_overs_summary", []),
        day_number=day_number,
        match_state=match_state,
        fetched_at=time.time(),
    )


def _extract_innings_from_nj_text(text: str) -> List[InningsScore]:
    innings = []
    seen: set = set()
    for m in re.finditer(
        r'"batTeamName"\s*:\s*"([A-Z]{2,5})"'
        r'.*?"score"\s*:\s*(\d+)'
        r'.*?"wickets"\s*:\s*(\d+)'
        r'.*?"overs"\s*:\s*([\d.]+)',
        text, re.DOTALL
    ):
        team = m.group(1)
        if team in seen:
            continue
        seen.add(team)
        r, w, o = m.group(2), m.group(3), m.group(4)
        innings.append(InningsScore(team=team, runs=r, wickets=w, overs=o,
            display=f"{team} {r}/{w} ({o})"))
    return innings


def _enrich_batsmen_from_nj(batsmen: List[ScorecardBatsman], text: str, ms: Dict) -> None:
    for role in ["batsmanStriker", "batsmanNonStriker"]:
        m = re.search(rf'"{role}"\s*:\s*\{{([^}}]+)\}}', text, re.DOTALL)
        if not m:
            continue
        block = m.group(1)

        def get_val(key: str) -> str:
            km = re.search(rf'"{key}"\s*:\s*"?([^",}}]+)"?', block)
            return km.group(1).strip() if km else NF

        name = get_val("name")
        runs = get_val("runs")
        balls = get_val("balls")
        fours = get_val("fours")
        sixes = get_val("sixes")
        sr = get_val("strikeRate")

        matched = False
        for bat in batsmen:
            if bat.name == name or (name != NF and name in bat.name):
                bat.fours = fours if fours != NF else bat.fours
                bat.sixes = sixes if sixes != NF else bat.sixes
                bat.strike_rate = sr if sr != NF else bat.strike_rate
                if role == "batsmanStriker":
                    bat.is_striker = True
                matched = True
                break

        if not matched and name != NF and len(name) > 3:
            batsmen.append(ScorecardBatsman(
                name=name, runs=runs, balls=balls,
                fours=fours, sixes=sixes, strike_rate=sr,
                is_striker=(role == "batsmanStriker")
            ))


def _extract_bowler_from_nj(text: str, ms: Dict) -> ScorecardBowler:
    for role in ["bowlerStriker", "bowlerNonStriker"]:
        m = re.search(rf'"{role}"\s*:\s*\{{([^}}]+)\}}', text, re.DOTALL)
        if not m:
            continue
        block = m.group(1)

        def get_val(key: str) -> str:
            km = re.search(rf'"{key}"\s*:\s*"?([^",}}]+)"?', block)
            return km.group(1).strip() if km else NF

        name = get_val("name")
        if name == NF or len(name) < 3:
            continue
        return ScorecardBowler(
            name=name, overs=get_val("overs"), maidens=get_val("maidens"),
            runs=get_val("runs"), wickets=get_val("wickets"), economy=get_val("economy"),
        )
    return ScorecardBowler()


# ─────────────────────────────────────────────────────────────────────────────
# Scorecard Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_scorecard_html(html: str, mid: str) -> ScorecardResponse:
    nj, soup = _parse_page_html(html, "scorecard")
    full = nj.get("raw_texts", [""])[0]
    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    og_desc = _og(soup, "og:description")
    result = NF
    rm = re.search(r"((?:won|tied|no result|abandoned|drawn)[^.]{0,80})", og_desc, re.IGNORECASE)
    if rm:
        result = rm.group(1).strip()

    innings_list: List[InningsScorecard] = []
    current_inn: Optional[InningsScorecard] = None
    in_batting = in_bowling = False

    for div in soup.find_all("div", class_=True):
        classes = " ".join(div.get("class", []))
        raw = _soup_text(div)

        if ("cb-ltst-wgt-hdr" in classes or "cb-col-100" in classes) and \
                re.search(r"(?:Innings?|Inns?)\s*[-–]?\s*\d*|1st|2nd|3rd|4th", raw, re.IGNORECASE):
            team_m = re.match(r"^(.+?)\s+(?:Innings?|Inns?)", raw, re.IGNORECASE)
            score_m = re.search(r"(\d+(?:/\d+)?)\s*\(?([\d.]+)\s*Ov\)?", raw)
            if team_m and len(team_m.group(1)) > 2:
                if current_inn:
                    innings_list.append(current_inn)
                current_inn = InningsScorecard(
                    team=team_m.group(1).strip(),
                    score=score_m.group(0) if score_m else NF,
                )
                in_batting, in_bowling = True, False
            continue

        if current_inn is None:
            continue

        if "cb-scrd-hdr-rw" in classes and re.search(r"\bBowler\b", raw, re.IGNORECASE):
            in_batting, in_bowling = False, True
            continue

        if in_batting and "cb-scrd-itms" in classes and "cb-col-100" in classes:
            if re.search(r"^Extras", raw, re.IGNORECASE):
                current_inn.extras = raw; continue
            if re.search(r"^Yet to bat", raw, re.IGNORECASE):
                names_raw = re.sub(r"^Yet to bat[:\s]*", "", raw, flags=re.IGNORECASE)
                current_inn.yet_to_bat = [n.strip() for n in re.split(r",\s*", names_raw) if n.strip()]
                continue
            if re.search(r"^\s*Total\s", raw, re.IGNORECASE):
                continue
            entry = _parse_batting_row(div)
            if entry:
                current_inn.batting.append(entry)
            continue

        if in_bowling and "cb-scrd-itms" in classes and "cb-col-100" in classes:
            entry = _parse_bowling_row(div)
            if entry:
                current_inn.bowling.append(entry)

    if current_inn:
        innings_list.append(current_inn)

    if not innings_list:
        inn_list_m = re.search(r'"inningsScoreList"\s*:\s*\[(.*?)\]', full, re.DOTALL)
        if inn_list_m:
            for item_m in re.finditer(
                r'\{[^}]*?"batTeamName"\s*:\s*"([^"]+)"[^}]*?"score"\s*:\s*(\d+)'
                r'[^}]*?"wickets"\s*:\s*(\d+)[^}]*?"overs"\s*:\s*([\d.]+)[^}]*?\}',
                inn_list_m.group(1)
            ):
                team, r, w, o = item_m.group(1), item_m.group(2), item_m.group(3), item_m.group(4)
                innings_list.append(InningsScorecard(team=team, score=f"{r}/{w} ({o})", overs=o))

    return ScorecardResponse(status="success", match_id=mid, title=title,
        result=result, innings=innings_list)


def _parse_batting_row(div: Tag) -> Optional[BattingEntry]:
    name_el = div.find("a", href=re.compile(r"/profiles/\d+"))
    if not name_el:
        name_div = div.find("div", class_=re.compile(r"cb-scard-name|cb-col-50"))
        name_el = name_div.find("a") if name_div else None
    if not name_el:
        return None
    name = _soup_text(name_el).rstrip("* ").strip()
    if not name or len(name) < 2:
        return None
    dis_el = div.find("div", class_=re.compile(r"cb-scard-dis|cb-col-33"))
    dismissal = _soup_text(dis_el) if dis_el else NF
    stat_els = div.find_all("div", class_=re.compile(r"cb-col-8|cb-col-10"))

    def nth(n: int) -> str:
        if n < len(stat_els):
            v = _soup_text(stat_els[n])
            return v if v not in ("", NF, "-") else NF
        return NF

    return BattingEntry(name=name, dismissal=dismissal,
        runs=nth(0), balls=nth(1), fours=nth(2), sixes=nth(3), strike_rate=nth(4))


def _parse_bowling_row(div: Tag) -> Optional[BowlingEntry]:
    name_el = div.find("a", href=re.compile(r"/profiles/\d+"))
    if not name_el:
        return None
    name = _soup_text(name_el).strip()
    if not name or len(name) < 2:
        return None
    stat_els = div.find_all("div", class_=re.compile(r"cb-col-8|cb-col-10"))

    def nth(n: int) -> str:
        if n < len(stat_els):
            v = _soup_text(stat_els[n])
            return v if v not in ("", NF, "-") else NF
        return NF

    return BowlingEntry(name=name, overs=nth(0), maidens=nth(1),
        runs=nth(2), wickets=nth(3), no_balls=nth(4), wides=nth(5), economy=nth(6))


# ─────────────────────────────────────────────────────────────────────────────
# Match Info Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_match_info(html: str, mid: str) -> MatchInfo:
    nj, soup = _parse_page_html(html, "info")
    full = nj.get("raw_texts", [""])[0]
    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    info_map: Dict[str, str] = {}
    for row in soup.find_all("div", class_=re.compile(r"cb-mtch-info-itm|cb-col-100")):
        cols = row.find_all("div", recursive=False)
        if len(cols) >= 2:
            key = _soup_text(cols[0]).lower().rstrip(":").strip()
            val = _soup_text(cols[1])
            if key and val and val != NF:
                info_map[key] = val

    def pick(*keys: str) -> str:
        for k in keys:
            if k in info_map:
                return info_map[k]
        return NF

    umpires = []
    u_raw = pick("umpires", "on-field umpires", "field umpires")
    if u_raw != NF:
        umpires = [u.strip() for u in re.split(r"[,&]", u_raw) if u.strip()]

    series = pick("series", "tournament")
    if series == NF:
        sm = re.search(r'"seriesDesc"\s*:\s*"([^"]+)"', full)
        if sm: series = sm.group(1)

    venue = pick("venue", "ground", "stadium")
    city = venue.split(",")[-1].strip() if venue != NF and "," in venue else NF

    toss = pick("toss")
    if toss == NF:
        t_m = re.search(r'"tossWinnerName"\s*:\s*"([^"]+)".*?"decision"\s*:\s*"([^"]+)"', full, re.DOTALL)
        if t_m: toss = f"{t_m.group(1)} elected to {t_m.group(2).lower()}"

    state = NF
    state_m = re.search(r'"state"\s*:\s*"([^"]+)"', full)
    if state_m: state = state_m.group(1)

    og_desc = _og(soup, "og:description")
    result = NF
    for pat in [
        r"((?:won|tied|no result|abandoned|drawn)[^.\n]{0,80})",
        r"(match (?:tied|drawn|abandoned)[^.\n]{0,40})",
    ]:
        rm = re.search(pat, og_desc, re.IGNORECASE)
        if rm: result = rm.group(1).strip(); break

    mh = nj.get("matchHeader", {})
    match_format = _s(mh.get("matchFormat", ""))
    if match_format == NF:
        mf_m = re.search(r'"matchFormat"\s*:\s*"([^"]+)"', full)
        if mf_m: match_format = mf_m.group(1)

    return MatchInfo(
        status="success", match_id=mid, title=title, series=series,
        match_type=match_format if match_format != NF else _match_type_from_str(title),
        match_number=pick("match", "match number"),
        venue=venue, city=city, date=pick("date", "match date"),
        day_night=pick("day/night", "day / night"), toss=toss,
        umpires=umpires, third_umpire=pick("third umpire", "3rd umpire"),
        match_referee=pick("match referee", "referee"), result=result, state=state,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Squads Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_squads_html(html: str, mid: str) -> SquadsResponse:
    nj, soup = _parse_page_html(html, "squads")
    full = nj.get("raw_texts", [""])[0]
    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    squads: List[TeamSquad] = []
    seen_names: set = set()

    for pd_m in re.finditer(r'"playerDetails"\s*:\s*\[([^\]]+)\]', full, re.DOTALL):
        squad = TeamSquad()
        players_block = pd_m.group(1)
        context_start = max(0, pd_m.start() - 100)
        context = full[context_start:pd_m.start()]
        tn_m = re.search(r'"name"\s*:\s*"([^"]{3,40})"', context)
        if tn_m:
            squad.team = tn_m.group(1)

        for pm in re.finditer(r'\{[^}]*?"name"\s*:\s*"([^"]+)"[^}]*?"role"\s*:\s*"([^"]*)"[^}]*?\}', players_block):
            name = pm.group(1).strip()
            role = pm.group(2).strip()
            if not name or name in seen_names:
                continue
            obj_text = pm.group(0)
            is_captain = '"captain":true' in obj_text
            is_keeper = '"keeper":true' in obj_text
            is_sub = '"substitute":true' in obj_text
            if not is_sub and len(name) > 2:
                seen_names.add(name)
                squad.playing_xi.append(PlayerEntry(
                    name=name, role=role, is_captain=is_captain, is_keeper=is_keeper))

        if squad.playing_xi:
            squads.append(squad)

    return SquadsResponse(status="success", match_id=mid, title=title, squads=squads)


# ─────────────────────────────────────────────────────────────────────────────
# Over-by-Over Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_overs_html(html: str, mid: str) -> OversResponse:
    nj, soup = _parse_page_html(html, "overs")
    full = nj.get("raw_texts", [""])[0]
    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    overs: List[OverDetail] = []
    comm_pattern = re.compile(
        r'"(\d{13})"\s*:\s*\{'
        r'(?:[^}]|"[^"]*")*?"commText"\s*:\s*"([^"]*)"'
        r'(?:[^}]|"[^"]*")*?"ballMetric"\s*:\s*([\d.]+|\$undefined|"[^"]*")'
        r'(?:[^}]|"[^"]*")*?"overSeparator"\s*:\s*(\{[^}]*\}|null)',
        re.DOTALL
    )

    comm_entries = []
    for m in comm_pattern.finditer(full):
        ts = int(m.group(1))
        ball_metric_raw = m.group(3)
        bm = ball_metric_raw if re.match(r'[\d.]+$', ball_metric_raw) else NF
        over_sep = None
        over_sep_raw = m.group(4)
        if over_sep_raw and over_sep_raw != "null":
            try:
                over_sep = json.loads(over_sep_raw)
            except Exception:
                pass
        comm_entries.append({"ts": ts, "text": m.group(2), "ball_metric": bm, "over_separator": over_sep})

    comm_entries.sort(key=lambda x: x["ts"])

    current_over_balls_map: Dict[int, List] = {}
    over_separators: Dict[int, Dict] = {}

    for entry in comm_entries:
        bm = entry["ball_metric"]
        if bm == NF:
            continue
        bm_f = float(bm)
        over_num = int(bm_f)
        if over_num not in current_over_balls_map:
            current_over_balls_map[over_num] = []
        ball_event = _classify_ball_from_commentary(entry["text"], len(current_over_balls_map[over_num]) + 1)
        current_over_balls_map[over_num].append(ball_event)
        if entry["over_separator"]:
            over_separators[over_num + 1] = entry["over_separator"]

    for over_num in sorted(current_over_balls_map.keys()):
        balls = current_over_balls_map[over_num]
        sep = over_separators.get(over_num, {})
        bowler, batsmen, runs_in_over = NF, [], 0
        if sep:
            if isinstance(sep.get("bowlerObj"), dict):
                bowler = sep["bowlerObj"].get("playerName", NF)
            if isinstance(sep.get("batStrikerObj"), dict):
                batsmen.append(sep["batStrikerObj"].get("playerName", NF))
            if isinstance(sep.get("batNonStrikerObj"), dict):
                batsmen.append(sep["batNonStrikerObj"].get("playerName", NF))
            runs_in_over = sep.get("overRuns", 0)
        runs_in_over = runs_in_over or sum(b.runs for b in balls)
        wickets_in_over = sum(1 for b in balls if b.is_wicket)
        summary = " ".join(b.ball_label for b in balls)
        is_last = over_num == max(current_over_balls_map.keys())
        overs.append(OverDetail(
            over_number=over_num, innings_number=1, bowler=bowler,
            batsmen=[b for b in batsmen if b != NF],
            runs_in_over=runs_in_over, wickets_in_over=wickets_in_over,
            balls=balls, over_summary=summary, is_current=is_last,
        ))

    if not overs:
        for div in soup.find_all("div", class_=True):
            if re.search(r"cb-ovr-num", " ".join(div.get("class", [])), re.IGNORECASE):
                raw = _soup_text(div)
                ov_m = re.search(r"Ov\s+(\d+)", raw, re.IGNORECASE)
                if ov_m:
                    ov_num = int(ov_m.group(1))
                    rw_m = re.search(r"(\d+)-(\d+)", raw)
                    overs.append(OverDetail(
                        over_number=ov_num,
                        runs_in_over=int(rw_m.group(1)) if rw_m else 0,
                        wickets_in_over=int(rw_m.group(2)) if rw_m else 0,
                        is_current=False,
                    ))
        overs.reverse()

    current_ov = overs[-1].over_number if overs else None
    return OversResponse(status="success", match_id=mid, title=title,
        total_overs=len(overs), current_over=current_ov, overs=overs)


def _classify_ball_from_commentary(text: str, ball_num: int) -> BallEvent:
    text_lower = text.lower()
    is_four = bool(re.search(r'\bfour\b|\b4\b', text_lower))
    is_six = bool(re.search(r'\bsix\b|\b6\b', text_lower))
    is_wicket = bool(re.search(r'\bwicket\b|\bout\b|\blbw\b|\bcaught\b|\bbowled\b|\bstumped\b|\brunout\b', text_lower))
    is_wide = bool(re.search(r'\bwide\b', text_lower))
    is_nb = bool(re.search(r'\bno.?ball\b', text_lower))
    is_dot = bool(re.search(r'\bno run\b|\bdot\b', text_lower)) and not (is_four or is_six or is_wide or is_nb)
    runs = 0
    if is_four: runs = 4
    elif is_six: runs = 6
    elif not is_wicket and not is_dot:
        run_m = re.search(r'(\d+)\s+run', text_lower)
        if run_m: runs = int(run_m.group(1))
    label = '•' if is_dot else 'W' if is_wicket else '4' if is_four else '6' if is_six else 'Wd' if is_wide else 'Nb' if is_nb else str(runs)
    return BallEvent(
        ball_number=ball_num, ball_label=label, runs=runs,
        is_dot=is_dot, is_four=is_four, is_six=is_six,
        is_wicket=is_wicket, is_wide=is_wide, is_no_ball=is_nb,
        commentary=text[:200] if text else NF,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Match List Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_match_list(html: str, status: str) -> List[MatchCard]:
    nj, soup = _parse_page_html(html, "list")
    full = nj.get("raw_texts", [""])[0]
    cards: List[MatchCard] = []
    seen: set = set()

    for m in re.finditer(
        r'"matchId"\s*:\s*(\d+)'
        r'.*?"seriesName"\s*:\s*"([^"]*)"'
        r'.*?"matchDesc"\s*:\s*"([^"]*)"'
        r'.*?"matchFormat"\s*:\s*"([^"]*)"'
        r'.*?"state"\s*:\s*"([^"]*)"'
        r'.*?"status"\s*:\s*"([^"]*)"',
        full, re.DOTALL
    ):
        mid = m.group(1)
        if mid in seen:
            continue
        seen.add(mid)
        context = full[m.start():m.start() + 500]
        teams = [{"team": t} for t in re.findall(r'"teamName"\s*:\s*"([^"]+)"', context)]
        cards.append(MatchCard(
            match_id=mid, series=m.group(2), title=f"{m.group(3)} - {m.group(2)}",
            teams=teams[:2], match_type=m.group(4), status=m.group(5),
            overview=m.group(6)[:100] if m.group(6) else NF,
        ))

    if not cards:
        for a in soup.find_all("a", href=re.compile(r"/live-cricket-scores/\d+")):
            mid = _mid_from_href(a.get("href", ""))
            if mid == NF or mid in seen:
                continue
            seen.add(mid)
            cards.append(MatchCard(match_id=mid, title=_soup_text(a), status=status))

    return cards


# ─────────────────────────────────────────────────────────────────────────────
# Rule-based Summary
# ─────────────────────────────────────────────────────────────────────────────

def _generate_summary(score, scorecard, info) -> str:
    parts = []
    if info and info.result not in (NF, ""):
        parts.append(f"Result: {info.result}.")
        if info.title not in (NF, ""):
            parts.insert(0, info.title + ".")
        return " ".join(parts)

    if score:
        title = score.title if score.title != NF else "Match"
        parts.append(f"{title}.")
        if score.score != NF: parts.append(f"Score: {score.score}.")
        if score.match_status not in (NF, ""): parts.append(score.match_status + ".")
        strikers = [b for b in score.current_batsmen if b.is_striker]
        non_strikers = [b for b in score.current_batsmen if not b.is_striker]
        if strikers:
            b = strikers[0]
            parts.append(f"{b.name} is at the crease on {b.runs}({b.balls})" +
                         (f" with {b.fours} fours and {b.sixes} sixes." if b.fours != NF else "."))
        if non_strikers:
            b = non_strikers[0]
            parts.append(f"{b.name} is the non-striker on {b.runs}({b.balls}).")
        bl = score.current_bowler
        if bl.name != NF:
            parts.append(f"{bl.name} is bowling — {bl.overs} overs, {bl.runs} runs, {bl.wickets} wickets" +
                         (f" (economy {bl.economy})." if bl.economy != NF else "."))
        if score.current_run_rate not in (NF, "0"):
            line = f"CRR: {score.current_run_rate}."
            if score.required_run_rate not in (NF, "0", ""): line += f" RRR: {score.required_run_rate}."
            if score.target not in (NF, "0", ""): line += f" Target: {score.target}."
            parts.append(line)
        if score.current_over_balls:
            ball_str = " ".join(b.label for b in score.current_over_balls)
            parts.append(f"This over ({score.current_over_number}): {ball_str}.")

    return " ".join(parts) if parts else "Match data loading. Please refresh."


# ─────────────────────────────────────────────────────────────────────────────
# Preview Builder
# ─────────────────────────────────────────────────────────────────────────────

async def _build_preview(mid: str) -> PreviewResponse:
    t0 = time.monotonic()
    htmls = await _fetch_many_live(
        f"{CB}/live-cricket-scores/{mid}",
        f"{CB}/live-cricket-scorecard/{mid}/",
        f"{CB}/cricket-match-facts/{mid}",
        f"{CB}/live-cricket-over-by-over/{mid}",
    )
    score_h, sc_h, info_h, ov_h = htmls
    fetched = []
    score_data = scorecard_data = info_data = recent_over = None

    if score_h:
        fetched.append("score")
        try:
            nj, soup = _parse_page_html(score_h, "score")
            score_data = _parse_live_score_from_nj(nj, mid, soup)
        except Exception: pass

    if sc_h:
        fetched.append("scorecard")
        try:
            scorecard_data = _parse_scorecard_html(sc_h, mid)
        except Exception: pass

    if info_h:
        fetched.append("info")
        try:
            info_data = _parse_match_info(info_h, mid)
        except Exception: pass

    if ov_h:
        fetched.append("overs")
        try:
            overs_data = _parse_overs_html(ov_h, mid)
            if overs_data.overs:
                recent_over = overs_data.overs[-1]
        except Exception: pass

    fetch_ms = int((time.monotonic() - t0) * 1000)
    ai_text = _generate_summary(score_data, scorecard_data, info_data)

    title = NF
    for src in [score_data, scorecard_data, info_data]:
        if src and hasattr(src, 'title') and src.title != NF:
            title = src.title; break

    return PreviewResponse(
        status="success", match_id=mid, title=title,
        fetched_pages=fetched, fetch_time_ms=fetch_ms,
        ai_summary=ai_text, score=score_data,
        scorecard=scorecard_data, info=info_data, recent_over=recent_over,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ASCII Tree
# ─────────────────────────────────────────────────────────────────────────────

def _tree(d: LiveScoreResponse) -> str:
    bats = "\n".join(
        f"│   {'*' if b.is_striker else ' '} {b.name}  {b.runs}({b.balls})  4s:{b.fours}  6s:{b.sixes}  SR:{b.strike_rate}"
        for b in d.current_batsmen
    ) or "│   └── N/A"
    bl = d.current_bowler
    bowl_line = f"{bl.name}  {bl.overs}-{bl.maidens}-{bl.runs}-{bl.wickets}  ECO:{bl.economy}" if bl.name != NF else NF
    this_over = " ".join(b.label for b in d.current_over_balls) if d.current_over_balls else "(new over)"
    recent_ovs = " | ".join(d.recent_overs_summary) if d.recent_overs_summary else NF
    return (
        "🏏 Live Score\n│\n"
        f"├── Match        : {d.title}\n"
        f"├── InningsId    : {d.innings_id}\n"
        f"├── Score        : {d.score}\n"
        f"├── Status       : {d.match_status}\n"
        f"├── CRR/RRR/Tgt  : {d.current_run_rate} / {d.required_run_rate} / {d.target}\n"
        f"├── Bowler       : {bowl_line}\n"
        f"├── Partnership  : {d.partnership}\n"
        f"├── Last Wicket  : {d.last_wicket}\n"
        f"├── This Over    : Ov {d.current_over_number} [{d.balls_in_current_over} balls]  {this_over}\n"
        f"├── Recent Ovrs  : {recent_ovs}\n"
        f"├── Fetched At   : {time.strftime('%H:%M:%S', time.localtime(d.fetched_at))}\n"
        "├── Batsmen\n"
        f"{bats}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate(mid: str) -> bool:
    try:
        MatchValidator(match_id=mid)
        return True
    except Exception:
        return False


def _err422(msg: str = "invalid match id"):
    return JSONResponse(status_code=422, content={"status": "error", "code": 422, "message": msg})


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/docs", include_in_schema=False)
async def swagger():
    try:
        page = get_swagger_ui_html(openapi_url=app.openapi_url, title="Cricket Score API v8.0")
        r = HTMLResponse(content=page.body.decode("utf-8"))
        r.headers["Cache-Control"] = "no-store"
        return r
    except Exception:
        return HTMLResponse("<h2>Docs unavailable</h2>", status_code=500)


@app.get("/", summary="API info")
async def root(
    score: Optional[str] = Query(None),
    text: bool = Query(False),
):
    if score is None:
        return {
            "status": "success",
            "version": "8.0.0",
            "fixes": {
                "oscillation_root_cause": "_find_json_object now handles all spacing variants (key:{, key: {, key : {) — miniscore is always parsed correctly, never falls to broken regex fallback",
                "chunk_deduplication": "repeated __next_f chunks are deduplicated by content hash before joining — no more duplicate keys in text",
                "current_over_stable": "overs/overSummary/recentOvsStats read only from inside parsed miniscore object",
                "dual_source_current_over": "PRIMARY=overSummary, FALLBACK=recentOvsStats last segment (v5 logic) — both work",
                "no_live_cache": "new TCP connection per live request",
            },
            "docs": "/docs",
        }
    if not _validate(score):
        return _err422()
    html = await _fetch_live(f"{CB}/live-cricket-scores/{score}")
    if html is None:
        raise APIError(503, "upstream unavailable")
    nj, soup = _parse_page_html(html, "score")
    data = _parse_live_score_from_nj(nj, score, soup)
    return PlainTextResponse(_tree(data)) if text else data


@app.get("/match/{match_id}/score", response_model=LiveScoreResponse)
async def match_score(
    match_id: str = Path(...),
    text: bool = Query(False),
):
    """
    Always-fresh live score. No oscillation.
    - current_over_number: from miniscore.overs ONLY
    - current_over_balls: overSummary PRIMARY, recentOvsStats last segment FALLBACK
    - recent_overs_summary: completed overs from recentOvsStats (all segments except last)
    """
    if not _validate(match_id):
        return _err422()
    html = await _fetch_live(f"{CB}/live-cricket-scores/{match_id}")
    if html is None:
        raise APIError(503, "upstream unavailable")
    nj, soup = _parse_page_html(html, "score")
    data = _parse_live_score_from_nj(nj, match_id, soup)
    return PlainTextResponse(_tree(data)) if text else data


@app.get("/match/{match_id}/scorecard", response_model=ScorecardResponse)
async def match_scorecard(match_id: str = Path(...)):
    if not _validate(match_id):
        return _err422()
    html = await _fetch_live(f"{CB}/live-cricket-scorecard/{match_id}/")
    if html is None:
        raise APIError(503, "upstream unavailable")
    return _parse_scorecard_html(html, match_id)


@app.get("/match/{match_id}/info", response_model=MatchInfo)
async def match_info(match_id: str = Path(...)):
    if not _validate(match_id):
        return _err422()
    html = await _fetch_static(f"{CB}/cricket-match-facts/{match_id}")
    if html is None:
        raise APIError(503, "upstream unavailable")
    return _parse_match_info(html, match_id)


@app.get("/match/{match_id}/squads", response_model=SquadsResponse)
async def match_squads(match_id: str = Path(...)):
    if not _validate(match_id):
        return _err422()
    html = await _fetch_static(f"{CB}/cricket-match-playing-xi/{match_id}/")
    if html is None:
        raise APIError(503, "upstream unavailable")
    return _parse_squads_html(html, match_id)


@app.get("/match/{match_id}/overs", response_model=OversResponse)
async def match_overs(match_id: str = Path(...)):
    if not _validate(match_id):
        return _err422()
    html = await _fetch_live(f"{CB}/live-cricket-over-by-over/{match_id}")
    if html is None:
        raise APIError(503, "upstream unavailable")
    return _parse_overs_html(html, match_id)


@app.get("/match/{match_id}/preview", response_model=PreviewResponse)
async def match_preview(match_id: str = Path(...)):
    if not _validate(match_id):
        return _err422()
    return await _build_preview(match_id)


@app.get("/matches/{status}", response_model=MatchListResponse)
async def matches_by_status(
    status: str = Path(..., description="live | recent | upcoming"),
    type: str = Query("international"),
):
    _STATUS_SUFFIX = {"live": "", "recent": "/recent-matches", "upcoming": "/upcoming-matches"}
    _TYPE_SUFFIX = {"international": "", "league": "/league", "domestic": "/domestic", "women": "/women"}
    if status not in _STATUS_SUFFIX:
        return JSONResponse(status_code=422, content={"status": "error", "message": "status must be live, recent, or upcoming"})
    if type not in _TYPE_SUFFIX:
        return JSONResponse(status_code=422, content={"status": "error", "message": "type must be international, league, domestic, or women"})
    url = f"{CB}/cricket-match/live-scores{_STATUS_SUFFIX[status]}{_TYPE_SUFFIX[type]}"
    html = await _fetch_static(url)
    if html is None:
        raise APIError(503, "upstream unavailable")
    cards = _parse_match_list(html, status)
    return MatchListResponse(status="success", type=f"{status}/{type}", total=len(cards), matches=cards)


@app.get("/schedule", response_model=MatchListResponse)
async def schedule(type: str = Query("international")):
    return await matches_by_status("upcoming", type)


# ─────────────────────────────────────────────────────────────────────────────
# Error handlers
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(APIError)
async def _api_err(request: Request, exc: APIError):
    return JSONResponse(status_code=exc.status_code,
        content={"status": "error", "code": exc.status_code, "message": exc.message})

@app.exception_handler(StarletteHTTPException)
async def _http_err(request: Request, exc: StarletteHTTPException):
    return JSONResponse(status_code=exc.status_code,
        content={"status": "error", "code": exc.status_code, "message": "invalid route"})

@app.exception_handler(Exception)
async def _generic_err(request: Request, exc: Exception):
    return JSONResponse(status_code=500,
        content={"status": "error", "code": 500, "message": "internal server error"})
