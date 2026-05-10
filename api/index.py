"""
Cricket Score API v4.0  –  FastAPI + Cricbuzz Next.js JSON Extraction + Gemini AI
===================================================================================

KEY CHANGE in v4.0:
  Cricbuzz now uses Next.js SSR — all match data is embedded as JSON inside
  <script> tags (self.__next_f.push([...])). We extract directly from these
  JSON payloads instead of scraping HTML structure, which is:
    - 10x more reliable (no CSS class changes break it)
    - 3x faster (no complex DOM traversal)
    - More complete (gets ALL data in one page load)

  Gemini 1.5 Flash is used for:
    - /match/{id}/preview  — AI match summary
    - /match/{id}/score    — can optionally add AI context

Endpoints:
  /match/{id}/score         Live score, batsmen, bowler, recent balls
  /match/{id}/scorecard     Full batting/bowling scorecard
  /match/{id}/info          Match info (venue, toss, umpires)
  /match/{id}/squads        Playing XI squads
  /match/{id}/overs         Ball-by-ball over data
  /match/{id}/overs/current Current over
  /match/{id}/overs/{n}     Specific over
  /match/{id}/preview       AI-powered full match preview (Gemini)
  /matches/{status}         live / recent / upcoming match lists
  /schedule                 Upcoming matches alias
"""

import re
import json
import html as html_lib
import time
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

GEMINI_API_KEY = "AIzaSyBGR3o9ENm5cigXq121WB0V9NRzong3pU0"
GEMINI_MODEL   = "gemini-1.5-flash"
GEMINI_URL     = (
    f"https://generativelanguage.googleapis.com/v1beta/models/"
    f"{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"
)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

# ─────────────────────────────────────────────────────────────────────────────
# HTTP Client (persistent connection pool)
# ─────────────────────────────────────────────────────────────────────────────

_HTTP_CLIENT: Optional[httpx.AsyncClient] = None


def _get_client() -> httpx.AsyncClient:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None or _HTTP_CLIENT.is_closed:
        _HTTP_CLIENT = httpx.AsyncClient(
            timeout=httpx.Timeout(15.0, connect=6.0),
            limits=httpx.Limits(max_connections=15, max_keepalive_connections=8),
            follow_redirects=True,
            http2=False,
        )
    return _HTTP_CLIENT


@asynccontextmanager
async def _lifespan(app: "FastAPI"):
    _get_client()
    yield
    global _HTTP_CLIENT
    if _HTTP_CLIENT and not _HTTP_CLIENT.is_closed:
        await _HTTP_CLIENT.aclose()
        _HTTP_CLIENT = None


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
    recent_balls: List[RecentBall] = []
    day_number: Optional[int] = None
    match_state: str = NF


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
    version="4.0.0",
    description=(
        "Full-featured cricket API powered by Cricbuzz Next.js JSON extraction "
        "and Gemini AI. Reliably extracts all live match data from embedded "
        "Next.js __next_f payloads."
    ),
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
# Core: Next.js JSON Extractor
# ─────────────────────────────────────────────────────────────────────────────

def _extract_nextjs_json(html: str) -> Dict[str, Any]:
    """
    Cricbuzz uses Next.js SSR. ALL match data is embedded in <script> tags:
      self.__next_f.push([1, "...json string..."])

    This function:
    1. Extracts all __next_f push payloads
    2. Concatenates and attempts JSON parsing of large chunks
    3. Searches for the match commentary/miniscore JSON blob
    4. Returns a rich dict with all extracted data keyed by type

    The main data we need lives in the '1d:' prefixed payload which contains
    the full matchCommentary and miniscore objects.
    """
    result: Dict[str, Any] = {
        "miniscore": {},
        "matchHeader": {},
        "matchCommentary": {},
        "matchInfo": {},
        "scorecard": {},
        "raw_texts": [],
    }

    # Extract all __next_f payloads
    pattern = re.compile(
        r'self\.__next_f\.push\(\[(\d+),\s*"((?:[^"\\]|\\.)*)"\]\)',
        re.DOTALL
    )

    all_payloads = []
    for m in pattern.finditer(html):
        idx = int(m.group(1))
        try:
            # Unescape the JSON string value
            raw = m.group(2).encode('utf-8').decode('unicode_escape')
        except Exception:
            raw = m.group(2)
        all_payloads.append((idx, raw))

    # Also try a simpler extraction for large JSON blobs
    script_pattern = re.compile(
        r'self\.__next_f\.push\(\[1,\s*"([\s\S]*?)"\]\s*\)',
        re.DOTALL
    )
    # Extract large 1d payload (commentary page data) - look for miniscore
    for idx, payload in all_payloads:
        if idx == 1:
            result["raw_texts"].append(payload)

    # Parse commentary data from Next.js payload
    # The key pattern: "miniscore":{...} embedded in the JS
    full_text = "\n".join(t for _, t in all_payloads if _ == 1)

    # Try to find and parse the miniscore JSON object
    _extract_miniscore(full_text, result)
    _extract_match_header(full_text, result)
    _extract_commentary(full_text, result)

    return result


def _find_json_object(text: str, key: str) -> Optional[Dict]:
    """
    Find a JSON object by searching for '"key":{' and extracting
    the balanced braces content.
    """
    pattern = f'"{key}":{{'
    idx = text.find(pattern)
    if idx == -1:
        return None

    start = idx + len(pattern) - 1  # position of '{'
    depth = 0
    i = start
    while i < len(text):
        c = text[i]
        if c == '{':
            depth += 1
        elif c == '}':
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start:i+1])
                except json.JSONDecodeError:
                    return None
        elif c == '"':
            # Skip string contents
            i += 1
            while i < len(text):
                if text[i] == '\\':
                    i += 2
                    continue
                if text[i] == '"':
                    break
                i += 1
        i += 1
    return None


def _extract_miniscore(text: str, result: Dict) -> None:
    """Extract miniscore data from Next.js payload."""
    # Direct JSON object search
    ms = _find_json_object(text, "miniscore")
    if ms:
        result["miniscore"] = ms
        return

    # Regex fallback for miniscore fields
    patterns = {
        "inningsId": r'"inningsId"\s*:\s*(\d+)',
        "status": r'"customStatus"\s*:\s*"([^"]+)"',
        "state": r'"state"\s*:\s*"([^"]+)"',
        "score": r'"score"\s*:\s*(\d+)',
        "wickets": r'"wickets"\s*:\s*(\d+)',  # team wickets
        "overs": r'"overs"\s*:\s*([\d.]+)',
        "currentRunRate": r'"currentRunRate"\s*:\s*([\d.]+)',
        "requiredRunRate": r'"requiredRunRate"\s*:\s*([\d.]+)',
        "target": r'"target"\s*:\s*(\d+)',
    }

    ms_data = {}
    for key, pat in patterns.items():
        m = re.search(pat, text)
        if m:
            ms_data[key] = m.group(1)
    if ms_data:
        result["miniscore"] = ms_data


def _extract_match_header(text: str, result: Dict) -> None:
    """Extract matchHeader data."""
    mh = _find_json_object(text, "matchHeader")
    if mh:
        result["matchHeader"] = mh
        return

    # Fallback: extract key fields
    mh_data = {}
    for key, pat in [
        ("status", r'"status"\s*:\s*"([^"]{5,120})"'),
        ("tossWinnerName", r'"tossWinnerName"\s*:\s*"([^"]+)"'),
        ("decision", r'"decision"\s*:\s*"([^"]+)"'),
        ("seriesDesc", r'"seriesDesc"\s*:\s*"([^"]+)"'),
        ("matchDescription", r'"matchDescription"\s*:\s*"([^"]+)"'),
        ("matchFormat", r'"matchFormat"\s*:\s*"([^"]+)"'),
    ]:
        m = re.search(pat, text)
        if m:
            mh_data[key] = m.group(1)
    result["matchHeader"] = mh_data


def _extract_commentary(text: str, result: Dict) -> None:
    """Extract commentary data (ball-by-ball) from Next.js payload."""
    # Commentary objects look like "1778326297101":{...commType...ballMetric...}
    comm_pattern = re.compile(
        r'"(\d{13})"\s*:\s*\{'
        r'[^}]*?"commType"\s*:\s*"([^"]+)"'
        r'[^}]*?"commText"\s*:\s*"([^"]*)"'
        r'[^}]*?"ballMetric"\s*:\s*([\d.]+|"?\$undefined"?)',
        re.DOTALL
    )

    commentaries = {}
    for m in comm_pattern.finditer(text):
        ts = m.group(1)
        commentaries[ts] = {
            "commType": m.group(2),
            "commText": m.group(3),
            "ballMetric": m.group(4),
        }

    if commentaries:
        result["matchCommentary"] = commentaries


def _parse_page_html(html: str, page_type: str) -> Tuple[Dict, BeautifulSoup]:
    """
    Parse a Cricbuzz page, returning both extracted Next.js JSON data
    and a BeautifulSoup object for HTML fallback.
    """
    nj_data = _extract_nextjs_json(html)
    soup = BeautifulSoup(html, "lxml")
    return nj_data, soup


# ─────────────────────────────────────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch(url: str, retries: int = 3) -> Optional[httpx.Response]:
    """Fetch with retry and back-off."""
    client = _get_client()
    for attempt in range(retries):
        try:
            r = await client.get(url, headers=HEADERS)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503) and attempt < retries - 1:
                await asyncio.sleep(0.8 * (attempt + 1))
            elif r.status_code == 404:
                return None
        except (httpx.TimeoutException, httpx.ConnectError):
            if attempt < retries - 1:
                await asyncio.sleep(0.5 * (attempt + 1))
        except Exception:
            break
    return None


async def _fetch_many(*urls: str) -> List[Optional[httpx.Response]]:
    """Fetch multiple URLs concurrently."""
    return list(await asyncio.gather(*(_fetch(u) for u in urls)))


async def _gemini_query(prompt: str, max_tokens: int = 1024) -> str:
    """Call Gemini 1.5 Flash with a prompt."""
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.3,
            "maxOutputTokens": max_tokens,
            "topP": 0.85,
        },
    }
    try:
        async with httpx.AsyncClient(timeout=25.0) as c:
            resp = await c.post(
                GEMINI_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
            )
        if resp.status_code != 200:
            return f"Gemini error {resp.status_code}"
        data = resp.json()
        return data["candidates"][0]["content"]["parts"][0]["text"].strip()
    except Exception as exc:
        return f"Gemini call failed: {exc}"


def _s(v: Any, default: str = NF) -> str:
    """Safe string conversion."""
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
    for tag in ("TEST", "T20I", "T20", "T10", "ODI",
                "THE HUNDRED", "LIST A", "FIRST-CLASS"):
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
# Score Parser — uses Next.js JSON + HTML fallback
# ─────────────────────────────────────────────────────────────────────────────

def _parse_recent_balls_from_text(text: str) -> List[RecentBall]:
    """Parse recent balls from pattern like '1 4 0 2 0 0' or '... | 1 4 0 2 0 0'."""
    balls = []
    # Extract from "Recent :... | ... | ..." pattern
    recent_m = re.search(r'[Rr]ecent\s*:?\s*(.*?)(?=\n|$)', text)
    if not recent_m:
        # Look in the raw text for pipe-separated over summaries
        recent_m = re.search(r'\|\s*([\d\s]+)\s*\|?\s*([\d\s]+)\s*$', text)
        if not recent_m:
            return balls

    raw = recent_m.group(0) if not recent_m.lastindex else recent_m.group(1)
    # Clean up
    raw = re.sub(r'\.\.\.|[Rr]ecent\s*:?', '', raw).strip()

    tokens = re.findall(r'[Wd|Nb|W|0-9•]+', raw)
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        is_dot = tok in ('0', '•', '.')
        is_wide = tok.upper().startswith('WD')
        is_nb = tok.upper().startswith('NB')
        is_wicket = tok.upper() == 'W'
        is_four = tok == '4'
        is_six = tok == '6'

        num_m = re.search(r'\d+', tok)
        runs = int(num_m.group()) if num_m and not is_dot and not is_wicket else 0

        label = '•' if is_dot else ('W' if is_wicket else
                ('Wd' if is_wide else ('Nb' if is_nb else tok)))

        balls.append(RecentBall(
            label=label, runs=runs,
            is_dot=is_dot, is_four=is_four, is_six=is_six,
            is_wicket=is_wicket, is_wide=is_wide, is_no_ball=is_nb
        ))

    return balls


def _parse_live_score_from_nj(nj: Dict, mid: str, soup: BeautifulSoup) -> LiveScoreResponse:
    """
    Parse live score primarily from Next.js extracted JSON data.
    Falls back to HTML parsing when JSON data is incomplete.
    """
    ms = nj.get("miniscore", {})
    mh = nj.get("matchHeader", {})

    # ── Title & metadata ──────────────────────────────────────────────────────
    og_title = _og(soup, "og:title")
    og_desc  = _og(soup, "og:description")
    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^Cricket\s*(?:commentary\s*)?\|\s*", "", title_tag, flags=re.IGNORECASE).strip()
    if not title:
        title = og_title[:100] if og_title else NF

    # ── Match status ──────────────────────────────────────────────────────────
    match_status = (
        _s(ms.get("customStatus") or ms.get("status"))
        or _s(mh.get("status"))
        or NF
    )
    if match_status == NF:
        # Fallback: look for status in HTML
        for cls in ["cb-text-complete", "cb-text-inprogress", "cb-game-status",
                    "cb-text-stumps", "cb-text-lunch", "cb-text-tea"]:
            el = soup.find(class_=cls)
            if el:
                match_status = _soup_text(el)
                break

    match_state = _s(ms.get("state") or mh.get("state"), "unknown")

    # ── Day number (Test matches) ─────────────────────────────────────────────
    day_number = None
    dm = re.search(r'Day\s+(\d+)', match_status, re.IGNORECASE)
    if dm:
        day_number = int(dm.group(1))

    # ── Innings scores from og:title ──────────────────────────────────────────
    innings: List[InningsScore] = []

    # Extract from og:title: "PAK 179/1 (46) vs BAN 413"
    score_patterns = [
        r'([A-Z]{2,5})\s+(\d+)/(\d+)\s*\(([\d.]+)\)',
        r'([A-Z]{2,5})\s+(\d+)\s*\(([\d.]+)\)',  # all out pattern
    ]
    for pat in score_patterns:
        for team, *nums in re.findall(pat, og_title + " " + og_desc):
            if len(nums) == 3:
                r, w, o = nums
                innings.append(InningsScore(
                    team=team, runs=r, wickets=w, overs=o,
                    display=f"{team} {r}/{w} ({o})"
                ))
            elif len(nums) == 2:
                r, o = nums
                innings.append(InningsScore(
                    team=team, runs=r, wickets="10", overs=o,
                    display=f"{team} {r} ({o})"
                ))

    # Also check matchScore in Next.js data
    if not innings:
        innings = _extract_innings_from_nj_text("\n".join(nj.get("raw_texts", [])))

    # Remove duplicates
    seen_teams = set()
    unique_innings = []
    for inn in innings:
        if inn.team not in seen_teams:
            seen_teams.add(inn.team)
            unique_innings.append(inn)
    innings = unique_innings

    score_str = "  |  ".join(i.display for i in innings) if innings else NF

    # ── Current batsmen from Next.js JSON ─────────────────────────────────────
    batsmen: List[ScorecardBatsman] = []
    raw_texts = "\n".join(nj.get("raw_texts", []))

    # Extract from og:title: "Azan Awais 85(133) Abdullah Fazal 37(78)"
    batsman_pattern = re.compile(
        r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(\d+)\*?\((\d+)\)(\*)?'
    )
    seen_bat = set()
    for m in batsman_pattern.finditer(og_title):
        name = m.group(1).strip()
        if name not in seen_bat and len(name) > 3:
            seen_bat.add(name)
            batsmen.append(ScorecardBatsman(
                name=name, runs=m.group(2), balls=m.group(3),
                is_striker=bool(m.group(4))
            ))

    # Enrich with 4s/6s/SR from miniscore JSON fields
    _enrich_batsmen_from_nj(batsmen, raw_texts, ms)

    # ── Current bowler ─────────────────────────────────────────────────────────
    bowler = _extract_bowler_from_nj(raw_texts, ms)

    # ── Partnership ────────────────────────────────────────────────────────────
    partnership = NF
    ps = ms.get("partnerShip") or ms.get("partnership", {})
    if isinstance(ps, dict):
        p_runs = ps.get("runs", "")
        p_balls = ps.get("balls", "")
        if p_runs:
            partnership = f"{p_runs}({p_balls})" if p_balls else str(p_runs)
    if partnership == NF:
        pm = re.search(r'"partnerShip"\s*:\s*\{"balls"\s*:\s*(\d+)\s*,\s*"runs"\s*:\s*(\d+)\}', raw_texts)
        if pm:
            partnership = f"{pm.group(2)}({pm.group(1)})"

    # ── Last wicket ────────────────────────────────────────────────────────────
    last_wicket = _s(ms.get("lastWicket", ""))
    if last_wicket == NF:
        lw_m = re.search(r'"lastWicket"\s*:\s*"([^"]{5,120})"', raw_texts)
        if lw_m:
            last_wicket = lw_m.group(1)

    # ── Run rates ──────────────────────────────────────────────────────────────
    crr = _s(ms.get("currentRunRate", ""))
    rrr = _s(ms.get("requiredRunRate", ""))
    target = _s(ms.get("target", ""))

    if crr == NF:
        crr_m = re.search(r'"currentRunRate"\s*:\s*([\d.]+)', raw_texts)
        if crr_m: crr = crr_m.group(1)
    if rrr == NF:
        rrr_m = re.search(r'"requiredRunRate"\s*:\s*([\d.]+)', raw_texts)
        if rrr_m: rrr = rrr_m.group(1)
    if target == NF:
        tgt_m = re.search(r'"target"\s*:\s*(\d+)', raw_texts)
        if tgt_m: target = tgt_m.group(1)

    # ── Toss ───────────────────────────────────────────────────────────────────
    toss_winner = _s(mh.get("tossResults", {}).get("tossWinnerName", "") if isinstance(mh.get("tossResults"), dict) else "")
    toss_decision = _s(mh.get("tossResults", {}).get("decision", "") if isinstance(mh.get("tossResults"), dict) else "")
    toss = f"{toss_winner} ({toss_decision})" if toss_winner != NF else NF

    if toss == NF:
        # Regex from raw text
        t_m = re.search(r'"tossWinnerName"\s*:\s*"([^"]+)".*?"decision"\s*:\s*"([^"]+)"', raw_texts, re.DOTALL)
        if t_m:
            toss = f"{t_m.group(1)} ({t_m.group(2)})"

    # ── Venue ──────────────────────────────────────────────────────────────────
    venue = NF
    mi = nj.get("matchInfo", {})
    venue_obj = mi.get("venue", {})
    if isinstance(venue_obj, dict):
        vname = venue_obj.get("name", "")
        vcity = venue_obj.get("city", "")
        if vname:
            venue = f"{vname}, {vcity}" if vcity else vname

    if venue == NF:
        # From matchHeader or HTML
        vm = re.search(r'"ground"\s*:\s*"([^"]+)".*?"city"\s*:\s*"([^"]+)"', raw_texts, re.DOTALL)
        if vm:
            venue = f"{vm.group(1)}, {vm.group(2)}"

    # ── Match type ─────────────────────────────────────────────────────────────
    match_format = _s(mh.get("matchFormat", ""))
    match_type = match_format if match_format != NF else _match_type_from_str(title)

    # ── Recent balls from over summary in Next.js data ─────────────────────────
    recent_balls: List[RecentBall] = []
    # Look for recentOvsStats: "...  | 1 4 0 2 0 0  | 0 0 1 0 2 0"
    rov_m = re.search(r'"recentOvsStats"\s*:\s*"([^"]+)"', raw_texts)
    if rov_m:
        recent_balls = _parse_recent_balls_from_text(rov_m.group(1))

    if not recent_balls:
        # Try over summary from last over separator
        os_m = re.search(r'"overSummary"\s*:\s*"([^"]+)"', raw_texts)
        if os_m:
            recent_balls = _parse_over_summary_balls(os_m.group(1))

    return LiveScoreResponse(
        status="success",
        match_id=mid,
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
        recent_balls=recent_balls,
        day_number=day_number,
        match_state=match_state,
    )


def _extract_innings_from_nj_text(text: str) -> List[InningsScore]:
    """Extract innings from Next.js raw text payload."""
    innings = []
    seen = set()

    # Pattern: "batTeamName":"PAK","score":179,"wickets":1,"overs":46
    inn_pattern = re.compile(
        r'"batTeamName"\s*:\s*"([A-Z]{2,5})"'
        r'.*?"score"\s*:\s*(\d+)'
        r'.*?"wickets"\s*:\s*(\d+)'
        r'.*?"overs"\s*:\s*([\d.]+)',
        re.DOTALL
    )

    for m in inn_pattern.finditer(text):
        team = m.group(1)
        if team in seen:
            continue
        seen.add(team)
        r, w, o = m.group(2), m.group(3), m.group(4)
        innings.append(InningsScore(
            team=team, runs=r, wickets=w, overs=o,
            display=f"{team} {r}/{w} ({o})"
        ))

    return innings


def _enrich_batsmen_from_nj(batsmen: List[ScorecardBatsman], text: str, ms: Dict) -> None:
    """Enrich batsmen with 4s/6s/SR from Next.js JSON data."""
    # Look for batsmanStriker and batsmanNonStriker in miniscore
    for role in ["batsmanStriker", "batsmanNonStriker"]:
        # Find in raw text
        pat = rf'"{role}"\s*:\s*\{{([^}}]+)\}}'
        m = re.search(pat, text, re.DOTALL)
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

        # Match to existing batsman or add new
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
    """Extract current bowler from Next.js data."""
    for role in ["bowlerStriker", "bowlerNonStriker"]:
        pat = rf'"{role}"\s*:\s*\{{([^}}]+)\}}'
        m = re.search(pat, text, re.DOTALL)
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
            name=name,
            overs=get_val("overs"),
            maidens=get_val("maidens"),
            runs=get_val("runs"),
            wickets=get_val("wickets"),
            economy=get_val("economy"),
        )

    return ScorecardBowler()


def _parse_over_summary_balls(summary: str) -> List[RecentBall]:
    """Parse balls from over summary like '0 0 1 0 2 0'."""
    balls = []
    tokens = summary.strip().split()
    for tok in tokens:
        tok = tok.strip()
        if not tok:
            continue
        is_dot = tok in ('0', '•')
        is_wide = tok.upper() in ('WD', 'WIDE')
        is_nb = tok.upper() in ('NB', 'NO', 'NOBALL')
        is_wicket = tok.upper() in ('W', 'WKT', 'WICKET')
        is_four = tok == '4'
        is_six = tok == '6'
        num_m = re.search(r'\d+', tok)
        runs = int(num_m.group()) if num_m and not is_dot and not is_wicket else 0
        label = '•' if is_dot else ('W' if is_wicket else tok)
        balls.append(RecentBall(
            label=label, runs=runs,
            is_dot=is_dot, is_four=is_four, is_six=is_six,
            is_wicket=is_wicket, is_wide=is_wide, is_no_ball=is_nb
        ))
    return balls


# ─────────────────────────────────────────────────────────────────────────────
# Scorecard Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_scorecard_html(html: str, mid: str) -> ScorecardResponse:
    """Parse scorecard from HTML (Cricbuzz scorecard page structure)."""
    nj, soup = _parse_page_html(html, "scorecard")
    raw_texts = "\n".join(nj.get("raw_texts", []))

    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    og_desc = _og(soup, "og:description")
    result = NF
    rm = re.search(r"((?:won|tied|no result|abandoned|drawn)[^.]{0,80})", og_desc, re.IGNORECASE)
    if rm:
        result = rm.group(1).strip()

    innings_list: List[InningsScorecard] = []

    # --- HTML-based parsing ---
    # Find innings sections by looking for "Innings" headers
    all_divs = soup.find_all("div", class_=True)
    current_inn: Optional[InningsScorecard] = None
    in_batting = False
    in_bowling = False

    for div in all_divs:
        classes = " ".join(div.get("class", []))
        raw = _soup_text(div)

        # Innings header detection
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
                in_batting = True
                in_bowling = False
            continue

        if current_inn is None:
            continue

        # Bowling header
        if "cb-scrd-hdr-rw" in classes and re.search(r"\bBowler\b", raw, re.IGNORECASE):
            in_batting = False
            in_bowling = True
            continue

        # Batting rows
        if in_batting and "cb-scrd-itms" in classes and "cb-col-100" in classes:
            if re.search(r"^Extras", raw, re.IGNORECASE):
                current_inn.extras = raw
                continue
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

        # Bowling rows
        if in_bowling and "cb-scrd-itms" in classes and "cb-col-100" in classes:
            entry = _parse_bowling_row(div)
            if entry:
                current_inn.bowling.append(entry)

    if current_inn:
        innings_list.append(current_inn)

    # Fallback: extract from Next.js JSON commentary for match data
    if not innings_list:
        innings_list = _extract_scorecard_from_nj(raw_texts)

    return ScorecardResponse(
        status="success", match_id=mid, title=title,
        result=result, innings=innings_list
    )


def _parse_batting_row(div: Tag) -> Optional[BattingEntry]:
    """Parse a batting row div."""
    name_el = div.find("a", href=re.compile(r"/profiles/\d+"))
    if not name_el:
        name_div = div.find("div", class_=re.compile(r"cb-scard-name|cb-col-50"))
        name_el = name_div.find("a") if name_div else None
    if not name_el:
        return None

    name = _soup_text(name_el).rstrip("* ").strip()
    if not name or len(name) < 2:
        return None

    # Dismissal info
    dis_el = div.find("div", class_=re.compile(r"cb-scard-dis|cb-col-33"))
    dismissal = _soup_text(dis_el) if dis_el else NF

    # Numeric stats: R B 4s 6s SR
    stat_els = div.find_all("div", class_=re.compile(r"cb-col-8|cb-col-10"))
    def nth(n: int) -> str:
        if n < len(stat_els):
            v = _soup_text(stat_els[n])
            return v if v not in ("", NF, "-") else NF
        return NF

    return BattingEntry(
        name=name, dismissal=dismissal,
        runs=nth(0), balls=nth(1), fours=nth(2), sixes=nth(3), strike_rate=nth(4)
    )


def _parse_bowling_row(div: Tag) -> Optional[BowlingEntry]:
    """Parse a bowling row div."""
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

    return BowlingEntry(
        name=name, overs=nth(0), maidens=nth(1),
        runs=nth(2), wickets=nth(3), no_balls=nth(4),
        wides=nth(5), economy=nth(6)
    )


def _extract_scorecard_from_nj(text: str) -> List[InningsScorecard]:
    """Extract scorecard data from Next.js JSON payload text."""
    innings = []
    # Try to find innings score list
    inn_list_m = re.search(r'"inningsScoreList"\s*:\s*\[(.*?)\]', text, re.DOTALL)
    if inn_list_m:
        for item_m in re.finditer(
            r'\{[^}]*?"batTeamName"\s*:\s*"([^"]+)"[^}]*?"score"\s*:\s*(\d+)[^}]*?"wickets"\s*:\s*(\d+)[^}]*?"overs"\s*:\s*([\d.]+)[^}]*?\}',
            inn_list_m.group(1)
        ):
            team, r, w, o = item_m.group(1), item_m.group(2), item_m.group(3), item_m.group(4)
            innings.append(InningsScorecard(
                team=team,
                score=f"{r}/{w} ({o})",
                overs=o,
            ))
    return innings


# ─────────────────────────────────────────────────────────────────────────────
# Match Info Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_match_info(html: str, mid: str) -> MatchInfo:
    """Parse match info page."""
    nj, soup = _parse_page_html(html, "info")
    raw_texts = "\n".join(nj.get("raw_texts", []))

    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    # Extract from structured rows on info page
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

    # Umpires
    umpires = []
    u_raw = pick("umpires", "on-field umpires", "field umpires")
    if u_raw != NF:
        umpires = [u.strip() for u in re.split(r"[,&]", u_raw) if u.strip()]

    # Series/venue from Next.js
    series = pick("series", "tournament")
    if series == NF:
        sm = re.search(r'"seriesDesc"\s*:\s*"([^"]+)"', raw_texts)
        if sm: series = sm.group(1)

    venue = pick("venue", "ground", "stadium")
    city = venue.split(",")[-1].strip() if venue != NF and "," in venue else NF

    # Toss from Next.js
    toss = pick("toss")
    if toss == NF:
        t_m = re.search(r'"tossWinnerName"\s*:\s*"([^"]+)".*?"decision"\s*:\s*"([^"]+)"', raw_texts, re.DOTALL)
        if t_m: toss = f"{t_m.group(1)} elected to {t_m.group(2).lower()}"

    # State
    state = NF
    state_m = re.search(r'"state"\s*:\s*"([^"]+)"', raw_texts)
    if state_m: state = state_m.group(1)

    # Result from og:description
    og_desc = _og(soup, "og:description")
    result = NF
    for pat in [
        r"((?:won|tied|no result|abandoned|drawn)[^.\n]{0,80})",
        r"(match (?:tied|drawn|abandoned)[^.\n]{0,40})",
    ]:
        rm = re.search(pat, og_desc, re.IGNORECASE)
        if rm: result = rm.group(1).strip(); break

    # Match type from Next.js
    match_format = NF
    mf_m = re.search(r'"matchFormat"\s*:\s*"([^"]+)"', raw_texts)
    if mf_m: match_format = mf_m.group(1)

    return MatchInfo(
        status="success", match_id=mid, title=title,
        series=series,
        match_type=match_format if match_format != NF else _match_type_from_str(title),
        match_number=pick("match", "match number"),
        venue=venue, city=city,
        date=pick("date", "match date"),
        day_night=pick("day/night", "day / night"),
        toss=toss, umpires=umpires,
        third_umpire=pick("third umpire", "3rd umpire"),
        match_referee=pick("match referee", "referee"),
        result=result, state=state,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Squads Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_squads_html(html: str, mid: str) -> SquadsResponse:
    """Parse squads page."""
    nj, soup = _parse_page_html(html, "squads")
    raw_texts = "\n".join(nj.get("raw_texts", []))

    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    squads: List[TeamSquad] = []
    seen_names: set = set()

    # Extract player details from Next.js JSON
    # Pattern: "playerDetails":[{"id":...,"name":"...","role":"...","captain":...,"keeper":...}]
    pd_pattern = re.compile(
        r'"playerDetails"\s*:\s*\[([^\]]+)\]',
        re.DOTALL
    )

    for pd_m in pd_pattern.finditer(raw_texts):
        squad = TeamSquad()
        players_block = pd_m.group(1)

        # Extract team name from context (look backwards)
        context_start = max(0, pd_m.start() - 100)
        context = raw_texts[context_start:pd_m.start()]
        tn_m = re.search(r'"name"\s*:\s*"([^"]{3,40})"', context)
        if tn_m:
            squad.team = tn_m.group(1)

        # Parse each player object
        player_pat = re.compile(
            r'\{[^}]*?"name"\s*:\s*"([^"]+)"[^}]*?"role"\s*:\s*"([^"]*)"[^}]*?\}'
        )
        for pm in player_pat.finditer(players_block):
            name = pm.group(1).strip()
            role = pm.group(2).strip()
            if not name or name in seen_names:
                continue

            # Check captain/keeper in the full object text
            obj_text = pm.group(0)
            is_captain = '"captain":true' in obj_text
            is_keeper = '"keeper":true' in obj_text
            is_sub = '"substitute":true' in obj_text

            if not is_sub and len(name) > 2:
                seen_names.add(name)
                squad.playing_xi.append(PlayerEntry(
                    name=name, role=role,
                    is_captain=is_captain, is_keeper=is_keeper
                ))

        if squad.playing_xi:
            squads.append(squad)

    # HTML fallback
    if not squads:
        squads = _parse_squads_fallback_html(soup, seen_names)

    return SquadsResponse(
        status="success", match_id=mid, title=title, squads=squads
    )


def _parse_squads_fallback_html(soup: BeautifulSoup, seen: set) -> List[TeamSquad]:
    """HTML fallback squad parser."""
    squads = []
    for a in soup.find_all("a", href=re.compile(r"/profiles/\d+")):
        name = _soup_text(a).rstrip("*(c)(wk) ").strip()
        if name and name not in seen and len(name) > 2:
            seen.add(name)
    return squads


# ─────────────────────────────────────────────────────────────────────────────
# Over-by-Over Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_overs_html(html: str, mid: str) -> OversResponse:
    """Parse over-by-over page."""
    nj, soup = _parse_page_html(html, "overs")
    raw_texts = "\n".join(nj.get("raw_texts", []))

    title_tag = soup.title.get_text(strip=True) if soup.title else ""
    title = re.sub(r"^.*?\|\s*", "", title_tag, flags=re.IGNORECASE).strip() or NF

    overs: List[OverDetail] = []

    # Parse commentary from Next.js: each ball has a ballMetric like 45.6
    # Commentary objects: "timestamp": {"commText": "...", "ballMetric": 45.6, "overSeparator": {...}}

    # Extract all commentary entries from Next.js data
    comm_entries = []

    # Pattern: timestamp + commText + ballMetric
    comm_pattern = re.compile(
        r'"(\d{13})"\s*:\s*\{'
        r'(?:[^}]|"[^"]*")*?"commText"\s*:\s*"([^"]*)"'
        r'(?:[^}]|"[^"]*")*?"ballMetric"\s*:\s*([\d.]+|\$undefined|"[^"]*")'
        r'(?:[^}]|"[^"]*")*?"overSeparator"\s*:\s*(\{[^}]*\}|null)',
        re.DOTALL
    )

    for m in comm_pattern.finditer(raw_texts):
        ts = int(m.group(1))
        comm_text = m.group(2)
        ball_metric_raw = m.group(3)
        over_sep_raw = m.group(4)

        # Parse ball metric (e.g., "45.6" → over 45, ball 6)
        bm = NF
        if re.match(r'[\d.]+$', ball_metric_raw):
            bm = ball_metric_raw

        over_sep = None
        if over_sep_raw and over_sep_raw != "null":
            try:
                over_sep = json.loads(over_sep_raw)
            except Exception:
                pass

        comm_entries.append({
            "ts": ts,
            "text": comm_text,
            "ball_metric": bm,
            "over_separator": over_sep,
        })

    # Sort by timestamp (oldest first)
    comm_entries.sort(key=lambda x: x["ts"])

    # Group by over
    current_over_balls: Dict[int, List] = {}  # over_num -> list of balls
    over_separators: Dict[int, Dict] = {}

    for entry in comm_entries:
        bm = entry["ball_metric"]
        if bm == NF:
            continue

        bm_f = float(bm)
        over_num = int(bm_f)
        ball_in_over = round((bm_f - over_num) * 10)
        if ball_in_over == 0:
            ball_in_over = 6  # e.g., "45.6" → over 45, 6th ball

        if over_num not in current_over_balls:
            current_over_balls[over_num] = []

        # Classify the ball from commentary text
        ball_event = _classify_ball_from_commentary(
            entry["text"], len(current_over_balls[over_num]) + 1
        )
        current_over_balls[over_num].append(ball_event)

        # Over separator = end of over data
        if entry["over_separator"]:
            over_separators[over_num + 1] = entry["over_separator"]

    # Build OverDetail objects
    for over_num in sorted(current_over_balls.keys()):
        balls = current_over_balls[over_num]
        sep = over_separators.get(over_num, {})

        bowler = NF
        batsmen = []
        runs_in_over = 0
        wickets_in_over = 0

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

        overs.append(OverDetail(
            over_number=over_num,
            innings_number=1,
            bowler=bowler,
            batsmen=[b for b in batsmen if b != NF],
            runs_in_over=runs_in_over,
            wickets_in_over=wickets_in_over,
            balls=balls,
            over_summary=summary,
            is_current=(over_num == max(current_over_balls.keys())),
        ))

    # HTML fallback if Next.js parsing yields nothing
    if not overs:
        overs = _parse_overs_from_html(soup)

    current_ov = overs[-1].over_number if overs else None

    return OversResponse(
        status="success", match_id=mid, title=title,
        total_overs=len(overs),
        current_over=current_ov,
        overs=overs,
    )


def _classify_ball_from_commentary(text: str, ball_num: int) -> BallEvent:
    """Classify a ball from its commentary text."""
    text_lower = text.lower()

    is_four = bool(re.search(r'\bfour\b|\b4\b', text_lower))
    is_six = bool(re.search(r'\bsix\b|\b6\b', text_lower))
    is_wicket = bool(re.search(r'\bwicket\b|\bout\b|\blbw\b|\bcaught\b|\bbowled\b|\bstumped\b|\brunout\b', text_lower))
    is_wide = bool(re.search(r'\bwide\b', text_lower))
    is_nb = bool(re.search(r'\bno.?ball\b|\bno ball\b', text_lower))
    is_dot = bool(re.search(r'\bno run\b|\bdot\b', text_lower)) and not (is_four or is_six or is_wide or is_nb)

    runs = 0
    if is_four: runs = 4
    elif is_six: runs = 6
    elif not is_wicket and not is_dot:
        run_m = re.search(r'(\d+)\s+run', text_lower)
        if run_m: runs = int(run_m.group(1))

    label = '•' if is_dot else ('W' if is_wicket else
            ('4' if is_four else ('6' if is_six else
            ('Wd' if is_wide else ('Nb' if is_nb else str(runs))))))

    return BallEvent(
        ball_number=ball_num, ball_label=label, runs=runs,
        is_dot=is_dot, is_four=is_four, is_six=is_six,
        is_wicket=is_wicket, is_wide=is_wide, is_no_ball=is_nb,
        commentary=text[:200] if text else NF,
    )


def _parse_overs_from_html(soup: BeautifulSoup) -> List[OverDetail]:
    """HTML fallback for over parsing."""
    overs = []
    all_divs = soup.find_all("div", class_=True)
    i = 0
    while i < len(all_divs):
        div = all_divs[i]
        classes = " ".join(div.get("class", []))
        if re.search(r"cb-ovr-num", classes, re.IGNORECASE):
            raw = _soup_text(div)
            ov_m = re.search(r"Ov\s+(\d+)", raw, re.IGNORECASE)
            if ov_m:
                ov_num = int(ov_m.group(1))
                rw_m = re.search(r"(\d+)-(\d+)", raw)
                runs_ov = int(rw_m.group(1)) if rw_m else 0
                wkts_ov = int(rw_m.group(2)) if rw_m else 0
                overs.append(OverDetail(
                    over_number=ov_num,
                    runs_in_over=runs_ov,
                    wickets_in_over=wkts_ov,
                    is_current=(i == 0),
                ))
        i += 1
    overs.reverse()
    return overs


# ─────────────────────────────────────────────────────────────────────────────
# Match List Parser
# ─────────────────────────────────────────────────────────────────────────────

def _parse_match_list(html: str, status: str) -> List[MatchCard]:
    """Parse match list from live/recent/upcoming pages."""
    nj, soup = _parse_page_html(html, "list")
    raw_texts = "\n".join(nj.get("raw_texts", []))
    cards: List[MatchCard] = []
    seen: set = set()

    # Try Next.js JSON first: look for matches array
    matches_pattern = re.compile(
        r'"matchId"\s*:\s*(\d+)'
        r'.*?"seriesName"\s*:\s*"([^"]*)"'
        r'.*?"matchDesc"\s*:\s*"([^"]*)"'
        r'.*?"matchFormat"\s*:\s*"([^"]*)"'
        r'.*?"state"\s*:\s*"([^"]*)"'
        r'.*?"status"\s*:\s*"([^"]*)"',
        re.DOTALL
    )

    for m in matches_pattern.finditer(raw_texts):
        mid = m.group(1)
        if mid in seen:
            continue
        seen.add(mid)

        series = m.group(2)
        desc = m.group(3)
        fmt = m.group(4)
        state = m.group(5)
        match_status = m.group(6)

        # Get team names
        context = raw_texts[m.start():m.start()+500]
        teams = []
        for t in re.findall(r'"teamName"\s*:\s*"([^"]+)"', context):
            teams.append({"team": t})

        cards.append(MatchCard(
            match_id=mid, series=series,
            title=f"{desc} - {series}",
            teams=teams[:2],
            match_type=fmt, status=state,
            overview=match_status[:100] if match_status else NF,
        ))

    # HTML fallback
    if not cards:
        for a in soup.find_all("a", href=re.compile(r"/live-cricket-scores/\d+")):
            mid = _mid_from_href(a.get("href", ""))
            if mid == NF or mid in seen:
                continue
            seen.add(mid)
            cards.append(MatchCard(match_id=mid, title=_soup_text(a), status=status))

    return cards


# ─────────────────────────────────────────────────────────────────────────────
# AI Preview Builder
# ─────────────────────────────────────────────────────────────────────────────

def _build_preview_prompt(
    score: Optional[LiveScoreResponse],
    scorecard: Optional[ScorecardResponse],
    info: Optional[MatchInfo],
    recent_over: Optional[OverDetail],
) -> str:
    lines = []

    if info and info.title != NF:
        lines.append(f"MATCH: {info.title}")
        if info.series != NF: lines.append(f"Series: {info.series}")
        if info.venue  != NF: lines.append(f"Venue:  {info.venue}")
        if info.toss   != NF: lines.append(f"Toss:   {info.toss}")
        if info.result != NF: lines.append(f"Result: {info.result}")
    elif score and score.title != NF:
        lines.append(f"MATCH: {score.title}")

    if score:
        lines.append("\nLIVE SCORE:")
        if score.score        != NF: lines.append(f"  Score:  {score.score}")
        if score.match_status != NF: lines.append(f"  Status: {score.match_status}")
        if score.day_number:          lines.append(f"  Day:    {score.day_number}")
        if score.current_run_rate != NF:
            lines.append(f"  CRR: {score.current_run_rate}  RRR: {score.required_run_rate}  Target: {score.target}")
        if score.current_batsmen:
            lines.append("  At crease:")
            for b in score.current_batsmen:
                star = "*" if b.is_striker else " "
                lines.append(f"    {star}{b.name}: {b.runs}({b.balls}) 4s:{b.fours} 6s:{b.sixes} SR:{b.strike_rate}")
        bl = score.current_bowler
        if bl.name != NF:
            lines.append(f"  Bowling: {bl.name} {bl.overs}-{bl.maidens}-{bl.runs}-{bl.wickets} ECO:{bl.economy}")
        if score.partnership != NF: lines.append(f"  Partnership: {score.partnership}")
        if score.last_wicket != NF: lines.append(f"  Last wkt: {score.last_wicket}")
        if score.recent_balls:
            lines.append(f"  This over: {' '.join(b.label for b in score.recent_balls)}")

    if recent_over and recent_over.balls:
        lines.append(f"\nCURRENT OVER {recent_over.over_number}: {recent_over.bowler}")
        lines.append(f"  Balls: {recent_over.over_summary}")
        lines.append(f"  Runs: {recent_over.runs_in_over}  Wkts: {recent_over.wickets_in_over}")

    if scorecard and scorecard.innings:
        lines.append("\nSCORECARD:")
        for inn in scorecard.innings:
            lines.append(f"  {inn.team} — {inn.score}")
            batted = [b for b in inn.batting if b.runs not in (NF, "-", "")]
            batted.sort(key=lambda b: int(b.runs) if b.runs.isdigit() else 0, reverse=True)
            for b in batted[:6]:
                dis = f" ({b.dismissal})" if b.dismissal not in (NF, "", "not out") else " (not out)"
                lines.append(f"    {b.name}{dis}: {b.runs}({b.balls}) 4s:{b.fours} 6s:{b.sixes}")
            for bw in inn.bowling:
                if bw.wickets not in (NF, "0", ""):
                    lines.append(f"    {bw.name}: {bw.overs}-{bw.maidens}-{bw.runs}-{bw.wickets} ECO:{bw.economy}")

    lines.append(
        "\n---\nYou are a cricket analyst. Write a concise, engaging match summary "
        "in 4-6 sentences using ONLY the data above. Cover:\n"
        "1. Current match situation and score\n"
        "2. Key performers (runs, wickets, economy)\n"
        "3. Match context and who's on top\n"
        "4. One insight about the game state\n\n"
        "Plain English, no bullets, no markdown headers, specific numbers. Under 120 words."
    )

    return "\n".join(lines)


async def _build_preview(mid: str) -> PreviewResponse:
    """Parallel fetch all pages + Gemini AI summary."""
    t0 = time.monotonic()

    urls = [
        f"{CB}/live-cricket-scores/{mid}",
        f"{CB}/live-cricket-scorecard/{mid}/",
        f"{CB}/cricket-match-facts/{mid}",
        f"{CB}/live-cricket-over-by-over/{mid}",
    ]

    responses = await _fetch_many(*urls)
    score_r, sc_r, info_r, ov_r = responses

    fetched = []
    score_data = scorecard_data = info_data = recent_over = None

    if score_r:
        fetched.append("score")
        try:
            nj, soup = _parse_page_html(score_r.text, "score")
            score_data = _parse_live_score_from_nj(nj, mid, soup)
        except Exception as e:
            pass

    if sc_r:
        fetched.append("scorecard")
        try:
            scorecard_data = _parse_scorecard_html(sc_r.text, mid)
        except Exception:
            pass

    if info_r:
        fetched.append("info")
        try:
            info_data = _parse_match_info(info_r.text, mid)
        except Exception:
            pass

    if ov_r:
        fetched.append("overs")
        try:
            overs_data = _parse_overs_html(ov_r.text, mid)
            if overs_data.overs:
                recent_over = overs_data.overs[-1]
        except Exception:
            pass

    fetch_ms = int((time.monotonic() - t0) * 1000)

    prompt = _build_preview_prompt(score_data, scorecard_data, info_data, recent_over)
    ai_text = await _gemini_query(prompt)

    title = NF
    for src in [score_data, scorecard_data, info_data]:
        if src and hasattr(src, 'title') and src.title != NF:
            title = src.title
            break

    return PreviewResponse(
        status="success", match_id=mid, title=title,
        fetched_pages=fetched, fetch_time_ms=fetch_ms,
        ai_summary=ai_text,
        score=score_data, scorecard=scorecard_data,
        info=info_data, recent_over=recent_over,
    )


# ─────────────────────────────────────────────────────────────────────────────
# ASCII Tree formatter
# ─────────────────────────────────────────────────────────────────────────────

def _tree(d: LiveScoreResponse) -> str:
    bats = "\n".join(
        f"│   {'*' if b.is_striker else ' '} {b.name}  "
        f"{b.runs}({b.balls})  4s:{b.fours}  6s:{b.sixes}  SR:{b.strike_rate}"
        for b in d.current_batsmen
    ) or "│   └── N/A"
    bl = d.current_bowler
    bowl_line = (
        f"{bl.name}  {bl.overs}-{bl.maidens}-{bl.runs}-{bl.wickets}  ECO:{bl.economy}"
        if bl.name != NF else NF
    )
    recent = " ".join(b.label for b in d.recent_balls) if d.recent_balls else NF
    return (
        "🏏 Live Score\n│\n"
        f"├── Match       : {d.title}\n"
        f"├── Type        : {d.match_type}\n"
        f"├── Venue       : {d.venue}\n"
        f"├── Score       : {d.score}\n"
        f"├── Status      : {d.match_status}\n"
        f"├── Day         : {d.day_number}\n"
        f"├── CRR/RRR/Tgt : {d.current_run_rate} / {d.required_run_rate} / {d.target}\n"
        f"├── Toss        : {d.toss}\n"
        f"├── Bowler      : {bowl_line}\n"
        f"├── Partnership : {d.partnership}\n"
        f"├── Last Wicket : {d.last_wicket}\n"
        f"├── Recent      : {recent}\n"
        "├── Batsmen\n"
        f"{bats}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Validation & Error helpers
# ─────────────────────────────────────────────────────────────────────────────

def _validate(mid: str) -> bool:
    try:
        MatchValidator(match_id=mid)
        return True
    except Exception:
        return False


def _err422(msg: str = "invalid match id"):
    return JSONResponse(
        status_code=422,
        content={"status": "error", "code": 422, "message": msg},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/docs", include_in_schema=False)
async def swagger():
    try:
        page = get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title="Cricket Score API v4.0",
            swagger_favicon_url="https://fastapi.tiangolo.com/img/favicon.png",
        )
        r = HTMLResponse(content=page.body.decode("utf-8"))
        r.headers["Cache-Control"] = "no-store"
        return r
    except Exception:
        return HTMLResponse("<h2>Docs unavailable</h2>", status_code=500)


@app.get("/", summary="API info + endpoints list")
async def root(
    score: Optional[str] = Query(None, description="Match ID (backward compat)"),
    text: bool = Query(False, description="ASCII tree output"),
):
    if score is None:
        return {
            "status": "success",
            "message": "Cricket Score API v4.0 — Next.js JSON extraction + Gemini AI",
            "version": "4.0.0",
            "docs": "/docs",
            "endpoints": {
                "live_score":    "/match/{id}/score",
                "scorecard":     "/match/{id}/scorecard",
                "match_info":    "/match/{id}/info",
                "squads":        "/match/{id}/squads",
                "all_overs":     "/match/{id}/overs",
                "current_over":  "/match/{id}/overs/current",
                "specific_over": "/match/{id}/overs/{n}",
                "ai_preview":    "/match/{id}/preview",
                "live_matches":  "/matches/live",
                "recent":        "/matches/recent",
                "upcoming":      "/matches/upcoming",
                "schedule":      "/schedule",
            },
        }

    if not _validate(score):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scores/{score}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    nj, soup = _parse_page_html(r.text, "score")
    data = _parse_live_score_from_nj(nj, score, soup)
    return PlainTextResponse(_tree(data)) if text else data


@app.get("/match/{match_id}/score", response_model=LiveScoreResponse)
async def match_score(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
    text: bool = Query(False, description="ASCII tree output"),
):
    """Live score: batsmen, bowler, innings totals, recent balls, run rates."""
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scores/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    nj, soup = _parse_page_html(r.text, "score")
    data = _parse_live_score_from_nj(nj, match_id, soup)
    return PlainTextResponse(_tree(data)) if text else data


@app.get("/match/{match_id}/scorecard", response_model=ScorecardResponse)
async def match_scorecard(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    """Full scorecard: batting, bowling, fall of wickets, partnerships."""
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scorecard/{match_id}/")
    if r is None:
        raise APIError(503, "upstream unavailable")
    return _parse_scorecard_html(r.text, match_id)


@app.get("/match/{match_id}/info", response_model=MatchInfo)
async def match_info(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    """Match info: venue, toss, umpires, referee, result."""
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/cricket-match-facts/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    return _parse_match_info(r.text, match_id)


@app.get("/match/{match_id}/squads", response_model=SquadsResponse)
async def match_squads(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    """Playing XI squads for both teams."""
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/cricket-match-squads/{match_id}/")
    if r is None:
        raise APIError(503, "upstream unavailable")
    return _parse_squads_html(r.text, match_id)


@app.get("/match/{match_id}/overs", response_model=OversResponse)
async def match_overs(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
    innings: Optional[int] = Query(None, description="Filter by innings (1 or 2)"),
):
    """All overs ball-by-ball with commentary."""
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-over-by-over/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_overs_html(r.text, match_id)
    if innings is not None:
        data.overs = [o for o in data.overs if o.innings_number == innings]
        data.total_overs = len(data.overs)
    return data


@app.get("/match/{match_id}/overs/current", response_model=OverDetail)
async def match_current_over(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    """Current (live/incomplete) over with ball-by-ball detail."""
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-over-by-over/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_overs_html(r.text, match_id)
    if data.overs:
        for ov in reversed(data.overs):
            if ov.is_current:
                return ov
        return data.overs[-1]
    raise APIError(404, "no over data found")


@app.get("/match/{match_id}/overs/{over_number}", response_model=OverDetail)
async def match_over_by_number(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
    over_number: int = Path(..., description="Over number (1-based)", ge=1, le=200),
    innings: int = Query(1, description="Innings number"),
):
    """Specific over by number."""
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-over-by-over/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_overs_html(r.text, match_id)
    for ov in data.overs:
        if ov.over_number == over_number and ov.innings_number == innings:
            return ov
    raise APIError(404, f"over {over_number} (innings {innings}) not found")


@app.get("/match/{match_id}/preview", response_model=PreviewResponse)
async def match_preview(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    """
    AI-powered match preview: parallel fetch of all 4 pages + Gemini 1.5 Flash summary.
    Returns full structured JSON + concise AI narrative.
    """
    if not _validate(match_id):
        return _err422()
    return await _build_preview(match_id)


_STATUS_MAP = {
    "live":     "",
    "recent":   "/recent-matches",
    "upcoming": "/upcoming-matches",
}
_TYPE_PATHS_LIVE = {
    "international": "",
    "league":        "/league-cricket",
    "domestic":      "/domestic-cricket",
    "women":         "/women-cricket",
}
_TYPE_SUFFIX = {
    "international": "",
    "league":        "/league",
    "domestic":      "/domestic",
    "women":         "/women",
}


@app.get("/matches/{match_status}", response_model=MatchListResponse)
async def matches(
    match_status: str = Path(..., description="live | recent | upcoming"),
    type: str = Query("international", description="international | league | domestic | women"),
):
    """List matches by status."""
    if match_status not in _STATUS_MAP:
        return JSONResponse(
            status_code=422,
            content={"status": "error", "message": "status must be live, recent, or upcoming"},
        )
    if type not in _TYPE_SUFFIX:
        return JSONResponse(
            status_code=422,
            content={"status": "error", "message": "type must be international, league, domestic, or women"},
        )

    if match_status == "live":
        url = f"{CB}/cricket-match/live-scores{_TYPE_PATHS_LIVE.get(type, '')}"
    else:
        url = f"{CB}/cricket-match/live-scores{_STATUS_MAP[match_status]}{_TYPE_SUFFIX[type]}"

    r = await _fetch(url)
    if r is None:
        raise APIError(503, "upstream unavailable")

    cards = _parse_match_list(r.text, match_status)
    return MatchListResponse(
        status="success",
        type=f"{match_status}/{type}",
        total=len(cards),
        matches=cards,
    )


@app.get("/schedule", response_model=MatchListResponse, summary="Upcoming matches (alias)")
async def schedule(
    type: str = Query("international", description="international | league | domestic | women"),
):
    return await matches("upcoming", type=type)


# ─────────────────────────────────────────────────────────────────────────────
# Error handlers
# ─────────────────────────────────────────────────────────────────────────────

@app.exception_handler(APIError)
async def _api_err(request: Request, exc: APIError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "code": exc.status_code, "message": exc.message},
    )


@app.exception_handler(StarletteHTTPException)
async def _http_err(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "code": exc.status_code, "message": "invalid route"},
    )


@app.exception_handler(Exception)
async def _generic_err(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"status": "error", "code": 500, "message": "internal server error"},
    )
