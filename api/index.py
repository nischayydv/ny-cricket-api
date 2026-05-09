"""
Cricket Score API v3.2  –  FastAPI + BeautifulSoup
Vercel serverless entry-point: api/index.py

New in v3.2:
  - /match/{id}/overs          ← all overs ball-by-ball
  - /match/{id}/overs/current  ← current/latest over only
  - /match/{id}/overs/{n}      ← specific over number

Fixes in v3.1:
  - current batsmen now include fours, sixes, strike_rate (parsed from HTML rows)
  - match_status correctly detects "Innings Break" and similar structural states
  - scorecard parser improved: powerplay, partnerships, fall of wickets sections

Page scrapers:
  /match/{id}/info          ← cricket-match-facts/{id}
  /match/{id}/score         ← live-cricket-scores/{id}
  /match/{id}/scorecard     ← live-cricket-scorecard/{id}
  /match/{id}/squads        ← cricket-match-squads/{id}
  /match/{id}/partnerships  ← live-cricket-scorecard/{id}
  /match/{id}/fow           ← live-cricket-scorecard/{id}
  /match/{id}/powerplay     ← live-cricket-scorecard/{id}
  /match/{id}/overs         ← live-cricket-over-by-over/{id}
  /matches/{status}         ← live / recent / upcoming lists
  /schedule                 ← alias for upcoming
"""

import re
import html as html_lib
import time
import asyncio
from typing import List, Optional, Dict, Any

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
    "Sec-Ch-Ua": '"Chromium";v="124","Google Chrome";v="124"',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": '"Windows"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
}

# ─────────────────────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────────────────────


class APIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message


# ── /match/{id}/info ─────────────────────────────────────────────────────────

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


# ── /match/{id}/score ────────────────────────────────────────────────────────

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


# ── /match/{id}/scorecard ────────────────────────────────────────────────────

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


class PowerplayEntry(BaseModel):
    type: str = NF      # e.g. "Mandatory", "Batting", "Fielding"
    overs: str = NF
    runs: str = NF


class PartnershipEntry(BaseModel):
    batsman1: str = NF
    batsman1_runs: str = NF
    batsman1_balls: str = NF
    batsman2: str = NF
    batsman2_runs: str = NF
    batsman2_balls: str = NF
    partnership_runs: str = NF
    partnership_balls: str = NF


class FowEntry(BaseModel):
    batsman: str = NF
    score: str = NF
    over: str = NF


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


# ── /match/{id}/partnerships ──────────────────────────────────────────────────

class InningsPartnerships(BaseModel):
    team: str = NF
    innings_number: int = 0
    partnerships: List[PartnershipEntry] = []


class PartnershipsResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    innings: List[InningsPartnerships] = []


# ── /match/{id}/fow ──────────────────────────────────────────────────────────

class InningsFow(BaseModel):
    team: str = NF
    innings_number: int = 0
    fall_of_wickets: List[FowEntry] = []


class FowResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    innings: List[InningsFow] = []


# ── /match/{id}/powerplay ─────────────────────────────────────────────────────

class InningsPowerplay(BaseModel):
    team: str = NF
    innings_number: int = 0
    powerplays: List[PowerplayEntry] = []


class PowerplayResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    innings: List[InningsPowerplay] = []


# ── /match/{id}/overs ────────────────────────────────────────────────────────

# Ball event types as returned by the API
# "run"     – normal delivery, n runs scored (0 = dot "•")
# "wide"    – wide delivery (may also carry runs, e.g. Wd+1 = 2 penalty)
# "noball"  – no-ball (may carry runs)
# "wicket"  – wicket taken (may also carry runs on the same ball)
# "four"    – boundary 4 (run=4)
# "six"     – boundary 6 (run=6)
# "byes"    – byes
# "legbyes" – leg byes
# "penalty" – 5-run penalty

class BallEvent(BaseModel):
    ball_number: int           # 1-based delivery number in the over
    ball_label: str            # display string: "1", "6", "•", "Wd", "Nb", "W", "4", "6"
    runs: int = 0              # runs attributed to this delivery
    is_dot: bool = False
    is_wide: bool = False
    is_no_ball: bool = False
    is_wicket: bool = False
    is_four: bool = False
    is_six: bool = False
    is_byes: bool = False
    is_leg_byes: bool = False
    extras: int = 0            # extra runs (wide/noball penalty beyond the ball run)
    commentary: str = NF       # short commentary text if available


class OverDetail(BaseModel):
    over_number: int           # 1-based over number in the innings
    innings_number: int = 1
    bowler: str = NF
    batsmen: List[str] = []    # one or two names at crease for this over
    runs_in_over: int = 0
    wickets_in_over: int = 0
    balls: List[BallEvent] = []
    over_summary: str = NF     # e.g. "1 6 • 2 • Wd 1"
    is_current: bool = False   # True for the live/incomplete over


class OversResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    total_overs: int = 0
    current_over: Optional[int] = None   # over_number of the live over
    overs: List[OverDetail] = []


# ── /match/{id}/squads ───────────────────────────────────────────────────────

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


# ── /matches/{status} ────────────────────────────────────────────────────────

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
# App
# ─────────────────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Cricket Score API",
    version="3.2.0",
    description=(
        "Full-featured cricket API: live scores, full scorecards, "
        "match info, squads, partnerships, fall of wickets, powerplays & "
        "ball-by-ball over detail — powered by Cricbuzz scraping."
    ),
    docs_url=None,
    redoc_url=None,
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
        "Expires": "0",
        "X-Content-Type-Options": "nosniff",
        "X-Frame-Options": "DENY",
        "Referrer-Policy": "no-referrer",
        "X-Robots-Tag": "noindex, nofollow",
        "Strict-Transport-Security": "max-age=31536000",
        "Content-Security-Policy": (
            "default-src 'self';"
            "connect-src 'self' https://cdn.jsdelivr.net;"
            "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net;"
            "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net;"
            "img-src 'self' data: https://fastapi.tiangolo.com;"
            "object-src 'none';frame-ancestors 'none';"
        ),
    })
    return resp


# ─────────────────────────────────────────────────────────────────────────────
# HTTP
# ─────────────────────────────────────────────────────────────────────────────

async def _fetch(url: str, retries: int = 2) -> Optional[httpx.Response]:
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(timeout=15.0, follow_redirects=True) as c:
                r = await c.get(url, headers=HEADERS)
            if r.status_code == 200:
                return r
            if r.status_code in (429, 503) and attempt < retries - 1:
                await asyncio.sleep(1.0)
        except (httpx.TimeoutException, httpx.ConnectError):
            if attempt < retries - 1:
                await asyncio.sleep(0.5)
        except Exception:
            break
    return None


def _soup(text: str) -> BeautifulSoup:
    return BeautifulSoup(text, "lxml")


def _t(el: Any) -> str:
    """Safe get_text + clean."""
    if el is None:
        return NF
    raw = el.get_text(" ", strip=True) if isinstance(el, Tag) else str(el)
    out = html_lib.unescape(" ".join(raw.split())).strip()
    return out or NF


def _mid(href: str) -> str:
    m = re.search(r"/(\d{4,})", href or "")
    return m.group(1) if m else NF


def _match_type(text: str) -> str:
    t = (text or "").upper()
    for tag in ("TEST", "T20I", "T20", "T10", "ODI",
                "THE HUNDRED", "LIST A", "FIRST-CLASS"):
        if tag in t:
            return tag
    return NF


def _og(soup: BeautifulSoup, prop: str) -> str:
    el = soup.find("meta", property=prop)
    return (el.get("content", "") if el else "") or ""


# ─────────────────────────────────────────────────────────────────────────────
# Scraper 1 — match-facts  →  /match/{id}/info
# ─────────────────────────────────────────────────────────────────────────────

def _parse_info(soup: BeautifulSoup, mid: str) -> MatchInfo:
    title_raw = soup.title.get_text(strip=True) if soup.title else NF
    title = _t(re.sub(r"^.*?\|\s*", "", title_raw, flags=re.IGNORECASE)) or NF

    info: Dict[str, str] = {}
    for row in soup.find_all("div", class_=re.compile(r"cb-mtch-info-itm")):
        cols = row.find_all("div", recursive=False)
        if len(cols) >= 2:
            key = _t(cols[0]).lower().rstrip(":")
            val = _t(cols[1])
            info[key] = val
        else:
            raw = _t(row)
            if ":" in raw:
                k, _, v = raw.partition(":")
                info[k.strip().lower()] = v.strip()

    page_text = _t(soup.get_text(" ", strip=True))

    def _pick(*keys: str) -> str:
        for k in keys:
            if k in info and info[k] != NF:
                return info[k]
        return NF

    venue_full = _pick("venue", "ground", "stadium")
    city = NF
    if venue_full != NF and "," in venue_full:
        city = venue_full.split(",")[-1].strip()

    umpires: List[str] = []
    for k in ("umpires", "on-field umpires", "field umpires", "umpire"):
        if k in info and info[k] != NF:
            for u in re.split(r"[,&]", info[k]):
                u = u.strip()
                if u:
                    umpires.append(u)
            break

    result = NF
    og_desc = _og(soup, "og:description")
    for pat in (
        r"((?:won|tied|no result|abandoned|drawn)[^.]{0,80})",
        r"(match (?:tied|drawn|abandoned)[^.]{0,40})",
    ):
        m = re.search(pat, og_desc + " " + page_text, re.IGNORECASE)
        if m:
            result = m.group(1).strip()
            break

    return MatchInfo(
        status="success",
        match_id=mid,
        title=title,
        series=_pick("series", "tournament"),
        match_type=_match_type(title + " " + page_text[:500]),
        match_number=_pick("match", "match number"),
        venue=venue_full,
        city=city,
        date=_pick("date", "match date"),
        day_night=_pick("day/night", "day / night", "day-night"),
        toss=_pick("toss"),
        umpires=umpires,
        third_umpire=_pick("third umpire", "3rd umpire"),
        match_referee=_pick("match referee", "referee"),
        result=result,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Scraper 2 — live-scores  →  /match/{id}/score
# ─────────────────────────────────────────────────────────────────────────────

# Structural break phrases that take priority over everything else.
# If any of these appear prominently in the page, that IS the match status.
_STRUCTURAL_STATES = (
    r"innings\s+break",
    r"lunch\s+break",
    r"tea\s+break",
    r"drinks\s+break",
    r"stumps",
    r"rain\s+(?:break|stopped|delay)",
    r"bad\s+light",
    r"match\s+abandoned",
    r"match\s+drawn",
    r"match\s+tied",
    r"match\s+complete",
)
_STRUCTURAL_RE = re.compile(
    r"(?:" + "|".join(_STRUCTURAL_STATES) + r")",
    re.IGNORECASE,
)


def _detect_match_status(soup: BeautifulSoup, page_text: str, og_desc: str) -> str:
    """
    Improved match-status detection.

    Priority order:
      1. Structural break keywords (Innings Break, Lunch, Tea, Stumps…)
         — found in dedicated status div classes on Cricbuzz.
      2. Result phrases ("won by", etc.)
      3. Chase / run-rate phrases.

    We deliberately do NOT pull from generic commentary to avoid false positives.
    """

    # ── Priority 1: dedicated status divs ────────────────────────────────────
    # Cricbuzz wraps structural states in specific div classes:
    #   cb-text-inprogress, cb-text-complete, cb-text-lunch, cb-text-tea …
    for cls_pat in (
        r"cb-text-(?:inprogress|complete|lunch|tea|stumps|rain|abandon|draw|tied)",
        r"cb-game-status",
        r"cbz-ui-status",
    ):
        el = soup.find("div", class_=re.compile(cls_pat, re.IGNORECASE))
        if el:
            txt = _t(el).strip()
            if txt and len(txt) < 120:
                return txt

    # ── Priority 1b: any small element whose text IS a structural keyword ─────
    for el in soup.find_all(["div", "span", "p"]):
        txt = el.get_text(strip=True)
        if _STRUCTURAL_RE.fullmatch(txt.strip()):
            return txt.strip().title()

    # ── Priority 1c: og:description starts with or contains a structural word ─
    og_m = _STRUCTURAL_RE.search(og_desc[:200])
    if og_m:
        # Extract the surrounding sentence for context
        start = max(0, og_m.start() - 10)
        end   = min(len(og_desc), og_m.end() + 60)
        snippet = og_desc[start:end].strip().split(".")[0].strip()
        if len(snippet) < 100:
            return snippet

    # ── Priority 2: result phrase ─────────────────────────────────────────────
    res_m = re.search(
        r"((?:[A-Za-z ]{3,40})\s+won\s+by\s+[^.\n]{5,60})",
        og_desc + " " + page_text[:2000],
        re.IGNORECASE,
    )
    if res_m:
        candidate = res_m.group(1).strip()
        if not re.search(r"\b[A-Z]{4,}\b.*\b[A-Z]{4,}\b", candidate):
            return candidate

    # ── Priority 3: chase / run-rate phrase ──────────────────────────────────
    for pat in (
        r"((?:needs?|require)\s+\d+\s+(?:run|more run)[^.\n]{0,60})",
        r"((?:innings break|lunch|tea|stumps|drinks)[^.\n]{0,40})",
    ):
        sm = re.search(pat, page_text[:3000], re.IGNORECASE)
        if sm:
            candidate = sm.group(1).strip()
            if not re.search(r"\b[A-Z]{3,}\b.*\b[A-Z]{3,}\b", candidate):
                return candidate

    return NF


def _parse_live_score(soup: BeautifulSoup, mid: str) -> LiveScoreResponse:
    title_raw = soup.title.get_text(strip=True) if soup.title else NF
    title = _t(re.sub(r"^Cricket(?:\s+commentary)?\s*[|\-–]\s*",
                      "", title_raw, flags=re.IGNORECASE))

    og_title = _og(soup, "og:title")
    og_desc  = _og(soup, "og:description")

    # ── Innings scores from og:title ─────────────────────────────────────────
    innings: List[InningsScore] = []
    for team, runs, wkts, ovs in re.findall(
        r"([A-Z]{2,5})\s+(\d+)/(\d+)\s*\(([\d.]+)\)", og_title
    ):
        innings.append(InningsScore(
            team=team, runs=runs, wickets=wkts, overs=ovs,
            display=f"{team} {runs}/{wkts} ({ovs})"
        ))
    score_str = "  |  ".join(i.display for i in innings) if innings else NF

    # ── Both batsmen from og:title ────────────────────────────────────────────
    batsmen: List[ScorecardBatsman] = []
    seen_batsmen: set = set()

    for name, runs, balls, star in re.findall(
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(\d+)\*?\((\d+)\)(\*?)",
        og_title,
    ):
        clean_name = name.strip()
        if clean_name not in seen_batsmen:
            seen_batsmen.add(clean_name)
            batsmen.append(ScorecardBatsman(
                name=clean_name,
                runs=runs,
                balls=balls,
                is_striker=bool(star),
            ))

    # Enrich with HTML stats (fours, sixes, strike_rate) and fallback if needed
    batsmen = _extract_batsmen_from_html(soup, batsmen, seen_batsmen)

    page_text = " ".join(soup.get_text(" ", strip=True).split())

    # ── Current bowler ────────────────────────────────────────────────────────
    bowler = _extract_current_bowler(soup, page_text)

    # ── Venue ─────────────────────────────────────────────────────────────────
    venue = NF
    vm = re.search(
        r"at\s+((?:[A-Z][a-zA-Z]+[\s,]*){1,6}(?:Stadium|Ground|Oval|Park|Arena|Maidan|Maidaan))",
        og_desc,
    )
    if vm:
        venue = vm.group(1).strip().rstrip(",")
    if venue == NF:
        v2 = re.search(
            r"(?:venue|ground)[:\s]+([A-Za-z ,]{5,60}(?:Stadium|Ground|Oval|Park|Arena|Maidan))",
            page_text, re.IGNORECASE,
        )
        if v2:
            venue = v2.group(1).strip()

    # ── Match type ────────────────────────────────────────────────────────────
    match_type = _match_type(og_title + " " + title)
    if match_type == NF:
        if re.search(r"\bIPL\b|Indian Premier League", og_title + " " + title, re.IGNORECASE):
            match_type = "T20"

    # ── Toss ──────────────────────────────────────────────────────────────────
    toss = NF
    tm = re.search(
        r"Toss[:\s]+([A-Za-z ()]{5,60}?)(?:\s+(?:CRR|P'SHIP|Last|Partnership|\d{2,})|\s*$)",
        page_text, re.IGNORECASE,
    )
    if tm:
        toss = tm.group(1).strip()
    else:
        tm2 = re.search(
            r"([A-Za-z ]{5,40}won the toss[^.\n]{0,60})",
            og_desc, re.IGNORECASE,
        )
        if tm2:
            toss = tm2.group(1).strip()

    # ── Match status (improved) ───────────────────────────────────────────────
    match_status = _detect_match_status(soup, page_text, og_desc)

    def _rex(pat: str) -> str:
        m = re.search(pat, page_text, re.IGNORECASE)
        return m.group(1).strip() if m else NF

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
        last_wicket=_rex(
            r"last\s+wicket[:\s]+([A-Za-z .'\-]+\s+\d+\([^)]+\)[^|]{0,40})"
        ),
        partnership=_rex(r"partnership[:\s*]+(\d+\s*\(\s*\d+\s*balls?\s*\))"),
        current_run_rate=_rex(r"CRR[:\s]+(\d+[\d.]*)"),
        required_run_rate=_rex(r"RRR[:\s]+(\d+[\d.]*)"),
        target=_rex(r"target[:\s]+(\d+)"),
    )


def _find_scorecard_boundary(soup: BeautifulSoup):
    """
    Split profile anchors into batter vs bowler side using the
    'Bowler O M R W ECO' header as the boundary.
    Returns (batter_anchors, bowler_anchors).
    """
    all_profile_anchors = soup.find_all("a", href=re.compile(r"/profiles/\d+"))

    boundary = None
    for el in soup.find_all(True):
        txt = el.get_text(" ", strip=True)
        if (re.match(r"^Bowler\b", txt, re.IGNORECASE)
                and "ECO" in txt
                and len(txt) < 60):
            boundary = el
            break
    if boundary is None:
        for el in soup.find_all(True):
            txt = el.get_text(strip=True)
            if txt.lower() == "bowler":
                boundary = el
                break

    if boundary is None:
        return all_profile_anchors, []

    all_tags = list(soup.find_all(True))
    try:
        boundary_idx = all_tags.index(boundary)
    except ValueError:
        return all_profile_anchors, []

    anchor_positions = {a: all_tags.index(a) for a in all_profile_anchors
                        if a in all_tags}
    batter_anchors = [a for a, pos in anchor_positions.items()
                      if pos < boundary_idx]
    bowler_anchors = [a for a, pos in anchor_positions.items()
                      if pos > boundary_idx]

    return batter_anchors, bowler_anchors


def _anchor_row_stats(anchor: Tag) -> Optional[tuple]:
    """
    Walk up the DOM to find a row with ≥5 numeric tokens after the player name.
    Returns (name, overs, maidens, runs, wickets, economy) or None.
    """
    name = _t(anchor).strip().rstrip("* ").strip()
    if not name or len(name) < 3:
        return None

    row_el = anchor.parent
    for _ in range(4):
        if row_el is None:
            break
        row_text = _t(row_el)
        stats_text = re.sub(re.escape(name), "", row_text, count=1,
                            flags=re.IGNORECASE).lstrip("* ").strip()
        nums = re.findall(r"\d+(?:\.\d+)?", stats_text)
        if len(nums) >= 5:
            ovs  = nums[0]
            mdn  = nums[1]
            runs = nums[2]
            wkts = nums[3]
            eco  = nums[6] if len(nums) >= 7 else nums[4]
            try:
                if float(ovs) <= 25 and float(eco) <= 36:
                    return (name, ovs, mdn, runs, wkts, eco)
            except ValueError:
                pass
        row_el = row_el.parent

    return None


def _extract_batsmen_from_html(
    soup: BeautifulSoup,
    existing: List[ScorecardBatsman],
    seen: set,
) -> List[ScorecardBatsman]:
    """
    Parse current batsmen from the live scores page HTML, including
    fours (4s), sixes (6s), and strike rate from the scorecard row.

    Cricbuzz live page row layout (after stripping player name):
      R  B  4s  6s  SR
      36 18  3   2  200.00
    So: nums[0]=R, nums[1]=B, nums[2]=4s, nums[3]=6s, nums[4]=SR
    """
    batter_anchors, _ = _find_scorecard_boundary(soup)
    batsmen = list(existing)

    # Build a map of names we already found so we can ENRICH them rather than duplicate
    existing_map: Dict[str, ScorecardBatsman] = {b.name: b for b in batsmen}

    for anchor in batter_anchors:
        name = _t(anchor).strip().rstrip("* ").strip()
        if not name or name == NF or len(name) < 3:
            continue

        row_el = anchor.parent
        runs = balls = fours = sixes = sr = NF
        is_striker = False

        for _ in range(5):
            if row_el is None:
                break
            row_text = _t(row_el)
            if "*" in row_text:
                is_striker = True
            # Strip the name to isolate stat numbers
            stats_text = re.sub(re.escape(name), "", row_text, count=1,
                                flags=re.IGNORECASE).replace("*", " ").strip()
            nums = re.findall(r"\d+(?:\.\d+)?", stats_text)
            if len(nums) >= 5:
                runs   = nums[0]
                balls  = nums[1]
                fours  = nums[2]
                sixes  = nums[3]
                sr     = nums[4]
                break
            elif len(nums) >= 2:
                runs  = nums[0]
                balls = nums[1]
                # don't break — keep climbing for a richer row
            row_el = row_el.parent

        if name in existing_map:
            # Enrich the already-found batsman
            b = existing_map[name]
            if fours != NF:
                b.fours = fours
            if sixes != NF:
                b.sixes = sixes
            if sr != NF:
                b.strike_rate = sr
            if is_striker:
                b.is_striker = True
        elif name not in seen:
            seen.add(name)
            batsmen.append(ScorecardBatsman(
                name=name,
                runs=runs,
                balls=balls,
                fours=fours,
                sixes=sixes,
                strike_rate=sr,
                is_striker=is_striker,
            ))

        if len(batsmen) >= 2:
            break

    return batsmen


def _extract_current_bowler(soup: BeautifulSoup, page_text: str) -> ScorecardBowler:
    """
    Extract the current bowler from the Bowler section.

    Strategy 1 — HTML boundary split.
    Strategy 2 — page-text regex after "Bowler O M R W ECO" header.
    Strategy 3 — name-only fallback.
    """
    _, bowler_anchors = _find_scorecard_boundary(soup)

    for anchor in bowler_anchors:
        result = _anchor_row_stats(anchor)
        if result:
            name, ovs, mdn, runs, wkts, eco = result
            return ScorecardBowler(
                name=name, overs=ovs, maidens=mdn,
                runs=runs, wickets=wkts, economy=eco,
            )

    bm = re.search(
        r"Bowler\s+O\s+M\s+R\s+W\s+(?:NB\s+WD\s+)?ECO\s*"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*\*?\s*"
        r"(\d+(?:\.\d+)?)\s+(\d+)\s+(\d+)\s+(\d+)\s+"
        r"(?:\d+\s+\d+\s+)?"
        r"(\d+(?:\.\d+)?)",
        page_text, re.IGNORECASE,
    )
    if bm:
        return ScorecardBowler(
            name=bm.group(1).strip(),
            overs=bm.group(2),
            maidens=bm.group(3),
            runs=bm.group(4),
            wickets=bm.group(5),
            economy=bm.group(6),
        )

    nm = re.search(
        r"(?:Bowler|bowling)[:\-]?\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+\d+",
        page_text, re.IGNORECASE,
    )
    if nm:
        return ScorecardBowler(name=nm.group(1).strip())

    return ScorecardBowler()


# ─────────────────────────────────────────────────────────────────────────────
# Scraper 3 — live-cricket-scorecard  →  /match/{id}/scorecard
# ─────────────────────────────────────────────────────────────────────────────

def _parse_fow_row(raw: str) -> List[FowEntry]:
    """
    Parse Fall of Wickets text.
    Cricbuzz format on scorecard page:
      "Score Over Batsman_Name"   (tab-separated or space-separated columns)

    Two formats appear:
      (A) "118-1 10.5 Sai Sudharsan"
      (B) Structured divs with separate columns
    We handle the text representation here.
    """
    entries: List[FowEntry] = []
    # Strip header words
    raw = re.sub(r"^fall\s+of\s+wickets?[\s:]*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\bscore\b|\bover\b", "", raw, flags=re.IGNORECASE)
    # Each entry looks like "118-1 10.5 Sai Sudharsan" or similar
    for m in re.finditer(
        r"(\d{1,4}-\d{1,2})\s+([\d.]+)\s+([A-Z][a-zA-Z .'-]{2,40}?)(?=\s+\d|\s*$)",
        raw,
    ):
        entries.append(FowEntry(
            score=m.group(1).strip(),
            over=m.group(2).strip(),
            batsman=m.group(3).strip(),
        ))
    return entries


def _parse_powerplay_row(raw: str) -> List[PowerplayEntry]:
    """
    Parse Powerplay text.
    Format: "Mandatory 0.1 - 6 82"  or  "Batting 7-10 45"
    """
    entries: List[PowerplayEntry] = []
    raw = re.sub(r"^powerplay[s]?[\s:]*", "", raw, flags=re.IGNORECASE).strip()
    raw = re.sub(r"\bovers?\b|\bruns?\b", "", raw, flags=re.IGNORECASE)
    # Match "Type overs runs"
    for m in re.finditer(
        r"(Mandatory|Batting|Fielding|Power Play \d+)\s+([\d.\s\-–]+?)\s+(\d+)(?=\s|$)",
        raw, re.IGNORECASE,
    ):
        overs_range = re.sub(r"\s+", "", m.group(2)).strip(" -–")
        entries.append(PowerplayEntry(
            type=m.group(1).strip(),
            overs=overs_range,
            runs=m.group(3).strip(),
        ))
    return entries


def _parse_partnership_section(soup: BeautifulSoup) -> List[List[PartnershipEntry]]:
    """
    Parse partnership data per innings.
    Cricbuzz scorecard shows partnerships in a table-like section.
    Each row: Bat1  runs(balls)  total_runs(total_balls)  Bat2  runs(balls)

    Returns a list of lists (one per innings).
    """
    all_innings_parts: List[List[PartnershipEntry]] = []
    current: List[PartnershipEntry] = []

    partnership_sections = soup.find_all(
        "div", class_=re.compile(r"cb-part-wrp|cb-partner|partnerships", re.IGNORECASE)
    )

    for section in partnership_sections:
        rows = section.find_all("div", class_=re.compile(r"cb-part-row|cb-partner-row"))
        for row in rows:
            cells = [_t(c) for c in row.find_all("div", recursive=False)]
            if len(cells) < 3:
                continue
            # Try to extract batsmen and numbers
            entry = _parse_partnership_cells(cells)
            if entry:
                current.append(entry)

        if current:
            all_innings_parts.append(current)
            current = []

    # Fallback: text-based parsing
    if not all_innings_parts:
        all_innings_parts = _text_partnership_parse(soup)

    return all_innings_parts


def _parse_partnership_cells(cells: List[str]) -> Optional[PartnershipEntry]:
    """Parse a list of cell texts into a PartnershipEntry."""
    # Typical cell layout varies; extract all "Name runs(balls)" patterns
    full_text = " | ".join(cells)
    matches = re.findall(
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(\d+)\((\d+)\)",
        full_text,
    )
    partnership_m = re.search(
        r"(\d+)\s*\(\s*(\d+)\s*\)",
        cells[len(cells) // 2] if len(cells) > 2 else full_text,
    )

    if len(matches) >= 2:
        b1_name, b1_r, b1_b = matches[0]
        b2_name, b2_r, b2_b = matches[-1]
        # Middle cell usually has partnership total
        p_runs = p_balls = NF
        if partnership_m:
            p_runs  = partnership_m.group(1)
            p_balls = partnership_m.group(2)
        return PartnershipEntry(
            batsman1=b1_name.strip(),
            batsman1_runs=b1_r,
            batsman1_balls=b1_b,
            batsman2=b2_name.strip(),
            batsman2_runs=b2_r,
            batsman2_balls=b2_b,
            partnership_runs=p_runs,
            partnership_balls=p_balls,
        )
    return None


def _text_partnership_parse(soup: BeautifulSoup) -> List[List[PartnershipEntry]]:
    """
    Text-based fallback partnership parser for Cricbuzz scorecard.

    Cricbuzz HTML layout (simplified):
      <div class="cb-col cb-col-100 cb-part-wrp">
        <!-- Bat1 column (cb-col-50 left): name + runs(balls) stacked -->
        <!-- Centre column (cb-col-8): partnership_runs(balls) -->
        <!-- Bat2 column (cb-col-50 right): name + runs(balls) stacked -->
      </div>

    We also handle the pure text fallback by scanning for the Partnerships
    heading and extracting "Name runs(balls)" triplets.
    """
    all_innings: List[List[PartnershipEntry]] = []
    current_innings: List[PartnershipEntry] = []

    # Strategy A: look for wrapping divs with class containing "part"
    for wrapper in soup.find_all(
        "div", class_=re.compile(r"cb-part-wrp|cb-col-100.*part|partner", re.IGNORECASE)
    ):
        raw = _t(wrapper)
        # Each partnership block: one or two "Name runs(balls)" + a total
        matches = re.findall(
            r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(\d+)\((\d+)\)",
            raw,
        )
        # Extract partnership total (the first standalone "runs(balls)" that
        # doesn't follow a name)
        total_m = re.search(r"(\d+)\((\d+)\)", raw)
        p_runs = total_m.group(1) if total_m else NF
        p_balls = total_m.group(2) if total_m else NF

        if len(matches) >= 2:
            entry = PartnershipEntry(
                batsman1=matches[0][0].strip(),
                batsman1_runs=matches[0][1],
                batsman1_balls=matches[0][2],
                batsman2=matches[-1][0].strip(),
                batsman2_runs=matches[-1][1],
                batsman2_balls=matches[-1][2],
                partnership_runs=p_runs,
                partnership_balls=p_balls,
            )
            current_innings.append(entry)

    if current_innings:
        all_innings.append(current_innings)

    # Strategy B: raw text after "Partnerships" heading
    if not all_innings:
        page_text = soup.get_text(" ", strip=True)
        for section_m in re.finditer(
            r"Partnerships\s*(.*?)(?=Fall of Wickets|Powerplay|$)",
            page_text, re.IGNORECASE | re.DOTALL,
        ):
            section = section_m.group(1)
            matches = re.findall(
                r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(\d+)\((\d+)\)\s+"
                r"(\d+)\((\d+)\)\s+"  # partnership total
                r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s+(\d+)\((\d+)\)",
                section,
            )
            entries = []
            for m in matches:
                entries.append(PartnershipEntry(
                    batsman1=m[0].strip(),
                    batsman1_runs=m[1],
                    batsman1_balls=m[2],
                    partnership_runs=m[3],
                    partnership_balls=m[4],
                    batsman2=m[5].strip(),
                    batsman2_runs=m[6],
                    batsman2_balls=m[7],
                ))
            if entries:
                all_innings.append(entries)

    return all_innings


def _parse_fow_from_soup(soup: BeautifulSoup) -> List[List[FowEntry]]:
    """
    Parse Fall of Wickets from HTML.
    Cricbuzz shows FoW as a dedicated section per innings:

      <div class="cb-col-100 cb-ltst-wgt-hdr">Fall of Wickets</div>
      <div class="cb-col-100 cb-scrd-itms">
        <div>Score</div><div>Over</div>  ← header
        <div>118-1</div><div>10.5</div><div>Sai Sudharsan</div>
        ...
      </div>
    """
    all_innings: List[List[FowEntry]] = []
    current: List[FowEntry] = []

    # Find all FoW sections (there's one per innings)
    for section in soup.find_all(
        "div",
        string=re.compile(r"fall\s+of\s+wickets?", re.IGNORECASE),
    ):
        sib = section.find_next_sibling("div")
        if sib is None:
            continue
        raw = _t(sib)
        entries = _parse_fow_row(raw)
        if entries:
            all_innings.append(entries)
            continue

        # Try row-by-row
        rows = sib.find_all("div", class_=re.compile(r"cb-scrd-itms"))
        temp: List[FowEntry] = []
        for row in rows:
            cells = [_t(c) for c in row.find_all("div", recursive=False)]
            # Expect: score, over, batsman_name
            if len(cells) >= 3:
                score_m = re.match(r"\d+-\d+", cells[0])
                over_m  = re.match(r"[\d.]+", cells[1])
                if score_m and over_m:
                    temp.append(FowEntry(
                        score=cells[0],
                        over=cells[1],
                        batsman=cells[2],
                    ))
        if temp:
            all_innings.append(temp)

    # Text fallback using the page text
    if not all_innings:
        page_text = soup.get_text(" ", strip=True)
        for block_m in re.finditer(
            r"Fall of Wickets\s+(.*?)(?=(?:Powerplay|Partnerships|Fall of Wickets|$))",
            page_text, re.IGNORECASE | re.DOTALL,
        ):
            entries = _parse_fow_row(block_m.group(1))
            if entries:
                all_innings.append(entries)

    return all_innings


def _parse_powerplay_from_soup(soup: BeautifulSoup) -> List[List[PowerplayEntry]]:
    """Parse Powerplay sections from HTML, one list per innings."""
    all_innings: List[List[PowerplayEntry]] = []

    for section in soup.find_all(
        "div",
        string=re.compile(r"powerplay", re.IGNORECASE),
    ):
        sib = section.find_next_sibling("div")
        raw = _t(sib) if sib else _t(section.parent)
        entries = _parse_powerplay_row(raw)
        if entries:
            all_innings.append(entries)

    # Text fallback
    if not all_innings:
        page_text = soup.get_text(" ", strip=True)
        for block_m in re.finditer(
            r"Powerplay[s]?\s+(.*?)(?=(?:Fall of Wickets|Partnerships|Powerplay|$))",
            page_text, re.IGNORECASE | re.DOTALL,
        ):
            entries = _parse_powerplay_row(block_m.group(1))
            if entries:
                all_innings.append(entries)

    return all_innings


def _parse_scorecard(soup: BeautifulSoup, mid: str) -> ScorecardResponse:
    title_raw = soup.title.get_text(strip=True) if soup.title else NF
    title = _t(re.sub(r"^.*?\|\s*", "", title_raw, flags=re.IGNORECASE)) or NF

    result = NF
    og_desc = _og(soup, "og:description")
    rm = re.search(
        r"((?:won|tied|no result|abandoned|drawn)[^.]{0,80})",
        og_desc, re.IGNORECASE
    )
    if rm:
        result = rm.group(1).strip()

    # Pre-parse partnerships and FoW per innings
    all_partnerships = _parse_partnership_section(soup)
    all_fow          = _parse_fow_from_soup(soup)
    all_powerplays   = _parse_powerplay_from_soup(soup)

    innings_list: List[InningsScorecard] = []
    current_inn: Optional[InningsScorecard] = None
    in_batting = False
    in_bowling = False
    innings_idx = 0

    all_divs = soup.find_all("div", class_=True)

    for div in all_divs:
        classes = " ".join(div.get("class", []))

        # ── Innings header ────────────────────────────────────────────────
        if "cb-ltst-wgt-hdr" in classes and "cb-col-100" in classes:
            raw = _t(div)
            if not re.search(r"\d+/\d+|\d+\s+ov|\bInn\b|innings", raw, re.IGNORECASE):
                if not re.search(r"Innings", raw, re.IGNORECASE):
                    continue
            if current_inn is not None:
                # Attach per-innings data before appending
                _attach_innings_extras(
                    current_inn, innings_idx,
                    all_partnerships, all_fow, all_powerplays,
                )
                innings_list.append(current_inn)
                innings_idx += 1

            team_m = re.match(r"^(.+?)\s+Innings?", raw, re.IGNORECASE)
            score_m = re.search(r"(\d+(?:/\d+)?)\s*\(?([\d.]+)\s*Ov\)?", raw)
            current_inn = InningsScorecard(
                team=team_m.group(1).strip() if team_m else raw[:40],
                score=score_m.group(0) if score_m else NF,
            )
            in_batting = True
            in_bowling = False
            continue

        if current_inn is None:
            continue

        # ── Bowling header row ────────────────────────────────────────────
        if "cb-scrd-hdr-rw" in classes and "cb-scrd-itms" in classes:
            raw = _t(div)
            if re.search(r"\bBowler\b", raw, re.IGNORECASE):
                in_batting = False
                in_bowling = True
            continue

        # ── Batting rows ──────────────────────────────────────────────────
        if in_batting and "cb-scrd-itms" in classes and "cb-col-100" in classes \
                and "cb-scrd-hdr-rw" not in classes:
            raw = _t(div)
            if re.search(r"^Extras", raw, re.IGNORECASE):
                current_inn.extras = raw
                continue
            if re.search(r"^Fall of", raw, re.IGNORECASE):
                # FoW inline text — use text parser as supplement
                continue
            if re.search(r"^Yet to bat", raw, re.IGNORECASE):
                names_raw = re.sub(r"^Yet to bat[:\s]*", "", raw, flags=re.IGNORECASE)
                current_inn.yet_to_bat = [
                    n.strip() for n in re.split(r",\s*", names_raw) if n.strip()
                ]
                continue
            if re.search(r"^\s*Total\s", raw, re.IGNORECASE):
                continue
            entry = _parse_batting_row(div)
            if entry:
                current_inn.batting.append(entry)
            continue

        # ── Bowling rows ──────────────────────────────────────────────────
        if in_bowling and "cb-scrd-itms" in classes and "cb-col-100" in classes \
                and "cb-scrd-hdr-rw" not in classes:
            entry = _parse_bowling_row(div)
            if entry:
                current_inn.bowling.append(entry)
            continue

    if current_inn is not None:
        _attach_innings_extras(
            current_inn, innings_idx,
            all_partnerships, all_fow, all_powerplays,
        )
        innings_list.append(current_inn)

    if not innings_list:
        innings_list = _regex_fallback_scorecard(soup)

    return ScorecardResponse(
        status="success",
        match_id=mid,
        title=title,
        result=result,
        innings=innings_list,
    )


def _attach_innings_extras(
    inn: InningsScorecard,
    idx: int,
    all_partnerships: List[List[PartnershipEntry]],
    all_fow: List[List[FowEntry]],
    all_powerplays: List[List[PowerplayEntry]],
) -> None:
    """Attach partnerships, FoW, and powerplay data to an innings by index."""
    if idx < len(all_partnerships):
        inn.partnerships = all_partnerships[idx]
    if idx < len(all_fow):
        inn.fall_of_wickets = all_fow[idx]
    if idx < len(all_powerplays):
        inn.powerplays = all_powerplays[idx]


def _parse_batting_row(div: Tag) -> Optional[BattingEntry]:
    name_div = div.find("div", class_=re.compile(r"cb-scard-name"))
    if not name_div:
        return None

    name = _t(name_div.find("a") or name_div)
    if not name or name == NF:
        return None

    dis_div = div.find("div", class_=re.compile(r"cb-scard-dis"))
    dismissal = _t(dis_div) if dis_div else NF

    # Collect numeric stat columns: R, B, 4s, 6s, SR
    num_cols = [
        c for c in div.find_all("div", recursive=False)
        if c.get("class") and any(
            cls in " ".join(c.get("class", []))
            for cls in ("cb-col-8", "cb-col-10")
        )
    ]

    def _nth(n: int) -> str:
        if n < len(num_cols):
            v = _t(num_cols[n])
            return v if v not in ("", NF, "-") else NF
        return NF

    return BattingEntry(
        name=name,
        dismissal=dismissal,
        runs=_nth(0),
        balls=_nth(1),
        fours=_nth(2),
        sixes=_nth(3),
        strike_rate=_nth(4),
    )


def _parse_bowling_row(div: Tag) -> Optional[BowlingEntry]:
    name_col = div.find("div", class_=re.compile(r"cb-col-40"))
    if not name_col:
        children = [c for c in div.children if isinstance(c, Tag)]
        name_col = children[0] if children else None
    if not name_col:
        return None

    name = _t(name_col.find("a") or name_col)
    if not name or name == NF:
        return None

    stat_cols = [
        c for c in div.find_all("div", recursive=False)
        if c.get("class") and any(
            cls in " ".join(c.get("class", []))
            for cls in ("cb-col-10", "cb-col-8")
        )
    ]

    def _nth(n: int) -> str:
        if n < len(stat_cols):
            v = _t(stat_cols[n])
            return v if v not in ("", NF, "-") else NF
        return NF

    # Layout: O M R W NB WD ECO  →  indices 0-6
    return BowlingEntry(
        name=name,
        overs=_nth(0),
        maidens=_nth(1),
        runs=_nth(2),
        wickets=_nth(3),
        no_balls=_nth(4),
        wides=_nth(5),
        economy=_nth(6),
    )


def _regex_fallback_scorecard(soup: BeautifulSoup) -> List[InningsScorecard]:
    text = " ".join(soup.get_text(" ", strip=True).split())
    innings: List[InningsScorecard] = []
    for m in re.finditer(
        r"([A-Za-z ]{4,40}?)\s+Innings?\s*[-–]\s*([\d/()Ov. ]+)",
        text, re.IGNORECASE
    ):
        inn = InningsScorecard(team=m.group(1).strip(), score=m.group(2).strip())
        innings.append(inn)
    return innings


# ─────────────────────────────────────────────────────────────────────────────
# Scraper 3b — live-cricket-over-by-over  →  /match/{id}/overs
# ─────────────────────────────────────────────────────────────────────────────
#
# Cricbuzz over-by-over page HTML structure (simplified):
#
#   <div class="cb-col cb-col-100 cb-ovr-num">
#     <a>Ov 2</a>          ← over header; "19-0" = runs-wickets in over
#     <span>19-0</span>
#   </div>
#   <div class="cb-col cb-col-100 cb-mid-strip">
#     Kagiso Rabada to Vaibhav Sooryavanshi
#   </div>
#   <div class="cb-col cb-col-100 cb-ovr-card">
#       <div class="cb-col cb-col-8 cb-ball-txt cb-dot">•</div>
#       <div class="cb-col cb-col-8 cb-ball-txt cb-six">6</div>
#       <div class="cb-col cb-col-8 cb-ball-txt cb-wide">Wd</div>
#       <div class="cb-col cb-col-8 cb-ball-txt cb-wicket">W</div>
#       ...
#   </div>
#
# CSS class suffixes that identify ball type:
#   cb-dot      → dot ball
#   cb-four     → boundary 4
#   cb-six      → boundary 6
#   cb-wide     → wide
#   cb-nb / cb-noball → no ball
#   cb-wicket   → wicket
#   cb-bye      → byes
#   cb-legbye   → leg byes
#   cb-penalty  → penalty
#   (plain cb-ball-txt with a digit) → normal run
#
# The current/live over is the FIRST block on the page (Cricbuzz puts newest first).
# Completed overs follow in reverse chronological order.
# We reverse the list before returning so over 1 is index 0.


# Tokens Cricbuzz renders for special deliveries (case-insensitive match)
_WIDE_LABELS    = {"wd", "wide"}
_NB_LABELS      = {"nb", "noball", "no ball", "no-ball"}
_WICKET_LABELS  = {"w", "wkt", "wicket", "out"}
_BYE_LABELS     = {"b", "bye", "byes"}
_LEGBYE_LABELS  = {"lb", "legbye", "leg bye", "leg-bye"}
_PENALTY_LABELS = {"p", "pen", "penalty"}


def _classify_ball(label_text: str, css_classes: str) -> Dict[str, Any]:
    """
    Given the display text and CSS classes of a ball element, return a dict
    of boolean flags and the integer runs for that delivery.
    """
    txt   = label_text.strip()
    lower = txt.lower()
    css   = css_classes.lower()

    is_dot     = "cb-dot"    in css or txt == "•" or txt == "0"
    is_wide    = "cb-wide"   in css or lower in _WIDE_LABELS
    is_nb      = "cb-nb"     in css or "cb-noball" in css or lower in _NB_LABELS
    is_wicket  = "cb-wicket" in css or lower in _WICKET_LABELS
    is_four    = "cb-four"   in css or (not is_wide and not is_nb and txt == "4")
    is_six     = "cb-six"    in css or (not is_wide and not is_nb and txt == "6")
    is_byes    = "cb-bye"    in css or lower in _BYE_LABELS
    is_legbye  = "cb-legbye" in css or lower in _LEGBYE_LABELS

    # Extract numeric runs from label (e.g. "Wd" → 0, "1" → 1, "6" → 6, "W" → 0)
    run_match = re.search(r"\d+", txt)
    runs = int(run_match.group()) if run_match else 0
    if is_dot or is_wicket:
        runs = 0

    extras = 0
    if is_wide or is_nb:
        # A wide/noball costs 1 penalty + whatever runs are scored off it
        extras = 1 + runs

    return {
        "is_dot":     is_dot,
        "is_wide":    is_wide,
        "is_no_ball": is_nb,
        "is_wicket":  is_wicket,
        "is_four":    is_four,
        "is_six":     is_six,
        "is_byes":    is_byes,
        "is_leg_byes":is_legbye,
        "runs":       runs,
        "extras":     extras,
    }


def _parse_over_header(header_div: Tag) -> tuple:
    """
    Parse an over header div into (over_number, runs_in_over, wickets_in_over).
    Header text examples:
      "Ov 2 19-0"   → (2, 19, 0)
      "Ov 11 1-0"   → (11, 1, 0)   ← current/incomplete over
      "Ov 20 38-1"  → (20, 38, 1)
    """
    raw = header_div.get_text(" ", strip=True)
    # Extract over number
    ov_m = re.search(r"Ov\s+(\d+)", raw, re.IGNORECASE)
    ov_num = int(ov_m.group(1)) if ov_m else 0
    # Extract "runs-wickets" summary
    rw_m = re.search(r"(\d+)-(\d+)", raw)
    runs_ov  = int(rw_m.group(1)) if rw_m else 0
    wkts_ov  = int(rw_m.group(2)) if rw_m else 0
    return ov_num, runs_ov, wkts_ov


def _parse_over_card(
    card_div: Tag,
    over_number: int,
    innings_number: int,
    bowler: str,
    batsmen: List[str],
    runs_in_over: int,
    wickets_in_over: int,
    is_current: bool,
) -> OverDetail:
    """
    Parse all ball elements inside an over card div.
    Returns a fully populated OverDetail.
    """
    balls: List[BallEvent] = []
    ball_num = 0

    # Ball elements: any div/span with class containing "cb-ball-txt"
    ball_els = card_div.find_all(
        True,
        class_=re.compile(r"cb-ball-txt|cb-ovr-ball", re.IGNORECASE),
    )

    for el in ball_els:
        txt     = el.get_text(strip=True)
        classes = " ".join(el.get("class", []))

        if not txt:
            continue

        # Skip column-header-like elements that aren't real deliveries
        # (e.g. a stray "Over" label)
        if re.match(r"^(over|ov|bowler|batsman)$", txt, re.IGNORECASE):
            continue

        flags = _classify_ball(txt, classes)
        ball_num += 1

        # Friendly label: replace "0" with "•" for dots if not already
        display_label = txt if txt != "0" else "•"
        if display_label == "" :
            display_label = "•"

        # Try to get per-ball commentary from a sibling/parent title attr or
        # adjacent commentary div
        commentary = NF
        parent = el.parent
        if parent:
            comm_el = parent.find(
                True,
                class_=re.compile(r"cb-com-ln|cb-comm|cb-ball-comm", re.IGNORECASE),
            )
            if comm_el:
                commentary = _t(comm_el)

        balls.append(BallEvent(
            ball_number=ball_num,
            ball_label=display_label,
            commentary=commentary,
            **flags,
        ))

    # Build a human-readable over summary string
    summary = " ".join(b.ball_label for b in balls)

    return OverDetail(
        over_number=over_number,
        innings_number=innings_number,
        bowler=bowler,
        batsmen=batsmen,
        runs_in_over=runs_in_over,
        wickets_in_over=wickets_in_over,
        balls=balls,
        over_summary=summary or NF,
        is_current=is_current,
    )


def _parse_overs(soup: BeautifulSoup, mid: str) -> OversResponse:
    """
    Parse the full over-by-over page.

    Cricbuzz renders each over as a cluster of 3-4 divs:
      1. .cb-col-100.cb-ovr-num   — "Ov N  runs-wkts"  (over header)
      2. .cb-col-100.cb-mid-strip — "Bowler to Batsman(s)" (over description)
      3. .cb-col-100.cb-ovr-card  — ball-by-ball delivery divs
      4. (optional) .cb-col-100.cb-col-rt — per-over commentary lines

    The page is ordered NEWEST FIRST so index 0 = current/latest over.
    We build overs in page order (current first) then reverse so over 1 is first.
    """
    title_raw = soup.title.get_text(strip=True) if soup.title else NF
    title = _t(re.sub(r"^.*?\|\s*", "", title_raw, flags=re.IGNORECASE)) or NF

    # Collect all top-level container divs
    all_divs = soup.find_all("div", class_=True)

    overs: List[OverDetail] = []
    innings_number = 1
    seen_innings_break = False

    i = 0
    while i < len(all_divs):
        div = all_divs[i]
        classes = " ".join(div.get("class", []))

        # ── Detect innings separator ─────────────────────────────────────────
        # Cricbuzz inserts an innings-break banner; treat it as a boundary.
        if re.search(r"cb-inn-hdr|cb-inn-break|innings.+header", classes, re.IGNORECASE):
            if overs:          # only bump if we already have overs for inn 1
                innings_number += 1
            i += 1
            continue

        # ── Over header ──────────────────────────────────────────────────────
        if re.search(r"cb-ovr-num", classes, re.IGNORECASE):
            ov_num, runs_ov, wkts_ov = _parse_over_header(div)
            if ov_num == 0:
                i += 1
                continue

            # The very first over block on the page is always the live/current one
            is_current = (len(overs) == 0)

            # ── Bowler / batsmen line (immediately follows the header) ───────
            bowler   = NF
            batsmen: List[str] = []
            desc_div = None
            if i + 1 < len(all_divs):
                next_div = all_divs[i + 1]
                next_cls = " ".join(next_div.get("class", []))
                if re.search(r"cb-mid-strip|cb-ovr-bwl|cb-over-desc", next_cls, re.IGNORECASE):
                    desc_div = next_div
                    i += 1  # consume it

            if desc_div is not None:
                desc_text = _t(desc_div)
                # Format: "Bowler to Bat1 & Bat2"  or  "Bowler to Bat1"
                to_m = re.split(r"\s+to\s+", desc_text, maxsplit=1, flags=re.IGNORECASE)
                if len(to_m) == 2:
                    bowler = to_m[0].strip()
                    bats_raw = to_m[1]
                    batsmen = [b.strip() for b in re.split(r"\s*&\s*", bats_raw) if b.strip()]
                else:
                    bowler = desc_text

            # ── Over card (ball-by-ball) ─────────────────────────────────────
            card_div = None
            if i + 1 < len(all_divs):
                next_div = all_divs[i + 1]
                next_cls = " ".join(next_div.get("class", []))
                if re.search(r"cb-ovr-card|cb-ball-card|cb-over-card", next_cls, re.IGNORECASE):
                    card_div = next_div
                    i += 1

            if card_div is not None:
                over_detail = _parse_over_card(
                    card_div,
                    over_number=ov_num,
                    innings_number=innings_number,
                    bowler=bowler,
                    batsmen=batsmen,
                    runs_in_over=runs_ov,
                    wickets_in_over=wkts_ov,
                    is_current=is_current,
                )
            else:
                # No card found — build a minimal OverDetail from header only
                over_detail = OverDetail(
                    over_number=ov_num,
                    innings_number=innings_number,
                    bowler=bowler,
                    batsmen=batsmen,
                    runs_in_over=runs_ov,
                    wickets_in_over=wkts_ov,
                    is_current=is_current,
                )

            overs.append(over_detail)

        i += 1

    # Cricbuzz page is newest-first; reverse so over 1 comes first
    overs.reverse()

    # After reversing, the last entry in the list is the current over (if live)
    current_ov_num: Optional[int] = None
    for ov in reversed(overs):
        if ov.is_current:
            current_ov_num = ov.over_number
            break

    return OversResponse(
        status="success",
        match_id=mid,
        title=title,
        total_overs=len(overs),
        current_over=current_ov_num,
        overs=overs,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Scraper 4 — cricket-match-squads  →  /match/{id}/squads
# ─────────────────────────────────────────────────────────────────────────────

def _parse_squads(soup: BeautifulSoup, mid: str) -> SquadsResponse:
    title_raw = soup.title.get_text(strip=True) if soup.title else NF
    title = _t(re.sub(r"^.*?\|\s*", "", title_raw, flags=re.IGNORECASE)) or NF

    squads: List[TeamSquad] = []

    team_headers = soup.find_all(
        ["h2", "h3"],
        class_=re.compile(r"cb-font-20|cb-hdr-lgn|cb-teams-hdr")
    )

    if not team_headers:
        team_headers = [
            h for h in soup.find_all(["h2", "h3"])
            if 3 < len(_t(h)) < 60
            and not re.search(r"squad|playing|bench|reserves|squad|xi",
                              _t(h), re.IGNORECASE)
        ]

    for header in team_headers[:2]:
        team_name = _t(header)
        if not team_name or team_name == NF:
            continue

        squad = TeamSquad(team=team_name)
        in_bench = False

        for sib in header.find_next_siblings():
            sib_tag = sib.name if hasattr(sib, "name") else ""
            if sib_tag in ("h2", "h3"):
                break
            sib_text = _t(sib)
            if re.search(r"bench|travelling|reserves|squad", sib_text, re.IGNORECASE):
                in_bench = True
                continue
            if re.search(r"playing\s*xi|playing\s+eleven", sib_text, re.IGNORECASE):
                in_bench = False
                continue
            for player_el in sib.find_all(
                ["a", "li"],
                class_=re.compile(r"cb-player|cb-scard-name|cb-plyr", re.IGNORECASE)
            ):
                player = _parse_player(player_el)
                if player:
                    if in_bench:
                        squad.bench.append(player)
                    else:
                        squad.playing_xi.append(player)

        squads.append(squad)

    if not squads or all(
        len(s.playing_xi) == 0 and len(s.bench) == 0 for s in squads
    ):
        squads = _fallback_squad_parse(soup)

    return SquadsResponse(
        status="success",
        match_id=mid,
        title=title,
        squads=squads,
    )


def _parse_player(el: Tag) -> Optional[PlayerEntry]:
    raw = _t(el)
    if not raw or raw == NF or len(raw) < 3:
        return None
    if re.search(r"^(home|cricket|scores|news|schedule|squad|playing)$",
                 raw, re.IGNORECASE):
        return None

    is_captain = bool(re.search(r"\(c\)", raw, re.IGNORECASE))
    is_keeper  = bool(re.search(r"\(wk\)", raw, re.IGNORECASE))
    name = re.sub(r"\s*\(c\s*(?:&\s*wk)?\)|\s*\(wk\)", "", raw,
                  flags=re.IGNORECASE).strip()

    if not name or len(name) < 3:
        return None

    role = NF
    classes = " ".join(el.get("class", []))
    if re.search(r"bat", classes, re.IGNORECASE):
        role = "Batsman"
    elif re.search(r"bowl", classes, re.IGNORECASE):
        role = "Bowler"
    elif re.search(r"all", classes, re.IGNORECASE):
        role = "All-rounder"
    elif is_keeper:
        role = "Wicket-keeper"

    return PlayerEntry(name=name, role=role,
                       is_captain=is_captain, is_keeper=is_keeper)


def _fallback_squad_parse(soup: BeautifulSoup) -> List[TeamSquad]:
    players_all = []
    seen = set()
    for a in soup.find_all("a", href=re.compile(r"/cricket-players/")):
        p = _parse_player(a)
        if p and p.name not in seen:
            seen.add(p.name)
            players_all.append(p)

    if not players_all:
        return []

    mid_idx = len(players_all) // 2
    return [
        TeamSquad(team="Team A", playing_xi=players_all[:mid_idx]),
        TeamSquad(team="Team B", playing_xi=players_all[mid_idx:]),
    ]


# ─────────────────────────────────────────────────────────────────────────────
# Scraper 5 — match list  →  /matches/{status}
# ─────────────────────────────────────────────────────────────────────────────

def _parse_match_list(soup: BeautifulSoup, status: str) -> List[MatchCard]:
    cards: List[MatchCard] = []
    seen: set = set()

    for block in soup.find_all("div", class_=lambda c: c and "cb-lv-main" in c):
        series_el = block.find(
            ["h2", "h3"], class_=lambda c: c and "cb-lv-scr-mtch-hdr" in c
        )
        series = _t(series_el) if series_el else NF

        for card in block.find_all(
            "div",
            class_=lambda c: c and "cb-lv-scrs-col" in c
                             and "cb-scr-wll-chvrn" not in (c or ""),
        ):
            link_el = card.find("a", href=re.compile(r"/live-cricket-scores/\d+"))
            href = link_el.get("href", "") if link_el else ""
            mid = _mid(href)
            if mid == NF or mid in seen:
                continue
            seen.add(mid)

            title_el = card.find(class_=lambda c: c and "cb-lv-scr-mtch-hdr" in c
                                                  and "inline-block" in c)
            if not title_el:
                title_el = card.find(["h3", "h4"])
            title = _t(title_el) if title_el else NF

            teams: List[Dict[str, str]] = []
            score_wrap = card.find("div", class_=re.compile(r"cb-scr-wll-chvrn"))
            if score_wrap:
                for sd in score_wrap.find_all(
                    "div", class_=re.compile(r"cb-lv-scrs"), recursive=False
                ):
                    txt = _t(sd)
                    if txt and txt != NF:
                        teams.append({"score": txt})
            if not teams:
                for sd in card.find_all("div", class_=re.compile(r"cb-lv-scrs")):
                    txt = _t(sd)
                    if txt and txt != NF and not any(t["score"] == txt for t in teams):
                        teams.append({"score": txt})

            tm_el = card.find("div", class_=re.compile(r"cb-lv-scr-mtch-tm"))
            tv = _t(tm_el) if tm_el else NF
            date_str = time_str = venue_str = NF
            if tv != NF:
                for p in re.split(r"[•·|]", tv):
                    p = p.strip()
                    if re.search(r"\d{1,2}:\d{2}", p):
                        time_str = p
                    elif re.search(r"\btoday\b|\btomorrow\b|\b\d{1,2}\s+\w+\b",
                                   p, re.IGNORECASE):
                        date_str = p
                    elif re.search(r"\bat\b|stadium|ground|oval|park", p,
                                   re.IGNORECASE):
                        venue_str = re.sub(r"^\s*at\s+", "", p,
                                          flags=re.IGNORECASE).strip()

            overview_el = card.find(
                "div",
                class_=re.compile(
                    r"cb-text-(complete|live|inprogress|preview|abandon)"
                ),
            )
            overview = _t(overview_el) if overview_el else NF

            cards.append(MatchCard(
                match_id=mid, series=series, title=title, teams=teams,
                venue=venue_str, date=date_str, time=time_str,
                match_type=_match_type(title + " " + series),
                status=status, overview=overview,
            ))

    if not cards:
        for a in soup.find_all("a", href=re.compile(r"/live-cricket-scores/\d+")):
            mid = _mid(a.get("href", ""))
            if mid == NF or mid in seen:
                continue
            seen.add(mid)
            cards.append(MatchCard(match_id=mid, title=_t(a), status=status))

    return cards


# ─────────────────────────────────────────────────────────────────────────────
# Tree-view formatter  (for ?text=true)
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
    return (
        "🏏 Live Score\n│\n"
        f"├── Match       : {d.title}\n"
        f"├── Type        : {d.match_type}\n"
        f"├── Venue       : {d.venue}\n"
        f"├── Score       : {d.score}\n"
        f"├── Status      : {d.match_status}\n"
        f"├── CRR/RRR/Tgt : {d.current_run_rate} / {d.required_run_rate} / {d.target}\n"
        f"├── Toss        : {d.toss}\n"
        f"├── Bowler      : {bowl_line}\n"
        f"├── Partnership : {d.partnership}\n"
        f"├── Last Wicket : {d.last_wicket}\n"
        "├── Batsmen\n"
        f"{bats}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Routes
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


@app.get("/docs", include_in_schema=False)
async def swagger():
    try:
        page = get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title="Cricket Score API v3.2",
            swagger_favicon_url="https://fastapi.tiangolo.com/img/favicon.png",
        )
        content = page.body.decode("utf-8").replace(
            "</head>",
            """<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
html,body{margin:0;padding:0;width:100%;overflow-x:hidden}
.swagger-ui .wrapper{max-width:100%!important;padding:10px!important;box-sizing:border-box}
.swagger-ui .opblock-summary-path{white-space:normal!important;word-break:break-word!important}
.swagger-ui pre,.swagger-ui code{white-space:pre-wrap!important;word-break:break-word!important;
  max-height:220px!important;overflow-y:auto!important;font-size:12px!important}
</style></head>""",
        )
        r = HTMLResponse(content=content)
        r.headers["Cache-Control"] = "no-store"
        return r
    except Exception:
        return HTMLResponse("<h2>Docs unavailable</h2>", status_code=500)


# ── Root / backward-compat  ──────────────────────────────────────────────────

@app.get("/", summary="Live score (backward-compat)")
async def root(
    score: Optional[str] = Query(None, description="Match ID"),
    text: bool = Query(False, description="ASCII tree output"),
):
    """Backward-compatible endpoint. Use /match/{id}/score instead."""
    if score is None:
        return {
            "status": "success",
            "message": "Cricket Score API v3.1",
            "docs": "/docs",
            "endpoints": [
                "/match/{id}/info",
                "/match/{id}/score",
                "/match/{id}/scorecard",
                "/match/{id}/squads",
                "/match/{id}/partnerships",
                "/match/{id}/fow",
                "/match/{id}/powerplay",
                "/match/{id}/overs",
                "/match/{id}/overs/current",
                "/match/{id}/overs/{over_number}",
                "/matches/live",
                "/matches/recent",
                "/matches/upcoming",
                "/schedule",
            ],
        }
    if not _validate(score):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scores/{score}?_={time.time_ns()}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_live_score(_soup(r.text), score)
    return PlainTextResponse(_tree(data)) if text else data


# ── Match sub-routes ─────────────────────────────────────────────────────────

@app.get(
    "/match/{match_id}/info",
    response_model=MatchInfo,
    summary="Match info (venue, toss, umpires, result)",
)
async def match_info(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/cricket-match-facts/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    return _parse_info(_soup(r.text), match_id)


@app.get(
    "/match/{match_id}/score",
    response_model=LiveScoreResponse,
    summary="Live score (current batsmen, bowler, innings)",
)
async def match_score(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
    text: bool = Query(False, description="ASCII tree output"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scores/{match_id}?_={time.time_ns()}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_live_score(_soup(r.text), match_id)
    return PlainTextResponse(_tree(data)) if text else data


@app.get(
    "/match/{match_id}/scorecard",
    response_model=ScorecardResponse,
    summary="Full scorecard (batting, bowling, FoW, partnerships, powerplays)",
)
async def match_scorecard(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scorecard/{match_id}/")
    if r is None:
        raise APIError(503, "upstream unavailable")
    return _parse_scorecard(_soup(r.text), match_id)


@app.get(
    "/match/{match_id}/squads",
    response_model=SquadsResponse,
    summary="Playing XI & bench squads for both teams",
)
async def match_squads(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/cricket-match-squads/{match_id}/")
    if r is None:
        raise APIError(503, "upstream unavailable")
    return _parse_squads(_soup(r.text), match_id)


@app.get(
    "/match/{match_id}/partnerships",
    response_model=PartnershipsResponse,
    summary="Batting partnerships per innings",
)
async def match_partnerships(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scorecard/{match_id}/")
    if r is None:
        raise APIError(503, "upstream unavailable")

    sc = _parse_scorecard(_soup(r.text), match_id)
    innings_parts = [
        InningsPartnerships(
            team=inn.team,
            innings_number=i + 1,
            partnerships=inn.partnerships,
        )
        for i, inn in enumerate(sc.innings)
    ]
    return PartnershipsResponse(
        status="success",
        match_id=match_id,
        title=sc.title,
        innings=innings_parts,
    )


@app.get(
    "/match/{match_id}/fow",
    response_model=FowResponse,
    summary="Fall of wickets per innings",
)
async def match_fow(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scorecard/{match_id}/")
    if r is None:
        raise APIError(503, "upstream unavailable")

    sc = _parse_scorecard(_soup(r.text), match_id)
    innings_fow = [
        InningsFow(
            team=inn.team,
            innings_number=i + 1,
            fall_of_wickets=inn.fall_of_wickets,
        )
        for i, inn in enumerate(sc.innings)
    ]
    return FowResponse(
        status="success",
        match_id=match_id,
        title=sc.title,
        innings=innings_fow,
    )


@app.get(
    "/match/{match_id}/powerplay",
    response_model=PowerplayResponse,
    summary="Powerplay summary per innings",
)
async def match_powerplay(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-scorecard/{match_id}/")
    if r is None:
        raise APIError(503, "upstream unavailable")

    sc = _parse_scorecard(_soup(r.text), match_id)
    innings_pp = [
        InningsPowerplay(
            team=inn.team,
            innings_number=i + 1,
            powerplays=inn.powerplays,
        )
        for i, inn in enumerate(sc.innings)
    ]
    return PowerplayResponse(
        status="success",
        match_id=match_id,
        title=sc.title,
        innings=innings_pp,
    )


@app.get(
    "/match/{match_id}/overs",
    response_model=OversResponse,
    summary="All overs ball-by-ball (newest first reversed to over 1 first)",
)
async def match_overs(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
    innings: Optional[int] = Query(None, description="Filter by innings number (1 or 2)"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-over-by-over/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_overs(_soup(r.text), match_id)
    if innings is not None:
        data.overs = [o for o in data.overs if o.innings_number == innings]
        data.total_overs = len(data.overs)
    return data


@app.get(
    "/match/{match_id}/overs/current",
    response_model=OverDetail,
    summary="Current (live/incomplete) over with ball-by-ball detail",
)
async def match_current_over(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-over-by-over/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_overs(_soup(r.text), match_id)
    # Current over is the last item after reversing (latest in the match)
    for ov in reversed(data.overs):
        if ov.is_current:
            return ov
    # Fallback: return the last over parsed
    if data.overs:
        return data.overs[-1]
    raise APIError(404, "no over data found")


@app.get(
    "/match/{match_id}/overs/{over_number}",
    response_model=OverDetail,
    summary="Specific over by number (1-based)",
)
async def match_over_by_number(
    match_id: str = Path(..., description="Cricbuzz numeric match ID"),
    over_number: int = Path(..., description="Over number (1-based)", ge=1, le=100),
    innings: int = Query(1, description="Innings number (1 or 2)"),
):
    if not _validate(match_id):
        return _err422()
    r = await _fetch(f"{CB}/live-cricket-over-by-over/{match_id}")
    if r is None:
        raise APIError(503, "upstream unavailable")
    data = _parse_overs(_soup(r.text), match_id)
    for ov in data.overs:
        if ov.over_number == over_number and ov.innings_number == innings:
            return ov
    raise APIError(404, f"over {over_number} (innings {innings}) not found")


# ── Match lists ──────────────────────────────────────────────────────────────

_STATUS_MAP = {
    "live":     "",
    "recent":   "/recent-matches",
    "upcoming": "/upcoming-matches",
}
_TYPE_SUFFIX = {
    "international": "",
    "league":        "/league",
    "domestic":      "/domestic",
    "women":         "/women",
}


@app.get(
    "/matches/{match_status}",
    response_model=MatchListResponse,
    summary="List matches by status (live / recent / upcoming)",
)
async def matches(
    match_status: str = Path(..., description="live | recent | upcoming"),
    type: str = Query("international",
                      description="international | league | domestic | women"),
):
    if match_status not in _STATUS_MAP:
        return JSONResponse(
            status_code=422,
            content={"status": "error",
                     "message": "status must be live, recent, or upcoming"},
        )
    if type not in _TYPE_SUFFIX:
        return JSONResponse(
            status_code=422,
            content={"status": "error",
                     "message": "type must be international, league, domestic, or women"},
        )

    if match_status == "live":
        type_path = {
            "international": "",
            "league": "/league-cricket",
            "domestic": "/domestic-cricket",
            "women": "/women-cricket",
        }[type]
        url = f"{CB}/cricket-match/live-scores{type_path}"
    else:
        base = _STATUS_MAP[match_status]
        url = f"{CB}/cricket-match/live-scores{base}{_TYPE_SUFFIX[type]}"

    r = await _fetch(url)
    if r is None:
        raise APIError(503, "upstream unavailable")

    cards = _parse_match_list(_soup(r.text), match_status)
    return MatchListResponse(
        status="success",
        type=f"{match_status}/{type}",
        total=len(cards),
        matches=cards,
    )


@app.get("/schedule", response_model=MatchListResponse,
         summary="Upcoming matches (alias)")
async def schedule(
    type: str = Query("international",
                      description="international | league | domestic | women"),
):
    return await matches("upcoming", type=type)


# ── Error handlers ───────────────────────────────────────────────────────────

@app.exception_handler(APIError)
async def _api_err(request: Request, exc: APIError):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "code": exc.status_code,
                 "message": exc.message},
    )


@app.exception_handler(StarletteHTTPException)
async def _http_err(request: Request, exc: StarletteHTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={"status": "error", "code": exc.status_code,
                 "message": "invalid route"},
    )


@app.exception_handler(Exception)
async def _generic_err(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={"status": "error", "code": 500,
                 "message": "internal server error"},
    )
