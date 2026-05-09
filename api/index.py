"""
Cricket Score API v3  –  FastAPI + BeautifulSoup
Vercel serverless entry-point: api/index.py

4 Cricbuzz page scrapers:
  /match/{id}/info       ← cricket-match-facts/{id}
  /match/{id}/score      ← live-cricket-scores/{id}
  /match/{id}/scorecard  ← live-cricket-scorecard/{id}
  /match/{id}/squads     ← cricket-match-squads/{id}
  /matches/{status}      ← live / recent / upcoming lists
  /schedule              ← alias for upcoming
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
    economy: str = NF


class InningsScorecard(BaseModel):
    team: str = NF
    score: str = NF
    overs: str = NF
    batting: List[BattingEntry] = []
    bowling: List[BowlingEntry] = []
    extras: str = NF
    fall_of_wickets: str = NF


class ScorecardResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    result: str = NF
    innings: List[InningsScorecard] = []


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
    version="3.0.0",
    description=(
        "Full-featured cricket API: live scores, full scorecards, "
        "match info & squads — powered by Cricbuzz scraping."
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
    # og:title format: "IPL | GT 228/4 (19.4) (Washington Sundar 36(18)* Rahul Tewatia 14(4))"
    # Scan the FULL og_title for all "Name Runs(Balls)*?" patterns in one pass.
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

    # Fallback: parse from HTML (anchors before the Bowler boundary)
    if len(batsmen) < 2:
        batsmen = _extract_batsmen_from_html(soup, batsmen, seen_batsmen)

    page_text = " ".join(soup.get_text(" ", strip=True).split())

    # ── Current bowler ────────────────────────────────────────────────────────
    bowler = _extract_current_bowler(soup, page_text)

    # ── Venue — from og:description only (clean, no page noise) ──────────────
    venue = NF
    vm = re.search(
        r"at\s+((?:[A-Z][a-zA-Z]+[\s,]*){1,6}(?:Stadium|Ground|Oval|Park|Arena|Maidan|Maidaan))",
        og_desc,
    )
    if vm:
        venue = vm.group(1).strip().rstrip(",")
    if venue == NF:
        # Try "Venue: Sawai Mansingh Stadium" pattern in page structured info
        v2 = re.search(
            r"(?:venue|ground)[:\s]+([A-Za-z ,]{5,60}(?:Stadium|Ground|Oval|Park|Arena|Maidan))",
            page_text, re.IGNORECASE,
        )
        if v2:
            venue = v2.group(1).strip()

    # ── Match type — detect IPL/T20 from title or series ─────────────────────
    match_type = _match_type(og_title + " " + title)
    if match_type == NF:
        # IPL is a T20 league
        if re.search(r"\bIPL\b|Indian Premier League", og_title + " " + title, re.IGNORECASE):
            match_type = "T20"

    # ── Toss — from "Toss: Team (Batting/Bowling)" label in page ─────────────
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

    # ── Match status — only from structured live-score lines, not commentary ──
    match_status = NF
    # Priority 1: "needs X runs in Y balls/overs"
    for pat in (
        r"((?:needs?|require)\s+\d+\s+(?:run|more run)[^.\n]{0,60})",
        r"(\bRR\b[^.\n]{3,60}(?:need|require|target)[^.\n]{0,40})",
        r"((?:innings break|lunch|tea|stumps|drinks)[^.\n]{0,40})",
        r"((?:won\s+by)[^.\n]{5,60})",
    ):
        sm = re.search(pat, page_text, re.IGNORECASE)
        if sm:
            candidate = sm.group(1).strip()
            # Reject if it looks like navigation/ads (contains ALL CAPS words or URLs)
            if not re.search(r"\b[A-Z]{3,}\b.*\b[A-Z]{3,}\b", candidate):
                match_status = candidate
                break

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
    Find the HTML element that acts as the "Bowler" section header on the
    Cricbuzz live scores page.  Returns (batter_anchors, bowler_anchors) —
    two separate lists of <a href="/profiles/..."> elements so they can never
    be confused with each other.

    Cricbuzz live page structure (flat divs, no dedicated row class):
      ... <a href="/profiles/10945/washington-sundar">Washington Sundar</a> ...
      ... [stats for batsman] ...
      <div ...>Bowler</div>  ← or a span/td containing exactly "Bowler"
      ... <a href="/profiles/1467167/brijesh-sharma">Brijesh Sharma</a> ...
      ... [stats for bowler] ...

    We find the first element whose *text* is exactly "Bowler" (case-insensitive),
    then split all profile anchors into before/after that boundary.
    """
    all_profile_anchors = soup.find_all("a", href=re.compile(r"/profiles/\d+"))

    # Find the boundary element:
    #   Primary:  small element whose text starts with "Bowler" AND contains "ECO"
    #             e.g. the header row "Bowler O M R W ECO" (len < 60)
    #   Fallback: element whose stripped text is exactly "Bowler"
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

    # Use document position to split anchors into batter / bowler sides
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
    Given a profile anchor, walk up the DOM to find a row that contains
    at least 5 numeric tokens after the player name.
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
        # Strip the name out so its digits don't pollute stats
        stats_text = re.sub(re.escape(name), "", row_text, count=1,
                            flags=re.IGNORECASE).lstrip("* ").strip()
        nums = re.findall(r"\d+(?:\.\d+)?", stats_text)
        if len(nums) >= 5:
            ovs  = nums[0]
            mdn  = nums[1]
            runs = nums[2]
            wkts = nums[3]
            eco  = nums[6] if len(nums) >= 7 else nums[4]
            # Sanity: overs must be ≤ 25, economy ≤ 36
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
    Parse current batsmen from the live scores page HTML.
    Uses _find_scorecard_boundary to only look at anchors BEFORE the Bowler header.
    """
    batter_anchors, _ = _find_scorecard_boundary(soup)
    batsmen = list(existing)

    for anchor in batter_anchors:
        name = _t(anchor).strip().rstrip("* ").strip()
        if not name or name in seen or name == NF or len(name) < 3:
            continue

        # Walk up to get the row and extract R / B
        row_el = anchor.parent
        runs = balls = NF
        is_striker = False
        for _ in range(4):
            if row_el is None:
                break
            row_text = _t(row_el)
            # Striker is marked with * next to name
            if "*" in row_text:
                is_striker = True
            stats_text = re.sub(re.escape(name), "", row_text, count=1,
                                flags=re.IGNORECASE).lstrip("* ").strip()
            nums = re.findall(r"\d+(?:\.\d+)?", stats_text)
            if len(nums) >= 2:
                runs  = nums[0]
                balls = nums[1]
                break
            row_el = row_el.parent

        seen.add(name)
        batsmen.append(ScorecardBatsman(
            name=name, runs=runs, balls=balls, is_striker=is_striker,
        ))
        if len(batsmen) >= 2:
            break

    return batsmen


def _extract_current_bowler(soup: BeautifulSoup, page_text: str) -> ScorecardBowler:
    """
    Extract the current bowler (marked * on the live page) from the Bowler section.

    Strategy 1 — HTML boundary split (most reliable, never confuses batsmen).
      Uses _find_scorecard_boundary to get only anchors AFTER the Bowler header,
      then reads stats from each anchor's row container.

    Strategy 2 — page-text regex after "Bowler O M R W ECO" header.
      Handles 5-col and 7-col (NB WD) layouts; absorbs the * marker.

    Strategy 3 — name-only fallback.
    """

    # ── Strategy 1: HTML boundary split ──────────────────────────────────────
    _, bowler_anchors = _find_scorecard_boundary(soup)

    for anchor in bowler_anchors:
        result = _anchor_row_stats(anchor)
        if result:
            name, ovs, mdn, runs, wkts, eco = result
            return ScorecardBowler(
                name=name, overs=ovs, maidens=mdn,
                runs=runs, wickets=wkts, economy=eco,
            )

    # ── Strategy 2: regex after "Bowler O M R W [NB WD] ECO" header ─────────
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

    # ── Strategy 3: name-only ─────────────────────────────────────────────────
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

    innings_list: List[InningsScorecard] = []
    current_inn: Optional[InningsScorecard] = None
    in_batting = False
    in_bowling = False

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
                innings_list.append(current_inn)
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
            if re.search(r"^Extras|^Fall of", raw, re.IGNORECASE):
                if raw.lower().startswith("extras"):
                    current_inn.extras = raw
                elif raw.lower().startswith("fall of"):
                    current_inn.fall_of_wickets = raw
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


def _parse_batting_row(div: Tag) -> Optional[BattingEntry]:
    name_div = div.find("div", class_=re.compile(r"cb-scard-name"))
    if not name_div:
        raw = _t(div)
        if re.search(r"did not bat|yet to bat|absent", raw, re.IGNORECASE):
            return None
        return None

    name = _t(name_div.find("a") or name_div)
    if not name or name == NF:
        return None

    dis_div = div.find("div", class_=re.compile(r"cb-scard-dis"))
    dismissal = _t(dis_div) if dis_div else NF

    num_cols = [
        c for c in div.find_all("div", recursive=False)
        if c.get("class") and "cb-col-8" in " ".join(c.get("class", []))
    ]
    if not num_cols:
        num_cols = [
            c for c in div.find_all("div", recursive=False)
            if c.get("class") and "cb-col-10" in " ".join(c.get("class", []))
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

    return BowlingEntry(
        name=name,
        overs=_nth(0),
        maidens=_nth(1),
        runs=_nth(2),
        wickets=_nth(3),
        economy=_nth(6),  # ECO is index 6 (NB=4, WD=5)
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

    mid = len(players_all) // 2
    return [
        TeamSquad(team="Team A", playing_xi=players_all[:mid]),
        TeamSquad(team="Team B", playing_xi=players_all[mid:]),
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
        f"│   {'*' if b.is_striker else ' '} {b.name}  {b.runs}({b.balls})"
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
            title="Cricket Score API v3",
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
        return {"status": "success", "message": "Cricket Score API v3",
                "docs": "/docs",
                "endpoints": [
                    "/match/{id}/info",
                    "/match/{id}/score",
                    "/match/{id}/scorecard",
                    "/match/{id}/squads",
                    "/matches/live",
                    "/matches/recent",
                    "/matches/upcoming",
                    "/schedule",
                ]}
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
    summary="Full scorecard (batting + bowling for all innings)",
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
