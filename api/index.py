"""
Live Cricket Score API  –  FastAPI + BeautifulSoup scraper
Vercel serverless entry-point: api/index.py
"""

import re
import html as html_lib
import time
import asyncio
from typing import List, Optional

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import JSONResponse, PlainTextResponse, HTMLResponse
from pydantic import BaseModel, field_validator
from starlette.exceptions import HTTPException as StarletteHTTPException

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

NF = "not found"
CB = "https://www.cricbuzz.com"

# Cricbuzz blocks server-side IPs without a realistic UA.
# These headers closely mimic a real Chrome desktop request.
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

# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------


class APIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message


class Batsman(BaseModel):
    name: str = NF
    runs: str = NF
    balls: str = NF
    fours: str = NF
    sixes: str = NF
    strike_rate: str = NF
    is_striker: bool = False


class Bowler(BaseModel):
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


class ScoreResponse(BaseModel):
    status: str
    match_id: str = NF
    title: str = NF
    match_type: str = NF
    venue: str = NF
    match_status: str = NF        # e.g. "ENG need 120 runs in 14 overs"
    toss: str = NF
    innings: List[InningsScore] = []
    score: str = NF               # backward-compat flat string
    current_batsmen: List[Batsman] = []
    current_bowler: Bowler = Bowler()
    last_wicket: str = NF
    partnership: str = NF
    current_run_rate: str = NF
    required_run_rate: str = NF
    target: str = NF


class MatchCard(BaseModel):
    match_id: str = NF
    series: str = NF
    title: str = NF
    teams: List[dict] = []         # [{"team": "IND", "score": "250/4 (45)"}]
    venue: str = NF
    date: str = NF
    time: str = NF
    match_type: str = NF
    status: str = NF               # "live" | "upcoming" | "recent"
    overview: str = NF             # result / "needs X runs" / starts-in


class MatchListResponse(BaseModel):
    status: str
    type: str
    total: int = 0
    matches: List[MatchCard] = []


class MatchValidator(BaseModel):
    score: str

    @field_validator("score")
    @classmethod
    def validate_match_id(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("match id cannot be empty")
        if not v.isdigit():
            raise ValueError("match id must be digits only")
        if len(v) < 4:
            raise ValueError("match id must be at least 4 digits")
        if len(v) > 20:
            raise ValueError("match id too long")
        return v


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Cricket Score API",
    version="2.0.0",
    description="Live, recent & upcoming cricket matches — powered by Cricbuzz",
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


# ---------------------------------------------------------------------------
# HTTP helper
# ---------------------------------------------------------------------------

async def _fetch(url: str, retries: int = 2) -> Optional[httpx.Response]:
    """GET with retry + 1 s back-off on 429/503."""
    for attempt in range(retries):
        try:
            async with httpx.AsyncClient(
                timeout=15.0, follow_redirects=True
            ) as client:
                r = await client.get(url, headers=HEADERS)
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


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def _clean(text) -> str:
    if not text:
        return NF
    return html_lib.unescape(" ".join(str(text).split())).strip() or NF


def _soup(text: str) -> BeautifulSoup:
    return BeautifulSoup(text, "lxml")


def _match_id_from_href(href: str) -> str:
    m = re.search(r"/(\d{4,})", href or "")
    return m.group(1) if m else NF


def _parse_match_type(text: str) -> str:
    """Infer Test / ODI / T20 / T10 etc. from any string."""
    t = (text or "").upper()
    for tag in ("TEST", "ODI", "T20I", "T20", "T10", "THE HUNDRED",
                "LIST A", "FIRST-CLASS", "FC", "50-OVER", "20-OVER"):
        if tag in t:
            return tag
    return NF


# ---------------------------------------------------------------------------
# Score parser  (individual match page)
# ---------------------------------------------------------------------------

def _parse_score_page(soup: BeautifulSoup, match_id: str) -> ScoreResponse:

    # ── title ────────────────────────────────────────────────────────────────
    raw_title = soup.title.get_text(strip=True) if soup.title else ""
    title = _clean(re.sub(
        r"^Cricket(?:\s+commentary)?\s*[|\-–]\s*", "", raw_title,
        flags=re.IGNORECASE
    ))

    # ── meta tags  (og:title is the richest single line on the page) ─────────
    og_title = ""
    og_desc = ""
    for meta in soup.find_all("meta"):
        prop = meta.get("property", "") or meta.get("name", "")
        if prop == "og:title":
            og_title = meta.get("content", "")
        elif prop == "og:description":
            og_desc = meta.get("content", "")

    # ── innings scores  ──────────────────────────────────────────────────────
    # og:title pattern: "IND 250/4 (45.2) | (SR Ten 80(120), ...) | Bowler ..."
    innings: List[InningsScore] = []
    for team, runs, wkts, ovs in re.findall(
        r"([A-Z]{2,5})\s+(\d+)/(\d+)\s*\(([\d.]+)\)", og_title
    ):
        innings.append(InningsScore(
            team=team, runs=runs, wickets=wkts, overs=ovs,
            display=f"{team} {runs}/{wkts} ({ovs})"
        ))

    score_str = "  |  ".join(i.display for i in innings) if innings else NF

    # ── batsmen  ─────────────────────────────────────────────────────────────
    # og:title: "IND 320/6 (50) | (Buttler 89(65)*, Stokes 42(38)) | Bumrah 2/38"
    batsmen: List[Batsman] = []
    for _seg in og_title.split(" | "):
        _seg = _seg.strip().strip("()")
        _found = re.findall(
            r"([A-Za-z][A-Za-z .'\-]{1,25}?)\s+(\d+)\*?\((\d+)\)(\*?)",
            _seg,
        )
        for _name, _runs, _balls, _star in _found:
            batsmen.append(Batsman(
                name=_clean(_name),
                runs=_runs,
                balls=_balls,
                is_striker=bool(_star),
            ))
        if len(batsmen) >= 2:
            break

    # ── page text for everything else ────────────────────────────────────────
    # Use the structured scorecard divs when available, fall back to raw text
    page_text = _clean(soup.get_text(" ", strip=True))

    # ── bowler (from scorecard table) ────────────────────────────────────────
    bowler = _extract_bowler(soup, page_text)

    # ── venue ────────────────────────────────────────────────────────────────
    venue = NF
    for div in soup.find_all("div", class_=re.compile(r"cb-mtch-info")):
        t = _clean(div.get_text())
        if any(k in t.lower() for k in ("stadium", "ground", "oval", "arena")):
            venue = t
            break
    if venue == NF:
        vm = re.search(r"(?:at|venue|ground)[:\s]+([A-Za-z ,]+?)(?:\.|,|$)",
                       og_desc, re.IGNORECASE)
        if vm:
            venue = _clean(vm.group(1))

    # ── match type ───────────────────────────────────────────────────────────
    match_type = _parse_match_type(og_title + " " + title + " " + og_desc)

    # ── status line (needs / won / trail) ────────────────────────────────────
    match_status = NF
    for pat in (
        r"((?:need|needs)\s[\w\s]+(?:run|over|ball|wicket)s?[^.]*)",
        r"((?:won|lead|trail)[^.]{5,60})",
        r"((?:innings break|lunch|tea|stumps)[^.]{0,40})",
    ):
        sm = re.search(pat, page_text, re.IGNORECASE)
        if sm:
            match_status = _clean(sm.group(1))
            break

    # ── toss ─────────────────────────────────────────────────────────────────
    toss = NF
    tm = re.search(r"(toss\s*:\s*[^.]{5,80})", page_text, re.IGNORECASE)
    if tm:
        toss = _clean(tm.group(1))

    # ── last wicket ──────────────────────────────────────────────────────────
    last_wkt = NF
    lm = re.search(
        r"last\s+wicket[:\s]+([A-Za-z .'\-]+\s+\d+\([^)]+\)[^|]{0,40})",
        page_text, re.IGNORECASE
    )
    if lm:
        last_wkt = _clean(lm.group(1))

    # ── partnership ──────────────────────────────────────────────────────────
    partnership = NF
    pm = re.search(
        r"partnership[:\s*]+(\d+\s*\(\s*\d+\s*balls?\s*\))",
        page_text, re.IGNORECASE
    )
    if pm:
        partnership = _clean(pm.group(1))

    # ── run rates ────────────────────────────────────────────────────────────
    crr = NF
    rrr = NF
    crr_m = re.search(r"CRR[:\s]+(\d+[\d.]*)", page_text, re.IGNORECASE)
    rrr_m = re.search(r"RRR[:\s]+(\d+[\d.]*)", page_text, re.IGNORECASE)
    if crr_m:
        crr = crr_m.group(1)
    if rrr_m:
        rrr = rrr_m.group(1)

    # ── target ───────────────────────────────────────────────────────────────
    target = NF
    tgt_m = re.search(r"target[:\s]+(\d+)", page_text, re.IGNORECASE)
    if tgt_m:
        target = tgt_m.group(1)

    return ScoreResponse(
        status="success",
        match_id=match_id,
        title=title,
        match_type=match_type,
        venue=venue,
        match_status=match_status,
        toss=toss,
        innings=innings,
        score=score_str,
        current_batsmen=batsmen,
        current_bowler=bowler,
        last_wicket=last_wkt,
        partnership=partnership,
        current_run_rate=crr,
        required_run_rate=rrr,
        target=target,
    )


def _extract_bowler(soup: BeautifulSoup, page_text: str) -> Bowler:
    """
    Primary: find the bowler scorecard table (cb-col-67/cb-col-100 with Bowler header).
    Fallback: regex over page text.
    """
    # Strategy 1 – structured table rows
    # Cricbuzz renders the bowling figures in divs like:
    # div.cb-col.cb-col-100  containing "Bowler O M R W ECO"
    # then each bowler as a row of divs.cb-col-8 / cb-col-10 / cb-col-16 etc.
    for section in soup.find_all("div", class_=re.compile(r"cb-col-100")):
        text = section.get_text(" ", strip=True)
        if not re.search(r"\bBowler\b", text, re.IGNORECASE):
            continue
        # Look for rows: Name  O  M  R  W  ECO
        rows = re.findall(
            r"([A-Za-z][A-Za-z .'\-]{2,30}?)\s+"   # name
            r"(\d+(?:\.\d+)?)\s+"                    # O
            r"(\d+)\s+"                              # M
            r"(\d+)\s+"                              # R
            r"(\d+)\s+"                              # W
            r"(\d+(?:\.\d+)?)",                      # ECO
            text
        )
        for name, ovs, mdn, runs, wkts, eco in rows:
            # Skip header row-like matches ("Bowler 0 M R W ECO")
            if name.strip().lower() in ("bowler", "o", "m", "r", "w", "eco"):
                continue
            return Bowler(
                name=_clean(name), overs=ovs, maidens=mdn,
                runs=runs, wickets=wkts, economy=eco
            )

    # Strategy 2 – regex over raw text
    bm = re.search(
        r"Bowler\s+O\s+M\s+R\s+W\s+ECO\s+"
        r"([A-Za-z][A-Za-z .'\-]{2,30}?)\s+"
        r"(\d+(?:\.\d+)?)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+(?:\.\d+)?)",
        page_text, re.IGNORECASE
    )
    if bm:
        return Bowler(
            name=_clean(bm.group(1)), overs=bm.group(2),
            maidens=bm.group(3), runs=bm.group(4),
            wickets=bm.group(5), economy=bm.group(6)
        )

    # Strategy 3 – just grab the name
    nm = re.search(
        r"(?:Bowler|bowling)\s*[:\-]?\s*([A-Za-z][A-Za-z .'\-]{2,30}?)"
        r"\s+\d+",
        page_text, re.IGNORECASE
    )
    if nm:
        return Bowler(name=_clean(nm.group(1)))

    return Bowler()


# ---------------------------------------------------------------------------
# Match list parser  (live / recent / upcoming pages)
# ---------------------------------------------------------------------------

def _parse_match_list_page(soup: BeautifulSoup, status: str) -> List[MatchCard]:
    """
    Cricbuzz live-scores page structure (2024-2025):

    <div class="cb-col cb-col-100 cb-plyr-tbody cb-rank-hdr cb-lv-main">
      <h2 class="cb-lv-grn-strip text-bold cb-lv-scr-mtch-hdr">Series name</h2>
      <div class="cb-lv-scrs-col">          ← one per match
        <a href="/live-cricket-scores/XXXXX/...">
          <div class="cb-lv-scr-mtch-hdr inline-block">Match title</div>
          <div class="cb-scr-wll-chvrn cb-lv-scrs-col">
            <div class="cb-lv-scrs">Team1  250/4 (45)</div>
            <div class="cb-lv-scrs">Team2  180/10 (38)</div>
          </div>
          <div class="cb-lv-scr-mtch-tm">... time/venue ...</div>
          <div class="cb-text-complete|cb-text-live|cb-text-inprogress">overview</div>
        </a>
      </div>
    </div>
    """
    matches: List[MatchCard] = []
    seen: set = set()

    # Each tournament/series block
    series_blocks = soup.find_all(
        "div",
        class_=lambda c: c and "cb-lv-main" in c
    )

    for block in series_blocks:
        # Series name
        series_el = block.find(
            ["h2", "h3"],
            class_=lambda c: c and "cb-lv-scr-mtch-hdr" in c
        )
        series = _clean(series_el.get_text()) if series_el else NF

        # Every match card inside this block
        # cb-lv-scrs-col at direct child level = one match
        for card in block.find_all(
            "div",
            class_=lambda c: c and "cb-scr-wll-chvrn" not in (c or "")
                             and "cb-lv-scrs-col" in (c or "")
        ):
            # match link + id
            link_el = card.find("a", href=re.compile(r"/live-cricket-scores/\d+"))
            if not link_el:
                # fallback: any anchor with score href
                link_el = card.find("a", href=True)
            href = link_el.get("href", "") if link_el else ""
            mid = _match_id_from_href(href)
            if mid == NF or mid in seen:
                continue
            seen.add(mid)

            # Match title (h3 or the inline-block div)
            title_el = card.find(
                class_=lambda c: c and "cb-lv-scr-mtch-hdr" in c
                                 and "inline-block" in c
            )
            if not title_el:
                title_el = card.find(["h3", "h4"])
            title = _clean(title_el.get_text()) if title_el else NF

            # Team scores – cb-lv-scrs divs (direct children to avoid duplicates)
            teams = []
            score_wrap = card.find(
                "div", class_=lambda c: c and "cb-scr-wll-chvrn" in c
            )
            if score_wrap:
                for sd in score_wrap.find_all(
                    "div", class_=lambda c: c and "cb-lv-scrs" in c,
                    recursive=False
                ):
                    txt = _clean(sd.get_text())
                    if txt and txt != NF:
                        teams.append({"score": txt})
                # fallback: direct children divs
                if not teams:
                    for sd in score_wrap.find_all("div", recursive=False):
                        txt = _clean(sd.get_text())
                        if txt and txt != NF and len(txt) < 50:
                            teams.append({"score": txt})
            if not teams:
                for score_div in card.find_all(
                    "div", class_=lambda c: c and "cb-lv-scrs" in c
                ):
                    txt = _clean(score_div.get_text())
                    if txt and txt != NF and len(txt) < 50:
                        if not any(t["score"] == txt for t in teams):
                            teams.append({"score": txt})

            # Time / venue line
            tm_el = card.find(
                "div",
                class_=lambda c: c and "cb-lv-scr-mtch-tm" in (c or "")
            )
            time_venue = _clean(tm_el.get_text()) if tm_el else NF

            date_str = NF
            time_str = NF
            venue_str = NF
            if time_venue != NF:
                # "Today • at Wankhede Stadium, Mumbai"
                parts = re.split(r"[•·|,]", time_venue)
                for p in parts:
                    p = p.strip()
                    if re.search(r"\d{1,2}:\d{2}", p):
                        time_str = _clean(p)
                    elif re.search(r"\btoday\b|\btomorrow\b|\b\d{1,2}\s+\w+\b",
                                   p, re.IGNORECASE):
                        date_str = _clean(p)
                    elif re.search(r"\bat\b|stadium|ground|oval|arena|park",
                                   p, re.IGNORECASE):
                        venue_str = _clean(re.sub(r"^\s*at\s+", "", p,
                                                  flags=re.IGNORECASE))

            # Overview / result text
            overview_el = card.find(
                "div",
                class_=re.compile(r"cb-text-(complete|live|inprogress|preview|abandon)")
            )
            overview = _clean(overview_el.get_text()) if overview_el else NF

            match_type = _parse_match_type(title + " " + series)

            matches.append(MatchCard(
                match_id=mid,
                series=series,
                title=title,
                teams=teams,
                venue=venue_str,
                date=date_str,
                time=time_str,
                match_type=match_type,
                status=status,
                overview=overview,
            ))

    # ── fallback: if structured parse yielded nothing,
    #    use broad link-based sweep ──────────────────────────────────────────
    if not matches:
        matches = _fallback_link_parse(soup, status)

    return matches


def _fallback_link_parse(soup: BeautifulSoup, status: str) -> List[MatchCard]:
    """Last-resort: collect any /live-cricket-scores/<id> links."""
    seen: set = set()
    cards: List[MatchCard] = []
    for a in soup.find_all("a", href=re.compile(r"/live-cricket-scores/\d+")):
        mid = _match_id_from_href(a.get("href", ""))
        if mid == NF or mid in seen:
            continue
        seen.add(mid)
        title = _clean(a.get_text())
        cards.append(MatchCard(
            match_id=mid, title=title, status=status
        ))
    return cards


# ---------------------------------------------------------------------------
# Tree-view formatter
# ---------------------------------------------------------------------------

def _tree(data: ScoreResponse) -> str:
    batsmen_lines = "\n".join(
        f"│   {'*' if b.is_striker else ' '} {b.name}  {b.runs}({b.balls})"
        for b in data.current_batsmen
    ) or "│   └── N/A"

    bowler = data.current_bowler
    bowler_line = (
        f"{bowler.name}  {bowler.overs}-{bowler.maidens}-"
        f"{bowler.runs}-{bowler.wickets}"
        if bowler.name != NF else NF
    )

    return (
        "🏏 Live Score\n"
        "│\n"
        f"├── Match       : {data.title}\n"
        f"├── Type        : {data.match_type}\n"
        f"├── Venue       : {data.venue}\n"
        f"├── Score       : {data.score}\n"
        f"├── Status      : {data.match_status}\n"
        f"├── CRR         : {data.current_run_rate}  "
        f"RRR : {data.required_run_rate}  Target : {data.target}\n"
        f"├── Toss        : {data.toss}\n"
        f"├── Bowler      : {bowler_line}\n"
        f"├── Partnership : {data.partnership}\n"
        f"├── Last Wicket : {data.last_wicket}\n"
        "├── Batsmen\n"
        f"{batsmen_lines}"
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/docs", include_in_schema=False)
async def swagger():
    try:
        page = get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title="Cricket Score API",
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


# -- Individual match score --------------------------------------------------

@app.get("/", response_model=ScoreResponse, summary="Live match score")
async def root(
    score: Optional[str] = Query(None, min_length=4, max_length=20,
                                 description="Cricbuzz match ID"),
    text: bool = Query(False, description="Return tree-text view"),
):
    """
    Fetch live score for a specific match.

    - **score**: Cricbuzz numeric match ID (from the URL)
    - **text**: `true` → ASCII tree view, omit → JSON
    """
    if score is None:
        return ScoreResponse(
            status="success",
            title="Cricket Score API v2 — pass ?score=<match_id>",
        )
    try:
        MatchValidator(score=score)
    except Exception:
        return JSONResponse(
            status_code=422,
            content={"status": "error", "code": 422,
                     "message": "score id must be at least 4 digits"},
        )
    r = await _fetch(
        f"{CB}/live-cricket-scores/{score}?_={time.time_ns()}"
    )
    if r is None:
        raise APIError(503, "upstream request failed")

    data = _parse_score_page(_soup(r.text), score)

    if text:
        return PlainTextResponse(_tree(data))
    return data


# -- Match lists  (live / recent / upcoming) ---------------------------------

@app.get(
    "/matches/{match_status}",
    response_model=MatchListResponse,
    summary="List matches by status",
)
async def matches(
    match_status: str,
    type: str = Query(
        "international",
        description="international | league | domestic | women",
    ),
):
    """
    Get matches by status and category.

    - **match_status**: `live` | `recent` | `upcoming`
    - **type**: `international` (default) | `league` | `domestic` | `women`
    """
    valid_statuses = {"live", "recent", "upcoming"}
    valid_types = {"international", "league", "domestic", "women"}
    if match_status not in valid_statuses:
        return JSONResponse(
            status_code=422,
            content={"status": "error",
                     "message": f"status must be one of {valid_statuses}"},
        )
    if type not in valid_types:
        return JSONResponse(
            status_code=422,
            content={"status": "error",
                     "message": f"type must be one of {valid_types}"},
        )

    # Cricbuzz URL patterns:
    #   /cricket-match/live-scores                       (live, international)
    #   /cricket-match/live-scores/league-cricket        (live, league)
    #   /cricket-match/live-scores/recent-matches        (recent, international)
    #   /cricket-match/live-scores/upcoming-matches      (upcoming, international)
    if match_status == "live":
        suffix = {
            "international": "",
            "league":        "/league-cricket",
            "domestic":      "/domestic-cricket",
            "women":         "/women-cricket",
        }[type]
        url = f"{CB}/cricket-match/live-scores{suffix}"
    elif match_status == "recent":
        suffix = {
            "international": "/recent-matches",
            "league":        "/recent-matches/league",
            "domestic":      "/recent-matches/domestic",
            "women":         "/recent-matches/women",
        }[type]
        url = f"{CB}/cricket-match/live-scores{suffix}"
    else:  # upcoming
        suffix = {
            "international": "/upcoming-matches",
            "league":        "/upcoming-matches/league",
            "domestic":      "/upcoming-matches/domestic",
            "women":         "/upcoming-matches/women",
        }[type]
        url = f"{CB}/cricket-match/live-scores{suffix}"

    r = await _fetch(url)
    if r is None:
        raise APIError(503, "upstream request failed")

    cards = _parse_match_list_page(_soup(r.text), match_status)

    return MatchListResponse(
        status="success",
        type=f"{match_status}/{type}",
        total=len(cards),
        matches=cards,
    )


# -- Schedule alias  (backward compat) ----------------------------------------

@app.get("/schedule", response_model=MatchListResponse, summary="Upcoming matches")
async def schedule(
    type: str = Query("international",
                      description="international | league | domestic | women"),
):
    """Alias for `/matches/upcoming`."""
    return await matches("upcoming", type=type)


# -- Error handlers ------------------------------------------------------------

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
