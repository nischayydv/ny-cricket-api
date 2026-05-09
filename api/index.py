import re
import html
import time
import json
from typing import List, Optional
from datetime import datetime

import httpx
from bs4 import BeautifulSoup
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import (
    JSONResponse,
    PlainTextResponse,
    HTMLResponse,
)
from pydantic import BaseModel, field_validator
from starlette.exceptions import HTTPException as StarletteHTTPException


NOT_FOUND = "score not found"
REQUEST_TIMEOUT = "request timeout"
INVALID_MATCH_ID = "invalid score id"


class APIError(Exception):
    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        self.message = message


class Batsman(BaseModel):
    name: str = NOT_FOUND
    score: str = NOT_FOUND


class Bowler(BaseModel):
    name: str = NOT_FOUND


class ScoreResponse(BaseModel):
    status: str
    title: str
    score: str
    current_batsmen: List[Batsman]
    current_bowler: Bowler


class MatchSchedule(BaseModel):
    series: str = NOT_FOUND
    match: str = NOT_FOUND
    date: str = NOT_FOUND
    time: str = NOT_FOUND
    venue: str = NOT_FOUND
    match_id: str = NOT_FOUND
    teams: str = NOT_FOUND


class TeamPlayers(BaseModel):
    team: str = NOT_FOUND
    players: List[str] = []


class MatchDetails(BaseModel):
    title: str = NOT_FOUND
    toss: str = NOT_FOUND
    teams: List[TeamPlayers] = []
    playing_xi: List[TeamPlayers] = []
    schedule: MatchSchedule = MatchSchedule()


class ScheduleResponse(BaseModel):
    status: str
    matches: List[MatchSchedule]


class MatchValidator(BaseModel):
    score: str

    @field_validator("score")
    @classmethod
    def validate_match_id(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise ValueError(INVALID_MATCH_ID)

        if not value.isdigit():
            raise ValueError("score id must contain digits only")

        if len(value) < 4:
            raise ValueError("score id must be at least 4 digits")

        if len(value) > 20:
            raise ValueError("score id too long")

        return value


app = FastAPI(
    title="Score API",
    version="0.0.1",
    description="Live Cricket Score JSON API",
    docs_url=None,
    redoc_url=None
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)

    response.headers["Cache-Control"] = (
        "no-store, no-cache, must-revalidate, "
        "proxy-revalidate, max-age=0"
    )
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    response.headers["Surrogate-Control"] = "no-store"

    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["X-Robots-Tag"] = "noindex, nofollow"

    response.headers["Strict-Transport-Security"] = (
        "max-age=31536000"
    )

    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "connect-src 'self' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "img-src 'self' data: https://fastapi.tiangolo.com; "
        "object-src 'none'; "
        "frame-ancestors 'none';"
    )

    return response


class ScoreService:
    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(X11; Linux x86_64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/146.0.0.0 "
            "Safari/537.36"
        ),
        "Referer": "https://www.cricbuzz.com/",
        "Origin": "https://www.cricbuzz.com",
        "Cache-Control": "no-cache, no-store, max-age=0",
        "Pragma": "no-cache",
        "Expires": "0",
        "Connection": "close",
        "Accept": (
            "text/html,"
            "application/xhtml+xml,"
            "application/xml;q=0.9,"
            "*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
    }

    @staticmethod
    def clean(text: str) -> str:
        if not text:
            return NOT_FOUND
        return html.escape(" ".join(text.split()))

    @classmethod
    def default_batsmen(cls) -> List[Batsman]:
        return [Batsman(), Batsman()]

    @classmethod
    def format_tree(cls, data: ScoreResponse) -> str:
        batsmen_lines = "\n".join(
            f"│   ├── {player.name} : {player.score}"
            for player in data.current_batsmen
        )

        return (
            "🏏 Live Score\n"
            "│\n"
            f"├── Match    : {data.title}\n"
            f"├── Score    : {data.score}\n"
            f"├── Bowler   : {data.current_bowler.name}\n"
            "├── Batsmen\n"
            f"{batsmen_lines}"
        )

    @classmethod
    async def fetch_score(cls, match_id: str) -> ScoreResponse:
        try:
            url = (
                "https://www.cricbuzz.com/live-cricket-scores/"
                f"{match_id}?_={time.time_ns()}"
            )

            async with httpx.AsyncClient(
                timeout=10.0,
                follow_redirects=True
            ) as client:
                response = await client.get(
                    url,
                    headers=cls.HEADERS
                )
                response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            title = cls.clean(
                re.sub(
                    r"^Cricket commentary\s*\|\s*",
                    "",
                    soup.title.get_text(strip=True)
                    if soup.title
                    else NOT_FOUND,
                    flags=re.IGNORECASE
                )
            )

            og_tag = soup.find("meta", property="og:title")
            og_title = og_tag.get("content", "") if og_tag else ""

            score = NOT_FOUND

            score_match = re.search(
                r"([A-Z]{2,4})\s+(\d+)/(\d+)\s*\(([\d.]+)\)",
                og_title
            )

            if score_match:
                team, runs, wickets, overs = score_match.groups()
                score = f"{team} {runs}/{wickets} ({overs})"

            batsmen = []

            batsman_match = re.search(
                r"\((.*?)\)\s*\|",
                og_title
            )

            if batsman_match:
                players = re.findall(
                    r"([A-Za-z\s.'-]+)\s+(\d+\(\d+\))",
                    batsman_match.group(1)
                )

                batsmen = [
                    Batsman(
                        name=cls.clean(name),
                        score=cls.clean(score_value)
                    )
                    for name, score_value in players[:2]
                ]

            if len(batsmen) < 2:
                batsmen = cls.default_batsmen()

            page_text = cls.clean(
                soup.get_text(" ", strip=True)
            )

            bowler_match = re.search(
                r"Bowler.*?([A-Za-z.'\- ]+?)\s+\d+\s+\d+",
                page_text,
                re.IGNORECASE
            )

            bowler_name = (
                cls.clean(bowler_match.group(1))
                if bowler_match
                else NOT_FOUND
            )

            return ScoreResponse(
                status="success",
                title=title,
                score=score,
                current_batsmen=batsmen,
                current_bowler=Bowler(name=bowler_name)
            )

        except httpx.TimeoutException:
            raise APIError(408, REQUEST_TIMEOUT)

        except httpx.HTTPStatusError:
            raise APIError(404, "score data unavailable")

        except Exception:
            raise APIError(500, "failed to process score data")

    @classmethod
    async def fetch_schedule(cls) -> List[MatchSchedule]:
        """
        Advanced schedule fetching with 5 strategies including Cricbuzz API
        """
        try:
            url = "https://www.cricbuzz.com/cricket-match/live-scores/upcoming-matches?_=" + str(time.time_ns())

            async with httpx.AsyncClient(
                timeout=15.0,
                follow_redirects=True
            ) as client:
                response = await client.get(
                    url,
                    headers=cls.HEADERS
                )
                response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            matches = []

            # Strategy 1: Try API endpoint directly
            matches = await cls._parse_schedule_api()
            if matches:
                return matches

            # Strategy 2: Parse new Cricbuzz structure
            matches = cls._parse_schedule_v2(soup)
            if matches:
                return matches

            # Strategy 3: Parse via JSON embedded in script tags
            matches = cls._parse_schedule_v3(soup)
            if matches:
                return matches

            # Strategy 4: Link-based parsing with better context
            matches = cls._parse_schedule_v4(soup)
            if matches:
                return matches

            return []

        except httpx.TimeoutException:
            return []
        except Exception:
            return []

    @classmethod
    async def _parse_schedule_api(cls) -> List[MatchSchedule]:
        """Try to fetch from Cricbuzz API endpoint"""
        matches = []
        try:
            api_url = "https://www.cricbuzz.com/api/cricket-match/live-scores/upcoming-matches"
            
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(api_url, headers=cls.HEADERS)
                if response.status_code == 200:
                    data = response.json()
                    
                    if isinstance(data, dict):
                        # Try various possible keys
                        for key in ['matches', 'upcoming', 'schedules', 'matchList', 'data']:
                            if key in data:
                                items = data[key] if isinstance(data[key], list) else []
                                for match in items:
                                    try:
                                        match_id = str(match.get('id') or match.get('matchId', ''))
                                        match_title = match.get('title') or match.get('description', '')
                                        date_str = match.get('date') or match.get('scheduleDate', '')
                                        time_str = match.get('time') or match.get('scheduleTime', '')
                                        venue = match.get('venue') or match.get('ground', '')
                                        series = match.get('series') or match.get('seriesName', '')
                                        
                                        if match_id and match_title:
                                            matches.append(MatchSchedule(
                                                series=series or NOT_FOUND,
                                                match=match_title,
                                                date=date_str or NOT_FOUND,
                                                time=time_str or NOT_FOUND,
                                                venue=venue or NOT_FOUND,
                                                match_id=match_id
                                            ))
                                    except Exception:
                                        continue
                                break
        except Exception:
            pass
        
        return matches

    @classmethod
    def _parse_schedule_v2(cls, soup: BeautifulSoup) -> List[MatchSchedule]:
        """Parse using cb-mtch-lst and modern Cricbuzz classes"""
        matches = []
        try:
            # Try main schedule container
            schedule_container = soup.find('div', class_='cb-mtch-lst') or soup.find('div', class_='cb-schdl')
            if not schedule_container:
                return []

            # Look for all match cards
            match_cards = (
                schedule_container.find_all('div', class_=re.compile(r'cb-match|match-card', re.IGNORECASE)) +
                schedule_container.find_all('a', href=re.compile(r'/live-cricket-scores/\d+'))
            )

            seen_ids = set()

            for card in match_cards[:30]:  # Limit to 30 cards
                try:
                    # Extract match ID from link
                    link = card if card.name == 'a' else card.find('a', href=True)
                    if not link:
                        continue

                    href = link.get('href', '')
                    match_id_match = re.search(r'/(\d+)(?:/|$)', href)
                    match_id = match_id_match.group(1) if match_id_match else None

                    if not match_id or match_id in seen_ids:
                        continue

                    seen_ids.add(match_id)

                    # Extract match text
                    match_text = link.get_text(strip=True) if link.name == 'a' else card.get_text(strip=True)
                    
                    # Parse match info from text or structure
                    date_str = NOT_FOUND
                    time_str = NOT_FOUND
                    venue = NOT_FOUND
                    series = NOT_FOUND

                    # Look for date pattern in text: "9 May" or "May 9"
                    date_match = re.search(r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})', match_text, re.IGNORECASE)
                    if date_match:
                        date_str = cls.clean(date_match.group(1))

                    # Look for time pattern: "7:30 PM" or "19:30"
                    time_match = re.search(r'(\d{1,2}:\d{2}\s*(?:am|pm|AM|PM|IST|UTC)?)', match_text)
                    if time_match:
                        time_str = cls.clean(time_match.group(1))

                    # Look for venue pattern
                    venue_match = re.search(r'(?:at|@|venue:?)\s+([A-Za-z\s,]+?)(?:,|$|\n)', match_text, re.IGNORECASE)
                    if venue_match:
                        venue = cls.clean(venue_match.group(1))

                    matches.append(MatchSchedule(
                        series=series,
                        match=cls.clean(match_text[:100]),
                        date=date_str,
                        time=time_str,
                        venue=venue,
                        match_id=match_id
                    ))

                except Exception:
                    continue

            return matches
        except Exception:
            return []

    @classmethod
    def _parse_schedule_v3(cls, soup: BeautifulSoup) -> List[MatchSchedule]:
        """Parse JSON data embedded in script tags"""
        matches = []
        try:
            scripts = soup.find_all('script', type='application/json')
            for script in scripts:
                try:
                    data = json.loads(script.string) if script.string else {}
                    
                    if isinstance(data, dict):
                        # Try to find matches in various JSON structures
                        for key in ['matches', 'upcoming', 'schedules', 'events', 'matchList']:
                            if key in data and isinstance(data[key], list):
                                for match in data[key]:
                                    if isinstance(match, dict):
                                        try:
                                            match_id = str(match.get('id') or match.get('matchId', ''))
                                            match_title = match.get('title') or match.get('description', '')
                                            date_str = match.get('date', '')
                                            time_str = match.get('time', '')
                                            venue = match.get('venue', '')
                                            series = match.get('series', '')

                                            if match_id and match_title:
                                                matches.append(MatchSchedule(
                                                    series=series or NOT_FOUND,
                                                    match=match_title,
                                                    date=date_str or NOT_FOUND,
                                                    time=time_str or NOT_FOUND,
                                                    venue=venue or NOT_FOUND,
                                                    match_id=match_id
                                                ))
                                        except Exception:
                                            continue
                                break
                except Exception:
                    continue

            return matches
        except Exception:
            return []

    @classmethod
    def _parse_schedule_v4(cls, soup: BeautifulSoup) -> List[MatchSchedule]:
        """Enhanced fallback: Extract links with full context from parents"""
        matches = []
        try:
            seen_ids = set()

            # Find all match links
            for link in soup.find_all('a', href=re.compile(r'/live-cricket-scores/\d+|/match/\d+')):
                try:
                    href = link.get('href', '')
                    match_id_match = re.search(r'/(\d+)(?:/|$)', href)
                    match_id = match_id_match.group(1) if match_id_match else None

                    if not match_id or match_id in seen_ids or len(match_id) < 4:
                        continue

                    seen_ids.add(match_id)

                    # Get full match text
                    match_text = link.get_text(strip=True)
                    if not match_text or len(match_text) < 3:
                        continue

                    # Extract details from text and parent elements
                    date_str = NOT_FOUND
                    time_str = NOT_FOUND
                    venue = NOT_FOUND
                    series = NOT_FOUND

                    # Search in parent hierarchy
                    parent = link.parent
                    full_context = match_text
                    
                    for _ in range(4):  # Go up to 4 levels
                        if parent:
                            parent_text = parent.get_text(strip=True)
                            full_context = parent_text
                            parent = parent.parent

                    # Extract date from context
                    date_match = re.search(
                        r'(\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2})',
                        full_context,
                        re.IGNORECASE
                    )
                    if date_match:
                        date_str = cls.clean(date_match.group(1))

                    # Extract time
                    time_match = re.search(r'(\d{1,2}:\d{2}\s*(?:am|pm|AM|PM|IST|UTC)?)', full_context)
                    if time_match:
                        time_str = cls.clean(time_match.group(1))

                    # Extract venue
                    venue_match = re.search(r'(?:ground|venue|at|@)\s+([A-Za-z\s]+?)(?:,|\n|$)', full_context, re.IGNORECASE)
                    if venue_match:
                        venue_text = venue_match.group(1).strip()
                        if venue_text and len(venue_text) < 100:
                            venue = cls.clean(venue_text)

                    matches.append(MatchSchedule(
                        series=series,
                        match=cls.clean(match_text),
                        date=date_str,
                        time=time_str,
                        venue=venue,
                        match_id=match_id
                    ))

                except Exception:
                    continue

            return matches[:20]
        except Exception:
            return []

    @classmethod
    async def fetch_match_details(cls, match_id: str) -> MatchDetails:
        try:
            url = f"https://www.cricbuzz.com/cricket-match-facts/{match_id}"

            async with httpx.AsyncClient(
                timeout=10.0,
                follow_redirects=True
            ) as client:
                response = await client.get(
                    url,
                    headers=cls.HEADERS
                )
                response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")

            title = cls.clean(soup.title.get_text(strip=True)) if soup.title else NOT_FOUND

            toss = NOT_FOUND
            toss_div = soup.find('div', class_='cb-match-toss')
            if toss_div:
                toss = cls.clean(toss_div.get_text(strip=True))

            teams = []
            team_divs = soup.find_all('div', class_='cb-team')
            for team_div in team_divs:
                team_name = cls.clean(team_div.find('h3').get_text(strip=True)) if team_div.find('h3') else NOT_FOUND
                players = [cls.clean(p.get_text(strip=True)) for p in team_div.find_all('a', class_='cb-player-name')]
                teams.append(TeamPlayers(team=team_name, players=players))

            playing_xi = []
            xi_divs = soup.find_all('div', class_='cb-playing-xi')
            for xi_div in xi_divs:
                team_name = cls.clean(xi_div.find('h4').get_text(strip=True)) if xi_div.find('h4') else NOT_FOUND
                players = [cls.clean(p.get_text(strip=True)) for p in xi_div.find_all('a')]
                playing_xi.append(TeamPlayers(team=team_name, players=players))

            schedule = MatchSchedule()

            return MatchDetails(
                title=title,
                toss=toss,
                teams=teams,
                playing_xi=playing_xi,
                schedule=schedule
            )

        except Exception:
            return MatchDetails()

@app.get("/docs", include_in_schema=False)
async def custom_swagger_docs():
    try:
        html = get_swagger_ui_html(
            openapi_url=app.openapi_url,
            title="Live Cricket Score API Docs",
            swagger_favicon_url="https://fastapi.tiangolo.com/img/favicon.png"
        )

        content = html.body.decode("utf-8")

        if "</head>" not in content:
            raise ValueError("Invalid Swagger HTML")

        custom_style = """
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1">
        <style>
            html, body {
                margin: 0;
                padding: 0;
                width: 100%;
                overflow-x: hidden;
                -webkit-text-size-adjust: 100%;
            }

            .swagger-ui {
                width: 100%;
                overflow-x: hidden;
            }

            .swagger-ui .wrapper {
                width: 100%;
                max-width: 100% !important;
                padding: 10px !important;
                box-sizing: border-box;
            }

            .swagger-ui .opblock-summary {
                flex-wrap: wrap !important;
                gap: 6px;
            }

            .swagger-ui .opblock-summary-path {
                white-space: normal !important;
                word-break: break-word !important;
                overflow-wrap: anywhere !important;
                font-size: 14px !important;
                line-height: 1.4;
            }

            .swagger-ui pre,
            .swagger-ui code,
            .swagger-ui .microlight,
            .swagger-ui .highlight-code {
                white-space: pre-wrap !important;
                word-break: break-word !important;
                overflow-wrap: anywhere !important;
                overflow-x: auto !important;
                max-width: 100% !important;
                max-height: 220px !important;
                overflow-y: auto !important;
                box-sizing: border-box;
                font-size: 12px !important;
                line-height: 1.5 !important;
                border-radius: 8px;
            }

            .swagger-ui table {
                display: block;
                width: 100%;
                overflow-x: auto;
            }

            .swagger-ui textarea,
            .swagger-ui input,
            .swagger-ui select {
                width: 100% !important;
                box-sizing: border-box;
                font-size: 16px !important;
            }

            .swagger-ui .btn {
                min-height: 42px !important;
                white-space: normal !important;
            }

            @media (max-width: 768px) {
                .swagger-ui .wrapper {
                    padding: 8px !important;
                }

                .swagger-ui pre,
                .swagger-ui code {
                    max-height: 180px !important;
                    font-size: 11px !important;
                }
            }
        </style>
        """

        content = content.replace(
            "</head>",
            custom_style + "</head>"
        )

        response = HTMLResponse(content=content)

        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"

        return response

    except Exception:
        return HTMLResponse(
            content="""
            <html>
                <head>
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                    <title>Docs Error</title>
                </head>
                <body style="font-family:sans-serif;padding:20px;">
                    <h2>Unable to load Swagger docs</h2>
                </body>
            </html>
            """,
            status_code=500
        )

@app.get("/", response_model=ScoreResponse)
async def root(
    score: Optional[str] = Query(
        None,
        min_length=4,
        max_length=20
    ),
    text: bool = Query(False)
):
    if score is None:
        return ScoreResponse(
            status="success",
            title="Live Score API",
            score=NOT_FOUND,
            current_batsmen=ScoreService.default_batsmen(),
            current_bowler=Bowler()
        )

    try:
        MatchValidator(score=score)
    except Exception as exc:
        return JSONResponse(
            status_code=422,
            content={
                "status": "error",
                "code": 422,
                "message": "score id must be at least 4 digits"
            }
        )

    result = await ScoreService.fetch_score(score)

    if text:
        return PlainTextResponse(
            ScoreService.format_tree(result)
        )

    return result


@app.get("/schedule", response_model=ScheduleResponse)
async def get_schedule():
    matches = await ScoreService.fetch_schedule()
    return ScheduleResponse(status="success", matches=matches)


@app.get("/match/{match_id}/details", response_model=MatchDetails)
async def get_match_details(match_id: str):
    try:
        MatchValidator(score=match_id)
    except Exception:
        return JSONResponse(
            status_code=422,
            content={
                "status": "error",
                "message": "invalid match id"
            }
        )
    details = await ScoreService.fetch_match_details(match_id)
    return details


@app.exception_handler(APIError)
async def api_error_handler(request: Request, exc: APIError):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "code": exc.status_code,
            "message": "score id must be at least 4 digits"
        }
    )


@app.exception_handler(StarletteHTTPException)
async def http_error_handler(
    request: Request,
    exc: StarletteHTTPException
):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "code": exc.status_code,
            "message": "invalid api route"
        }
    )


@app.exception_handler(Exception)
async def global_error_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "status": "error",
            "code": 500,
            "message": "internal server error"
        }
    )
