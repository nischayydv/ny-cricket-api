#!/usr/bin/env python3

import argparse
import json
import re
import sys
import time
from typing import Any, Dict

import requests
from bs4 import BeautifulSoup


class ScoreCLI:
    BASE_URL = "https://www.cricbuzz.com/live-cricket-scores/"

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(X11; Linux x86_64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/146.0.0.0 "
            "Safari/537.36"
        ),
        "Accept": (
            "text/html,"
            "application/xhtml+xml,"
            "application/xml;q=0.9,"
            "*/*;q=0.8"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Cache-Control": "no-cache",
    }

    @staticmethod
    def clean(text: str) -> str:
        return " ".join(text.split()) if text else ""

    @staticmethod
    def not_found() -> str:
        return "score not found"

    @staticmethod
    def no_live_match() -> str:
        return "currently no live match"

    @classmethod
    def validate_match_id(cls, value: str) -> str:
        value = value.strip()

        if not value:
            raise argparse.ArgumentTypeError(
                "match_id cannot be empty"
            )

        if not value.isdigit():
            raise argparse.ArgumentTypeError(
                "match_id must contain digits only"
            )

        if len(value) < 4:
            raise argparse.ArgumentTypeError(
                "match_id must be at least 4 digits"
            )

        return value

    @classmethod
    def build_url(cls, match_id: str) -> str:
        return (
            f"{cls.BASE_URL}"
            f"{match_id}/?_={int(time.time() * 1000)}"
        )

    @classmethod
    def extract_bowler(cls, full_text: str) -> Dict[str, str]:
        text = cls.clean(full_text)

        block_match = re.search(
            r"Bowler\s+O\s+M\s+R\s+W\s+ECO\s+(.*?)(?:Partnership|Last wicket|CRR|$)",
            text,
            re.IGNORECASE,
        )

        if not block_match:
            return {"name": cls.not_found()}

        block = cls.clean(block_match.group(1))

        name_match = re.search(
            r"([A-Za-z.'\- ]+?)\s*\*?\s+\d+\s+\d+\s+\d+\s+\d+\s+\d+(?:\.\d+)?",
            block,
        )

        return {
            "name": cls.clean(name_match.group(1))
            if name_match
            else cls.not_found()
        }

    @classmethod
    def extract_match_data(cls, match_id: str) -> Dict[str, Any]:
        url = cls.build_url(match_id)

        default_response = {
            "title": cls.not_found(),
            "score": cls.not_found(),
            "current_batsmen": [],
            "current_bowler": {"name": cls.not_found()},
        }

        try:
            response = requests.get(
                url,
                headers=cls.HEADERS,
                timeout=10,
            )
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            full_text = cls.clean(
                soup.get_text(" ", strip=True)
            )

            raw_title = (
                soup.title.get_text(strip=True)
                if soup.title
                else ""
            )

            title = re.sub(
                r"^Cricket commentary\s*\|\s*",
                "",
                raw_title,
                flags=re.IGNORECASE,
            )

            og_title_tag = soup.find(
                "meta",
                property="og:title",
            )

            og_title = cls.clean(
                og_title_tag.get("content", "")
                if og_title_tag
                else ""
            )

            score = cls.not_found()

            score_match = re.search(
                r"([A-Z]{2,4})\s+(\d+)/(\d+)\s*\(([\d.]+)\)",
                og_title,
            )

            if score_match:
                team, runs, wickets, overs = score_match.groups()

                score = {
                    "team": team,
                    "runs": int(runs),
                    "wickets": int(wickets),
                    "overs": float(overs),
                    "display": f"{team} {runs}/{wickets} ({overs})",
                }

            batsmen = []
            batsman_match = re.search(
                r"\((.*?)\)\s*\|",
                og_title,
            )

            if batsman_match:
                players = re.findall(
                    r"([A-Za-z\s.'-]+)\s+(\d+\(\d+\))",
                    batsman_match.group(1),
                )

                batsmen = [
                    {
                        "name": cls.clean(name),
                        "score": score_value,
                    }
                    for name, score_value in players[:2]
                ]

            bowler = cls.extract_bowler(full_text)

            return {
                "title": title or cls.not_found(),
                "score": score,
                "current_batsmen": batsmen,
                "current_bowler": bowler,
            }

        except requests.Timeout:
            default_response["error"] = "request timeout"
            return default_response

        except requests.RequestException as e:
            default_response["error"] = f"network error: {e}"
            return default_response

        except Exception as e:
            default_response["error"] = f"parse error: {e}"
            return default_response

    @classmethod
    def format_tree(cls, data: Dict[str, Any]) -> str:
        score = (
            data["score"]["display"]
            if isinstance(data["score"], dict)
            else data["score"]
        )

        batsmen = (
            "\n".join(
                f"│   ├── {p['name']} : {p['score']}"
                for p in data["current_batsmen"]
            )
            if data["current_batsmen"]
            else "│   └── N/A"
        )

        return (
            "🏏 Live Score\n"
            "│\n"
            f"├── Match    : {data['title']}\n"
            f"├── Score    : {score}\n"
            f"├── Bowler   : {data['current_bowler']['name']}\n"
            "├── Batsmen\n"
            f"{batsmen}"
        )

    @classmethod
    def parse_arguments(cls):
        parser = argparse.ArgumentParser(
            prog="scorecli",
            description="Fetch live cricket score in tree, text, or JSON format",
            formatter_class=argparse.RawTextHelpFormatter,
            epilog="""
Examples:
  scorecli 12345
  scorecli 12345 --json
  scorecli 12345 --text
            """,
        )

        parser.add_argument(
            "match_id",
            type=cls.validate_match_id,
            help="numeric match id",
        )

        group = parser.add_mutually_exclusive_group()

        group.add_argument(
            "--json",
            action="store_true",
            help="show JSON output",
        )

        group.add_argument(
            "--text",
            action="store_true",
            help="show tree text output",
        )

        return parser.parse_args()

    @classmethod
    def run(cls):
        args = cls.parse_arguments()

        data = cls.extract_match_data(args.match_id)

        if args.json:
            print(
                json.dumps(
                    data,
                    indent=2,
                    ensure_ascii=False,
                )
            )
            return

        print(cls.format_tree(data))


if __name__ == "__main__":
    try:
        ScoreCLI.run()
    except KeyboardInterrupt:
        print("\nInterrupted by user")
        sys.exit(130)