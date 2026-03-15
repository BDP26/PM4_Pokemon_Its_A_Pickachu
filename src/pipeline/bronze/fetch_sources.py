import re
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from src.pipeline.common.http import build_session
from src.pipeline.common.io import write_json
from src.pipeline.settings import BRONZE_DIR, BULBA_API, POKEAPI, ensure_medallion_dirs
from src.pipeline.silver.game_config import get_games_config


_TITLE_EXISTS_CACHE: dict[str, bool] = {}


def page_exists(session, title: str) -> bool:
    if title in _TITLE_EXISTS_CACHE:
        return _TITLE_EXISTS_CACHE[title]

    try:
        r = session.get(
            BULBA_API,
            params={
                "action": "query",
                "titles": title,
                "format": "json",
                "formatversion": 2,
            },
            timeout=15,
        )
        r.raise_for_status()
        response = r.json()

        print(f"[debug] title={title!r}")
        print(f"[debug] response={response}")

        pages = response.get("query", {}).get("pages", [])
        exists = bool(pages) and not pages[0].get("missing", False)

    except Exception as e:
        print(f"[debug] page_exists failed for {title!r}: {e}")
        exists = False

    _TITLE_EXISTS_CACHE[title] = exists
    return exists


def resolve_existing_root_title(session, candidate_titles: list[str]) -> Optional[str]:
    for title in candidate_titles:
        if title and page_exists(session, title):
            return title
    return None


def get_walkthrough_parts(session, root_title: str) -> list[dict]:
    response = session.get(
        BULBA_API,
        params={"action": "parse", "page": root_title, "prop": "text", "format": "json", "formatversion": 2},
        timeout=20,
    ).json()

    page_html = response.get("parse", {}).get("text")
    if not page_html:
        return []

    soup = BeautifulSoup(page_html, "lxml")
    parts: list[dict] = []
    for anchor in soup.select("div.mw-parser-output a[href*='/Part']"):
        title = anchor.get("title")
        if title and root_title in title:
            match = re.search(r"Part[_ ](\d+)", title)
            if match:
                parts.append({"part": int(match.group(1)), "title": title})
    return sorted(parts, key=lambda item: item["part"])


def fetch_bronze_sources(output_dir: Optional[Path] = None) -> None:
    ensure_medallion_dirs()
    output = output_dir or BRONZE_DIR
    output.mkdir(parents=True, exist_ok=True)
    bulbapedia_dir = output / "bulbapedia"
    pokeapi_dir = output / "pokeapi"
    bulbapedia_dir.mkdir(parents=True, exist_ok=True)
    pokeapi_dir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    location_index = session.get(f"{POKEAPI}/location", params={"limit": 2000}, timeout=30).json()
    write_json(pokeapi_dir / "location_index.json", location_index)

    for config in get_games_config():
        game_key = config["game_key"]
        resolved_root_title = resolve_existing_root_title(session, config["candidate_root_titles"])
        if not resolved_root_title:
            print(f"[bronze] skip {game_key}: no matching walkthrough title")
            continue

        parts = get_walkthrough_parts(session, resolved_root_title)
        records: list[dict] = []
        for part in parts:
            response = session.get(
                BULBA_API,
                params={
                    "action": "parse",
                    "page": part["title"],
                    "prop": "text",
                    "format": "json",
                    "formatversion": 2,
                },
                timeout=20,
            ).json()
            records.append(
                {
                    "part": part["part"],
                    "title": part["title"],
                    "html": response.get("parse", {}).get("text", ""),
                }
            )

        payload = {
            "game_key": game_key,
            "route_prefix": config["route_prefix"],
            "bosses": config["bosses"],
            "resolved_root_title": resolved_root_title,
            "parts": records,
        }
        write_json(bulbapedia_dir / f"{game_key}.json", payload)
        print(f"[bronze] wrote {game_key}.json with {len(records)} parts")

if __name__ == "__main__":
    fetch_bronze_sources()
