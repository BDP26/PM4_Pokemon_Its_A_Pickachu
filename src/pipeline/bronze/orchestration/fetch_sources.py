import re
import shutil
from pathlib import Path
from typing import Optional

from bs4 import BeautifulSoup

from src.pipeline.common.http import build_session
from src.pipeline.common.io import write_json
from src.pipeline.bronze.orchestration.config_snapshot import write_bronze_config_snapshot
from src.pipeline.settings import (
    BRONZE_DIR,
    BULBA_API,
    KAGGLE_GYM_LEADERS_DATASET,
    KAGGLE_GYM_LEADERS_FILE_PATH,
    POKEAPI,
    ensure_medallion_dirs,
)
from src.pipeline.silver.inputs.game_config import get_games_config

import kagglehub
from kagglehub import KaggleDatasetAdapter

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
        title_text = str(title) if title is not None else ""
        if title_text and root_title in title_text:
            match = re.search(r"Part[_ ](\d+)", title_text)
            if match:
                parts.append({"part": int(match.group(1)), "title": title_text})
    return sorted(parts, key=lambda item: item["part"])


def _copy_kaggle_raw_files(dataset_dir: Path, output_dir: Path) -> list[str]:
    copied_files: list[str] = []
    output_dir.mkdir(parents=True, exist_ok=True)
    for source_file in dataset_dir.rglob("*"):
        if not source_file.is_file():
            continue
        relative_path = source_file.relative_to(dataset_dir)
        target_path = output_dir / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source_file, target_path)
        copied_files.append(str(relative_path))
    return sorted(copied_files)


def fetch_kaggle_gym_leaders_dataset(
    output_dir: Optional[Path] = None,
    dataset_handle: str = KAGGLE_GYM_LEADERS_DATASET,
    file_path: str = KAGGLE_GYM_LEADERS_FILE_PATH,
) -> None:
    kaggle_output_dir = (output_dir or BRONZE_DIR) / "kagglehub"

    dataset_dir = Path(kagglehub.dataset_download(dataset_handle))
    copied_files = _copy_kaggle_raw_files(dataset_dir, kaggle_output_dir / "raw")

    selected_file = file_path
    if not selected_file:
        preferred_suffixes = (".csv", ".json", ".parquet")
        for candidate in copied_files:
            if candidate.lower().endswith(preferred_suffixes):
                selected_file = candidate
                break

    table_output_path = None
    row_count = None
    columns = None
    if selected_file:
        dataset_loader = getattr(kagglehub, "dataset_load", None)
        if dataset_loader is None:
            dataset_loader = getattr(kagglehub, "load_dataset", None)
        if dataset_loader is None:
            raise AttributeError("kagglehub is missing dataset_load/load_dataset")

        dataframe = dataset_loader(
            KaggleDatasetAdapter.PANDAS,
            dataset_handle,
            selected_file,
        )
        table_output_path = kaggle_output_dir / "gym_leaders_elite_four.csv"
        dataframe.to_csv(table_output_path, index=False)
        row_count = int(len(dataframe))
        columns = list(dataframe.columns)
    else:
        print("[bronze] warning: No tabular file detected in Kaggle dataset; skipped dataframe export")

    metadata = {
        "dataset_handle": dataset_handle,
        "selected_file": selected_file,
        "raw_files": copied_files,
        "dataframe_export": str(table_output_path.relative_to(kaggle_output_dir)) if table_output_path else None,
        "row_count": row_count,
        "columns": columns,
    }
    write_json(kaggle_output_dir / "manifest.json", metadata)
    print(f"[bronze] wrote Kaggle dataset artifacts to {kaggle_output_dir}")


def fetch_bronze_sources(output_dir: Optional[Path] = None, include_kaggle: bool = True) -> None:
    ensure_medallion_dirs()
    output = output_dir or BRONZE_DIR
    output.mkdir(parents=True, exist_ok=True)
    write_bronze_config_snapshot(output)
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

    if include_kaggle:
        fetch_kaggle_gym_leaders_dataset(output)

if __name__ == "__main__":
    fetch_bronze_sources()


