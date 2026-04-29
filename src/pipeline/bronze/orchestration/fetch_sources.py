import re
import time
from collections import defaultdict
from pathlib import Path
from typing import Optional, cast

from bs4 import BeautifulSoup

from src.pipeline.common.http import build_session
from src.pipeline.common.io import write_json
from src.pipeline.bronze.orchestration.config_snapshot import write_bronze_config_snapshot
from src.pipeline.bronze.reporting.manifest import write_bronze_run_manifest
from src.pipeline.bronze.schemas.contracts import BronzeSourceState
from src.pipeline.bronze.writers.state import load_source_state, now_utc_iso, save_source_state, stable_signature
from src.pipeline.settings import (
    BRONZE_DIR,
    BULBA_API,
    KAGGLE_GYM_LEADERS_DATASET,
    KAGGLE_GYM_LEADERS_FILE_PATH,
    POKEAPI,
    ensure_medallion_dirs,
)
from src.pipeline.silver.config.game_config import get_games_config

import kagglehub
from kagglehub import KaggleDatasetAdapter

_TITLE_EXISTS_CACHE: dict[str, bool] = {}


def _should_write_source(source_state: dict[str, dict[str, object]], source_name: str, signature: str) -> bool:
    previous = source_state.get(source_name, {})
    return str(previous.get("signature") or "") != signature


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

        pages = response.get("query", {}).get("pages", [])
        exists = bool(pages) and not pages[0].get("missing", False)

    except Exception:
        exists = False

    _TITLE_EXISTS_CACHE[title] = exists
    return exists


def resolve_existing_root_title(session, candidate_titles: list[str]) -> Optional[str]:
    for title in candidate_titles:
        if title and page_exists(session, title):
            return title
    return None


def _build_location_area_parent_map(area_rows: list[dict[str, object]]) -> dict[str, str]:
    parent_map: dict[str, str] = {}
    for area in area_rows:
        area_name = str(area.get("name") or "").strip()
        if not area_name:
            continue
        if area_name.endswith("-area"):
            parent_map[area_name] = area_name.removesuffix("-area")
            continue
        if "-area-" in area_name:
            parent_map[area_name] = area_name.split("-area-", 1)[0]
    return parent_map


def _fetch_pokeapi_location_index(session) -> dict[str, object]:
    location_response = session.get(f"{POKEAPI}/location", params={"limit": 2000}, timeout=30)
    location_response.raise_for_status()
    location_payload = location_response.json()
    results = location_payload.get("results", [])

    area_response = session.get(f"{POKEAPI}/location-area", params={"limit": 20000}, timeout=30)
    area_response.raise_for_status()
    area_payload = area_response.json()
    area_results = area_payload.get("results", [])

    return {
        "count": location_payload.get("count"),
        "results": results if isinstance(results, list) else [],
        "location_area_count": area_payload.get("count"),
        "location_area_results": area_results if isinstance(area_results, list) else [],
        "location_area_parent_map": _build_location_area_parent_map(
            area_results if isinstance(area_results, list) else []
        ),
    }


def _fetch_capture_rate(session, pokemon_url: str, capture_rate_cache: dict[str, int | None]) -> int | None:
    if not pokemon_url:
        return None
    if pokemon_url in capture_rate_cache:
        return capture_rate_cache[pokemon_url]
    try:
        pokemon_id = pokemon_url.rstrip("/").split("/")[-1]
        species_url = f"{POKEAPI}/pokemon-species/{pokemon_id}"
        response = session.get(species_url, timeout=15)
        if response.status_code == 200:
            payload = response.json()
            value = payload.get("capture_rate")
            capture_rate_cache[pokemon_url] = int(value) if isinstance(value, int) else None
        else:
            capture_rate_cache[pokemon_url] = None
    except Exception:
        capture_rate_cache[pokemon_url] = None
    return capture_rate_cache[pokemon_url]


def _build_location_pokemon_snapshot(session, location_index: dict[str, object]) -> dict[str, object]:
    diagnostics: dict[str, object] = {"location_errors": [], "area_errors": [], "capture_rate_errors": []}
    payload: dict[str, dict[str, object]] = {}
    capture_rate_cache: dict[str, int | None] = {}

    location_rows = location_index.get("results", [])
    for location in location_rows if isinstance(location_rows, list) else []:
        slug = str((location or {}).get("name") or "").strip()
        if not slug:
            continue
        try:
            location_response = session.get(f"{POKEAPI}/location/{slug}", timeout=15)
            if location_response.status_code != 200:
                cast(list[dict[str, object]], diagnostics["location_errors"]).append({"slug": slug, "status": location_response.status_code})
                continue
            area_names = [str(entry.get("name") or "").strip() for entry in location_response.json().get("areas", []) if entry.get("name")]
        except Exception:
            cast(list[dict[str, object]], diagnostics["location_errors"]).append({"slug": slug, "reason": "request_exception"})
            continue

        version_species = defaultdict[str, set[str]](set)
        version_encounters = defaultdict[str, dict[str, dict[str, object]]](dict)
        area_details: dict[str, dict[str, object]] = {}
        for area_name in area_names:
            try:
                area_response = session.get(f"{POKEAPI}/location-area/{area_name}", timeout=15)
                if area_response.status_code != 200:
                    cast(list[dict[str, object]], diagnostics["area_errors"]).append({"slug": slug, "area": area_name, "status": area_response.status_code})
                    continue
                area_payload = area_response.json()
            except Exception:
                cast(list[dict[str, object]], diagnostics["area_errors"]).append({"slug": slug, "area": area_name, "reason": "request_exception"})
                continue

            by_version_species = defaultdict[str, set[str]](set)
            by_version_encounters = defaultdict[str, dict[str, dict[str, object]]](dict)
            for encounter in area_payload.get("pokemon_encounters", []):
                pokemon = encounter.get("pokemon") or {}
                species = str(pokemon.get("name") or "").strip()
                pokemon_url = str(pokemon.get("url") or "").strip()
                if not species:
                    continue
                version_details = encounter.get("version_details", []) or [{"version": {"name": "all"}, "encounter_details": []}]
                for detail in version_details:
                    version_name = str(((detail.get("version") or {}).get("name") or "all")).strip().lower()
                    by_version_species[version_name].add(species)
                    by_version_species["all"].add(species)
                    details = detail.get("encounter_details", []) or [{}]
                    entry = by_version_encounters[version_name].setdefault(
                        species,
                        {
                            "species": species,
                            "pokemon_url": pokemon_url,
                            "level_min": None,
                            "level_max": None,
                            "encounter_chance_min": None,
                            "encounter_chance_max": None,
                            "capture_rate": None,
                            "encounter_methods": set(),
                            "encounter_method_urls": set(),
                        },
                    )
                    all_entry = by_version_encounters["all"].setdefault(
                        species,
                        {
                            "species": species,
                            "pokemon_url": pokemon_url,
                            "level_min": None,
                            "level_max": None,
                            "encounter_chance_min": None,
                            "encounter_chance_max": None,
                            "capture_rate": None,
                            "encounter_methods": set(),
                            "encounter_method_urls": set(),
                        },
                    )
                    for point in details:
                        method = point.get("method") or {}
                        method_name = str(method.get("name") or "").strip()
                        method_url = str(method.get("url") or "").strip()
                        for target in (entry, all_entry):
                            for key in ("min_level", "max_level", "chance"):
                                value = point.get(key)
                                if not isinstance(value, int):
                                    continue
                                if key == "min_level":
                                    target["level_min"] = value if target["level_min"] is None else min(int(target["level_min"]), value)
                                elif key == "max_level":
                                    target["level_max"] = value if target["level_max"] is None else max(int(target["level_max"]), value)
                                elif key == "chance":
                                    target["encounter_chance_min"] = value if target["encounter_chance_min"] is None else min(int(target["encounter_chance_min"]), value)
                                    target["encounter_chance_max"] = value if target["encounter_chance_max"] is None else max(int(target["encounter_chance_max"]), value)
                            if method_name:
                                cast(set[str], target["encounter_methods"]).add(method_name)
                            if method_url:
                                cast(set[str], target["encounter_method_urls"]).add(method_url)
            for version_name, species_set in by_version_species.items():
                version_species[version_name].update(species_set)
            for version_name, species_map in by_version_encounters.items():
                for species_name, entry in species_map.items():
                    version_encounters[version_name].setdefault(species_name, entry)
            area_details[area_name] = {
                "by_version": {k: sorted(v) for k, v in by_version_species.items() if k != "all"},
                "all": sorted(by_version_species.get("all", set())),
                "by_version_encounters": {
                    k: [
                        {
                            **entry,
                            "encounter_methods": sorted(cast(set[str], entry.get("encounter_methods", set()))),
                            "encounter_method_urls": sorted(cast(set[str], entry.get("encounter_method_urls", set()))),
                        }
                        for entry in sorted(v.values(), key=lambda x: str(x.get("species") or ""))
                    ]
                    for k, v in by_version_encounters.items()
                    if k != "all"
                },
                "all_encounters": [
                    {
                        **entry,
                        "encounter_methods": sorted(cast(set[str], entry.get("encounter_methods", set()))),
                        "encounter_method_urls": sorted(cast(set[str], entry.get("encounter_method_urls", set()))),
                    }
                    for entry in sorted(by_version_encounters.get("all", {}).values(), key=lambda x: str(x.get("species") or ""))
                ],
            }

        for species_map in version_encounters.values():
            for entry in species_map.values():
                if entry.get("capture_rate") is None:
                    entry["capture_rate"] = _fetch_capture_rate(session, str(entry.get("pokemon_url") or ""), capture_rate_cache)
                    if entry["capture_rate"] is None:
                        cast(list[dict[str, object]], diagnostics["capture_rate_errors"]).append({"slug": slug, "species": entry.get("species")})

        payload[slug] = {
            "all": sorted(version_species.get("all", set())),
            "by_version": {k: sorted(v) for k, v in version_species.items() if k != "all"},
            "all_encounters": [
                {
                    **entry,
                    "encounter_methods": sorted(cast(set[str], entry.get("encounter_methods", set()))),
                    "encounter_method_urls": sorted(cast(set[str], entry.get("encounter_method_urls", set()))),
                }
                for entry in sorted(version_encounters.get("all", {}).values(), key=lambda x: str(x.get("species") or ""))
            ],
            "by_version_encounters": {
                k: [
                    {
                        **entry,
                        "encounter_methods": sorted(cast(set[str], entry.get("encounter_methods", set()))),
                        "encounter_method_urls": sorted(cast(set[str], entry.get("encounter_method_urls", set()))),
                    }
                    for entry in sorted(v.values(), key=lambda x: str(x.get("species") or ""))
                ]
                for k, v in version_encounters.items()
                if k != "all"
            },
            "areas": sorted(area_names),
            "areas_detail": {name: area_details[name] for name in sorted(area_details)},
        }

    return {"location_pokemon_map": payload, "diagnostics": diagnostics}


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


def fetch_kaggle_gym_leaders_dataset(
    output_dir: Optional[Path] = None,
    dataset_handle: str = KAGGLE_GYM_LEADERS_DATASET,
    file_path: str = KAGGLE_GYM_LEADERS_FILE_PATH,
) -> None:
    kaggle_output_dir = (output_dir or BRONZE_DIR) / "kagglehub"
    kaggle_output_dir.mkdir(parents=True, exist_ok=True)

    dataset_dir = Path(kagglehub.dataset_download(dataset_handle))
    dataset_files = sorted(
        str(source_file.relative_to(dataset_dir))
        for source_file in dataset_dir.rglob("*")
        if source_file.is_file()
    )

    selected_file = file_path
    if not selected_file:
        preferred_suffixes = (".csv", ".json", ".parquet")
        for candidate in dataset_files:
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
        "dataset_files": dataset_files,
        "dataframe_export": str(table_output_path.relative_to(kaggle_output_dir)) if table_output_path else None,
        "row_count": row_count,
        "columns": columns,
    }
    write_json(kaggle_output_dir / "manifest.json", metadata)
    print(f"[bronze] wrote Kaggle dataset artifacts (without raw copy) to {kaggle_output_dir}")


def fetch_bronze_sources(output_dir: Optional[Path] = None, include_kaggle: bool = True) -> None:
    started_at = now_utc_iso()
    ensure_medallion_dirs()
    output = output_dir or BRONZE_DIR
    output.mkdir(parents=True, exist_ok=True)
    write_bronze_config_snapshot(output)
    bulbapedia_dir = output / "bulbapedia"
    pokeapi_dir = output / "pokeapi"
    bulbapedia_dir.mkdir(parents=True, exist_ok=True)
    pokeapi_dir.mkdir(parents=True, exist_ok=True)

    source_state = load_source_state(output)
    updated_sources: list[str] = []
    unchanged_sources: list[str] = []
    errors: list[str] = []

    session = build_session()

    location_index = _fetch_pokeapi_location_index(session)
    location_signature = stable_signature(location_index)
    if _should_write_source(source_state, "pokeapi:location_index", location_signature):
        write_json(pokeapi_dir / "location_index.json", location_index)
        location_snapshot = _build_location_pokemon_snapshot(session, location_index)
        write_json(pokeapi_dir / "location_pokemon_snapshot.json", location_snapshot)
        source_state["pokeapi:location_index"] = BronzeSourceState(
            source="pokeapi:location_index",
            signature=location_signature,
            updated_at_utc=now_utc_iso(),
            output_paths=[
                str((pokeapi_dir / "location_index.json").relative_to(output)),
                str((pokeapi_dir / "location_pokemon_snapshot.json").relative_to(output)),
            ],
        ).as_dict()
        updated_sources.append("pokeapi:location_index")
    else:
        unchanged_sources.append("pokeapi:location_index")

    for config in get_games_config():
        game_key = config["game_key"]
        resolved_root_title = resolve_existing_root_title(session, config["candidate_root_titles"])
        if not resolved_root_title:
            print(f"[bronze] skip {game_key}: no matching walkthrough title")
            errors.append(f"missing walkthrough title: {game_key}")
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
        source_name = f"bulbapedia:{game_key}"
        signature = stable_signature(payload)
        if _should_write_source(source_state, source_name, signature):
            output_path = bulbapedia_dir / f"{game_key}.json"
            write_json(output_path, payload)
            source_state[source_name] = BronzeSourceState(
                source=source_name,
                signature=signature,
                updated_at_utc=now_utc_iso(),
                output_paths=[str(output_path.relative_to(output))],
            ).as_dict()
            updated_sources.append(source_name)
            print(f"[bronze] wrote {game_key}.json with {len(records)} parts")
        else:
            unchanged_sources.append(source_name)

    if include_kaggle:
        kaggle_started_at = time.perf_counter()
        fetch_kaggle_gym_leaders_dataset(output)
        kaggle_manifest_path = output / "kagglehub" / "manifest.json"
        if kaggle_manifest_path.exists():
            kaggle_manifest = kaggle_manifest_path.read_text(encoding="utf-8")
            kaggle_signature = stable_signature(kaggle_manifest)
            source_name = "kagglehub:gym_leaders"
            if _should_write_source(source_state, source_name, kaggle_signature):
                source_state[source_name] = BronzeSourceState(
                    source=source_name,
                    signature=kaggle_signature,
                    updated_at_utc=now_utc_iso(),
                    output_paths=[
                        str((output / "kagglehub" / "gym_leaders_elite_four.csv").relative_to(output)),
                        str(kaggle_manifest_path.relative_to(output)),
                    ],
                ).as_dict()
                updated_sources.append(source_name)
            else:
                unchanged_sources.append(source_name)
            print(f"[bronze] kaggle source processed elapsed_s={time.perf_counter() - kaggle_started_at:.2f}")

    save_source_state(source_state, output)
    write_bronze_run_manifest(
        started_at_utc=started_at,
        finished_at_utc=now_utc_iso(),
        updated_sources=updated_sources,
        unchanged_sources=unchanged_sources,
        errors=errors,
        bronze_dir=output,
    )

if __name__ == "__main__":
    fetch_bronze_sources()
