import re
import time
import hashlib
import logging
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Optional, cast

from bs4 import BeautifulSoup
from pokebase.api import get_data as pokebase_get_data

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
    ensure_medallion_dirs,
)
from src.pipeline.silver.config.game_config import get_games_config

_TITLE_EXISTS_CACHE: dict[str, bool] = {}
logger = logging.getLogger(__name__)


def _should_write_source(source_state: dict[str, dict[str, object]], source_name: str, signature: str) -> bool:
    return _should_write_source_with_force(source_state, source_name, signature, force_write=False)


def _should_write_source_with_force(
    source_state: dict[str, dict[str, object]],
    source_name: str,
    signature: str,
    *,
    force_write: bool,
) -> bool:
    if force_write:
        return True
    previous = source_state.get(source_name, {})
    return str(previous.get("signature") or "") != signature


def _compute_bronze_code_fingerprint() -> str:
    tracked_files = [
        Path(__file__).resolve(),
        (Path(__file__).resolve().parent / "config_snapshot.py"),
    ]
    digest = hashlib.sha256()
    for file_path in tracked_files:
        digest.update(str(file_path).encode("utf-8"))
        digest.update(b"\0")
        try:
            digest.update(file_path.read_bytes())
        except FileNotFoundError:
            digest.update(b"<missing>")
        digest.update(b"\0")
    return digest.hexdigest()


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


def _get_pokebase_payload(endpoint: str, resource_name_or_id: str | int | None = None) -> dict[str, object]:
    try:
        payload = pokebase_get_data(endpoint, resource_name_or_id)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _resource_id_from_url(resource_url: str) -> int | None:
    url = str(resource_url or "").strip()
    match = re.search(r"/(\d+)/?$", url)
    if match is None:
        return None
    try:
        return int(match.group(1))
    except (TypeError, ValueError):
        return None


def _get_payload_with_id_fallback(endpoint: str, primary: str | int | None, fallback_id: int | None) -> dict[str, object]:
    payload = _get_pokebase_payload(endpoint, primary)
    if payload:
        return payload
    if fallback_id is None:
        return {}
    return _get_pokebase_payload(endpoint, fallback_id)


def _fetch_pokeapi_location_index() -> dict[str, object]:
    location_payload = _get_pokebase_payload("location")
    results = location_payload.get("results", [])

    area_payload = _get_pokebase_payload("location-area")
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


def _fetch_capture_rate(pokemon_url: str, capture_rate_cache: dict[str, int | None]) -> int | None:
    if not pokemon_url:
        return None
    if pokemon_url in capture_rate_cache:
        return capture_rate_cache[pokemon_url]
    try:
        pokemon_id = pokemon_url.rstrip("/").split("/")[-1]
        payload = _get_pokebase_payload("pokemon-species", pokemon_id)
        value = payload.get("capture_rate")
        capture_rate_cache[pokemon_url] = int(value) if isinstance(value, int) else None
    except Exception:
        capture_rate_cache[pokemon_url] = None
    return capture_rate_cache[pokemon_url]


def _build_location_pokemon_snapshot(location_index: dict[str, object]) -> dict[str, object]:
    diagnostics: dict[str, object] = {"location_errors": [], "area_errors": [], "capture_rate_errors": []}
    payload: dict[str, dict[str, object]] = {}
    capture_rate_cache: dict[str, int | None] = {}
    snapshot_started_at = time.perf_counter()
    progress_interval = 25
    area_id_by_name: dict[str, int] = {}
    area_rows = location_index.get("location_area_results", [])
    for area_row in area_rows if isinstance(area_rows, list) else []:
        area_name = str((area_row or {}).get("name") or "").strip()
        area_id = _resource_id_from_url(str((area_row or {}).get("url") or ""))
        if area_name and area_id is not None:
            area_id_by_name[area_name] = area_id

    location_rows = location_index.get("results", [])
    safe_location_rows = location_rows if isinstance(location_rows, list) else []
    total_locations = len(safe_location_rows)
    for idx, location in enumerate(safe_location_rows, start=1):
        slug = str((location or {}).get("name") or "").strip()
        if idx == 1 or idx % progress_interval == 0 or idx == total_locations:
            logger.info(
                "[bronze][pokeapi] snapshot progress=%s/%s current_location=%s",
                idx,
                total_locations,
                slug or "<missing>",
            )
        location_id = _resource_id_from_url(str((location or {}).get("url") or ""))
        if not slug:
            continue
        try:
            location_payload = _get_payload_with_id_fallback("location", slug, location_id)
            if not location_payload:
                cast(list[dict[str, object]], diagnostics["location_errors"]).append({"slug": slug, "status": 404})
                continue
            area_names = [str(entry.get("name") or "").strip() for entry in location_payload.get("areas", []) if entry.get("name")]
        except Exception:
            cast(list[dict[str, object]], diagnostics["location_errors"]).append({"slug": slug, "reason": "request_exception"})
            continue

        version_species = defaultdict[str, set[str]](set)
        version_encounters = defaultdict[str, dict[str, dict[str, object]]](dict)
        area_details: dict[str, dict[str, object]] = {}
        for area_name in area_names:
            area_id = area_id_by_name.get(area_name)
            try:
                area_payload = _get_payload_with_id_fallback("location-area", area_name, area_id)
                if not area_payload:
                    cast(list[dict[str, object]], diagnostics["area_errors"]).append({"slug": slug, "area": area_name, "status": 404})
                    continue
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
                    entry["capture_rate"] = _fetch_capture_rate(str(entry.get("pokemon_url") or ""), capture_rate_cache)
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

    logger.info(
        "[bronze][pokeapi] snapshot done locations=%s mapped=%s location_errors=%s area_errors=%s capture_rate_errors=%s elapsed_s=%.2f",
        total_locations,
        len(payload),
        len(cast(list[dict[str, object]], diagnostics["location_errors"])),
        len(cast(list[dict[str, object]], diagnostics["area_errors"])),
        len(cast(list[dict[str, object]], diagnostics["capture_rate_errors"])),
        time.perf_counter() - snapshot_started_at,
    )
    return {"location_pokemon_map": payload, "diagnostics": diagnostics}


def _validate_location_snapshot(snapshot: dict[str, object]) -> None:
    location_map = snapshot.get("location_pokemon_map") if isinstance(snapshot, dict) else None
    if not isinstance(location_map, dict) or not location_map:
        diagnostics = snapshot.get("diagnostics") if isinstance(snapshot, dict) else {}
        location_errors = diagnostics.get("location_errors") if isinstance(diagnostics, dict) else []
        area_errors = diagnostics.get("area_errors") if isinstance(diagnostics, dict) else []
        raise ValueError(
            "Bronze location snapshot generation failed: location_pokemon_map is empty "
            f"location_errors={len(location_errors) if isinstance(location_errors, list) else 'n/a'} "
            f"area_errors={len(area_errors) if isinstance(area_errors, list) else 'n/a'}"
        )


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
    try:
        import kagglehub
        from kagglehub import KaggleDatasetAdapter
    except Exception as exc:
        raise RuntimeError(
            "Kaggle dependency import failed. Reinstall a healthy IPython + kagglehub in this venv, e.g. "
            "`python -m pip install --force-reinstall ipython kagglehub`."
        ) from exc

    logger.info("[bronze][kaggle] start dataset_handle=%s file_path=%s", dataset_handle, file_path or "<auto>")
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
        logger.info("[bronze][kaggle] loading file=%s", selected_file)
        table_output_path = kaggle_output_dir / "gym_leaders_elite_four.csv"
        source_file = dataset_dir / selected_file
        if source_file.suffix.lower() == ".csv":
            with source_file.open("r", encoding="utf-8", newline="") as handle:
                first_line = handle.readline()
                if first_line:
                    columns = [column.strip() for column in first_line.rstrip("\n\r").split(",")]
                row_count = sum(1 for _ in handle)
            shutil.copy2(source_file, table_output_path)
            logger.info(
                "[bronze][kaggle] copied csv rows=%s columns=%s",
                row_count,
                0 if columns is None else len(columns),
            )
        else:
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
            dataframe.to_csv(table_output_path, index=False)
            row_count = int(len(dataframe))
            columns = list(dataframe.columns)
            logger.info("[bronze][kaggle] loaded rows=%s columns=%s", row_count, len(columns))
    else:
        logger.warning("[bronze][kaggle] no tabular file detected; skipped dataframe export")

    metadata = {
        "dataset_handle": dataset_handle,
        "selected_file": selected_file,
        "dataset_files": dataset_files,
        "dataframe_export": str(table_output_path.relative_to(kaggle_output_dir)) if table_output_path else None,
        "row_count": row_count,
        "columns": columns,
    }
    write_json(kaggle_output_dir / "manifest.json", metadata)
    logger.info("[bronze][kaggle] wrote artifacts to %s", kaggle_output_dir)


def fetch_bronze_sources(output_dir: Optional[Path] = None, include_kaggle: bool = True) -> None:
    started_at = now_utc_iso()
    started_at_perf = time.perf_counter()
    ensure_medallion_dirs()
    output = output_dir or BRONZE_DIR
    output.mkdir(parents=True, exist_ok=True)
    write_bronze_config_snapshot(output)
    bulbapedia_dir = output / "bulbapedia"
    pokeapi_dir = output / "pokeapi"
    bulbapedia_dir.mkdir(parents=True, exist_ok=True)
    pokeapi_dir.mkdir(parents=True, exist_ok=True)

    source_state = load_source_state(output)
    state_meta = source_state.get("__meta__", {}) if isinstance(source_state.get("__meta__"), dict) else {}
    current_code_fingerprint = _compute_bronze_code_fingerprint()
    previous_code_fingerprint = str(state_meta.get("code_fingerprint") or "")
    force_rebuild = previous_code_fingerprint != current_code_fingerprint
    updated_sources: list[str] = []
    unchanged_sources: list[str] = []
    errors: list[str] = []

    logger.info("[bronze] start include_kaggle=%s output=%s", include_kaggle, output)
    if force_rebuild:
        logger.info("[bronze] code fingerprint changed -> forcing rebuild")

    session = build_session()

    pokeapi_started_at = time.perf_counter()
    logger.info("[bronze][pokeapi] loading location index")
    location_index = _fetch_pokeapi_location_index()
    logger.info(
        "[bronze][pokeapi] location index loaded locations=%s location_areas=%s",
        len(location_index.get("results", [])) if isinstance(location_index.get("results"), list) else "n/a",
        len(location_index.get("location_area_results", [])) if isinstance(location_index.get("location_area_results"), list) else "n/a",
    )
    location_signature = stable_signature(location_index)
    if _should_write_source_with_force(source_state, "pokeapi:location_index", location_signature, force_write=force_rebuild):
        write_json(pokeapi_dir / "location_index.json", location_index)
        logger.info("[bronze][pokeapi] building location snapshot")
        location_snapshot = _build_location_pokemon_snapshot(location_index)
        _validate_location_snapshot(location_snapshot)
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
        logger.info("[bronze][pokeapi] wrote location index + snapshot")
    else:
        unchanged_sources.append("pokeapi:location_index")
        logger.info("[bronze][pokeapi] unchanged, skipped write")
    logger.info("[bronze][pokeapi] done elapsed_s=%.2f", time.perf_counter() - pokeapi_started_at)

    game_configs = get_games_config()
    logger.info("[bronze][bulbapedia] processing games=%s", len(game_configs))
    for idx, config in enumerate(game_configs, start=1):
        game_key = config["game_key"]
        game_started_at = time.perf_counter()
        logger.info("[bronze][bulbapedia][%s/%s] loading game_key=%s", idx, len(game_configs), game_key)
        resolved_root_title = resolve_existing_root_title(session, config["candidate_root_titles"])
        if not resolved_root_title:
            logger.warning("[bronze][bulbapedia][%s] skip: no matching walkthrough title", game_key)
            errors.append(f"missing walkthrough title: {game_key}")
            continue

        logger.info("[bronze][bulbapedia][%s] resolved_root_title=%s", game_key, resolved_root_title)

        parts = get_walkthrough_parts(session, resolved_root_title)
        logger.info("[bronze][bulbapedia][%s] parts_detected=%s", game_key, len(parts))
        records: list[dict] = []
        for part_idx, part in enumerate(parts, start=1):
            if part_idx == 1 or part_idx % 5 == 0 or part_idx == len(parts):
                logger.info(
                    "[bronze][bulbapedia][%s] progress part=%s/%s",
                    game_key,
                    part_idx,
                    len(parts),
                )
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
        if _should_write_source_with_force(source_state, source_name, signature, force_write=force_rebuild):
            output_path = bulbapedia_dir / f"{game_key}.json"
            write_json(output_path, payload)
            source_state[source_name] = BronzeSourceState(
                source=source_name,
                signature=signature,
                updated_at_utc=now_utc_iso(),
                output_paths=[str(output_path.relative_to(output))],
            ).as_dict()
            updated_sources.append(source_name)
            logger.info("[bronze][bulbapedia][%s] wrote parts=%s", game_key, len(records))
        else:
            unchanged_sources.append(source_name)
            logger.info("[bronze][bulbapedia][%s] unchanged, skipped write", game_key)
        logger.info("[bronze][bulbapedia][%s] done elapsed_s=%.2f", game_key, time.perf_counter() - game_started_at)

    if include_kaggle:
        kaggle_started_at = time.perf_counter()
        fetch_kaggle_gym_leaders_dataset(output)
        kaggle_manifest_path = output / "kagglehub" / "manifest.json"
        if kaggle_manifest_path.exists():
            kaggle_manifest = kaggle_manifest_path.read_text(encoding="utf-8")
            kaggle_signature = stable_signature(kaggle_manifest)
            source_name = "kagglehub:gym_leaders"
            if _should_write_source_with_force(source_state, source_name, kaggle_signature, force_write=force_rebuild):
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
            logger.info("[bronze][kaggle] source processed elapsed_s=%.2f", time.perf_counter() - kaggle_started_at)

    source_state["__meta__"] = {
        "code_fingerprint": current_code_fingerprint,
        "updated_at_utc": now_utc_iso(),
    }

    save_source_state(source_state, output)
    write_bronze_run_manifest(
        started_at_utc=started_at,
        finished_at_utc=now_utc_iso(),
        updated_sources=updated_sources,
        unchanged_sources=unchanged_sources,
        errors=errors,
        bronze_dir=output,
    )
    logger.info(
        "[bronze] done updated=%s unchanged=%s errors=%s elapsed_s=%.2f",
        len(updated_sources),
        len(unchanged_sources),
        len(errors),
        time.perf_counter() - started_at_perf,
    )

if __name__ == "__main__":
    fetch_bronze_sources()
