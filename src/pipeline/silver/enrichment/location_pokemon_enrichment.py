import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from tqdm import tqdm

from src.pipeline.common.http import build_session
from src.pipeline.common.io import read_json, write_json
from src.pipeline.settings import POKEAPI
from src.pipeline.settings import SILVER_DIR


def _normalize_version_key(version_name: str) -> str:
    return str(version_name or "").strip().lower()


def _version_candidates(game_version: str) -> list[str]:
    game = _normalize_version_key(game_version)
    if not game:
        return ["all"]
    return [game, "all"]


def _version_matches(version_name: str, game_version: str) -> bool:
    key = _normalize_version_key(version_name)
    game = _normalize_version_key(game_version)
    if key == "all":
        return True
    if key == game:
        return True
    if "-" in key and game in key.split("-"):
        return True
    return False


def _extract_area_species_by_version(area_payload: dict[str, Any]) -> dict[str, set[str]]:
    by_version = defaultdict[str, set[str]](set)
    for encounter in area_payload.get("pokemon_encounters", []):
        species_name_raw = (encounter.get("pokemon") or {}).get("name")
        if not isinstance(species_name_raw, str) or not species_name_raw:
            continue
        species_name = species_name_raw

        version_details = encounter.get("version_details", [])
        if not version_details:
            by_version["all"].add(species_name)
            continue

        for detail in version_details:
            version_name = ((detail.get("version") or {}).get("name") or "").strip()
            if version_name:
                by_version[_normalize_version_key(version_name)].add(species_name)
                by_version["all"].add(species_name)
    return by_version


def _aggregate_area_encounters_by_version(
    area_payload: dict[str, Any],
) -> dict[str, dict[str, dict[str, Any]]]:
    by_version = defaultdict[str, dict[str, dict[str, Any]]](dict)

    for encounter in area_payload.get("pokemon_encounters", []):
        pokemon = encounter.get("pokemon") or {}
        species_name = (pokemon.get("name") or "").strip()
        pokemon_url = (pokemon.get("url") or "").strip()
        if not species_name:
            continue

        version_details = encounter.get("version_details", [])
        if not version_details:
            version_details = [{"version": {"name": "all"}, "encounter_details": []}]

        for version_detail in version_details:
            version_name = _normalize_version_key(((version_detail.get("version") or {}).get("name") or "all").strip() or "all")
            details = version_detail.get("encounter_details", [])
            if not details:
                details = [{}]

            entry = by_version[version_name].setdefault(
                species_name,
                {
                    "species": species_name,
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
            all_entry = by_version["all"].setdefault(
                species_name,
                {
                    "species": species_name,
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

            for detail in details:
                min_level = detail.get("min_level")
                max_level = detail.get("max_level")
                chance = detail.get("chance")
                method = detail.get("method") or {}
                method_name = (method.get("name") or "").strip()
                method_url = (method.get("url") or "").strip()

                for target in (entry, all_entry):
                    if isinstance(min_level, int):
                        if target["level_min"] is None or min_level < target["level_min"]:
                            target["level_min"] = min_level
                    if isinstance(max_level, int):
                        if target["level_max"] is None or max_level > target["level_max"]:
                            target["level_max"] = max_level
                    if isinstance(chance, int):
                        if target["encounter_chance_min"] is None or chance < target["encounter_chance_min"]:
                            target["encounter_chance_min"] = chance
                        if target["encounter_chance_max"] is None or chance > target["encounter_chance_max"]:
                            target["encounter_chance_max"] = chance
                    if method_name:
                        target["encounter_methods"].add(method_name)
                    if method_url:
                        target["encounter_method_urls"].add(method_url)

    return by_version


def _serialize_encounter_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "species": entry["species"],
        "pokemon_url": entry["pokemon_url"],
        "level_min": entry["level_min"],
        "level_max": entry["level_max"],
        "encounter_chance_min": entry.get("encounter_chance_min"),
        "encounter_chance_max": entry.get("encounter_chance_max"),
        "capture_rate": entry.get("capture_rate"),
        "encounter_methods": sorted(entry["encounter_methods"]),
        "encounter_method_urls": sorted(entry["encounter_method_urls"]),
    }


def _fetch_capture_rate(
    session: Any,
    pokemon_url: str,
    capture_rate_cache: dict[str, int | None],
) -> int | None:
    if not pokemon_url:
        return None
    if pokemon_url in capture_rate_cache:
        return capture_rate_cache[pokemon_url]

    try:
        pokemon_id = pokemon_url.rstrip("/").split("/")[-1]
        species_url = f"{POKEAPI}/pokemon-species/{pokemon_id}"
        response = session.get(species_url, timeout=10)
        if response.status_code == 200:
            payload = response.json()
            value = payload.get("capture_rate")
            capture_rate_cache[pokemon_url] = int(value) if isinstance(value, int) else None
        else:
            capture_rate_cache[pokemon_url] = None
    except Exception:
        capture_rate_cache[pokemon_url] = None

    return capture_rate_cache[pokemon_url]


def _pick_species_for_version(version_to_species: dict[str, list[str]], game_version: str) -> list[str]:
    game_key = _normalize_version_key(game_version)
    direct = version_to_species.get(game_key)
    if direct:
        return direct

    # PokeAPI liefert teils gruppierte Versionsnamen wie "red-blue".
    # Diese werden dynamisch aus den vorhandenen by_version-Keys erkannt.
    for candidate_key in sorted(version_to_species):
        if "-" not in candidate_key:
            continue
        if game_key in candidate_key.split("-") and version_to_species.get(candidate_key):
            return version_to_species[candidate_key]

    return version_to_species.get("all", [])


def _pick_encounters_for_version(
    version_to_encounters: dict[str, list[dict[str, Any]]],
    game_version: str,
) -> list[dict[str, Any]]:
    game_key = _normalize_version_key(game_version)
    direct = version_to_encounters.get(game_key)
    if direct:
        return direct

    for candidate_key in sorted(version_to_encounters):
        if "-" not in candidate_key:
            continue
        if game_key in candidate_key.split("-") and version_to_encounters.get(candidate_key):
            return version_to_encounters[candidate_key]

    return version_to_encounters.get("all", [])


def _load_location_cache(cache_path: Path) -> dict[str, dict[str, Any]]:
    if not cache_path.exists():
        return {}
    try:
        loaded = read_json(cache_path)
        return loaded if isinstance(loaded, dict) else {}
    except Exception:
        return {}


def _save_location_cache(cache_path: Path, payload: dict[str, dict[str, Any]]) -> None:
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(cache_path, payload)


def get_location_area_and_pokemon_maps(
    location_slugs: list[str],
    throttle_seconds: float = 0.1,
    allowed_versions: set[str] | None = None,
    silver_dir: Path = SILVER_DIR,
) -> tuple[dict[str, list[str]], dict[str, dict[str, Any]]]:
    session = build_session()
    state_dir = silver_dir / "_state"
    location_cache_path = state_dir / "location_pokemon_cache.json"
    capture_rate_cache_path = state_dir / "capture_rate_cache.json"

    location_cache = _load_location_cache(location_cache_path)
    loaded_capture_rate_cache = _load_location_cache(capture_rate_cache_path)
    capture_rate_cache: dict[str, int | None] = {
        str(key): (int(value) if isinstance(value, int) else None)
        for key, value in loaded_capture_rate_cache.items()
    }

    area_map: dict[str, list[str]] = {}
    location_pokemon_map: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, Any] = {
        "resolver": "location_pokemon_enrichment_v2",
        "locations_total": 0,
        "location_errors": [],
        "area_errors": [],
        "capture_rate_errors": [],
    }

    unique_locations = sorted(set(location_slugs))
    diagnostics["locations_total"] = len(unique_locations)
    for slug in tqdm(unique_locations, desc="[silver] mapping locations + pokemon"):
        cached_location = location_cache.get(slug)
        if isinstance(cached_location, dict):
            area_map[slug] = list(cached_location.get("area_names", []))
            location_pokemon_map[slug] = dict(cached_location.get("payload", {}))
            continue

        version_species = defaultdict[str, set[str]](set)
        version_encounters_by_species = defaultdict[str, dict[str, dict[str, Any]]](dict)
        area_details: dict[str, dict[str, Any]] = {}
        area_names: list[str] = []

        try:
            response = session.get(f"{POKEAPI}/location/{slug}", timeout=10)
            if response.status_code == 200:
                payload = response.json()
                area_names = [entry["name"] for entry in payload.get("areas", []) if entry.get("name")]
            else:
                diagnostics["location_errors"].append(
                    {"location_slug": slug, "status_code": int(response.status_code), "reason": "location_http_error"}
                )
        except Exception:
            diagnostics["location_errors"].append({"location_slug": slug, "reason": "location_request_exception"})
            area_names = []

        for area_name in area_names:
            try:
                area_response = session.get(f"{POKEAPI}/location-area/{area_name}", timeout=10)
                if area_response.status_code != 200:
                    diagnostics["area_errors"].append(
                        {
                            "location_slug": slug,
                            "area_slug": area_name,
                            "status_code": int(area_response.status_code),
                            "reason": "location_area_http_error",
                        }
                    )
                    continue
                area_payload = area_response.json()
                area_species = _extract_area_species_by_version(area_payload)
                area_encounters = _aggregate_area_encounters_by_version(area_payload)
                area_details[area_name] = {
                    "by_version": {
                        version_name: sorted(species_set)
                        for version_name, species_set in sorted(area_species.items())
                        if version_name != "all"
                    },
                    "all": sorted(area_species.get("all", set())),
                    "by_version_encounters": {
                        version_name: [
                            _serialize_encounter_entry(entry)
                            for _, entry in sorted(species_map.items())
                        ]
                        for version_name, species_map in sorted(area_encounters.items())
                        if version_name != "all"
                    },
                    "all_encounters": [
                        _serialize_encounter_entry(entry)
                        for _, entry in sorted(area_encounters.get("all", {}).items())
                    ],
                }
                for version_name, species_set in area_species.items():
                    version_species[version_name].update(species_set)
                for version_name, entries in area_encounters.items():
                    for species_name, entry in entries.items():
                        existing = version_encounters_by_species[version_name].get(species_name)
                        if not existing:
                            version_encounters_by_species[version_name][species_name] = entry
                            continue

                        if entry["level_min"] is not None and (
                            existing["level_min"] is None or entry["level_min"] < existing["level_min"]
                        ):
                            existing["level_min"] = entry["level_min"]
                        if entry["level_max"] is not None and (
                            existing["level_max"] is None or entry["level_max"] > existing["level_max"]
                        ):
                            existing["level_max"] = entry["level_max"]
                        existing["encounter_methods"].update(entry["encounter_methods"])
                        existing["encounter_method_urls"].update(entry["encounter_method_urls"])
                        if entry.get("encounter_chance_min") is not None:
                            if existing.get("encounter_chance_min") is None or entry["encounter_chance_min"] < existing["encounter_chance_min"]:
                                existing["encounter_chance_min"] = entry["encounter_chance_min"]
                        if entry.get("encounter_chance_max") is not None:
                            if existing.get("encounter_chance_max") is None or entry["encounter_chance_max"] > existing["encounter_chance_max"]:
                                existing["encounter_chance_max"] = entry["encounter_chance_max"]
                        if existing.get("capture_rate") is None:
                            existing["capture_rate"] = entry.get("capture_rate")
            except Exception:
                diagnostics["area_errors"].append(
                    {"location_slug": slug, "area_slug": area_name, "reason": "location_area_request_exception"}
                )
                continue

            time.sleep(throttle_seconds)

        area_map[slug] = area_names

        for species_map in version_encounters_by_species.values():
            for entry in species_map.values():
                if entry.get("capture_rate") is None:
                    entry["capture_rate"] = _fetch_capture_rate(session, str(entry.get("pokemon_url") or ""), capture_rate_cache)
                    if entry["capture_rate"] is None:
                        diagnostics["capture_rate_errors"].append(
                            {
                                "location_slug": slug,
                                "species": entry.get("species"),
                                "pokemon_url": entry.get("pokemon_url"),
                                "reason": "capture_rate_unavailable",
                            }
                        )

        # Filter versions to only include allowed_versions if specified
        filtered_versions = set(version_species.keys())
        if allowed_versions:
            expanded_allowed = set(allowed_versions)
            for known_version in list(version_species.keys()):
                if "-" in known_version and any(_version_matches(known_version, allowed) for allowed in allowed_versions):
                    expanded_allowed.add(known_version)
            filtered_versions = {v for v in filtered_versions if v in expanded_allowed or v == "all"}

        by_version_species = {
            version_name: sorted(species_set)
            for version_name, species_set in sorted(version_species.items())
            if version_name in filtered_versions and version_name != "all"
        }
        by_version_encounters = {
            version_name: [
                _serialize_encounter_entry(entry)
                for _, entry in sorted(species_map.items())
            ]
            for version_name, species_map in sorted(version_encounters_by_species.items())
            if version_name in filtered_versions and version_name != "all"
        }
        location_pokemon_map[slug] = {
            "all": sorted(version_species.get("all", set())),
            "by_version": by_version_species,
            "all_encounters": [
                _serialize_encounter_entry(entry)
                for _, entry in sorted(version_encounters_by_species.get("all", {}).items())
            ],
            "by_version_encounters": by_version_encounters,
            "areas": sorted(area_names),
            "areas_detail": {name: area_details[name] for name in sorted(area_details)},
        }

        location_cache[slug] = {
            "area_names": area_map[slug],
            "payload": location_pokemon_map[slug],
        }

        time.sleep(throttle_seconds)

    _save_location_cache(location_cache_path, location_cache)
    _save_location_cache(capture_rate_cache_path, {k: v for k, v in capture_rate_cache.items() if k})
    _save_location_cache(state_dir / "location_enrichment_diagnostics.json", diagnostics)

    return area_map, location_pokemon_map


def enrich_records_with_location_pokemon(
    records: list[dict],
    location_pokemon_map: dict[str, dict[str, Any]],
) -> None:
    diagnostics: dict[str, Any] = {
        "resolver": "location_record_enrichment_v2",
        "records": len(records),
        "location_misses": [],
        "version_fallbacks": [],
    }
    for record in records:
        version = _normalize_version_key(str(record.get("version") or record.get("game") or ""))
        location_to_species: dict[str, list[str]] = {}
        location_to_encounters: dict[str, list[dict[str, Any]]] = {}
        location_to_area_encounters: dict[str, dict[str, list[dict[str, Any]]]] = {}
        unique_species: set[str] = set()

        for slug in record.get("reachable_locations", []):
            slug_payload = location_pokemon_map.get(slug, {})
            if not slug_payload:
                diagnostics["location_misses"].append(
                    {
                        "boss_id": record.get("boss_id"),
                        "version": version,
                        "location_slug": slug,
                        "reason": "missing_slug_payload",
                    }
                )
            version_to_species = slug_payload.get("by_version", {})
            species = _pick_species_for_version(version_to_species, version)
            if not species:
                species = slug_payload.get("all", [])
                diagnostics["version_fallbacks"].append(
                    {
                        "boss_id": record.get("boss_id"),
                        "version": version,
                        "location_slug": slug,
                        "kind": "species",
                        "fallback": "all",
                    }
                )
            version_to_encounters = slug_payload.get("by_version_encounters", {})
            encounters = _pick_encounters_for_version(version_to_encounters, version)
            if not encounters:
                encounters = slug_payload.get("all_encounters", [])
                diagnostics["version_fallbacks"].append(
                    {
                        "boss_id": record.get("boss_id"),
                        "version": version,
                        "location_slug": slug,
                        "kind": "encounters",
                        "fallback": "all_encounters",
                    }
                )
            area_details = slug_payload.get("areas_detail", {})
            area_encounters_for_record: dict[str, list[dict[str, Any]]] = {}
            if isinstance(area_details, dict):
                for area_slug in sorted(area_details):
                    payload = area_details.get(area_slug, {})
                    by_ver = payload.get("by_version_encounters", {}) if isinstance(payload, dict) else {}
                    area_specific = _pick_encounters_for_version(by_ver, version)
                    if not area_specific and isinstance(payload, dict):
                        area_specific = payload.get("all_encounters", [])
                    if area_specific:
                        area_encounters_for_record[area_slug] = area_specific
            location_to_species[slug] = species
            location_to_encounters[slug] = encounters
            location_to_area_encounters[slug] = area_encounters_for_record
            unique_species.update(species)

        record["reachable_location_pokemon"] = location_to_species
        record["reachable_location_encounters"] = location_to_encounters
        record["reachable_location_area_encounters"] = location_to_area_encounters
        record["reachable_pokemon_count"] = len(unique_species)
        record["enrichment_diagnostics"] = {
            "resolver": "location_record_enrichment_v2",
            "version_candidates": _version_candidates(version),
            "reachable_locations": len(record.get("reachable_locations", [])),
            "reachable_species": len(unique_species),
        }
