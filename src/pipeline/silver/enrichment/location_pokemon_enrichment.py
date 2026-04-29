from collections import defaultdict
from pathlib import Path
from typing import Any

from tqdm import tqdm

from src.pipeline.common.io import read_json, write_json
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR


_EXPECTED_PLACEHOLDER_LOCATION_SLUGS = {
    "fork-in-the-road",
    "roadblock",
}


def _normalize_version_key(version_name: str) -> str:
    return str(version_name or "").strip().lower()


def _version_candidates(game_version: str) -> list[str]:
    game = _normalize_version_key(game_version)
    if not game:
        return []
    return [game]


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


def _is_expected_placeholder_slug(location_slug: str) -> bool:
    slug = _normalize_version_key(location_slug)
    if not slug:
        return False
    if slug in _EXPECTED_PLACEHOLDER_LOCATION_SLUGS:
        return True
    if slug.startswith("cave-") and slug.split("-")[-1].isdigit():
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

    return []


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

    return []


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
    allowed_versions: set[str] | None = None,
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> tuple[dict[str, list[str]], dict[str, dict[str, Any]]]:
    state_dir = silver_dir / "_state"
    snapshot_path = bronze_dir / "pokeapi" / "location_pokemon_snapshot.json"
    if not snapshot_path.exists():
        raise FileNotFoundError(
            f"Missing Bronze location snapshot: {snapshot_path}. Run bronze fetch before Silver."
        )
    snapshot = read_json(snapshot_path)
    snapshot_map = (snapshot or {}).get("location_pokemon_map", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(snapshot_map, dict):
        snapshot_map = {}

    area_map: dict[str, list[str]] = {}
    location_pokemon_map: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, Any] = {
        "resolver": "location_pokemon_enrichment_v2",
        "locations_total": 0,
        "location_errors": [],
        "placeholder_locations": [],
        "area_errors": [],
        "capture_rate_errors": [],
    }

    unique_locations = sorted(set(location_slugs))
    diagnostics["locations_total"] = len(unique_locations)
    for slug in tqdm(unique_locations, desc="[silver] mapping locations + pokemon"):
        slug_payload = snapshot_map.get(slug)
        if not isinstance(slug_payload, dict):
            if _is_expected_placeholder_slug(slug):
                diagnostics["placeholder_locations"].append(
                    {
                        "location_slug": slug,
                        "reason": "missing_in_bronze_snapshot",
                        "expected_placeholder_slug": True,
                    }
                )
            else:
                diagnostics["location_errors"].append({"location_slug": slug, "reason": "missing_in_bronze_snapshot"})
            area_map[slug] = []
            location_pokemon_map[slug] = {"all": [], "by_version": {}, "all_encounters": [], "by_version_encounters": {}, "areas": [], "areas_detail": {}}
            continue
        area_map[slug] = [str(area) for area in slug_payload.get("areas", []) if isinstance(area, str)]

        version_species = slug_payload.get("by_version", {}) if isinstance(slug_payload.get("by_version", {}), dict) else {}
        version_encounters_by_species = (
            slug_payload.get("by_version_encounters", {})
            if isinstance(slug_payload.get("by_version_encounters", {}), dict)
            else {}
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
        by_version_encounters: dict[str, list[dict[str, Any]]] = {}
        for version_name, species_map in sorted(version_encounters_by_species.items()):
            if version_name not in filtered_versions or version_name == "all":
                continue
            if isinstance(species_map, dict):
                by_version_encounters[version_name] = [
                    _serialize_encounter_entry(entry)
                    for _, entry in sorted(species_map.items())
                ]
            elif isinstance(species_map, list):
                by_version_encounters[version_name] = [entry for entry in species_map if isinstance(entry, dict)]
        location_pokemon_map[slug] = {
            "all": [str(species) for species in slug_payload.get("all", []) if isinstance(species, str)],
            "by_version": by_version_species,
            "all_encounters": list(slug_payload.get("all_encounters", [])),
            "by_version_encounters": by_version_encounters,
            "areas": sorted(area_map[slug]),
            "areas_detail": dict(slug_payload.get("areas_detail", {})),
        }

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
        "placeholder_location_misses": [],
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
                diagnostic_row = {
                    "boss_id": record.get("boss_id"),
                    "version": version,
                    "location_slug": slug,
                    "reason": "missing_slug_payload",
                }
                if _is_expected_placeholder_slug(str(slug)):
                    diagnostics["placeholder_location_misses"].append(
                        {
                            **diagnostic_row,
                            "expected_placeholder_slug": True,
                        }
                    )
                else:
                    diagnostics["location_misses"].append(diagnostic_row)
            version_to_species = slug_payload.get("by_version", {})
            species = _pick_species_for_version(version_to_species, version)
            version_to_encounters = slug_payload.get("by_version_encounters", {})
            encounters = _pick_encounters_for_version(version_to_encounters, version)
            area_details = slug_payload.get("areas_detail", {})
            area_encounters_for_record: dict[str, list[dict[str, Any]]] = {}
            if isinstance(area_details, dict):
                for area_slug in sorted(area_details):
                    payload = area_details.get(area_slug, {})
                    by_ver = payload.get("by_version_encounters", {}) if isinstance(payload, dict) else {}
                    area_specific = _pick_encounters_for_version(by_ver, version)
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
