import time
from collections import defaultdict
from typing import Any

from tqdm import tqdm

from src.pipeline.common.http import build_session
from src.pipeline.settings import POKEAPI


def _extract_area_species_by_version(area_payload: dict[str, Any]) -> dict[str, set[str]]:
    by_version: dict[str, set[str]] = defaultdict(set)
    for encounter in area_payload.get("pokemon_encounters", []):
        species_name = (encounter.get("pokemon") or {}).get("name")
        if not species_name:
            continue

        version_details = encounter.get("version_details", [])
        if not version_details:
            by_version["all"].add(species_name)
            continue

        for detail in version_details:
            version_name = ((detail.get("version") or {}).get("name") or "").strip()
            if version_name:
                by_version[version_name].add(species_name)
                by_version["all"].add(species_name)
    return by_version


def _aggregate_area_encounters_by_version(
    area_payload: dict[str, Any],
) -> dict[str, dict[str, dict[str, Any]]]:
    by_version: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)

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
            version_name = ((version_detail.get("version") or {}).get("name") or "all").strip() or "all"
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
                    "encounter_methods": set(),
                    "encounter_method_urls": set(),
                },
            )

            for detail in details:
                min_level = detail.get("min_level")
                max_level = detail.get("max_level")
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
        "encounter_methods": sorted(entry["encounter_methods"]),
        "encounter_method_urls": sorted(entry["encounter_method_urls"]),
    }


def _pick_species_for_version(version_to_species: dict[str, list[str]], game_version: str) -> list[str]:
    direct = version_to_species.get(game_version)
    if direct:
        return direct

    # PokeAPI liefert teils gruppierte Versionsnamen wie "red-blue".
    # Diese werden dynamisch aus den vorhandenen by_version-Keys erkannt.
    for candidate_key in sorted(version_to_species):
        if "-" not in candidate_key:
            continue
        if game_version in candidate_key.split("-") and version_to_species.get(candidate_key):
            return version_to_species[candidate_key]

    return version_to_species.get("all", [])


def _pick_encounters_for_version(
    version_to_encounters: dict[str, list[dict[str, Any]]],
    game_version: str,
) -> list[dict[str, Any]]:
    direct = version_to_encounters.get(game_version)
    if direct:
        return direct

    for candidate_key in sorted(version_to_encounters):
        if "-" not in candidate_key:
            continue
        if game_version in candidate_key.split("-") and version_to_encounters.get(candidate_key):
            return version_to_encounters[candidate_key]

    return version_to_encounters.get("all", [])


def get_location_area_and_pokemon_maps(
    location_slugs: list[str],
    throttle_seconds: float = 0.1,
    allowed_versions: set[str] | None = None,
) -> tuple[dict[str, list[str]], dict[str, dict[str, Any]]]:
    session = build_session()
    area_map: dict[str, list[str]] = {}
    location_pokemon_map: dict[str, dict[str, Any]] = {}

    unique_locations = sorted(set(location_slugs))
    for slug in tqdm(unique_locations, desc="[silver] mapping locations + pokemon"):
        version_species: dict[str, set[str]] = defaultdict(set)
        version_encounters_by_species: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        area_names: list[str] = []

        try:
            response = session.get(f"{POKEAPI}/location/{slug}", timeout=10)
            if response.status_code == 200:
                payload = response.json()
                area_names = [entry["name"] for entry in payload.get("areas", []) if entry.get("name")]
        except Exception:
            area_names = []

        for area_name in area_names:
            try:
                area_response = session.get(f"{POKEAPI}/location-area/{area_name}", timeout=10)
                if area_response.status_code != 200:
                    continue
                area_payload = area_response.json()
                area_species = _extract_area_species_by_version(area_payload)
                area_encounters = _aggregate_area_encounters_by_version(area_payload)
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
            except Exception:
                continue

            time.sleep(throttle_seconds)

        area_map[slug] = area_names

        # Filter versions to only include allowed_versions if specified
        filtered_versions = version_species.keys()
        if allowed_versions:
            filtered_versions = [v for v in filtered_versions if v in allowed_versions or v == "all"]

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
        }

        time.sleep(throttle_seconds)

    return area_map, location_pokemon_map


def enrich_records_with_location_pokemon(
    records: list[dict],
    location_pokemon_map: dict[str, dict[str, Any]],
) -> None:
    for record in records:
        version = record.get("version") or record.get("game")
        location_to_species: dict[str, list[str]] = {}
        location_to_encounters: dict[str, list[dict[str, Any]]] = {}
        unique_species: set[str] = set()

        for slug in record.get("reachable_locations", []):
            slug_payload = location_pokemon_map.get(slug, {})
            version_to_species = slug_payload.get("by_version", {})
            species = _pick_species_for_version(version_to_species, version)
            if not species:
                species = slug_payload.get("all", [])
            version_to_encounters = slug_payload.get("by_version_encounters", {})
            encounters = _pick_encounters_for_version(version_to_encounters, version)
            if not encounters:
                encounters = slug_payload.get("all_encounters", [])
            location_to_species[slug] = species
            location_to_encounters[slug] = encounters
            unique_species.update(species)

        record["reachable_location_pokemon"] = location_to_species
        record["reachable_location_encounters"] = location_to_encounters
        record["reachable_pokemon_count"] = len(unique_species)




