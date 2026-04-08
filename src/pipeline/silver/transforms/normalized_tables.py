"""Build normalized Silver tables for references, progression, and teams."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from src.pipeline.silver.config.team_config import GAME_TO_VERSION_GROUP
from src.pipeline.silver.schemas.contracts import BossSnapshotContract
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, normalize_key_part

_REGION_BY_ROUTE_PREFIX: dict[str, str] = {
    "kanto-route": "kanto",
    "johto-route": "johto",
    "hoenn-route": "hoenn",
    "sinnoh-route": "sinnoh",
    "unova-route": "unova",
    "kalos-route": "kalos",
}

_GENERATION_BY_REGION: dict[str, int] = {
    "kanto": 1,
    "johto": 2,
    "hoenn": 3,
    "sinnoh": 4,
    "unova": 5,
    "kalos": 6,
}


def build_games_table(games_config: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for game in games_config:
        version = str(game.get("game_key") or "").strip().lower()
        if not version:
            continue
        route_prefix = str(game.get("route_prefix") or "").strip().lower()
        region = _REGION_BY_ROUTE_PREFIX.get(route_prefix, "unknown")
        rows.append(
            {
                "game_version": version,
                "version_group": GAME_TO_VERSION_GROUP.get(version, version),
                "region": region,
                "generation": _GENERATION_BY_REGION.get(region),
                "is_supported": True,
            }
        )
    return sorted(rows, key=lambda row: row["game_version"])


def build_bosses_table(boss_mapping_by_version: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for game_version, payload in sorted(boss_mapping_by_version.items()):
        mapping_rows = payload.get("boss_mapping", []) if isinstance(payload, dict) else []
        for mapping in mapping_rows:
            if not isinstance(mapping, dict):
                continue
            canonical_name = str(mapping.get("boss_name_canonical") or "").strip()
            role = "gym"
            order = int(mapping.get("boss_order") or 0)
            if "champion" in canonical_name.lower() or order == len(mapping_rows):
                role = "champion"
            elif order > max(1, len(mapping_rows) - 4):
                role = "elite_four"
            rows.append(
                {
                    "boss_id": str(mapping.get("boss_id") or "").strip().lower(),
                    "game_version": str(game_version).strip().lower(),
                    "boss_name_canonical": canonical_name,
                    "boss_name_kaggle": (mapping.get("dataset_boss_candidates") or [canonical_name])[0],
                    "boss_role": role,
                    "boss_order": order,
                }
            )
    return rows


def build_locations_table(
    records: list[dict[str, Any]],
    area_map: dict[str, Any],
    misses: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows_by_id: dict[str, dict[str, Any]] = {}
    for record in records:
        game_version = str(record.get("game") or "").strip().lower()
        for location_slug in record.get("reachable_locations", []):
            location_slug = str(location_slug).strip().lower()
            if not location_slug:
                continue
            location_id = f"{game_version}:{location_slug}"
            mapped_area = area_map.get(location_slug)
            rows_by_id[location_id] = {
                "location_id": location_id,
                "game_version": game_version,
                "walkthrough_location_name": location_slug,
                "normalized_location_name": location_slug,
                "pokeapi_area_slug": mapped_area,
                "mapping_status": "mapped" if mapped_area else "unmapped",
            }

    for miss in misses:
        raw_title = str(miss.get("raw_title") or "").strip()
        tried_slug = str(miss.get("tried_slug") or "").strip().lower()
        if not tried_slug:
            continue
        location_id = f"unknown:{tried_slug}"
        rows_by_id.setdefault(
            location_id,
            {
                "location_id": location_id,
                "game_version": "unknown",
                "walkthrough_location_name": raw_title or tried_slug,
                "normalized_location_name": tried_slug,
                "pokeapi_area_slug": None,
                "mapping_status": "unmapped",
            },
        )

    return sorted(rows_by_id.values(), key=lambda row: (row["game_version"], row["location_id"]))


def build_snapshot_available_pokemon_table(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    for record in records:
        snapshot = BossSnapshotContract.from_record(record)
        location_encounters = record.get("reachable_location_encounters", {})
        if not isinstance(location_encounters, dict):
            continue

        for location_slug, encounter_rows in location_encounters.items():
            if not isinstance(encounter_rows, list):
                continue
            for encounter in encounter_rows:
                if not isinstance(encounter, dict):
                    continue
                species = normalize_key_part(encounter.get("species"))
                if not species:
                    continue
                dedupe_key = (snapshot.snapshot_id, species)
                if dedupe_key in seen:
                    continue
                seen.add(dedupe_key)
                methods = encounter.get("encounter_methods") or []
                rows.append(
                    {
                        "snapshot_id": snapshot.snapshot_id,
                        "game_version": snapshot.version,
                        "boss_id": snapshot.boss_id,
                        "pokemon_species": species,
                        "first_available_location_id": f"{snapshot.version}:{normalize_key_part(location_slug)}",
                        "encounter_method": methods[0] if methods else None,
                        "min_level": encounter.get("level_min"),
                        "max_level": encounter.get("level_max"),
                    }
                )

    return rows


def build_team_members_table(teams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for team in teams:
        team_id = str(team.get("team_id") or "").strip()
        game_version = str(team.get("game_version") or "").strip().lower()
        pokemon = team.get("pokemon", [])
        levels = team.get("levels", [])
        instance_ids = team.get("pokemon_instance_ids", [])
        starter = str(team.get("starter_evolved_species") or "").strip().lower()
        if not isinstance(pokemon, list):
            continue

        for slot, species_raw in enumerate(pokemon, start=1):
            species = normalize_key_part(species_raw)
            if not species:
                continue
            level = int(levels[slot - 1] or team.get("avg_level") or 0) if slot - 1 < len(levels) else int(team.get("avg_level") or 0)
            team_member_id = (
                str(instance_ids[slot - 1]).strip() if slot - 1 < len(instance_ids) and str(instance_ids[slot - 1]).strip()
                else make_pokemon_instance_id(team_id, slot, species)
            )
            rows.append(
                {
                    "team_member_id": team_member_id,
                    "team_id": team_id,
                    "game_version": game_version,
                    "slot": slot,
                    "pokemon_species": species,
                    "level": level,
                    "form": None,
                    "is_starter": bool(starter and species == starter),
                    "nickname": None,
                }
            )
    return rows


def build_team_member_moves_table(teams: list[dict[str, Any]], move_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    move_data_by_member = move_data if isinstance(move_data, dict) else {}

    provided_by_member: dict[str, set[str]] = defaultdict(set)
    learnable_by_member: dict[str, set[str]] = defaultdict(set)
    for member_id, payload in move_data_by_member.items():
        if not isinstance(payload, dict):
            continue
        provided = {normalize_key_part(move) for move in payload.get("provided_moves", []) if str(move).strip()}
        learnable = {normalize_key_part(move) for move in payload.get("learnable_moves", []) if str(move).strip()}
        provided_by_member[str(member_id)] = provided
        learnable_by_member[str(member_id)] = learnable

    for team in teams:
        pokemon = team.get("pokemon", [])
        moves = team.get("moves", [])
        instance_ids = team.get("pokemon_instance_ids", [])
        team_id = str(team.get("team_id") or "").strip()
        game_version = str(team.get("game_version") or "").strip().lower()
        if not isinstance(pokemon, list) or not isinstance(moves, list):
            continue

        for slot, species_raw in enumerate(pokemon, start=1):
            member_id = (
                str(instance_ids[slot - 1]).strip() if slot - 1 < len(instance_ids) and str(instance_ids[slot - 1]).strip()
                else make_pokemon_instance_id(team_id, slot, normalize_key_part(species_raw))
            )
            slot_moves = moves[slot - 1] if slot - 1 < len(moves) and isinstance(moves[slot - 1], list) else []
            for move_slot, move_name in enumerate(slot_moves, start=1):
                move_norm = normalize_key_part(move_name)
                if not move_norm:
                    continue
                rows.append(
                    {
                        "team_member_id": member_id,
                        "team_id": team_id,
                        "game_version": game_version,
                        "move_slot": move_slot,
                        "move_name": move_norm,
                        "is_provided_move": move_norm in provided_by_member.get(member_id, set()) or not provided_by_member.get(member_id),
                        "is_learnable_at_level": move_norm in learnable_by_member.get(member_id, set()) or not learnable_by_member.get(member_id),
                    }
                )
    return rows


def build_move_reference_table(move_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows_by_move: dict[str, dict[str, Any]] = {}
    if not isinstance(move_data, dict):
        return []

    for payload in move_data.values():
        if not isinstance(payload, dict):
            continue
        move_details = payload.get("move_details", {})
        if not isinstance(move_details, dict):
            continue
        for move_name, details in move_details.items():
            move_norm = normalize_key_part(move_name)
            if not move_norm:
                continue
            detail_dict = details if isinstance(details, dict) else {}
            rows_by_move[move_norm] = {
                "move_name": move_norm,
                "power": int(detail_dict.get("power") or 0),
                "damage_class": str(detail_dict.get("damage_class") or "status"),
                "type": detail_dict.get("type"),
                "accuracy": detail_dict.get("accuracy"),
                "pp": detail_dict.get("pp"),
            }
    return sorted(rows_by_move.values(), key=lambda row: row["move_name"])


def build_learnable_moves_table(move_data: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    if not isinstance(move_data, dict):
        return rows

    for payload in move_data.values():
        if not isinstance(payload, dict):
            continue
        game_version = str(payload.get("game_version") or "").strip().lower()
        species = normalize_key_part(payload.get("species"))
        for move_name in payload.get("learnable_moves", []):
            move_norm = normalize_key_part(move_name)
            if not game_version or not species or not move_norm:
                continue
            dedupe_key = (game_version, species, move_norm)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            rows.append(
                {
                    "game_version": game_version,
                    "pokemon_species": species,
                    "move_name": move_norm,
                    "learned_level": None,
                    "learn_method": "level-up",
                }
            )
    return rows

