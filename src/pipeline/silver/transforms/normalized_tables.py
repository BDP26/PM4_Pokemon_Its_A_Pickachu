"""Build normalized Silver tables for references, progression, and teams."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

from src.pipeline.silver.config.boss_config import STRIATON_CONDITIONAL_BOSSES, boss_id
from src.pipeline.silver.config.team_config import GAME_TO_VERSION_GROUP
from src.pipeline.silver.move_power import resolve_effective_power
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

NON_SIMULATABLE_BOSSES_BY_GAME: dict[str, set[str]] = {}


def build_games_table(games_config: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen_versions: set[str] = set()
    for game in games_config:
        version = str(game.get("game_key") or "").strip().lower()
        if not version:
            continue
        seen_versions.add(version)
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
    if {"black", "white"}.issubset(seen_versions):
        rows.append(
            {
                "game_version": "black-white",
                "version_group": "black-white",
                "region": "unova",
                "generation": 5,
                "is_supported": True,
            }
        )
    return sorted(rows, key=lambda row: row["game_version"])


def build_bosses_table(boss_mapping_by_version: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    striaton_injected = False
    striaton_names = {
        str(conditional_boss["boss_name"]).strip()
        for conditional_boss in STRIATON_CONDITIONAL_BOSSES
    }
    for game_version, payload in sorted(boss_mapping_by_version.items()):
        mapping_rows = payload.get("boss_mapping", []) if isinstance(payload, dict) else []
        for mapping in mapping_rows:
            if not isinstance(mapping, dict):
                continue
            canonical_name = str(mapping.get("boss_name_canonical") or "").strip()
            if game_version in {"black", "white"} and canonical_name in striaton_names:
                if not striaton_injected:
                    for conditional_boss in STRIATON_CONDITIONAL_BOSSES:
                        conditional_name = str(conditional_boss["boss_name"])
                        conditional_game_version = str(conditional_boss["game_version"])
                        conditional_gym_index = int(conditional_boss["gym_index"])
                        rows.append(
                            {
                                "boss_id": boss_id(conditional_game_version, conditional_name),
                                "game_version": conditional_game_version,
                                "boss_name_canonical": conditional_name,
                                "boss_name_kaggle": str(conditional_boss["boss_name_kaggle"]),
                                "boss_name_aliases": [str(conditional_boss["boss_name_kaggle"]), conditional_name],
                                "boss_role": "gym",
                                "boss_order": conditional_gym_index,
                                "boss_index": conditional_gym_index,
                                "gym_index": conditional_gym_index,
                                "starter_condition": str(conditional_boss["starter_condition"]),
                                "is_simulatable": True,
                            }
                        )
                    striaton_injected = True
                continue
            role = "gym"
            order = int(mapping.get("boss_order") or 0)
            if "champion" in canonical_name.lower() or order == len(mapping_rows):
                role = "champion"
            elif order > max(1, len(mapping_rows) - 4):
                role = "elite_four"
            dataset_candidates = [
                str(candidate).strip()
                for candidate in list(mapping.get("dataset_boss_candidates") or [canonical_name])
                if str(candidate).strip()
            ]
            alias_candidates = list(dict.fromkeys([canonical_name, *dataset_candidates]))
            rows.append(
                {
                    "boss_id": str(mapping.get("boss_id") or "").strip().lower(),
                    "game_version": str(game_version).strip().lower(),
                    "boss_name_canonical": canonical_name,
                    "boss_name_kaggle": dataset_candidates[0] if dataset_candidates else canonical_name,
                    "boss_name_aliases": alias_candidates,
                    "boss_role": role,
                    "boss_order": order,
                    "boss_index": order,
                    "gym_index": order,
                    "starter_condition": None,
                    "is_simulatable": canonical_name not in NON_SIMULATABLE_BOSSES_BY_GAME.get(str(game_version).strip().lower(), set()),
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

    # Keep unmapped misses in diagnostics artifacts only. Including synthetic
    # "unknown:*" rows here pollutes reference FKs with non-game versions.
    _ = misses

    return sorted(rows_by_id.values(), key=lambda row: (row["game_version"], row["location_id"]))

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
            raw_power = detail_dict.get("power")
            if isinstance(raw_power, float) and raw_power != raw_power:
                raw_power = None
            effective_power, power_handling = resolve_effective_power(
                move_name=move_norm,
                power=raw_power,
                damage_class=detail_dict.get("damage_class"),
            )
            rows_by_move[move_norm] = {
                "move_name": move_norm,
                "power": raw_power,
                "raw_power": raw_power,
                "damage_class": str(detail_dict.get("damage_class") or "status"),
                "type": detail_dict.get("type"),
                "accuracy": detail_dict.get("accuracy"),
                "pp": detail_dict.get("pp"),
                "effective_power": detail_dict.get("effective_power", effective_power),
                "power_handling": detail_dict.get("power_handling", power_handling),
                "is_status_move": detail_dict.get("is_status_move", str(detail_dict.get("damage_class") or "").strip().lower() == "status"),
                "is_damage_move": detail_dict.get("is_damage_move", effective_power > 0),
                "is_null_power": detail_dict.get("is_null_power", raw_power is None),
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
        move_levels_raw = payload.get("learnable_move_levels")
        move_levels = move_levels_raw if isinstance(move_levels_raw, dict) else {}
        move_names = move_levels.keys() if move_levels else payload.get("learnable_moves", [])
        for move_name in move_names:
            move_norm = normalize_key_part(move_name)
            if not game_version or not species or not move_norm:
                continue
            dedupe_key = (game_version, species, move_norm)
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            learned_level_raw = move_levels.get(move_name)
            try:
                learned_level = int(learned_level_raw or 0)
            except (TypeError, ValueError):
                learned_level = 0
            rows.append(
                {
                    "game_version": game_version,
                    "pokemon_species": species,
                    "move_name": move_norm,
                    "learned_level": learned_level if learned_level > 0 else 1,
                    "learn_method": "level-up",
                }
            )
    return rows
