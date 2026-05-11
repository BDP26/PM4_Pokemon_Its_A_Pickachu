"""Bootstrap entry and move-reference building helpers for Silver."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.silver.config.game_config import get_starter_choices, get_starter_family_members
from src.pipeline.silver.inputs.connectors.pokeapi_evolution import get_species_evolution_rules
from src.pipeline.silver.inputs.reference_context import normalize_move_name, normalize_species_slug
from src.pipeline.silver.move_power import resolve_effective_power
from src.pipeline.silver.transforms.keys import normalize_key_part

logger = logging.getLogger(__name__)


def _move_profiles_from_reference(move_reference_df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    for row in move_reference_df.to_dict(orient="records"):
        move_name = normalize_move_name(row.get("move_name"))
        if not move_name:
            continue
        raw_power = row.get("power")
        if isinstance(raw_power, float) and pd.isna(raw_power):
            raw_power = None
        effective_power, power_handling = resolve_effective_power(
            move_name=move_name,
            power=raw_power,
            damage_class=row.get("damage_class"),
        )
        stored_effective_power = row.get("effective_power", effective_power)
        if isinstance(stored_effective_power, float) and pd.isna(stored_effective_power):
            stored_effective_power = effective_power
        stored_power_handling = row.get("power_handling", power_handling)
        if not isinstance(stored_power_handling, str) or not stored_power_handling.strip():
            stored_power_handling = power_handling
        profiles[move_name] = {
            "move_name": move_name,
            "type": str(row.get("type") or "Normal"),
            "power": raw_power,
            "raw_power": raw_power,
            "damage_class": str(row.get("damage_class") or "status"),
            "accuracy": row.get("accuracy"),
            "pp": row.get("pp"),
            "effective_power": stored_effective_power,
            "power_handling": stored_power_handling,
            "is_status_move": row.get("is_status_move", str(row.get("damage_class") or "").strip().lower() == "status"),
            "is_damage_move": row.get("is_damage_move", effective_power > 0),
            "is_null_power": row.get("is_null_power", raw_power is None),
            "level_learned_at": 0,
            "version_group": "reference",
        }
    return profiles


def _ensure_moves_in_combat_profiles(
    move_data: dict[str, dict[str, Any]],
    required_moves: set[str],
    move_reference_profiles: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    enriched = {key: dict(value) for key, value in move_data.items()}
    if not required_moves:
        return enriched

    available_moves: set[str] = set()
    for payload in enriched.values():
        details = payload.get("move_details")
        if not isinstance(details, dict):
            continue
        for move in details.keys():
            move_name = normalize_move_name(move)
            if move_name:
                available_moves.add(move_name)

    missing_moves = sorted(move for move in required_moves if move and move not in available_moves)
    if not missing_moves:
        return enriched

    if not enriched:
        return enriched

    first_key = next(iter(enriched))
    first_payload = dict(enriched[first_key])
    move_details = dict(first_payload.get("move_details") or {})
    for move in missing_moves:
        profile = move_reference_profiles.get(move)
        if profile is None:
            continue
        move_details[move] = profile
    first_payload["move_details"] = move_details
    enriched[first_key] = first_payload
    return enriched


def _build_bootstrap_move_entries(
    records_with_game_keys: list[tuple[str, list[dict[str, Any]]]],
) -> list[tuple[str, int, str, list[str]]]:
    bootstrap_entries: list[tuple[str, int, str, list[str]]] = []
    for game_key, records in records_with_game_keys:
        for record in records:
            reachable_encounters = record.get("reachable_location_encounters", {})
            if isinstance(reachable_encounters, dict):
                for encounters in reachable_encounters.values():
                    if not isinstance(encounters, list):
                        continue
                    for encounter in encounters:
                        if not isinstance(encounter, dict):
                            continue
                        species = str(encounter.get("species") or "").strip().lower()
                        if not species:
                            continue
                        try:
                            level = int(encounter.get("level_max") or encounter.get("level") or record.get("boss_avg_level") or 20)
                        except (TypeError, ValueError):
                            level = 20
                        bootstrap_entries.append((species, max(level, 1), game_key, []))

    deduped_entries: list[tuple[str, int, str, list[str]]] = []
    seen: set[tuple[str, int, str]] = set()
    for species, level, game_version, moves in bootstrap_entries:
        key = (str(species).strip().lower(), int(level), str(game_version).strip().lower())
        if key in seen:
            continue
        seen.add(key)
        deduped_entries.append((species, level, game_version, moves))
    return deduped_entries


def _collect_starter_chain_species_by_game(
    games_config: list[dict[str, Any]],
) -> dict[str, set[str]]:
    starter_species_by_game: dict[str, set[str]] = {}
    for game in games_config:
        game_key = str(game.get("game_key") or "").strip().lower()
        if not game_key:
            continue
        species_set = starter_species_by_game.setdefault(game_key, set())
        for starter in get_starter_choices(game_key):
            starter_slug = normalize_species_slug(starter)
            if starter_slug:
                species_set.add(starter_slug)
            try:
                rules = get_species_evolution_rules(starter_slug)
            except Exception:  # noqa: BLE001
                rules = {}
            if rules:
                for species_name in rules.keys():
                    species_slug = normalize_species_slug(species_name)
                    if species_slug:
                        species_set.add(species_slug)
            else:
                for species_name in get_starter_family_members(starter_slug):
                    species_slug = normalize_species_slug(species_name)
                    if species_slug:
                        species_set.add(species_slug)
    return starter_species_by_game


def _starter_chain_bootstrap_entries(
    starter_chain_species_by_game: dict[str, set[str]],
) -> list[tuple[str, int, str, list[str]]]:
    entries: list[tuple[str, int, str, list[str]]] = []
    for game_version in sorted(starter_chain_species_by_game):
        for species in sorted(starter_chain_species_by_game[game_version]):
            entries.append((species, 100, game_version, []))
    return entries


def _all_starter_family_bootstrap_entries(
    games_config: list[dict[str, Any]],
) -> list[tuple[str, int, str, list[str]]]:
    game_keys = sorted(
        {
            str(game.get("game_key") or "").strip().lower()
            for game in games_config
            if str(game.get("game_key") or "").strip()
        }
    )
    if not game_keys:
        return []

    starter_bases = sorted(
        {
            normalize_species_slug(starter)
            for game_key in game_keys
            for starter in get_starter_choices(game_key)
            if normalize_species_slug(starter)
        }
    )
    family_species: set[str] = set()
    for base in starter_bases:
        family_species.add(base)
        for species_name in get_starter_family_members(base):
            species_slug = normalize_species_slug(species_name)
            if species_slug:
                family_species.add(species_slug)

    entries: list[tuple[str, int, str, list[str]]] = []
    for game_version in game_keys:
        for species in sorted(family_species):
            entries.append((species, 100, game_version, []))
    return entries


def _species_by_game_from_bootstrap_entries(
    entries: list[tuple[str, int, str, list[str]]],
) -> dict[str, set[str]]:
    species_by_game: dict[str, set[str]] = {}
    for species, _, game_version, _ in entries:
        game_key = str(game_version or "").strip().lower()
        species_slug = normalize_species_slug(species)
        if not game_key or not species_slug:
            continue
        species_by_game.setdefault(game_key, set()).add(species_slug)
    return species_by_game


def _dedupe_bootstrap_entries(
    entries: list[tuple[str, int, str, list[str]]],
) -> list[tuple[str, int, str, list[str]]]:
    merged: dict[tuple[str, int, str], list[str]] = {}
    for species, level, game_version, moves in entries:
        key = (normalize_species_slug(species), max(1, int(level)), str(game_version).strip().lower())
        if not key[0] or not key[2]:
            continue
        slot = merged.setdefault(key, [])
        for move in moves:
            normalized_move = normalize_move_name(move)
            if normalized_move and normalized_move not in slot:
                slot.append(normalized_move)

    return [
        (species, level, game_version, moves)
        for (species, level, game_version), moves in merged.items()
    ]


def _expand_bootstrap_entries_with_evolution_lines(
    entries: list[tuple[str, int, str, list[str]]],
) -> list[tuple[str, int, str, list[str]]]:
    if not entries:
        return []

    family_cache: dict[str, set[str]] = {}

    def _family_species(seed_species: str) -> set[str]:
        species_slug = normalize_species_slug(seed_species)
        if not species_slug:
            return set()
        cached = family_cache.get(species_slug)
        if cached is not None:
            return set(cached)

        expanded: set[str] = {species_slug}
        try:
            rules = get_species_evolution_rules(species_slug)
        except Exception:  # noqa: BLE001
            rules = {}

        if isinstance(rules, dict) and rules:
            for species_name in rules.keys():
                normalized = normalize_species_slug(species_name)
                if normalized:
                    expanded.add(normalized)
        else:
            for species_name in get_starter_family_members(species_slug):
                normalized = normalize_species_slug(species_name)
                if normalized:
                    expanded.add(normalized)

        family_cache[species_slug] = set(expanded)
        return expanded

    expanded_entries: list[tuple[str, int, str, list[str]]] = []
    for species, level, game_version, moves in entries:
        game_key = str(game_version or "").strip().lower()
        normalized_moves = [normalize_move_name(move) for move in list(moves or []) if normalize_move_name(move)]
        for family_species in sorted(_family_species(str(species or ""))):
            expanded_entries.append((family_species, max(1, int(level or 1)), game_key, normalized_moves))

    return _dedupe_bootstrap_entries(expanded_entries)


def _collect_missing_learnable_species_pairs(
    learnable_moves_df: pd.DataFrame,
    species_by_game: dict[str, set[str]],
    *,
    reason: str,
) -> list[dict[str, Any]]:
    observed_pairs = {
        (
            str(row.get("game_version") or "").strip().lower(),
            normalize_species_slug(row.get("pokemon_species") or ""),
        )
        for row in learnable_moves_df.to_dict(orient="records")
        if str(row.get("game_version") or "").strip() and normalize_species_slug(row.get("pokemon_species") or "")
    }

    missing_rows: list[dict[str, Any]] = []
    for game_version in sorted(species_by_game):
        for species in sorted(species_by_game[game_version]):
            if (game_version, species) not in observed_pairs:
                missing_rows.append(
                    {
                        "species_name": species,
                        "game_version": game_version,
                        "reason": reason,
                    }
                )
    return missing_rows


def _validate_starter_chain_move_coverage(
    learnable_moves_df: pd.DataFrame,
    starter_chain_species_by_game: dict[str, set[str]],
    diagnostics_dir: Path,
) -> list[dict[str, Any]]:
    missing_rows = _collect_missing_learnable_species_pairs(
        learnable_moves_df,
        starter_chain_species_by_game,
        reason="starter_chain_missing_moves",
    )

    diagnostics_path = diagnostics_dir / "starter_chain_move_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(missing_rows).to_csv(diagnostics_path, index=False)

    if missing_rows:
        preview = ", ".join(
            f"{row['game_version']}:{row['species_name']}"
            for row in missing_rows[:20]
        )
        raise ValueError(
            "Starter-chain move reference validation failed: "
            f"missing_species_count={len(missing_rows)} "
            f"first_20=[{preview}] "
            f"diagnostics={diagnostics_path}"
        )

    return missing_rows


def _validate_universal_starter_family_move_coverage(
    learnable_moves_df: pd.DataFrame,
    universal_starter_species_by_game: dict[str, set[str]],
    diagnostics_dir: Path,
) -> list[dict[str, Any]]:
    missing_rows = _collect_missing_learnable_species_pairs(
        learnable_moves_df,
        universal_starter_species_by_game,
        reason="universal_starter_family_missing_moves",
    )

    diagnostics_path = diagnostics_dir / "starter_family_move_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(missing_rows).to_csv(diagnostics_path, index=False)

    if missing_rows:
        logger.warning(
            "[silver/moves] universal starter-family coverage gaps detected; non-blocking check "
            "missing_species_count=%s diagnostics=%s",
            len(missing_rows),
            diagnostics_path,
        )

    return missing_rows


def _diagnose_player_missing_damaging_moves(
    progression_source_teams: list[dict[str, Any]],
    reference_context: Any,
    diagnostics_dir: Path,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for team in progression_source_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        boss_id = str(team.get("boss_id") or "").strip().lower()
        source_team_id = str(team.get("source_team_id") or "").strip().lower()
        pokemon = team.get("pokemon")
        levels = team.get("levels")
        if not game_version or not isinstance(pokemon, list):
            continue

        for idx, species in enumerate(pokemon, start=1):
            species_slug = normalize_species_slug(species)
            if not species_slug:
                continue
            raw_level = 1
            if isinstance(levels, list) and idx - 1 < len(levels):
                raw_level = levels[idx - 1]
            try:
                level = max(1, int(raw_level or 1))
            except (TypeError, ValueError):
                level = 1

            damaging_moves = reference_context.damaging_moves(species_slug, level, game_version)
            if damaging_moves:
                continue

            learnable_levels = reference_context.learnable_levels(species_slug, game_version)
            moves_by_level = [
                move_name
                for move_name, learned_level in learnable_levels.items()
                if int(learned_level) <= level
            ]
            if not learnable_levels:
                reason = "missing_learnable_reference"
            elif not moves_by_level:
                reason = "no_learnable_moves_within_level_cap"
            else:
                reason = "learnable_moves_without_damage_profile"

            rows.append(
                {
                    "game_version": game_version,
                    "boss_id": boss_id or None,
                    "source_team_id": source_team_id or None,
                    "slot": idx,
                    "pokemon_species": species_slug,
                    "level": level,
                    "learnable_move_count": len(learnable_levels),
                    "learnable_within_level_cap": len(moves_by_level),
                    "damaging_move_count": 0,
                    "reason": reason,
                }
            )

    diagnostics_path = diagnostics_dir / "player_no_damaging_move_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostics_df = pd.DataFrame(
        rows,
        columns=[
            "game_version",
            "boss_id",
            "source_team_id",
            "slot",
            "pokemon_species",
            "level",
            "learnable_move_count",
            "learnable_within_level_cap",
            "damaging_move_count",
            "reason",
        ],
    )
    diagnostics_df.to_csv(diagnostics_path, index=False)

    if not diagnostics_df.empty:
        summary_df = (
            diagnostics_df.groupby(["game_version", "pokemon_species", "level", "reason"], dropna=False)
            .agg(
                affected_team_count=("source_team_id", "nunique"),
                affected_member_slots=("slot", "count"),
                learnable_move_count_max=("learnable_move_count", "max"),
                learnable_within_level_cap_max=("learnable_within_level_cap", "max"),
            )
            .reset_index()
            .sort_values(
                ["affected_member_slots", "affected_team_count", "game_version", "pokemon_species", "level"],
                ascending=[False, False, True, True, True],
            )
        )
    else:
        summary_df = pd.DataFrame(
            columns=[
                "game_version",
                "pokemon_species",
                "level",
                "reason",
                "affected_team_count",
                "affected_member_slots",
                "learnable_move_count_max",
                "learnable_within_level_cap_max",
            ]
        )
    summary_df.to_csv(diagnostics_dir / "player_no_damaging_move_gaps_summary.csv", index=False)
    return diagnostics_df


def _build_progression_bootstrap_entries(
    progression_source_teams: list[dict[str, Any]],
) -> list[tuple[str, int, str, list[str]]]:
    entries: list[tuple[str, int, str, list[str]]] = []
    for team in progression_source_teams:
        game_version = str(team.get("game_version") or "").strip().lower()
        pokemon = team.get("pokemon")
        levels = team.get("levels")
        if not game_version or not isinstance(pokemon, list):
            continue
        for idx, species in enumerate(pokemon):
            species_slug = normalize_species_slug(species)
            if not species_slug:
                continue
            raw_level = 1
            if isinstance(levels, list) and idx < len(levels):
                raw_level = levels[idx]
            try:
                level = max(1, int(raw_level or 1))
            except (TypeError, ValueError):
                level = 1
            entries.append((species_slug, level, game_version, []))
    return entries


def _build_boss_species_bootstrap_entries(
    boss_teams_df: pd.DataFrame,
) -> list[tuple[str, int, str, list[str]]]:
    required_columns = {"game_version", "pokemon_species"}
    if boss_teams_df.empty or not required_columns.issubset(set(boss_teams_df.columns)):
        return []

    entries: list[tuple[str, int, str, list[str]]] = []
    for row in boss_teams_df.to_dict(orient="records"):
        game_version = str(row.get("game_version") or "").strip().lower()
        species = normalize_species_slug(row.get("pokemon_species") or "")
        if not game_version or not species:
            continue
        entries.append((species, 100, game_version, []))
    return _dedupe_bootstrap_entries(entries)


def _build_evolution_rules_by_game_from_encounters(
    encounters_df: pd.DataFrame,
) -> dict[str, dict[str, dict[str, Any]]]:
    rules_by_game: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    if encounters_df.empty:
        return rules_by_game

    required_columns = {"game", "pokemon"}
    if not required_columns.issubset(encounters_df.columns):
        return rules_by_game

    normalized = encounters_df[list(required_columns)].copy()
    normalized["game"] = normalized["game"].map(lambda value: str(value or "").strip().lower())
    normalized["pokemon"] = normalized["pokemon"].map(normalize_species_slug)
    normalized = normalized[(normalized["game"] != "") & (normalized["pokemon"] != "")]

    for game_version, game_rows in normalized.groupby("game", dropna=False):
        species_seen: set[str] = set()
        for species in sorted(game_rows["pokemon"].dropna().unique().tolist()):
            if not species or species in species_seen:
                continue
            species_seen.add(species)
            try:
                species_rules = get_species_evolution_rules(species)
            except Exception:  # noqa: BLE001
                species_rules = {}
            if not isinstance(species_rules, dict):
                continue
            for species_name, payload in species_rules.items():
                species_slug = normalize_species_slug(species_name)
                if species_slug and isinstance(payload, dict):
                    rules_by_game[str(game_version)][species_slug] = payload

    return rules_by_game


def _evolution_rules_rows_from_map(
    evolution_rules_by_game: dict[str, dict[str, dict[str, Any]]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for game_version, species_map in evolution_rules_by_game.items():
        game_key = normalize_key_part(game_version)
        if not game_key or not isinstance(species_map, dict):
            continue
        for species_name, payload in species_map.items():
            species_slug = normalize_species_slug(species_name)
            if not species_slug or not isinstance(payload, dict):
                continue
            special_conditions = payload.get("special_evolution_conditions")
            if isinstance(special_conditions, list):
                serialized_conditions = special_conditions
            elif special_conditions is None:
                serialized_conditions = []
            elif hasattr(special_conditions, "tolist"):
                converted = special_conditions.tolist()
                serialized_conditions = converted if isinstance(converted, list) else [converted]
            else:
                serialized_conditions = [special_conditions]
            rows.append(
                {
                    "game_version": game_key,
                    "species_name": species_slug,
                    "base_species": normalize_species_slug(payload.get("base_species") or species_slug),
                    "evolution_stage": int(payload.get("evolution_stage") or 1),
                    "min_valid_level": payload.get("min_valid_level"),
                    "min_level_from_previous": payload.get("min_level_from_previous"),
                    "special_evolution_conditions_json": json.dumps(serialized_conditions, ensure_ascii=True),
                }
            )
    return rows


def _evolution_rules_map_from_rows(
    evolution_rules_df: pd.DataFrame,
) -> dict[str, dict[str, dict[str, Any]]]:
    rules_by_game: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    if evolution_rules_df.empty:
        return rules_by_game
    required_columns = {
        "game_version",
        "species_name",
        "base_species",
        "evolution_stage",
        "min_valid_level",
        "min_level_from_previous",
        "special_evolution_conditions_json",
    }
    if not required_columns.issubset(evolution_rules_df.columns):
        return rules_by_game
    for row in evolution_rules_df[list(required_columns)].to_dict(orient="records"):
        game_version = normalize_key_part(row.get("game_version"))
        species_name = normalize_species_slug(row.get("species_name"))
        if not game_version or not species_name:
            continue
        raw_conditions = row.get("special_evolution_conditions_json")
        try:
            parsed_conditions = json.loads(str(raw_conditions or "[]"))
            if not isinstance(parsed_conditions, list):
                parsed_conditions = []
        except Exception:  # noqa: BLE001
            parsed_conditions = []
        rules_by_game[game_version][species_name] = {
            "species_name": species_name,
            "base_species": normalize_species_slug(row.get("base_species") or species_name),
            "evolution_stage": int(row.get("evolution_stage") or 1),
            "min_valid_level": row.get("min_valid_level"),
            "min_level_from_previous": row.get("min_level_from_previous"),
            "special_evolution_conditions": parsed_conditions,
        }
    return rules_by_game


def _build_kaggle_bootstrap_entries(
    kaggle_rows_by_game: dict[str, list[dict[str, Any]]],
    learnable_moves_df: pd.DataFrame | None = None,
    move_reference_df: pd.DataFrame | None = None,
) -> list[tuple[str, int, str, list[str]]]:
    existing_pairs: set[tuple[str, str]] = set()
    existing_moves: set[str] = set()
    if learnable_moves_df is not None and not learnable_moves_df.empty:
        required_columns = {"game_version", "pokemon_species"}
        if required_columns.issubset(learnable_moves_df.columns):
            for row in learnable_moves_df.to_dict(orient="records"):
                game_version = str(row.get("game_version") or "").strip().lower()
                species = normalize_species_slug(row.get("pokemon_species"))
                if game_version and species:
                    existing_pairs.add((game_version, species))
    if move_reference_df is not None and not move_reference_df.empty and "move_name" in move_reference_df.columns:
        existing_moves = {
            normalize_move_name(row.get("move_name"))
            for row in move_reference_df.to_dict(orient="records")
            if normalize_move_name(row.get("move_name"))
        }

    entries: list[tuple[str, int, str, list[str]]] = []
    for game_key, rows in kaggle_rows_by_game.items():
        game_norm = str(game_key or "").strip().lower()
        if not game_norm:
            continue
        for row in rows:
            species = normalize_species_slug(row.get("Pokemon") or "")
            if not species:
                continue
            try:
                level = int(row.get("Level") or 20)
            except (TypeError, ValueError):
                level = 20
            moves = [
                normalize_move_name(row.get("Move 1", "")),
                normalize_move_name(row.get("Move 2", "")),
                normalize_move_name(row.get("Move 3", "")),
                normalize_move_name(row.get("Move 4", "")),
            ]
            normalized_moves = [move for move in moves if move]
            if (game_norm, species) in existing_pairs and all(move in existing_moves for move in normalized_moves):
                continue
            entries.append((species, max(level, 1), game_norm, normalized_moves))
    return entries


def _validate_kaggle_moves_in_move_reference(
    kaggle_rows_by_game: dict[str, list[dict[str, Any]]],
    move_reference_df: pd.DataFrame,
    diagnostics_dir: Path,
) -> None:
    move_reference_moves = {
        normalize_move_name(row.get("move_name"))
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name"))
    }

    missing_rows: list[dict[str, Any]] = []
    move_columns = ("Move 1", "Move 2", "Move 3", "Move 4")
    for game_key, rows in sorted(kaggle_rows_by_game.items()):
        game_norm = str(game_key or "").strip().lower()
        for row in rows:
            species_slug = normalize_species_slug(row.get("Pokemon") or "")
            for move_column in move_columns:
                normalized_move = normalize_move_name(row.get(move_column) or "")
                if not normalized_move:
                    continue
                if normalized_move in move_reference_moves:
                    continue
                missing_rows.append(
                    {
                        "game_version": game_norm,
                        "pokemon_species": species_slug,
                        "move_name": normalized_move,
                        "source_column": move_column,
                        "reason": "kaggle_move_missing_from_move_reference",
                    }
                )

    diagnostics_path = diagnostics_dir / "kaggle_move_reference_gaps.csv"
    diagnostics_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        missing_rows,
        columns=["game_version", "pokemon_species", "move_name", "source_column", "reason"],
    ).to_csv(diagnostics_path, index=False)

    if missing_rows:
        preview = ", ".join(
            f"{row['game_version']}:{row['pokemon_species']}:{row['move_name']}"
            for row in missing_rows[:20]
        )
        raise ValueError(
            "Kaggle move reference validation failed: "
            f"missing_moves={len(missing_rows)} "
            f"first_20=[{preview}] "
            f"diagnostics={diagnostics_path}"
        )


def _collect_missing_bootstrap_move_hints(
    entries: list[tuple[str, int, str, list[str]]],
    move_reference_df: pd.DataFrame,
) -> set[str]:
    present_moves = {
        normalize_move_name(row.get("move_name"))
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name"))
    }
    required_moves = {
        normalize_move_name(move)
        for _, _, _, moves in entries
        for move in moves
        if normalize_move_name(move)
    }
    return {move for move in required_moves if move not in present_moves}
