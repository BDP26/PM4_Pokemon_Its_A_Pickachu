"""Compact offline player-team generation for Silver.

Silver persists logical team/member/move-option references only.
Concrete move-set expansion is deferred to Gold/simulation.
"""

from __future__ import annotations

import logging
import time
from itertools import combinations, islice
from typing import Any

import pandas as pd

from src.pipeline.silver.config.game_config import (
    get_starter_choices,
    get_starter_type,
    get_starters_by_type,
    resolve_starter_species_for_level,
)
from src.pipeline.silver.config.team_config import (
    DEFAULT_CATCH_POOL_SIZE,
    DEFAULT_MEMBER_LEVEL,
    DEFAULT_MEMBER_MOVESET_COMBO_LIMIT,
    DEFAULT_MEMBER_MOVE_OPTION_LIMIT,
    DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
    DEFAULT_SOURCE_TEAM_POOL_SIZE,
    DEFAULT_TEAM_MEMBER_LIMIT,
    MOVESET_WIDTH,
)
from src.pipeline.silver.inputs.builders.evolution_normalization import (
    legal_species_pool_for_level,
    normalize_candidate_pool_for_level,
    validate_candidate_pool,
    validate_generated_team,
)
from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.inputs.species_classification import is_restricted_encounter_species
from src.pipeline.silver.transforms.keys import (
    make_moveset_combo_id,
    make_pokemon_instance_id,
    make_team_id,
    normalize_key_part,
    stable_digest,
)
from src.pipeline.silver.transforms.progression_depth import (
    ProgressionDepthContext,
    build_progression_depth_context,
)


logger = logging.getLogger(__name__)
MAX_SOURCE_TEAM_SIZE = 5
EARLY_GAME_LEVEL_OFFSET = 6
LATE_GAME_LEVEL_OFFSET = 1
INVALID_NULLABLE_KEY_TOKENS = {"", "nan", "none", "null", "<na>", "na"}


def _normalize_nullable_key_part(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    normalized = normalize_key_part(value)
    if normalized in INVALID_NULLABLE_KEY_TOKENS:
        return None
    return normalized or None


def _normalize_boss_id(value: Any) -> str:
    return str(value or "").strip().lower()


def _boss_slug_from_id(value: Any) -> str:
    normalized = _normalize_boss_id(value)
    if not normalized:
        return ""
    return normalize_key_part(normalized.split(":")[-1])


def _family_root_for_species(species: str) -> str:
    normalized = normalize_key_part(species)
    if not normalized:
        return ""
    return normalized


def _resolve_starters_for_condition(game_version: str, starter_condition: str | None) -> list[str]:
    condition = _normalize_nullable_key_part(starter_condition)
    if condition is None:
        return get_starter_choices(game_version)
    starters_by_type = get_starters_by_type(game_version, condition)
    if starters_by_type:
        return starters_by_type
    return [condition]


def _stable_species_signature(species_names: list[str]) -> str:
    return "|".join(sorted(normalize_key_part(name) for name in species_names if normalize_key_part(name)))


def _clamp_progression_depth(value: Any) -> float:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        numeric = 0.0
    return max(0.0, min(1.0, numeric))


def _progression_level_offset(progression_depth: float) -> int:
    depth = _clamp_progression_depth(progression_depth)
    span = EARLY_GAME_LEVEL_OFFSET - LATE_GAME_LEVEL_OFFSET
    return max(LATE_GAME_LEVEL_OFFSET, int(round(LATE_GAME_LEVEL_OFFSET + ((1.0 - depth) * span))))


def _level_cap_from_progression(*, boss_ace_level: int, progression_depth: float) -> int:
    ace_level = max(1, int(boss_ace_level or DEFAULT_MEMBER_LEVEL))
    return max(1, ace_level - _progression_level_offset(progression_depth))


def _effective_member_level(*, level_cap: int, encounter_level_max: int) -> int:
    capped_level_cap = max(1, int(level_cap or DEFAULT_MEMBER_LEVEL))
    capped_encounter_level = max(1, int(encounter_level_max or capped_level_cap))
    return min(capped_level_cap, capped_encounter_level)


def _legacy_progression_depth_context_from_boss_teams(
    *,
    encounters_df: pd.DataFrame,
    bosses_df: pd.DataFrame,
    boss_teams: list[dict[str, Any]],
) -> ProgressionDepthContext:
    bosses = bosses_df.copy()
    bosses["game_version"] = bosses["game_version"].map(normalize_key_part)
    bosses["boss_id"] = bosses["boss_id"].map(normalize_key_part)
    bosses["boss_name"] = bosses["boss_name_canonical"].map(normalize_key_part)
    bosses["boss_order"] = pd.to_numeric(bosses["boss_order"], errors="coerce").fillna(0).astype(int)
    bosses = bosses[(bosses["game_version"] != "") & (bosses["boss_id"] != "") & (bosses["boss_name"] != "")]

    encounters = encounters_df.copy()
    encounters["game_version"] = encounters["game"].map(normalize_key_part)
    encounters["boss_id"] = encounters["boss_id"].map(_normalize_boss_id)
    encounters["pokemon"] = encounters["pokemon"].map(normalize_key_part)
    encounters = encounters[
        (encounters["game_version"] != "")
        & (encounters["boss_id"] != "")
        & (encounters["pokemon"] != "")
    ]

    progression_rows: list[dict[str, Any]] = []
    for game_version, bosses_game in bosses.groupby("game_version", sort=True):
        bosses_sorted = bosses_game.sort_values(["boss_order", "boss_id"]).reset_index(drop=True)
        max_boss_index = len(bosses_sorted)
        species_by_boss = {
            str(boss_id): set(group["pokemon"].tolist())
            for boss_id, group in encounters[encounters["game_version"] == game_version].groupby("boss_id")
        }
        max_species_count = len({species for values in species_by_boss.values() for species in values}) or 1
        seen_species: set[str] = set()
        for boss_index, boss in enumerate(bosses_sorted.itertuples(index=False), start=1):
            boss_species = species_by_boss.get(str(boss.boss_id), set())
            seen_species.update(boss_species)
            available_species_count = len(seen_species) if seen_species else len(boss_species)
            progression_rows.append(
                {
                    "game_version": game_version,
                    "boss_id": str(boss.boss_id),
                    "boss_name": str(boss.boss_name),
                    "boss_index": boss_index,
                    "max_boss_index": max_boss_index,
                    "available_species_count": available_species_count,
                    "max_species_count": max_species_count,
                    "progression_depth": (0.6 * (boss_index / max_boss_index)) + (0.4 * (available_species_count / max_species_count)),
                }
            )

    progression_depth_df = pd.DataFrame(progression_rows)

    boss_level_rows: list[dict[str, Any]] = []
    for team in boss_teams:
        game_version = normalize_key_part(team.get("game_version"))
        boss_name = normalize_key_part(team.get("boss_name") or team.get("gym"))
        if not game_version or not boss_name:
            continue
        levels = [int(level) for level in list(team.get("levels") or []) if int(level or 0) > 0]
        if not levels:
            fallback = int(team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
            levels = [fallback]
        boss_level_rows.append(
            {
                "game_version": game_version,
                "boss_name": boss_name,
                "boss_ace_level": max(levels),
                "boss_avg_level": int(round(sum(levels) / len(levels))),
            }
        )

    boss_level_df = pd.DataFrame(boss_level_rows)
    if boss_level_df.empty:
        raise ValueError("boss_teams is empty; cannot derive fallback progression depth context")
    boss_level_df = (
        boss_level_df.groupby(["game_version", "boss_name"], as_index=False)
        .agg(boss_ace_level=("boss_ace_level", "max"), boss_avg_level=("boss_avg_level", "max"))
        .sort_values(["game_version", "boss_name"])
    )
    return build_progression_depth_context(
        progression_depth_df=progression_depth_df,
        boss_level_df=boss_level_df,
    )


def _extract_species_candidates(record: dict[str, Any]) -> list[tuple[str, int, int, int]]:
    def _safe_int(value: Any) -> int:
        try:
            return int(value or 0)
        except (TypeError, ValueError):
            return 0

    by_species: dict[str, dict[str, int]] = {}
    location_to_encounters = record.get("reachable_location_encounters", {})
    if isinstance(location_to_encounters, dict):
        for encounters in location_to_encounters.values():
            if not isinstance(encounters, list):
                continue
            for encounter in encounters:
                if not isinstance(encounter, dict):
                    continue
                species = normalize_key_part(encounter.get("species"))
                if not species or is_restricted_encounter_species(species):
                    continue
                slot = by_species.setdefault(species, {"level_max": 0, "chance_max": 0, "capture_rate": 0})
                slot["level_max"] = max(slot["level_max"], _safe_int(encounter.get("level_max")))
                slot["chance_max"] = max(slot["chance_max"], _safe_int(encounter.get("encounter_chance_max")))
                slot["capture_rate"] = max(slot["capture_rate"], _safe_int(encounter.get("capture_rate")))

    candidates = [
        (species, payload["chance_max"], payload["level_max"], payload["capture_rate"])
        for species, payload in by_species.items()
    ]
    candidates.sort(key=lambda item: (-item[1], -item[2], -item[3], item[0]))
    return candidates


def _dedupe_species_by_family(candidates: list[tuple[str, int, int, int]]) -> list[tuple[str, int, int, int]]:
    seen_roots: set[str] = set()
    result: list[tuple[str, int, int, int]] = []
    for species, chance_max, level_max, capture_rate in candidates:
        root = _family_root_for_species(species)
        if root in seen_roots:
            continue
        seen_roots.add(root)
        result.append((species, chance_max, level_max, capture_rate))
    return result


def _rank_candidate_pool(
    candidates: list[tuple[str, int, int, int]],
    *,
    boss_level: int,
    pool_size: int,
) -> tuple[list[tuple[str, int, int, int]], dict[str, int]]:
    if not candidates:
        return [], {"input": 0, "output": 0, "pruned": 0, "family_pruned": 0}

    family_deduped = _dedupe_species_by_family(candidates)
    family_pruned = max(0, len(candidates) - len(family_deduped))
    scored: list[tuple[float, tuple[str, int, int, int]]] = []

    for species, chance_max, level_max, capture_rate in family_deduped:
        level_gap = abs(int(level_max or 0) - int(boss_level or DEFAULT_MEMBER_LEVEL))
        level_realism = max(0.0, 1.0 - min(level_gap, 25) / 25.0)
        chance_signal = min(max(float(chance_max), 0.0), 100.0) / 100.0
        capture_signal = min(max(float(capture_rate), 0.0), 255.0) / 255.0
        score = (0.20 * chance_signal) + (0.15 * capture_signal) + (0.65 * level_realism)
        scored.append((score, (species, chance_max, level_max, capture_rate)))

    scored.sort(key=lambda item: (-item[0], -item[1][1], -item[1][2], -item[1][3], item[1][0]))
    ranked = [row for _, row in scored]
    constrained = ranked[: max(1, pool_size)]

    diagnostics = {
        "input": len(candidates),
        "output": len(constrained),
        "pruned": max(0, len(ranked) - len(constrained)),
        "family_pruned": family_pruned,
    }
    return constrained, diagnostics


def build_boss_progression_pools(progression_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Build cumulative boss pools with incremental deltas."""
    pools: list[dict[str, Any]] = []
    seen_locations_by_game: dict[str, set[str]] = {}
    cumulative_candidates_by_game: dict[str, dict[str, tuple[str, int, int, int]]] = {}
    candidate_sources_by_game: dict[str, dict[str, set[str]]] = {}

    for record in progression_records:
        game_version = normalize_key_part(record.get("game") or record.get("version"))
        boss_name = normalize_key_part(record.get("boss_name"))
        if not game_version or not boss_name:
            continue

        location_to_encounters = record.get("reachable_location_encounters", {})
        known_locations = seen_locations_by_game.setdefault(game_version, set())
        cumulative_candidates = cumulative_candidates_by_game.setdefault(game_version, {})
        candidate_sources = candidate_sources_by_game.setdefault(game_version, {})

        delta_locations: list[str] = []
        for location_slug in record.get("reachable_locations", []):
            location = normalize_key_part(location_slug)
            if not location or location in known_locations:
                continue
            delta_locations.append(location)

        delta_species_count = 0
        for location in delta_locations:
            encounters = location_to_encounters.get(location, [])
            if not isinstance(encounters, list):
                continue
            for encounter in encounters:
                if not isinstance(encounter, dict):
                    continue
                species = normalize_key_part(encounter.get("species"))
                if not species or is_restricted_encounter_species(species):
                    continue
                chance_max = int(encounter.get("encounter_chance_max") or 0)
                level_max = int(encounter.get("level_max") or 0)
                capture_rate = int(encounter.get("capture_rate") or 0)
                prior = cumulative_candidates.get(species)
                updated = (
                    species,
                    max((prior[1] if prior else 0), chance_max),
                    max((prior[2] if prior else 0), level_max),
                    max((prior[3] if prior else 0), capture_rate),
                )
                if prior is None:
                    delta_species_count += 1
                cumulative_candidates[species] = updated
                candidate_sources.setdefault(species, set()).add(location)

        known_locations.update(delta_locations)
        pool_candidates = sorted(
            cumulative_candidates.values(),
            key=lambda item: (-item[1], -item[2], -item[3], item[0]),
        )

        pools.append(
            {
                "progression_pool_id": f"pool:{game_version}:{stable_digest(game_version, boss_name, record.get('part'))}",
                "game_version": game_version,
                "boss_name": boss_name,
                "part": normalize_key_part(record.get("part")),
                "pool_candidates": pool_candidates,
                "pool_species_count": len(pool_candidates),
                "delta_location_count": len(delta_locations),
                "delta_species_count": delta_species_count,
                "pool_delta_added": [species for species, _, _, _ in pool_candidates][-delta_species_count:] if delta_species_count else [],
                "candidate_sources": {k: sorted(v) for k, v in candidate_sources.items()},
            }
        )

    return pools


def _base_team_diversity_score(species_combo: tuple[tuple[str, int, int, int], ...]) -> tuple[int, int, int, str]:
    chance_sum = sum(item[1] for item in species_combo)
    level_sum = sum(item[2] for item in species_combo)
    capture_sum = sum(item[3] for item in species_combo)
    signature = _stable_species_signature([item[0] for item in species_combo])
    return (chance_sum, level_sum, capture_sum, signature)


def _generate_diverse_species_combos(
    candidates: list[tuple[str, int, int, int]],
    team_fill_size: int,
    combo_limit: int,
) -> list[list[str]]:
    if team_fill_size <= 0:
        return [[]]

    unique_family_candidates = _dedupe_species_by_family(candidates)
    if len(unique_family_candidates) < team_fill_size:
        unique_family_candidates = candidates
    if len(unique_family_candidates) < team_fill_size:
        return []

    raw_combos = combinations(unique_family_candidates, team_fill_size)
    scored: list[tuple[tuple[int, int, int, str], list[str]]] = []
    for combo in islice(raw_combos, max(combo_limit * 20, combo_limit)):
        scored.append((_base_team_diversity_score(combo), [item[0] for item in combo]))

    scored.sort(key=lambda item: (-item[0][0], -item[0][1], -item[0][2], item[0][3]))
    seen: set[str] = set()
    out: list[list[str]] = []
    for _, species_list in scored:
        signature = _stable_species_signature(species_list)
        if signature in seen:
            continue
        seen.add(signature)
        out.append(species_list)
        if len(out) >= combo_limit:
            break
    return out


def _boss_level_lookup(boss_teams: list[dict[str, Any]]) -> dict[tuple[str, str], int]:
    lookup: dict[tuple[str, str], int] = {}
    for team in boss_teams:
        game_version = normalize_key_part(team.get("game_version"))
        boss_name = normalize_key_part(team.get("boss_name"))
        if not game_version or not boss_name:
            continue
        lookup[(game_version, boss_name)] = int(team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
    return lookup


def build_progression_source_teams(
    progression_records: list[dict[str, Any]],
    boss_teams: list[dict[str, Any]],
    catch_pool_size: int = DEFAULT_CATCH_POOL_SIZE,
    evolution_rules_by_game: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    allow_trade_evolutions: bool = False,
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    level_by_boss = _boss_level_lookup(boss_teams)
    progression_pools = build_boss_progression_pools(progression_records)

    evolution_rules_by_game = evolution_rules_by_game or {}

    team_fill_size = max(1, min(catch_pool_size, MAX_SOURCE_TEAM_SIZE))
    candidate_pool_size = max(team_fill_size, DEFAULT_SOURCE_TEAM_POOL_SIZE)

    for pool in progression_pools:
        game_version = pool["game_version"]
        boss_name = pool["boss_name"]
        boss_level = level_by_boss.get((game_version, boss_name), DEFAULT_MEMBER_LEVEL)
        raw_candidates = list(pool["pool_candidates"])
        evolution_rules = evolution_rules_by_game.get(game_version, {})
        legal_species = legal_species_pool_for_level(
            raw_candidates,
            member_level=boss_level,
            evolution_rules=evolution_rules,
            allow_trade_evolutions=allow_trade_evolutions,
        )
        normalized_candidates, normalization_diag = normalize_candidate_pool_for_level(
            raw_candidates,
            member_level=boss_level,
            evolution_rules=evolution_rules,
            legal_species=legal_species if legal_species else None,
            allow_trade_evolutions=allow_trade_evolutions,
        )
        if legal_species:
            validate_candidate_pool(normalized_candidates, legal_species=legal_species, game_version=game_version)

        candidate_pool, rank_diag = _rank_candidate_pool(
            normalized_candidates,
            boss_level=boss_level,
            pool_size=candidate_pool_size,
        )
        logger.debug(
            "[silver/teams] candidate pool diagnostics game=%s boss=%s raw=%s game_filtered_removed=%s progression_filtered_removed=%s evolved=%s post_validation_removed=%s final=%s rank_pruned=%s",
            game_version,
            boss_name,
            len(raw_candidates),
            0,
            0,
            normalization_diag.get("transformed", 0),
            normalization_diag.get("removed_after_validation", 0),
            len(candidate_pool),
            rank_diag.get("pruned", 0),
        )
        species_combos = _generate_diverse_species_combos(
            candidates=candidate_pool,
            team_fill_size=team_fill_size,
            combo_limit=DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
        )
        if not species_combos:
            fallback = [species for species, _, _, _ in candidate_pool[:team_fill_size]]
            if fallback:
                species_combos = [fallback]
        level_by_species = {
            species: _effective_member_level(boss_level=boss_level, encounter_level_max=level_max)
            for species, _, level_max, _ in candidate_pool
        }

        part = pool.get("part")
        for combo_index, selected_species in enumerate(species_combos, start=1):
            if legal_species:
                validate_generated_team(
                    selected_species,
                    legal_species=legal_species,
                    game_version=game_version,
                    boss_name=boss_name,
                )
            source_team_id = make_team_id(
                "progression",
                game_version,
                boss_name,
                variant=f"{part or 'na'}:{combo_index}:{_stable_species_signature(selected_species)}",
            )
            sources.append(
                {
                    "team_id": source_team_id,
                    "game_version": game_version,
                    "team_role": "player",
                    "origin": "generated",
                    "is_player_candidate": True,
                    "boss_name": boss_name,
                    "gym": str(boss_name).strip() or None,
                    "avg_level": boss_level,
                    "pokemon": selected_species,
                    "levels": [level_by_species.get(species, boss_level) for species in selected_species],
                    "progression_pool_id": pool["progression_pool_id"],
                    "part": part,
                }
            )

    logger.info(
        "[silver/teams] built compact progression source teams pools=%s source_teams=%s",
        len(progression_pools),
        len(sources),
    )
    return sources


def build_progression_source_teams_from_encounters(
    encounters_df: pd.DataFrame,
    bosses_df: pd.DataFrame,
    boss_teams: list[dict[str, Any]] | None = None,
    progression_depth_context: ProgressionDepthContext | None = None,
    catch_pool_size: int = DEFAULT_CATCH_POOL_SIZE,
    evolution_rules_by_game: dict[str, dict[str, list[dict[str, Any]]]] | None = None,
    allow_trade_evolutions: bool = False,
) -> list[dict[str, Any]]:
    """Build player source teams using persisted Silver references only."""
    required_encounter_columns = {"boss_id", "location", "pokemon", "level_min", "level_max", "game"}
    optional_encounter_columns = {"encounter_chance_max", "capture_rate"}
    missing_encounter_columns = sorted(required_encounter_columns - set(encounters_df.columns))
    if missing_encounter_columns:
        raise ValueError(f"encounters.parquet missing required columns: {missing_encounter_columns}")
    missing_optional_columns = sorted(optional_encounter_columns - set(encounters_df.columns))
    if missing_optional_columns:
        logger.warning(
            "[silver/teams] encounters parquet missing optional ranking columns %s; defaulting to 0 for backward compatibility",
            missing_optional_columns,
        )

    required_boss_columns = {"boss_id", "game_version", "boss_name_canonical", "boss_order"}
    missing_boss_columns = sorted(required_boss_columns - set(bosses_df.columns))
    if missing_boss_columns:
        raise ValueError(f"bosses.parquet missing required columns: {missing_boss_columns}")

    encounters = encounters_df.rename(
        columns={
            "game": "game_version",
            "pokemon": "pokemon_species",
        }
    ).copy()
    encounters["game_version"] = encounters["game_version"].map(normalize_key_part)
    encounters["boss_id"] = encounters["boss_id"].map(_normalize_boss_id)
    encounters["boss_slug"] = encounters["boss_id"].map(_boss_slug_from_id)
    encounters["location"] = encounters["location"].map(normalize_key_part)
    encounters["pokemon_species"] = encounters["pokemon_species"].map(normalize_key_part)
    encounters["level_min"] = pd.to_numeric(encounters["level_min"], errors="coerce").fillna(0).astype(int)
    encounters["level_max"] = pd.to_numeric(encounters["level_max"], errors="coerce").fillna(0).astype(int)
    if "encounter_chance_max" not in encounters.columns:
        encounters["encounter_chance_max"] = 0
    if "capture_rate" not in encounters.columns:
        encounters["capture_rate"] = 0
    encounters["encounter_chance_max"] = pd.to_numeric(encounters["encounter_chance_max"], errors="coerce").fillna(0).astype(int)
    encounters["capture_rate"] = pd.to_numeric(encounters["capture_rate"], errors="coerce").fillna(0).astype(int)
    encounters = encounters[
        (encounters["game_version"] != "")
        & (encounters["boss_id"] != "")
        & (encounters["pokemon_species"] != "")
    ]

    bosses = bosses_df.copy()
    bosses["game_version"] = bosses["game_version"].map(normalize_key_part)
    bosses["boss_id"] = bosses["boss_id"].map(_normalize_boss_id)
    bosses["boss_name"] = bosses["boss_name_canonical"].map(normalize_key_part)
    bosses["boss_slug"] = bosses["boss_name"].map(normalize_key_part)
    bosses["boss_order"] = pd.to_numeric(bosses["boss_order"], errors="coerce").fillna(0).astype(int)
    if "gym_index" in bosses.columns:
        bosses["gym_index"] = pd.to_numeric(bosses["gym_index"], errors="coerce").fillna(bosses["boss_order"]).astype(int)
    else:
        bosses["gym_index"] = bosses["boss_order"]
    bosses["starter_condition"] = (
        bosses["starter_condition"].map(_normalize_nullable_key_part) if "starter_condition" in bosses.columns else None
    )
    bosses = bosses[(bosses["game_version"] != "") & (bosses["boss_id"] != "")]

    logger.info(
        "[silver/teams] encounters rows loaded=%s bosses loaded=%s",
        len(encounters),
        len(bosses),
    )
    per_game_rows = encounters.groupby("game_version", dropna=False).size().to_dict()
    logger.info("[silver/teams] per-game encounter rows=%s", per_game_rows)

    boss_id_by_game_slug = {
        (str(row.game_version), str(row.boss_slug)): str(row.boss_id)
        for row in bosses[["game_version", "boss_slug", "boss_id"]].drop_duplicates().itertuples(index=False)
        if str(row.game_version) and str(row.boss_slug) and str(row.boss_id)
    }
    encounters["boss_id"] = [
        boss_id_by_game_slug.get((str(game_version), str(boss_slug)), str(boss_id))
        for game_version, boss_slug, boss_id in encounters[["game_version", "boss_slug", "boss_id"]].itertuples(index=False, name=None)
    ]

    legal_pairs = {
        (str(row.game_version), str(row.boss_id))
        for row in encounters[["game_version", "boss_id"]].drop_duplicates().itertuples(index=False)
    }
    if progression_depth_context is None:
        if boss_teams is None:
            raise ValueError("progression_depth_context is required when boss_teams fallback is not supplied")
        progression_depth_context = _legacy_progression_depth_context_from_boss_teams(
            encounters_df=encounters_df,
            bosses_df=bosses_df,
            boss_teams=boss_teams,
        )
    evolution_rules_by_game = evolution_rules_by_game or {}
    team_fill_size = max(1, min(catch_pool_size, MAX_SOURCE_TEAM_SIZE))
    candidate_pool_size = max(team_fill_size, DEFAULT_SOURCE_TEAM_POOL_SIZE)
    sources: list[dict[str, Any]] = []

    dropped_missing_boss = 0
    for boss in bosses.sort_values(["game_version", "boss_order", "boss_id"]).itertuples(index=False):
        game_version = str(boss.game_version)
        boss_id = str(boss.boss_id)
        boss_name = normalize_key_part(getattr(boss, "boss_name", None) or boss_id)
        starter_condition = _normalize_nullable_key_part(getattr(boss, "starter_condition", None))
        gym_index = int(getattr(boss, "gym_index", getattr(boss, "boss_order", 0)) or 0)
        part = f"order-{gym_index}"

        if (game_version, boss_id) not in legal_pairs:
            dropped_missing_boss += 1
            logger.error(
                "[silver/teams] candidates dropped missing boss encounter pool game=%s boss_id=%s boss_name=%s",
                game_version,
                boss_id,
                boss_name,
            )
            raise ValueError(f"Missing encounter pool for game={game_version} boss_id={boss_id} boss_name={boss_name}")

        boss_rows = encounters[(encounters["game_version"] == game_version) & (encounters["boss_id"] == boss_id)]
        if boss_rows.empty:
            continue
        progression = progression_depth_context.require(
            game_version=game_version,
            boss_id=boss_id,
            boss_name=boss_name,
        )
        player_level_cap = _level_cap_from_progression(
            boss_ace_level=progression.boss_ace_level,
            progression_depth=progression.progression_depth,
        )

        grouped = (
            boss_rows.groupby("pokemon_species", as_index=False)
            .agg(
                level_max=("level_max", "max"),
                level_min=("level_min", "min"),
                encounter_chance_max=("encounter_chance_max", "max"),
                capture_rate=("capture_rate", "max"),
            )
            .sort_values(["pokemon_species"])
        )
        raw_candidates = [
            (
                str(row.pokemon_species),
                int(row.encounter_chance_max),
                int(row.level_max),
                int(row.capture_rate),
            )
            for row in grouped.itertuples(index=False)
            if not is_restricted_encounter_species(str(row.pokemon_species))
        ]
        raw_species = {species for species, _, _, _ in raw_candidates}
        logger.debug(
            "[silver/teams] per-boss raw candidate count game=%s boss_id=%s boss_name=%s count=%s",
            game_version,
            boss_id,
            boss_name,
            len(raw_candidates),
        )

        evolution_rules = evolution_rules_by_game.get(game_version, {})
        legal_species = legal_species_pool_for_level(
            raw_candidates,
            member_level=player_level_cap,
            evolution_rules=evolution_rules,
            allow_trade_evolutions=allow_trade_evolutions,
        )
        normalized_candidates, normalization_diag = normalize_candidate_pool_for_level(
            raw_candidates,
            member_level=player_level_cap,
            evolution_rules=evolution_rules,
            legal_species=legal_species if legal_species else None,
            allow_trade_evolutions=allow_trade_evolutions,
        )
        validate_candidate_pool(normalized_candidates, legal_species=legal_species or raw_species, game_version=game_version)

        candidate_pool, rank_diag = _rank_candidate_pool(
            normalized_candidates,
            boss_level=player_level_cap,
            pool_size=candidate_pool_size,
        )
        logger.debug(
            "[silver/teams] per-boss final candidate count game=%s boss_id=%s boss_name=%s raw=%s final=%s evolved=%s removed=%s pruned=%s progression_depth=%.4f level_cap=%s offset=%s ace_level=%s",
            game_version,
            boss_id,
            boss_name,
            len(raw_candidates),
            len(candidate_pool),
            normalization_diag.get("transformed", 0),
            normalization_diag.get("removed_after_validation", 0),
            rank_diag.get("pruned", 0),
            progression.progression_depth,
            player_level_cap,
            _progression_level_offset(progression.progression_depth),
            progression.boss_ace_level,
        )

        species_combos = _generate_diverse_species_combos(
            candidates=candidate_pool,
            team_fill_size=team_fill_size,
            combo_limit=DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
        )
        if not species_combos:
            fallback = [species for species, _, _, _ in candidate_pool[:team_fill_size]]
            if fallback:
                species_combos = [fallback]
        level_by_species = {
            species: _effective_member_level(level_cap=player_level_cap, encounter_level_max=level_max)
            for species, _, level_max, _ in candidate_pool
        }

        for combo_index, selected_species in enumerate(species_combos, start=1):
            invalid_reasons: list[dict[str, str]] = []
            for pokemon_species in selected_species:
                if pokemon_species not in (legal_species or raw_species):
                    invalid_reasons.append(
                        {
                            "game_version": game_version,
                            "boss_id": boss_id,
                            "boss_name": boss_name,
                            "pokemon_species": pokemon_species,
                            "reason": "species_not_in_encounters_for_boss",
                        }
                    )
            if invalid_reasons:
                raise ValueError(f"Invalid player candidates detected: {invalid_reasons[:20]}")

            validate_generated_team(
                selected_species,
                legal_species=legal_species or raw_species,
                game_version=game_version,
                boss_name=boss_name,
            )
            source_team_id = make_team_id(
                "progression",
                game_version,
                boss_name,
                variant=f"{boss_id}:{part}:{combo_index}:{_stable_species_signature(selected_species)}",
            )
            sources.append(
                {
                    "team_id": source_team_id,
                    "game_version": game_version,
                    "team_role": "player",
                    "origin": "generated",
                    "is_player_candidate": True,
                    "boss_id": boss_id,
                    "boss_name": boss_name,
                    "gym": str(boss_name).strip() or None,
                    "gym_index": gym_index,
                    "starter_condition": starter_condition,
                    "starter_type": starter_condition,
                    "avg_level": player_level_cap,
                    "pokemon": selected_species,
                    "levels": [level_by_species.get(species, player_level_cap) for species in selected_species],
                    "progression_pool_id": f"pool:{game_version}:{boss_id}",
                    "part": part,
                    "boss_index": progression.boss_index,
                    "max_boss_index": progression.max_boss_index,
                    "available_species_count": progression.available_species_count,
                    "max_species_count": progression.max_species_count,
                    "progression_depth": progression.progression_depth,
                    "boss_ace_level": progression.boss_ace_level,
                    "boss_avg_level": progression.boss_avg_level,
                    "level_cap_offset": _progression_level_offset(progression.progression_depth),
                }
            )
        logger.info(
            "[silver/teams] source teams generated game=%s boss_id=%s boss_name=%s teams=%s",
            game_version,
            boss_id,
            boss_name,
            len(species_combos),
        )

    logger.info(
        "[silver/teams] built progression source teams from encounters source_teams=%s dropped_missing_boss=%s",
        len(sources),
        dropped_missing_boss,
    )
    return sources


def build_player_team_compact_tables(
    progression_source_teams: list[dict[str, Any]],
    reference_context: MoveReferenceContext,
) -> dict[str, list[dict[str, Any]]]:
    started_at = time.perf_counter()
    source_teams: list[dict[str, Any]] = []
    source_team_members: list[dict[str, Any]] = []
    member_move_options: list[dict[str, Any]] = []
    member_moveset_combos: list[dict[str, Any]] = []
    pokemon_moveset_options: list[dict[str, Any]] = []
    sampling_plan: list[dict[str, Any]] = []

    moveset_context_seen: set[tuple[str, str, int, str]] = set()
    moveset_choice_seen: set[tuple[str, str, int, str, str]] = set()
    estimated_avoided_variants = 0

    for progression_team in progression_source_teams:
        team_role = normalize_key_part(progression_team.get("team_role"))
        origin = normalize_key_part(progression_team.get("origin"))
        is_player_candidate = bool(progression_team.get("is_player_candidate", True))
        boss_name = normalize_key_part(progression_team.get("boss_name") or progression_team.get("gym"))
        boss_id = str(progression_team.get("boss_id") or "").strip().lower()
        starter_condition = _normalize_nullable_key_part(progression_team.get("starter_condition"))
        if team_role == "boss" or origin == "kaggle" or not is_player_candidate:
            continue
        game_version = normalize_key_part(progression_team.get("game_version"))
        avg_level = int(progression_team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
        starters = _resolve_starters_for_condition(game_version, starter_condition)
        if not game_version or not starters:
            continue

        for starter_base in starters:
            starter_species = resolve_starter_species_for_level(starter_base, avg_level)
            starter_type = get_starter_type(starter_base)
            source_team_id = make_team_id(
                "player-source",
                game_version,
                boss_name,
                source_team_id=str(progression_team.get("team_id") or ""),
                variant=f"starter:{starter_base}",
            )
            logical_species = [starter_species] + list(progression_team.get("pokemon") or [])
            levels = [avg_level] + [int(level) for level in list(progression_team.get("levels") or [])]
            logical_species = logical_species[:DEFAULT_TEAM_MEMBER_LIMIT]
            levels = levels[: len(logical_species)]

            source_teams.append(
                {
                    "source_team_id": source_team_id,
                    "game_version": game_version,
                    "team_role": "player",
                    "origin": "generated",
                    "is_player_candidate": True,
                    "boss_id": boss_id,
                    "boss_name": boss_name,
                    "gym_index": progression_team.get("gym_index"),
                    "starter_condition": starter_condition,
                    "starter_type": starter_type,
                    "starter_base": normalize_key_part(starter_base),
                    "starter_evolved_species": normalize_key_part(starter_species),
                    "progression_source_team_id": progression_team.get("team_id"),
                    "progression_pool_id": progression_team.get("progression_pool_id"),
                    "avg_level": avg_level,
                    "member_count": len(logical_species),
                    "boss_index": progression_team.get("boss_index"),
                    "max_boss_index": progression_team.get("max_boss_index"),
                    "available_species_count": progression_team.get("available_species_count"),
                    "max_species_count": progression_team.get("max_species_count"),
                    "progression_depth": progression_team.get("progression_depth"),
                    "boss_ace_level": progression_team.get("boss_ace_level"),
                    "boss_avg_level": progression_team.get("boss_avg_level"),
                    "level_cap_offset": progression_team.get("level_cap_offset"),
                }
            )

            member_combo_counts: list[int] = []
            for slot, species in enumerate(logical_species, start=1):
                species_norm = normalize_key_part(species)
                level = int(levels[slot - 1] if slot - 1 < len(levels) else avg_level)
                member_id = make_pokemon_instance_id(source_team_id, slot, species_norm)
                source_team_members.append(
                    {
                        "team_member_id": member_id,
                        "source_team_id": source_team_id,
                        "game_version": game_version,
                        "boss_id": boss_id,
                        "boss_name": boss_name,
                        "gym_index": progression_team.get("gym_index"),
                        "starter_condition": starter_condition,
                        "starter_type": starter_type,
                        "slot": slot,
                        "pokemon_species": species_norm,
                        "level": level,
                        "progression_pool_id": progression_team.get("progression_pool_id"),
                        "is_starter": slot == 1,
                    }
                )

                learnable = reference_context.damaging_moves(species_norm, level, game_version)
                ranked_moves = sorted(learnable)[:DEFAULT_MEMBER_MOVE_OPTION_LIMIT]
                member_combos = _build_member_moveset_combos(
                    ranked_moves,
                    combo_limit=DEFAULT_MEMBER_MOVESET_COMBO_LIMIT,
                )
                member_combo_counts.append(len(member_combos))

                context_key = (game_version, species_norm, level, "damaging-level-up-v1")
                if context_key not in moveset_context_seen:
                    moveset_context_seen.add(context_key)
                    pokemon_moveset_options.append(
                        {
                            "moveset_context_id": f"ctx:{stable_digest(*context_key, length=20)}",
                            "game_version": game_version,
                            "pokemon_species": species_norm,
                            "level": level,
                            "move_policy": "damaging-level-up-v1",
                            "candidate_move_count": len(ranked_moves),
                        }
                    )

                for rank, move_name in enumerate(ranked_moves, start=1):
                    score = float(max(0, DEFAULT_MEMBER_MOVE_OPTION_LIMIT - rank + 1))
                    member_move_options.append(
                        {
                            "team_member_id": member_id,
                            "source_team_id": source_team_id,
                            "game_version": game_version,
                            "slot": slot,
                            "pokemon_species": species_norm,
                            "level": level,
                            "move_name": move_name,
                            "option_rank": rank,
                            "option_score": score,
                            "moveset_context_id": f"ctx:{stable_digest(*context_key, length=20)}",
                        }
                    )
                    global_key = (game_version, species_norm, level, "damaging-level-up-v1", move_name)
                    if global_key in moveset_choice_seen:
                        continue
                    moveset_choice_seen.add(global_key)
                    pokemon_moveset_options.append(
                        {
                            "moveset_context_id": f"ctx:{stable_digest(*context_key, length=20)}",
                            "game_version": game_version,
                            "pokemon_species": species_norm,
                            "level": level,
                            "move_policy": "damaging-level-up-v1",
                            "move_name": move_name,
                            "option_rank": rank,
                            "option_score": score,
                        }
                    )

                for combo_rank, combo_moves in enumerate(member_combos, start=1):
                    normalized_moves = sorted({normalize_key_part(move) for move in combo_moves if normalize_key_part(move)})
                    combo_row: dict[str, Any] = {
                        "moveset_combo_id": make_moveset_combo_id(member_id, normalized_moves),
                        "team_id": source_team_id,
                        "pokemon_instance_id": member_id,
                        "slot_index": slot,
                        "game_version": game_version,
                        "pokemon_name": species_norm,
                        "level": level,
                        "moves": normalized_moves,
                        "move_count": len(normalized_moves),
                        "combo_rank": combo_rank,
                        "combo_score": float(max(1, len(member_combos) - combo_rank + 1)),
                        "source": "damaging-level-up-v1",
                    }
                    for idx in range(MOVESET_WIDTH):
                        combo_row[f"move_{idx + 1}"] = normalized_moves[idx] if idx < len(normalized_moves) else None
                    member_moveset_combos.append(combo_row)

            team_combo_space = estimate_team_moveset_space(member_combo_counts)
            estimated_avoided_variants += max(0, team_combo_space - 1)
            sampling_plan.append(
                {
                    "source_team_id": source_team_id,
                    "sampling_seed": stable_digest(source_team_id, "simulation"),
                    "move_policy": "top_rank_then_seeded_shuffle",
                    "max_moves_per_member": MOVESET_WIDTH,
                    "estimated_combo_space": team_combo_space,
                }
            )

    logger.info(
        "[silver/teams] built player teams source_teams=%s candidate_teams=%s members=%s moveset_combos=%s member_move_options=%s pokemon_moveset_options=%s progression_sources=%s avoided_full_cartesian_estimate=%s elapsed_s=%.2f",
        len(source_teams),
        len(source_teams),
        len(source_team_members),
        len(member_moveset_combos),
        len(member_move_options),
        len(pokemon_moveset_options),
        len(progression_source_teams),
        estimated_avoided_variants,
        time.perf_counter() - started_at,
    )

    return {
        "source_teams": source_teams,
        "source_team_members": source_team_members,
        "member_moveset_combos": member_moveset_combos,
        "member_move_options": member_move_options,
        "pokemon_moveset_options": pokemon_moveset_options,
        "simulation_sampling_plan": sampling_plan,
    }


def estimate_team_moveset_space(member_combo_counts: list[int]) -> int:
    space = 1
    for count in member_combo_counts:
        normalized = max(1, int(count or 0))
        space *= normalized
    return space


def _build_member_moveset_combos(moves: list[str], *, combo_limit: int) -> list[list[str]]:
    unique_moves = sorted({normalize_key_part(move) for move in moves if normalize_key_part(move)})
    if not unique_moves:
        return [[]]
    if len(unique_moves) <= MOVESET_WIDTH:
        return [unique_moves]
    combos = [list(combo) for combo in combinations(unique_moves, MOVESET_WIDTH)]
    return combos[: max(1, int(combo_limit or 1))]


def build_player_teams_from_progression_context(
    progression_source_teams: list[dict[str, Any]],
    reference_context: MoveReferenceContext | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Build compact candidate teams and per-member moveset combos (no full-team Cartesian expansion)."""
    if reference_context is None:
        raise ValueError("reference_context is required for offline team generation")

    compact = build_player_team_compact_tables(progression_source_teams, reference_context)
    candidate_teams = [
        {
            "team_id": row.get("source_team_id"),
            "source_team_id": row.get("progression_source_team_id"),
            "game_version": row.get("game_version"),
            "boss_id": row.get("boss_id"),
            "gym": row.get("boss_name"),
            "boss_name": row.get("boss_name"),
            "gym_index": row.get("gym_index"),
            "starter_condition": row.get("starter_condition"),
            "starter_type": row.get("starter_type"),
            "avg_level": row.get("avg_level"),
            "starter_base": row.get("starter_base"),
            "starter_evolved_species": row.get("starter_evolved_species"),
            "team_role": "player",
            "is_player_candidate": True,
            "boss_index": row.get("boss_index"),
            "max_boss_index": row.get("max_boss_index"),
            "available_species_count": row.get("available_species_count"),
            "max_species_count": row.get("max_species_count"),
            "progression_depth": row.get("progression_depth"),
            "boss_ace_level": row.get("boss_ace_level"),
            "boss_avg_level": row.get("boss_avg_level"),
            "level_cap_offset": row.get("level_cap_offset"),
        }
        for row in compact["source_teams"]
    ]
    candidate_team_members = compact["source_team_members"]
    member_moveset_combos = compact["member_moveset_combos"]
    return candidate_teams, candidate_team_members, member_moveset_combos
