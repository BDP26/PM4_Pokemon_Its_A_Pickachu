"""Compact offline player-team generation for Silver.

Silver persists logical team/member/move-option references only.
Concrete move-set expansion is deferred to Gold/simulation.
"""

from __future__ import annotations

import logging
import time
from itertools import combinations, islice
from typing import Any

from src.pipeline.silver.config.game_config import (
    get_starter_choices,
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
from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.transforms.keys import (
    make_moveset_combo_id,
    make_pokemon_instance_id,
    make_team_id,
    normalize_key_part,
    stable_digest,
)


logger = logging.getLogger(__name__)
MAX_SOURCE_TEAM_SIZE = 5


def _family_root_for_species(species: str) -> str:
    normalized = normalize_key_part(species)
    if not normalized:
        return ""
    return normalized


def _stable_species_signature(species_names: list[str]) -> str:
    return "|".join(sorted(normalize_key_part(name) for name in species_names if normalize_key_part(name)))


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
                if not species:
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

    for record in progression_records:
        game_version = normalize_key_part(record.get("game") or record.get("version"))
        boss_name = normalize_key_part(record.get("boss_name"))
        if not game_version or not boss_name:
            continue

        location_to_encounters = record.get("reachable_location_encounters", {})
        known_locations = seen_locations_by_game.setdefault(game_version, set())
        cumulative_candidates = cumulative_candidates_by_game.setdefault(game_version, {})

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
                if not species:
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
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    level_by_boss = _boss_level_lookup(boss_teams)
    progression_pools = build_boss_progression_pools(progression_records)

    team_fill_size = max(1, min(catch_pool_size, MAX_SOURCE_TEAM_SIZE))
    candidate_pool_size = max(team_fill_size, DEFAULT_SOURCE_TEAM_POOL_SIZE)

    for pool in progression_pools:
        game_version = pool["game_version"]
        boss_name = pool["boss_name"]
        boss_level = level_by_boss.get((game_version, boss_name), DEFAULT_MEMBER_LEVEL)
        candidate_pool, _ = _rank_candidate_pool(
            list(pool["pool_candidates"]),
            boss_level=boss_level,
            pool_size=candidate_pool_size,
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

        part = pool.get("part")
        for combo_index, selected_species in enumerate(species_combos, start=1):
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
                    "boss_name": boss_name,
                    "gym": str(boss_name).strip() or None,
                    "avg_level": boss_level,
                    "pokemon": selected_species,
                    "levels": [boss_level for _ in selected_species],
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
        game_version = normalize_key_part(progression_team.get("game_version"))
        boss_name = normalize_key_part(progression_team.get("boss_name") or progression_team.get("gym"))
        avg_level = int(progression_team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
        starters = get_starter_choices(game_version)
        if not game_version or not starters:
            continue

        for starter_base in starters:
            starter_species = resolve_starter_species_for_level(starter_base, avg_level)
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
                    "team_role": "player_source",
                    "boss_name": boss_name,
                    "starter_base": normalize_key_part(starter_base),
                    "starter_evolved_species": normalize_key_part(starter_species),
                    "progression_source_team_id": progression_team.get("team_id"),
                    "progression_pool_id": progression_team.get("progression_pool_id"),
                    "avg_level": avg_level,
                    "member_count": len(logical_species),
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
                        "boss_name": boss_name,
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
        "[silver/teams] built player teams source_teams=%s candidate_teams=%s members=%s moveset_combos=%s member_move_options=%s progression_sources=%s avoided_full_cartesian_estimate=%s elapsed_s=%.2f",
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
            "gym": row.get("boss_name"),
            "boss_name": row.get("boss_name"),
            "avg_level": row.get("avg_level"),
            "starter_base": row.get("starter_base"),
            "starter_evolved_species": row.get("starter_evolved_species"),
            "team_role": "player",
            "is_player_candidate": True,
        }
        for row in compact["source_teams"]
    ]
    candidate_team_members = compact["source_team_members"]
    member_moveset_combos = compact["member_moveset_combos"]
    return candidate_teams, candidate_team_members, member_moveset_combos
