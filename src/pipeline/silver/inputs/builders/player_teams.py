"""Player-team generation from progression context and starter choices.

Final refactor:
- per-game compatible
- parquet-first move universe
- controlled combinatorics
- no legacy logic
"""

import logging
import math
import time
from itertools import combinations, islice, product
from typing import Any, Iterable

from tqdm import tqdm

from src.pipeline.silver.config.game_config import (
    get_starter_family_root,
    get_starter_choices,
    resolve_starter_species_for_level,
)
from src.pipeline.silver.config.team_config import (
    ALLOW_LARGE_TEAM_VARIANTS,
    DEFAULT_CATCH_POOL_SIZE,
    DEFAULT_MEMBER_COMBO_LIMIT,
    DEFAULT_MEMBER_LEVEL,
    DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM,
    DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
    DEFAULT_SOURCE_TEAM_POOL_SIZE,
    DEFAULT_TEAM_MEMBER_LIMIT,
    DEFAULT_TEAM_VARIANT_LIMIT,
    MOVESET_WIDTH,
    PLAYER_TEAM_PROGRESS_LOG_INTERVAL,
    TEAM_VARIANT_CONFIRMATION_THRESHOLD,
)
from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, make_team_id, normalize_key_part


logger = logging.getLogger(__name__)

MAX_CANDIDATE_MOVES_PER_MEMBER = 8
MAX_SOURCE_TEAM_SIZE = 5


def _effective_team_variant_limit(variant_space_size: int) -> int | None:
    """Resolve effective team-variant cap.

    Returns:
        int: hard cap on generated variants
        None: no explicit cap (generate full variant space)
    """
    team_limit = DEFAULT_TEAM_VARIANT_LIMIT if DEFAULT_TEAM_VARIANT_LIMIT > 0 else None
    moveset_limit = (
        DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM
        if DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM > 0
        else None
    )

    if ALLOW_LARGE_TEAM_VARIANTS:
        if team_limit is None:
            return None
        return max(1, team_limit)

    # Conservative mode: keep both caps active.
    active_limits = [limit for limit in (team_limit, moveset_limit) if limit is not None]
    if not active_limits:
        return max(1, variant_space_size)
    return max(1, min(active_limits))


def _family_root_for_species(species: str) -> str:
    normalized = species.lower().strip()
    return get_starter_family_root(normalized)


def _dedupe_details_by_family(details: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    seen_roots: set[str] = set()
    result: list[dict[str, Any]] = []

    for item in details:
        name = item.get("name")
        if not isinstance(name, str):
            continue
        family_root = _family_root_for_species(name)
        if family_root in seen_roots:
            continue
        seen_roots.add(family_root)
        result.append(item)
        if len(result) >= limit:
            break

    return result


def _stable_species_signature(species_names: Iterable[str]) -> str:
    return "|".join(sorted(normalize_key_part(name) for name in species_names if normalize_key_part(name)))


def _trim_candidate_moves(raw_moves: list[str], max_moves: int = MAX_CANDIDATE_MOVES_PER_MEMBER) -> list[str]:
    moves = sorted({str(move).strip().lower() for move in raw_moves if str(move).strip()})
    return moves[:max_moves]


def _member_moveset_variants(raw_moves: list[str]) -> list[list[str]]:
    moves = _trim_candidate_moves(raw_moves)
    if not moves:
        logger.debug("[silver/teams] member has no moves; emitting empty moveset variant")
        return [[]]

    if len(moves) <= MOVESET_WIDTH:
        return [moves]

    variants: list[list[str]] = []
    for combo in combinations(moves, MOVESET_WIDTH):
        variants.append(list(combo))
        if len(variants) >= DEFAULT_MEMBER_COMBO_LIMIT:
            break

    return variants if variants else [moves[:MOVESET_WIDTH]]


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
                level_max = _safe_int(encounter.get("level_max"))
                chance_max = _safe_int(encounter.get("encounter_chance_max"))
                capture_rate = _safe_int(encounter.get("capture_rate"))
                slot = by_species.setdefault(
                    species,
                    {"level_max": 0, "chance_max": 0, "capture_rate": 0},
                )
                slot["level_max"] = max(slot["level_max"], level_max)
                slot["chance_max"] = max(slot["chance_max"], chance_max)
                slot["capture_rate"] = max(slot["capture_rate"], capture_rate)

    if not by_species:
        location_to_species = record.get("reachable_location_pokemon", {})
        if isinstance(location_to_species, dict):
            for species_list in location_to_species.values():
                if not isinstance(species_list, list):
                    continue
                for species_raw in species_list:
                    species = normalize_key_part(species_raw)
                    if not species:
                        continue
                    by_species.setdefault(
                        species,
                        {"level_max": 0, "chance_max": 0, "capture_rate": 0},
                    )

    candidates = [
        (species, payload["chance_max"], payload["level_max"], payload["capture_rate"])
        for species, payload in by_species.items()
    ]
    candidates.sort(key=lambda item: (-item[1], -item[2], -item[3], item[0]))
    return candidates


def _rank_candidate_pool(
    candidates: list[tuple[str, int, int, int]],
    *,
    boss_level: int,
    pool_size: int,
) -> tuple[list[tuple[str, int, int, int]], dict[str, int]]:
    """Score + constrain candidate pool instead of truncating by raw source order."""
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
        score = (0.45 * chance_signal) + (0.25 * capture_signal) + (0.30 * level_realism)
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


def build_boss_progression_pools(
    progression_records: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Build cumulative boss pools with incremental deltas.

    Pool growth is keyed by game and derived from newly-reachable locations only,
    so downstream team generation avoids recomputing full encounter universes
    for every boss.
    """
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
                "game_version": game_version,
                "boss_name": boss_name,
                "part": normalize_key_part(record.get("part")),
                "pool_candidates": pool_candidates,
                "pool_species_count": len(pool_candidates),
                "delta_location_count": len(delta_locations),
                "delta_species_count": delta_species_count,
            }
        )

        logger.info(
            "[silver/teams] boss progression pool game=%s boss=%s species_pool=%s delta_locations=%s delta_species=%s",
            game_version,
            boss_name,
            len(pool_candidates),
            len(delta_locations),
            delta_species_count,
        )

    return pools


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


def _base_team_diversity_score(species_combo: tuple[tuple[str, int, int, int], ...]) -> tuple[int, int, int, str]:
    chance_sum = sum(item[1] for item in species_combo)
    level_sum = sum(item[2] for item in species_combo)
    capture_sum = sum(item[3] for item in species_combo)
    signature = _stable_species_signature(item[0] for item in species_combo)
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
        species_list = [item[0] for item in combo]
        score = _base_team_diversity_score(combo)
        scored.append((score, species_list))

    scored.sort(key=lambda item: (-item[0][0], -item[0][1], -item[0][2], item[0][3]))

    seen_signatures: set[str] = set()
    result: list[list[str]] = []
    for _, species_list in scored:
        signature = _stable_species_signature(species_list)
        if signature in seen_signatures:
            continue
        seen_signatures.add(signature)
        result.append(species_list)
        if len(result) >= combo_limit:
            break

    return result


def _build_starter_variant(
    base_team: dict[str, Any],
    starter_base: str,
    reference_context: MoveReferenceContext,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    version = str(base_team.get("game_version", "unknown"))
    avg_level = int(base_team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
    starter_species = resolve_starter_species_for_level(starter_base.lower().strip(), avg_level)

    starter_member = reference_context.build_member_detail(
        name=starter_species,
        level=avg_level,
        moves=[],
        game_version=version,
        origin="starter",
    )

    team_details: list[dict[str, Any]] = []
    if starter_member is not None:
        team_details.append(starter_member)

    base_details = base_team.get("pokemon", [])
    base_moves = base_team.get("moves", [])
    base_levels = base_team.get("levels", [])

    if isinstance(base_details, list) and isinstance(base_moves, list):
        for idx, item in enumerate(base_details):
            if isinstance(item, dict):
                name = item.get("name")
                level = int(item.get("level") or avg_level)
            else:
                name = item
                level = int(base_levels[idx] or avg_level) if isinstance(base_levels, list) and idx < len(base_levels) else avg_level

            if not isinstance(name, str):
                continue

            moves = base_moves[idx] if idx < len(base_moves) else []
            member_detail = reference_context.build_member_detail(
                name=name,
                level=level,
                moves=list(moves) if isinstance(moves, list) else [],
                game_version=version,
                origin="progression",
            )
            if member_detail is not None:
                team_details.append(member_detail)

    team_details = _dedupe_details_by_family(team_details, limit=DEFAULT_TEAM_MEMBER_LIMIT)
    if not team_details:
        return [], {}

    per_member_variants = [_member_moveset_variants(list(member.get("moves", []))) for member in team_details]

    variant_space_size = 1
    for member_variants in per_member_variants:
        variant_space_size *= len(member_variants)

    effective_team_variant_limit = _effective_team_variant_limit(variant_space_size)
    effective_limit_label = str(effective_team_variant_limit) if effective_team_variant_limit is not None else "unbounded"
    if variant_space_size > TEAM_VARIANT_CONFIRMATION_THRESHOLD and not ALLOW_LARGE_TEAM_VARIANTS:
        logger.warning(
            "[silver/teams] large starter variant space detected source_team_id=%s game_version=%s starter_base=%s estimated_space=%s threshold=%s applied_limit=%s",
            base_team.get("team_id"),
            version,
            starter_base,
            variant_space_size,
            TEAM_VARIANT_CONFIRMATION_THRESHOLD,
            effective_limit_label,
        )

    team_variants: list[dict[str, Any]] = []
    move_storage: dict[str, Any] = {}
    source_team_id = str(base_team.get("team_id") or "")
    truncated_for_team_limit = False

    for combo in product(*per_member_variants):
        if effective_team_variant_limit is not None and len(team_variants) >= effective_team_variant_limit:
            truncated_for_team_limit = True
            break

        variant_signature_parts = []
        for member, moveset in zip(team_details, combo, strict=False):
            normalized_moves = sorted(set(str(move).strip().lower() for move in moveset if str(move).strip()))
            variant_signature_parts.append(
                f"{str(member.get('name') or '').strip().lower()}={'|'.join(normalized_moves)}"
            )
        variant_signature = ";".join(variant_signature_parts)

        team_id = make_team_id(
            "player",
            version,
            starter_base,
            source_team_id=source_team_id,
            variant=f"{starter_species}:{variant_signature}",
        )

        team_members: list[dict[str, Any]] = []
        for slot_index, (member, selected_moves) in enumerate(zip(team_details, combo, strict=False), start=1):
            member_name = str(member.get("name") or "").strip().lower()
            member_level = int(member.get("level") or avg_level)
            instance_id = make_pokemon_instance_id(team_id, slot_index, member_name)
            team_members.append(
                {
                    "name": member_name,
                    "level": member_level,
                    "moves": list(selected_moves),
                    "origin": member.get("origin"),
                    "pokemon_instance_id": instance_id,
                }
            )
            member_moves = reference_context.build_member_moves(
                name=member_name,
                level=member_level,
                moves=list(selected_moves),
                game_version=version,
            )
            if member_moves is not None:
                member_moves["pokemon_instance_id"] = instance_id
                member_moves["team_id"] = team_id
                member_moves["slot_index"] = slot_index
                move_storage[instance_id] = member_moves

        levels = [int(member.get("level") or avg_level) for member in team_members]
        team_avg_level = int(sum(levels) / len(levels)) if levels else avg_level
        team_variants.append(
            {
                "team_id": team_id,
                "boss_name": None,
                "gym": base_team.get("gym"),
                "game_version": version,
                "pokemon": [member["name"] for member in team_members],
                "levels": levels,
                "moves": [member.get("moves", []) for member in team_members],
                "pokemon_instance_ids": [str(member.get("pokemon_instance_id") or "") for member in team_members],
                "avg_level": team_avg_level,
                "starter_base": starter_base,
                "starter_evolved_species": starter_species,
                "source_team_id": base_team.get("team_id"),
                "team_role": "player",
                "is_player_candidate": True,
            }
        )

    if truncated_for_team_limit:
        logger.warning(
            "[silver/teams] starter variant generation truncated source_team_id=%s game_version=%s starter_base=%s generated=%s limit=%s estimated_space=%s",
            base_team.get("team_id"),
            version,
            starter_base,
            len(team_variants),
            effective_limit_label,
            variant_space_size,
        )

    logger.debug(
        "[silver/teams] built starter variants source_team_id=%s game_version=%s starter_base=%s team_variants=%s move_records=%s",
        base_team.get("team_id"),
        version,
        starter_base,
        len(team_variants),
        len(move_storage),
    )
    return team_variants, move_storage


def _boss_level_lookup(boss_teams: list[dict[str, Any]]) -> dict[tuple[str, str], int]:
    lookup: dict[tuple[str, str], int] = {}
    for team in boss_teams:
        game_version = normalize_key_part(team.get("game_version"))
        boss_name = normalize_key_part(team.get("boss_name"))
        if not game_version or not boss_name:
            continue
        try:
            level = int(team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
        except (TypeError, ValueError):
            level = DEFAULT_MEMBER_LEVEL
        lookup[(game_version, boss_name)] = level
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
    total_pruned_combos = 0
    total_pruned_candidates = 0
    total_family_pruned_candidates = 0

    for pool in progression_pools:
        game_version = pool["game_version"]
        boss_name = pool["boss_name"]
        boss_level = level_by_boss.get((game_version, boss_name), DEFAULT_MEMBER_LEVEL)
        candidates = list(pool["pool_candidates"])
        if not candidates:
            continue

        candidate_pool, pool_diagnostics = _rank_candidate_pool(
            candidates,
            boss_level=boss_level,
            pool_size=candidate_pool_size,
        )
        total_pruned_candidates += pool_diagnostics["pruned"]
        total_family_pruned_candidates += pool_diagnostics["family_pruned"]
        species_combos = _generate_diverse_species_combos(
            candidates=candidate_pool,
            team_fill_size=team_fill_size,
            combo_limit=DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
        )

        if not species_combos:
            fallback_species = [species for species, _, _, _ in candidate_pool[:team_fill_size]]
            if not fallback_species:
                continue
            species_combos = [fallback_species]

        theoretical_combo_count = (
            math.comb(len(candidate_pool), team_fill_size)
            if len(candidate_pool) >= team_fill_size
            else 0
        )
        total_pruned_combos += max(0, theoretical_combo_count - len(species_combos))
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
                    "gym": str(boss_name).strip() or None,
                    "avg_level": boss_level,
                    "pokemon": selected_species,
                    "levels": [boss_level for _ in selected_species],
                    "moves": [[] for _ in selected_species],
                }
            )

    logger.info(
        "[silver/teams] built progression source teams bosses=%s sources=%s source_team_size=%s candidate_pool_size=%s source_team_combo_limit=%s pruned_combos=%s pruned_candidates=%s family_pruned_candidates=%s",
        len(progression_pools),
        len(sources),
        team_fill_size,
        candidate_pool_size,
        DEFAULT_SOURCE_TEAM_COMBO_LIMIT,
        total_pruned_combos,
        total_pruned_candidates,
        total_family_pruned_candidates,
    )
    return sources


def build_player_teams_from_progression_context(
    progression_source_teams: list[dict[str, Any]],
    reference_context: MoveReferenceContext | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if reference_context is None:
        raise ValueError("reference_context is required for offline team generation")
    started_at = time.perf_counter()
    variants: list[dict[str, Any]] = []
    all_moves: dict[str, Any] = {}

    skipped_missing_game_version = 0
    skipped_missing_starters = 0
    processed_source_teams = 0
    total_source_teams = len(progression_source_teams)
    progress_interval = max(1, PLAYER_TEAM_PROGRESS_LOG_INTERVAL)

    progress_bar = tqdm(
        progression_source_teams,
        desc="[silver/teams] generating player teams",
        unit="source_team",
    )
    for team in progress_bar:
        processed_source_teams += 1
        game_version = team.get("game_version")
        if not isinstance(game_version, str):
            skipped_missing_game_version += 1
            continue

        starters = get_starter_choices(game_version)
        if not starters:
            skipped_missing_starters += 1
            continue

        for starter in starters:
            team_dicts, move_dict = _build_starter_variant(team, starter, reference_context)
            variants.extend(team_dicts)
            all_moves.update(move_dict)

        if processed_source_teams % progress_interval == 0 or processed_source_teams == total_source_teams:
            progress_bar.set_postfix(
                {
                    "player_teams": len(variants),
                    "move_records": len(all_moves),
                    "skip_gv": skipped_missing_game_version,
                    "skip_starters": skipped_missing_starters,
                },
                refresh=False,
            )

    logger.info(
        "[silver/teams] built player teams from progression source_teams=%s processed=%s player_teams=%s move_records=%s skipped_missing_game_version=%s skipped_missing_starters=%s progress_interval=%s elapsed_s=%.2f",
        len(progression_source_teams),
        processed_source_teams,
        len(variants),
        len(all_moves),
        skipped_missing_game_version,
        skipped_missing_starters,
        progress_interval,
        time.perf_counter() - started_at,
    )
    return variants, all_moves
