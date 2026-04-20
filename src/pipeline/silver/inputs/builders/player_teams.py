"""Player-team generation from progression context and starter choices."""

import logging
import time
from itertools import combinations, product
from typing import Any

from src.pipeline.silver.config.game_config import (
    get_starter_family_root,
    get_starter_choices,
    resolve_starter_species_for_level,
)
from src.pipeline.silver.config.team_config import (
    ALLOW_LARGE_TEAM_VARIANTS,
    DEFAULT_MEMBER_COMBO_LIMIT,
    DEFAULT_MEMBER_LEVEL,
    DEFAULT_TEAM_VARIANT_LIMIT,
    DEFAULT_TEAM_MEMBER_LIMIT,
    MOVESET_WIDTH,
    TEAM_VARIANT_CONFIRMATION_THRESHOLD,
)
from src.pipeline.silver.inputs.connectors.pokeapi_moves import _build_member_detail, _build_member_moves
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, make_team_id, normalize_key_part


logger = logging.getLogger(__name__)

DEFAULT_CATCH_POOL_SIZE = 5



def _family_root_for_species(species: str) -> str:
    normalized = species.lower().strip()
    return get_starter_family_root(normalized)


def _dedupe_details_by_family(details: list[dict[str, Any]], limit: int = 6) -> list[dict[str, Any]]:
    seen_roots: set[str] = set()
    result: list[dict[str, Any]] = []
    skipped_missing_name = 0
    skipped_duplicate_family = 0
    for item in details:
        name = item.get("name")
        if not isinstance(name, str):
            skipped_missing_name += 1
            continue
        family_root = _family_root_for_species(name)
        if family_root in seen_roots:
            skipped_duplicate_family += 1
            continue
        seen_roots.add(family_root)
        result.append(item)
        if len(result) >= limit:
            break
    logger.debug(
        "[silver/teams] dedupe by family input=%s output=%s skipped_missing_name=%s skipped_duplicate_family=%s limit=%s",
        len(details),
        len(result),
        skipped_missing_name,
        skipped_duplicate_family,
        limit,
    )
    return result


def _member_moveset_variants(raw_moves: list[str]) -> list[list[str]]:
    moves = sorted({str(move).strip().lower() for move in raw_moves if str(move).strip()})
    if not moves:
        logger.debug("[silver/teams] member has no moves; emitting empty moveset variant")
        return [[]]
    if len(moves) <= MOVESET_WIDTH:
        logger.debug(
            "[silver/teams] member moves within width unique_moves=%s width=%s variants=1",
            len(moves),
            MOVESET_WIDTH,
        )
        return [moves]

    variants: list[list[str]] = []
    for combo in combinations(moves, MOVESET_WIDTH):
        variants.append(list(combo))
        if len(variants) >= DEFAULT_MEMBER_COMBO_LIMIT:
            break
    logger.debug(
        "[silver/teams] generated moveset variants unique_moves=%s width=%s variant_count=%s combo_cap=%s",
        len(moves),
        MOVESET_WIDTH,
        len(variants),
        DEFAULT_MEMBER_COMBO_LIMIT,
    )
    return variants if variants else [moves[:MOVESET_WIDTH]]


def _build_starter_variant(base_team: dict[str, Any], starter_base: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build a player team variant with starter.

    Returns:
        - lean team dict with pokemon, levels, moves
        - dict mapping move keys -> move details
    """
    version = str(base_team.get("game_version", "unknown"))
    avg_level = int(base_team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
    starter_species = resolve_starter_species_for_level(starter_base.lower().strip(), avg_level)
    logger.debug(
        "[silver/teams] building starter variant source_team_id=%s game_version=%s starter_base=%s starter_species=%s avg_level=%s",
        base_team.get("team_id"),
        version,
        starter_base,
        starter_species,
        avg_level,
    )
    starter_member = _build_member_detail(
        name=starter_species,
        level=avg_level,
        moves=[],
        game_version=version,
        origin="starter",
    )

    team_details = []
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

            member_detail = _build_member_detail(
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
        logger.warning(
            "[silver/teams] no team details after dedupe source_team_id=%s game_version=%s starter_base=%s",
            base_team.get("team_id"),
            version,
            starter_base,
        )
        return [], {}

    per_member_variants = [_member_moveset_variants(list(member.get("moves", []))) for member in team_details]
    variant_space_size = 1
    for member_variants in per_member_variants:
        variant_space_size *= len(member_variants)
    logger.debug(
        "[silver/teams] starter variant space source_team_id=%s members=%s variant_space=%s",
        base_team.get("team_id"),
        len(team_details),
        variant_space_size,
    )
    effective_team_variant_limit = DEFAULT_TEAM_VARIANT_LIMIT
    if variant_space_size > TEAM_VARIANT_CONFIRMATION_THRESHOLD and not ALLOW_LARGE_TEAM_VARIANTS:
        # Keep pipeline execution safe by tightening the cap unless user explicitly opts in.
        effective_team_variant_limit = min(DEFAULT_TEAM_VARIANT_LIMIT, 250)
        logger.warning(
            "[silver/teams] large starter variant space detected source_team_id=%s game_version=%s starter_base=%s estimated_space=%s threshold=%s applied_limit=%s (set PM4_ALLOW_LARGE_TEAM_VARIANTS=1 to allow full cap=%s)",
            base_team.get("team_id"),
            version,
            starter_base,
            variant_space_size,
            TEAM_VARIANT_CONFIRMATION_THRESHOLD,
            effective_team_variant_limit,
            DEFAULT_TEAM_VARIANT_LIMIT,
        )
    team_variants: list[dict[str, Any]] = []
    move_storage: dict[str, Any] = {}
    source_team_id = str(base_team.get("team_id") or "")
    truncated_for_team_limit = False

    for combo in product(*per_member_variants):
        if len(team_variants) >= effective_team_variant_limit:
            truncated_for_team_limit = True
            break
        variant_signature_parts = []
        for member, moveset in zip(team_details, combo, strict=False):
            variant_signature_parts.append(
                f"{str(member.get('name') or '').strip().lower()}={'|'.join(sorted(set(str(move).strip().lower() for move in moveset if str(move).strip())))}"
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
            member_moves = _build_member_moves(
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
            effective_team_variant_limit,
            variant_space_size,
        )

    logger.info(
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
                    {
                        "level_max": 0,
                        "chance_max": 0,
                        "capture_rate": 0,
                    },
                )
                slot["level_max"] = max(slot["level_max"], level_max)
                slot["chance_max"] = max(slot["chance_max"], chance_max)
                slot["capture_rate"] = max(slot["capture_rate"], capture_rate)

    # Fallback to species-only map when encounter payload is missing.
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
                        {
                            "level_max": 0,
                            "chance_max": 0,
                            "capture_rate": 0,
                        },
                    )

    candidates = [
        (
            species,
            payload["chance_max"],
            payload["level_max"],
            payload["capture_rate"],
        )
        for species, payload in by_species.items()
    ]
    candidates.sort(key=lambda item: (-item[1], -item[2], -item[3], item[0]))
    return candidates


def build_progression_source_teams(
    progression_records: list[dict[str, Any]],
    boss_teams: list[dict[str, Any]],
    catch_pool_size: int = DEFAULT_CATCH_POOL_SIZE,
) -> list[dict[str, Any]]:
    sources: list[dict[str, Any]] = []
    level_by_boss = _boss_level_lookup(boss_teams)
    safe_catch_pool_size = max(1, min(catch_pool_size, DEFAULT_TEAM_MEMBER_LIMIT - 1))

    for record in progression_records:
        game_version = normalize_key_part(record.get("game") or record.get("version"))
        boss_name = normalize_key_part(record.get("boss_name"))
        if not game_version or not boss_name:
            continue

        boss_level = level_by_boss.get((game_version, boss_name), DEFAULT_MEMBER_LEVEL)
        candidates = _extract_species_candidates(record)
        selected_species = [species for species, _, _, _ in candidates[:safe_catch_pool_size]]
        if not selected_species:
            continue

        part = normalize_key_part(record.get("part"))
        source_team_id = make_team_id(
            "progression",
            game_version,
            boss_name,
            variant=part or None,
        )
        sources.append(
            {
                "team_id": source_team_id,
                "game_version": game_version,
                "gym": str(record.get("boss_name") or "").strip() or None,
                "avg_level": boss_level,
                "pokemon": selected_species,
                "levels": [boss_level for _ in selected_species],
                "moves": [[] for _ in selected_species],
            }
        )

    logger.info(
        "[silver/teams] built progression source teams records=%s sources=%s catch_pool_size=%s",
        len(progression_records),
        len(sources),
        safe_catch_pool_size,
    )
    return sources


def build_player_teams_from_progression_context(progression_source_teams: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build player candidate teams from progression context (boss milestones + starter choices).

    Returns:
        - list of lean player teams
        - dict mapping move keys -> move details
    """
    started_at = time.perf_counter()
    variants: list[dict[str, Any]] = []
    all_moves: dict[str, Any] = {}

    skipped_missing_game_version = 0
    skipped_missing_starters = 0

    for team in progression_source_teams:
        game_version = team.get("game_version")
        if not isinstance(game_version, str):
            skipped_missing_game_version += 1
            logger.debug(
                "[silver/teams] skipping source team missing game_version source_team_id=%s",
                team.get("team_id"),
            )
            continue
        starters = get_starter_choices(game_version)
        if not starters:
            skipped_missing_starters += 1
            logger.debug(
                "[silver/teams] skipping source team without starters source_team_id=%s game_version=%s",
                team.get("team_id"),
                game_version,
            )
            continue
        logger.debug(
            "[silver/teams] expanding source team source_team_id=%s game_version=%s starter_count=%s",
            team.get("team_id"),
            game_version,
            len(starters),
        )
        for starter in starters:
            team_dicts, move_dict = _build_starter_variant(team, starter)
            variants.extend(team_dicts)
            all_moves.update(move_dict)

    logger.info(
        "[silver/teams] built player teams from progression source_teams=%s player_teams=%s move_records=%s skipped_missing_game_version=%s skipped_missing_starters=%s elapsed_s=%.2f",
        len(progression_source_teams),
        len(variants),
        len(all_moves),
        skipped_missing_game_version,
        skipped_missing_starters,
        time.perf_counter() - started_at,
    )
    return variants, all_moves
