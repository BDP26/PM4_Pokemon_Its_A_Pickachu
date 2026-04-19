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
    DEFAULT_MEMBER_COMBO_LIMIT,
    DEFAULT_MEMBER_LEVEL,
    DEFAULT_TEAM_MEMBER_LIMIT,
    MOVESET_WIDTH,
)
from src.pipeline.silver.inputs.connectors.pokeapi_moves import _build_member_detail, _build_member_moves
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, make_team_id


logger = logging.getLogger(__name__)



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


def _member_moveset_variants(raw_moves: list[str]) -> list[list[str]]:
    moves = sorted({str(move).strip().lower() for move in raw_moves if str(move).strip()})
    if not moves:
        return [[]]
    if len(moves) <= MOVESET_WIDTH:
        return [moves]

    variants: list[list[str]] = []
    for combo in combinations(moves, MOVESET_WIDTH):
        variants.append(list(combo))
        if len(variants) >= DEFAULT_MEMBER_COMBO_LIMIT:
            break
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
        return [], {}

    per_member_variants = [_member_moveset_variants(list(member.get("moves", []))) for member in team_details]
    team_variants: list[dict[str, Any]] = []
    move_storage: dict[str, Any] = {}
    source_team_id = str(base_team.get("team_id") or "")

    for combo in product(*per_member_variants):
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

    return team_variants, move_storage


def build_player_teams_from_progression_context(boss_progression_teams: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Build player candidate teams from progression context (boss milestones + starter choices).

    Returns:
        - list of lean player teams
        - dict mapping move keys -> move details
    """
    started_at = time.perf_counter()
    variants: list[dict[str, Any]] = []
    all_moves: dict[str, Any] = {}

    for team in boss_progression_teams:
        game_version = team.get("game_version")
        if not isinstance(game_version, str):
            continue
        starters = get_starter_choices(game_version)
        if not starters:
            continue
        for starter in starters:
            team_dicts, move_dict = _build_starter_variant(team, starter)
            variants.extend(team_dicts)
            all_moves.update(move_dict)

    logger.info(
        "[silver/teams] built player teams from progression boss_teams=%s player_teams=%s move_records=%s elapsed_s=%.2f",
        len(boss_progression_teams),
        len(variants),
        len(all_moves),
        time.perf_counter() - started_at,
    )
    return variants, all_moves