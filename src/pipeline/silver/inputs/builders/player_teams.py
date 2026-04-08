"""Player-team generation from progression context and starter choices."""

import logging
import time
from typing import Any

from src.pipeline.silver.config.game_config import (
    STARTER_EVOLUTION_CHAINS_BY_BASE,
    get_starter_choices,
    resolve_starter_species_for_level,
)
from src.pipeline.silver.config.team_config import DEFAULT_MEMBER_LEVEL, DEFAULT_TEAM_MEMBER_LIMIT
from src.pipeline.silver.inputs.connectors.pokeapi_moves import _build_member_detail, _build_member_moves
from src.pipeline.silver.transforms.keys import make_pokemon_instance_id, make_team_id


logger = logging.getLogger(__name__)


_STARTER_FAMILY_LOOKUP: dict[str, str] = {
    species: base
    for base, chain in STARTER_EVOLUTION_CHAINS_BY_BASE.items()
    for _, species in chain
}


def _family_root_for_species(species: str) -> str:
    normalized = species.lower().strip()
    return _STARTER_FAMILY_LOOKUP.get(normalized, normalized)


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


def _build_starter_variant(base_team: dict[str, Any], starter_base: str) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build a player team variant with starter.

    Returns:
        - lean team dict with pokemon, levels, moves
        - dict mapping move keys -> move details
    """
    version = str(base_team.get("game_version", "unknown"))
    avg_level = int(base_team.get("avg_level") or DEFAULT_MEMBER_LEVEL)
    starter_species = resolve_starter_species_for_level(starter_base.lower().strip(), avg_level)
    team_id = make_team_id(
        "player",
        version,
        starter_base,
        source_team_id=str(base_team.get("team_id") or ""),
        variant=starter_species,
    )

    starter_member = _build_member_detail(
        name=starter_species,
        level=avg_level,
        moves=[],
        game_version=version,
        origin="starter",
    )

    move_storage: dict[str, Any] = {}

    team_details = []
    if starter_member is not None:
        starter_instance_id = make_pokemon_instance_id(team_id, 1, starter_species)
        starter_member["pokemon_instance_id"] = starter_instance_id
        team_details.append(starter_member)
        # Store move details for starter
        starter_moves = _build_member_moves(
            name=starter_species,
            level=avg_level,
            moves=[],
            game_version=version,
        )
        if starter_moves is not None:
            starter_moves["pokemon_instance_id"] = starter_instance_id
            starter_moves["team_id"] = team_id
            starter_moves["slot_index"] = 1
            move_storage[starter_instance_id] = starter_moves

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
                origin="kaggle",
            )
            if member_detail is not None:
                slot_index = idx + 2
                instance_id = make_pokemon_instance_id(team_id, slot_index, name)
                member_detail["pokemon_instance_id"] = instance_id
                team_details.append(member_detail)

                # Store move details
                member_moves = _build_member_moves(
                    name=name,
                    level=level,
                    moves=list(moves) if isinstance(moves, list) else [],
                    game_version=version,
                )
                if member_moves is not None:
                    member_moves["pokemon_instance_id"] = instance_id
                    member_moves["team_id"] = team_id
                    member_moves["slot_index"] = slot_index
                    move_storage[instance_id] = member_moves

    team_details = _dedupe_details_by_family(team_details, limit=DEFAULT_TEAM_MEMBER_LIMIT)
    levels = [int(member.get("level") or avg_level) for member in team_details]
    team_avg_level = int(sum(levels) / len(levels)) if levels else avg_level

    pokemon_moves = [member.get("moves", []) for member in team_details]
    pokemon_instance_ids = [str(member.get("pokemon_instance_id") or "") for member in team_details]
    team_dict = {
        "team_id": team_id,
        "boss_name": None,
        "gym": base_team.get("gym"),
        "game_version": version,
        "pokemon": [member["name"] for member in team_details],
        "levels": levels,
        "moves": pokemon_moves,
        "pokemon_instance_ids": pokemon_instance_ids,
        "avg_level": team_avg_level,
        "starter_base": starter_base,
        "starter_evolved_species": starter_species,
        "source_team_id": base_team.get("team_id"),
        "team_role": "player",
        "is_player_candidate": True,
    }

    return team_dict, move_storage


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
            team_dict, move_dict = _build_starter_variant(team, starter)
            variants.append(team_dict)
            all_moves.update(move_dict)

    logger.info(
        "[silver/teams] built player teams from progression boss_teams=%s player_teams=%s move_records=%s elapsed_s=%.2f",
        len(boss_progression_teams),
        len(variants),
        len(all_moves),
        time.perf_counter() - started_at,
    )
    return variants, all_moves




