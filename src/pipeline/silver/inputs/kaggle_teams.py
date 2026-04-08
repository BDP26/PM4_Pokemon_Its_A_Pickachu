"""Extract gym leader and elite four teams from Kaggle dataset for simulation."""
import csv
from itertools import combinations
import logging
import os
import time
from collections import defaultdict
from functools import lru_cache
from pathlib import Path
from typing import Any, Collection

import pokebase as pb

from src.pipeline.silver.inputs.game_config import (
    STARTER_EVOLUTION_CHAINS_BY_BASE,
    get_starter_choices,
    resolve_starter_species_for_level,
)


logger = logging.getLogger(__name__)


_REQUIRED_MOVES_LOOKUPS = 0
_NON_LEVEL_LEARN_METHODS = {"machine", "tm", "hm", "tutor", "egg"}
_HEURISTIC_MOVE_POWER: dict[str, int] = {
    "return": 80,
    "frustration": 70,
    "hidden-power": 60,
    "magnitude": 70,
    "sonic-boom": 20,
    "dragon-rage": 40,
    "seismic-toss": 50,
    "night-shade": 50,
    "psywave": 50,
}
_FORM_LOOKUP_FALLBACKS: dict[str, tuple[str, ...]] = {
    "meowstic": ("meowstic-male", "meowstic-female"),
    "pyroar": ("pyroar-male", "pyroar-female"),
    "jellicent": ("jellicent-male", "jellicent-female"),
    "gourgeist": ("gourgeist-average", "gourgeist-small", "gourgeist-large", "gourgeist-super"),
    "aegislash": ("aegislash-shield", "aegislash-blade"),
}
_GENERIC_FORM_SUFFIXES: tuple[str, ...] = ("male", "female", "average", "normal")
_MOVESET_WIDTH = 4
_DEFAULT_MEMBER_MOVE_POOL_CAP = 12
_DEFAULT_MEMBER_COMBO_LIMIT = 128


_GAME_TO_VERSION_GROUP: dict[str, str] = {
    "red": "red-blue",
    "blue": "red-blue",
    "gold": "gold-silver",
    "silver": "gold-silver",
    "ruby": "ruby-sapphire",
    "sapphire": "ruby-sapphire",
    "diamond": "diamond-pearl",
    "pearl": "diamond-pearl",
    "black": "black-white",
    "white": "black-white",
    "x": "x-y",
    "y": "x-y",
}

_STARTER_FAMILY_LOOKUP: dict[str, str] = {
    species: base
    for base, chain in STARTER_EVOLUTION_CHAINS_BY_BASE.items()
    for _, species in chain
}


def _family_root_for_species(species: str) -> str:
    normalized = species.lower().strip()
    return _STARTER_FAMILY_LOOKUP.get(normalized, normalized)


def _normalize_species_slug(species: str) -> str:
    normalized = species.strip().lower().replace(".", " ").replace("_", " ")
    normalized = " ".join(normalized.split())
    aliases = {
        "mr mime": "mr-mime",
        "mr. mime": "mr-mime",
        "mime jr": "mime-jr",
        "farfetch'd": "farfetchd",
        "nidoran f": "nidoran-f",
        "nidoran m": "nidoran-m",
    }
    if normalized in aliases:
        return aliases[normalized]
    return normalized.replace("'", "").replace(" ", "-")


def _species_lookup_candidates(species_slug: str) -> list[str]:
    candidates = [species_slug]
    candidates.extend(_FORM_LOOKUP_FALLBACKS.get(species_slug, ()))
    for suffix in _GENERIC_FORM_SUFFIXES:
        candidates.append(f"{species_slug}-{suffix}")

    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if candidate and candidate not in seen:
            seen.add(candidate)
            ordered.append(candidate)
    return ordered


def _resolve_pokemon_with_form_fallback(species_slug: str) -> tuple[Any | None, str]:
    for candidate in _species_lookup_candidates(species_slug):
        try:
            return pb.pokemon(candidate), candidate
        except Exception:
            continue
    return None, species_slug


@lru_cache(maxsize=8192)
def _move_profile(move_name: str) -> tuple[int, str]:
    started_at = time.perf_counter()
    try:
        move = pb.move(move_name)
        power = int(getattr(move, "power", 0) or 0)
        damage_class = str(getattr(getattr(move, "damage_class", None), "name", "status") or "status")
        if power <= 0 and damage_class in {"physical", "special"}:
            power = _HEURISTIC_MOVE_POWER.get(move_name, 0)
        return power, damage_class
    except Exception:
        return 0, "status"
    finally:
        elapsed = time.perf_counter() - started_at
        if elapsed >= 2.0:
            logger.warning("[silver/teams] slow move lookup move=%s elapsed_s=%.2f", move_name, elapsed)


@lru_cache(maxsize=4096)
def _learnable_moves_for_species(species: str, level: int, game_version: str) -> tuple[str, ...]:
    """Return all distinct learnable moves for a species in a game version context."""
    species_slug = _normalize_species_slug(species)
    version_group = _GAME_TO_VERSION_GROUP.get(game_version.lower().strip(), game_version.lower().strip())

    poke, _ = _resolve_pokemon_with_form_fallback(species_slug)
    if poke is None:
        return ("struggle",)

    moves_exact: set[str] = set()
    moves_cross: set[str] = set()
    for move_slot in getattr(poke, "moves", []):
        move_name = str(getattr(getattr(move_slot, "move", None), "name", "") or "")
        if not move_name:
            continue
        for detail in getattr(move_slot, "version_group_details", []):
            learn_method = str(getattr(getattr(detail, "move_learn_method", None), "name", "") or "")
            allow_non_level = learn_method in _NON_LEVEL_LEARN_METHODS
            if learn_method != "level-up" and not allow_non_level:
                continue
            learned_at = int(getattr(detail, "level_learned_at", 0) or 0)
            if learn_method == "level-up" and learned_at > level:
                continue
            detail_group = str(getattr(getattr(detail, "version_group", None), "name", "") or "")
            if detail_group == version_group:
                moves_exact.add(move_name)
            moves_cross.add(move_name)

    selected = moves_exact or moves_cross
    if not selected:
        return ("struggle",)
    return tuple(sorted(selected))


@lru_cache(maxsize=4096)
def _required_moves_for_species(species: str, level: int, game_version: str) -> tuple[str, ...]:
    global _REQUIRED_MOVES_LOOKUPS
    _REQUIRED_MOVES_LOOKUPS += 1
    started_at = time.perf_counter()

    if _REQUIRED_MOVES_LOOKUPS % 100 == 0:
        logger.info(
            "[silver/teams] progress required_moves lookups=%s species=%s level=%s version=%s",
            _REQUIRED_MOVES_LOOKUPS,
            species,
            level,
            game_version,
        )

    species_slug = _normalize_species_slug(species)
    learnable_moves = _learnable_moves_for_species(species, level, game_version)
    if learnable_moves == ("struggle",):
        elapsed = time.perf_counter() - started_at
        if elapsed >= 2.0:
            logger.warning(
                "[silver/teams] slow species lookup failed species=%s version=%s elapsed_s=%.2f",
                species_slug,
                game_version,
                elapsed,
            )
        return tuple(["struggle"])
    selected_pool: dict[str, tuple[int, int]] = {}
    for move_name in learnable_moves:
        power, damage_class = _move_profile(move_name)
        if power <= 0 or damage_class not in {"physical", "special"}:
            continue
        selected_pool[move_name] = (power, 0)
    if not selected_pool:
        return tuple(learnable_moves[:_MOVESET_WIDTH])

    ranked = sorted(
        selected_pool.items(),
        key=lambda item: (item[1][0], item[1][1], item[0]),
        reverse=True,
    )
    result = tuple(move_name for move_name, _ in ranked[:_MOVESET_WIDTH])
    elapsed = time.perf_counter() - started_at
    if elapsed >= 2.0:
        logger.warning(
            "[silver/teams] slow required-moves species=%s level=%s version=%s elapsed_s=%.2f",
            species_slug,
            level,
            game_version,
            elapsed,
        )
    return result


def _build_member_detail(
    name: str,
    level: int,
    moves: list[str],
    game_version: str,
    origin: str = "kaggle",
) -> dict[str, Any]:
    cleaned_moves = [str(move).strip().lower().replace(" ", "-") for move in moves if str(move).strip()]
    learnable_moves = list(_learnable_moves_for_species(name, level, game_version))
    required_moves = list(_required_moves_for_species(name, level, game_version))
    return {
        "name": str(name).strip().lower(),
        "level": int(level),
        "moves": cleaned_moves,
        "learnable_moves": learnable_moves,
        "required_moves": required_moves,
        "origin": origin,
    }


def _member_moveset_combinations(moves: list[str]) -> tuple[list[tuple[str, ...]], bool]:
    unique_moves = sorted({move for move in moves if move})
    if not unique_moves:
        return [("struggle",)], False
    if len(unique_moves) <= _MOVESET_WIDTH:
        return [tuple(unique_moves)], False

    pool_cap_raw = os.getenv("PIPELINE_MEMBER_MOVE_POOL_CAP", str(_DEFAULT_MEMBER_MOVE_POOL_CAP)).strip()
    combo_limit_raw = os.getenv("PIPELINE_MEMBER_COMBO_LIMIT", str(_DEFAULT_MEMBER_COMBO_LIMIT)).strip()

    try:
        pool_cap = int(pool_cap_raw)
    except ValueError:
        pool_cap = _DEFAULT_MEMBER_MOVE_POOL_CAP
    try:
        combo_limit = int(combo_limit_raw)
    except ValueError:
        combo_limit = _DEFAULT_MEMBER_COMBO_LIMIT

    truncated = False
    if pool_cap > 0 and len(unique_moves) > pool_cap:
        ranked = sorted(
            unique_moves,
            key=lambda move_name: (_move_profile(move_name)[0], move_name),
            reverse=True,
        )
        unique_moves = sorted(ranked[:pool_cap])
        truncated = True

    combos: list[tuple[str, ...]] = []
    for combo in combinations(unique_moves, _MOVESET_WIDTH):
        combos.append(combo)
        if combo_limit > 0 and len(combos) >= combo_limit:
            truncated = True
            break
    return combos or [("struggle",)], truncated


def build_member_movesets_dataset(teams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Expand team members into combinatorial moveset rows for big-data use cases."""
    rows: list[dict[str, Any]] = []
    team_counter = 0
    member_counter = 0
    truncated_members = 0
    for team in teams:
        team_counter += 1
        details = team.get("details")
        if not isinstance(details, list):
            continue
        for slot_idx, member in enumerate(details, start=1):
            member_counter += 1
            if not isinstance(member, dict):
                continue
            species = str(member.get("name", "")).strip().lower()
            if not species:
                continue
            learnable = member.get("learnable_moves", [])
            learnable_moves = learnable if isinstance(learnable, list) else []
            combos, was_truncated = _member_moveset_combinations([str(move) for move in learnable_moves])
            if was_truncated:
                truncated_members += 1
            for combo_idx, combo in enumerate(combos, start=1):
                row = {
                    "team_id": team.get("team_id"),
                    "game_version": team.get("game_version"),
                    "boss_name": team.get("boss_name"),
                    "member_slot": slot_idx,
                    "species": species,
                    "level": int(member.get("level") or 0),
                    "origin": member.get("origin"),
                    "learnable_moves_count": len({m for m in learnable_moves if m}),
                    "moveset_index": combo_idx,
                    "moveset_size": len(combo),
                    "moveset_key": "|".join(combo),
                }
                for move_idx in range(_MOVESET_WIDTH):
                    row[f"move_{move_idx + 1}"] = combo[move_idx] if move_idx < len(combo) else None
                rows.append(row)
    logger.info(
        "[silver/teams] member movesets built teams=%s members=%s rows=%s truncated_members=%s",
        team_counter,
        member_counter,
        len(rows),
        truncated_members,
    )
    return rows


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


def _build_starter_variant(base_team: dict[str, Any], starter_base: str) -> dict[str, Any]:
    version = str(base_team.get("game_version", "unknown"))
    avg_level = int(base_team.get("avg_level") or 20)
    starter_species = resolve_starter_species_for_level(starter_base.lower().strip(), avg_level)

    starter_member = _build_member_detail(
        name=starter_species,
        level=avg_level,
        moves=[],
        game_version=version,
        origin="starter",
    )

    base_details = base_team.get("details", [])
    team_details = [starter_member]
    if isinstance(base_details, list):
        for item in base_details:
            if not isinstance(item, dict):
                continue
            name = item.get("name")
            if not isinstance(name, str):
                continue
            team_details.append(
                _build_member_detail(
                    name=name,
                    level=int(item.get("level") or avg_level),
                    moves=list(item.get("moves", [])) if isinstance(item.get("moves"), list) else [],
                    game_version=version,
                    origin="kaggle",
                )
            )

    team_details = _dedupe_details_by_family(team_details, limit=6)
    levels = [int(member.get("level") or avg_level) for member in team_details]
    team_avg_level = int(sum(levels) / len(levels)) if levels else avg_level

    return {
        "team_id": f"STARTER_{version}_{starter_base}_{base_team.get('team_id')}",
        "boss_name": None,
        "gym": base_team.get("gym"),
        "game_version": version,
        "pokemon": [member["name"] for member in team_details],
        "levels": levels,
        "avg_level": team_avg_level,
        "details": team_details,
        "starter_base": starter_base,
        "starter_evolved_species": starter_species,
        "source_team_id": base_team.get("team_id"),
        "team_role": "player",
        "is_player_candidate": True,
    }


def _append_starter_variants(teams: list[dict[str, Any]]) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    for team in teams:
        game_version = team.get("game_version")
        if not isinstance(game_version, str):
            continue
        starters = get_starter_choices(game_version)
        if not starters:
            continue
        for starter in starters:
            variants.append(_build_starter_variant(team, starter))
    return teams + variants


def extract_kaggle_teams(
    bronze_dir: Path,
    allowed_versions: Collection[str] | None = None,
) -> list[dict[str, Any]]:
    """
    Extract gym leader and elite four teams from Kaggle CSV.

    Returns list of canonical team dicts.

    Team structure:
    {
        'team_id': 'KAGGLE_game_leader_idx',
        'boss_name': 'Leader Name',
        'gym': 'Gym Location',
        'game_version': 'game',
        'pokemon': ['pikachu', 'raichu', ...],
        'levels': [25, 30, ...],
        'avg_level': 28,
        'details': [{'name': 'pikachu', 'level': 25, 'moves': [...]}, ...]
    }
    """
    started_at = time.perf_counter()
    kaggle_file = bronze_dir / 'kagglehub' / 'gym_leaders_elite_four.csv'
    logger.info("[silver/teams] start extract file=%s", kaggle_file)

    if not kaggle_file.exists():
        logger.warning("[silver/teams] file not found; skipping file=%s", kaggle_file)
        return []

    allowed_versions_set = {version.lower() for version in allowed_versions} if allowed_versions else None
    skipped_versions: dict[str, int] = defaultdict(int)
    total_rows = 0
    kept_rows = 0

    # Parse Kaggle CSV
    teams_by_leader: dict[str, dict[str, Any]] = defaultdict(lambda: {'pokemon': [], 'game': None, 'gym': None})

    try:
        with open(kaggle_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                total_rows += 1
                game = row['Game'].strip().lower()
                if allowed_versions_set is not None and game not in allowed_versions_set:
                    skipped_versions[game] += 1
                    continue
                kept_rows += 1
                gym_leader = row['Gym leader'].lower()
                pokemon = row['Pokemon'].lower()
                level = int(row['Level']) if row['Level'] else 20
                moves = [
                    m.strip().lower() for m in [
                        row.get('Move 1', ''),
                        row.get('Move 2', ''),
                        row.get('Move 3', ''),
                        row.get('Move 4', '')
                    ] if m and m.strip()
                ]

                key = f"{game}:{gym_leader}"
                teams_by_leader[key]['game'] = game  # type: ignore
                teams_by_leader[key]['gym_leader'] = gym_leader  # type: ignore
                teams_by_leader[key]['gym'] = row['Gym']  # type: ignore
                member_detail = _build_member_detail(
                    name=pokemon,
                    level=level,
                    moves=moves[:4],
                    game_version=game,
                    origin="kaggle",
                )
                pokemon_list = teams_by_leader[key]['pokemon']
                if isinstance(pokemon_list, list):
                    pokemon_list.append(member_detail)
                if total_rows % 250 == 0:
                    logger.info(
                        "[silver/teams] progress csv rows=%s kept=%s leaders=%s",
                        total_rows,
                        kept_rows,
                        len(teams_by_leader),
                    )
    except Exception as e:
        logger.exception("[silver/teams] error while reading csv: %s", e)
        return []

    # Convert to team format
    teams = []
    for idx, (key, data) in enumerate(sorted(teams_by_leader.items())):
        game = data.get('game', 'unknown')
        gym_leader = data.get('gym_leader', 'unknown')
        gym = data.get('gym', 'unknown')
        pokemon_list = data.get('pokemon', [])

        team_id = f"KAGGLE_{game}_{gym_leader}_{idx}"
        team_entry = {
            'team_id': team_id,
            'boss_name': gym_leader.title() if isinstance(gym_leader, str) else 'Unknown',
            'gym': gym,
            'game_version': game,
            'pokemon': [p['name'] for p in pokemon_list] if isinstance(pokemon_list, list) else [],
            'levels': [p['level'] for p in pokemon_list] if isinstance(pokemon_list, list) else [],
            'avg_level': sum(p['level'] for p in pokemon_list) // len(pokemon_list) if isinstance(pokemon_list, list) and pokemon_list else 20,
            'details': pokemon_list if isinstance(pokemon_list, list) else [],
            'team_role': 'boss',
            'is_player_candidate': False,
        }
        teams.append(team_entry)

    base_teams_count = len(teams)
    teams = _append_starter_variants(teams)
    starter_variants = len(teams) - base_teams_count

    if skipped_versions:
        skipped_preview = ", ".join(
            f"{version}={count}" for version, count in sorted(skipped_versions.items())
        )
        logger.info("[silver/teams] skipped non-config versions: %s", skipped_preview)


    elapsed = time.perf_counter() - started_at
    logger.info(
        "[silver/teams] done extract rows_total=%s rows_kept=%s leaders=%s base_teams=%s starter_variants=%s teams_total=%s elapsed_s=%.2f",
        total_rows,
        kept_rows,
        len(teams_by_leader),
        base_teams_count,
        starter_variants,
        len(teams),
        elapsed,
    )
    return teams








