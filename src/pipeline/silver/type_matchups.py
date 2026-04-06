from __future__ import annotations

import logging
import os
from functools import lru_cache
import importlib
from pathlib import Path
import time
from typing import Any, TypedDict, cast

import pokebase as pb

from src.pipeline.common.io import read_json, write_json, write_parquet
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, SILVER_SIMULATION_DIRNAME


logger = logging.getLogger(__name__)

_LOOKUP_CACHE_DIRNAME = "lookup_cache"
_POKEMON_CACHE_FILENAME = "pokemon_profiles.json"
_MOVE_CACHE_FILENAME = "move_profiles.json"

_LOCAL_POKEMON_PROFILES: dict[str, dict[str, Any]] = {}
_LOCAL_MOVE_PROFILES: dict[str, MoveProfile] = {}
_LOCAL_CACHE_PATHS: tuple[Path, Path] | None = None


class MoveProfile(TypedDict):
    name: str
    type: str
    power: int
    damage_class: str
    level_learned_at: int
    version_group: str
    degraded_data: bool


class CombatProfile(TypedDict):
    species: str
    species_id: str
    level: int
    types: list[str]
    stats: dict[str, int]
    max_hp: int
    current_hp: int
    legal_moves: list[MoveProfile]
    degraded_data: bool


class DuelResult(TypedDict):
    winner: str
    attacker_remaining_hp: int
    defender_remaining_hp: int
    turns: int
    attacker_move_used: str
    defender_move_used: str
    degraded_data: bool


class TeamBattleResult(TypedDict):
    team_id_attacker: Any
    team_id_defender: Any
    attacker_win: bool
    winner_team_id: Any
    attacker_remaining_pokemon: int
    defender_remaining_pokemon: int
    attacker_total_remaining_hp: int
    defender_total_remaining_hp: int
    battle_turns: int
    simulation_score: float
    degraded_data: bool
    warnings: list[str]
    duel_summaries: list[dict[str, Any]]


class WarningCollector:
    def __init__(self) -> None:
        self._warnings: list[str] = []
        self._seen: set[str] = set()

    def warn(self, message: str) -> None:
        if message in self._seen:
            return
        self._seen.add(message)
        self._warnings.append(message)
        logger.warning(message)

    def all(self) -> list[str]:
        return list(self._warnings)

    def has_warnings(self) -> bool:
        return bool(self._warnings)


def _cache_paths(silver_dir: Path) -> tuple[Path, Path]:
    cache_dir = silver_dir / SILVER_SIMULATION_DIRNAME / _LOOKUP_CACHE_DIRNAME
    return cache_dir / _POKEMON_CACHE_FILENAME, cache_dir / _MOVE_CACHE_FILENAME


def _install_lookup_cache(
    pokemon_profiles: dict[str, dict[str, Any]],
    move_profiles: dict[str, MoveProfile],
) -> None:
    global _LOCAL_POKEMON_PROFILES, _LOCAL_MOVE_PROFILES
    _LOCAL_POKEMON_PROFILES = dict(pokemon_profiles)
    _LOCAL_MOVE_PROFILES = dict(move_profiles)
    _get_pokemon_profile_cached.cache_clear()
    _get_move_profile_cached.cache_clear()


def _load_lookup_cache_from_disk(silver_dir: Path) -> None:
    global _LOCAL_CACHE_PATHS
    pokemon_path, move_path = _cache_paths(silver_dir)
    _LOCAL_CACHE_PATHS = (pokemon_path, move_path)

    pokemon_profiles: dict[str, dict[str, Any]] = {}
    move_profiles: dict[str, MoveProfile] = {}

    if pokemon_path.exists():
        loaded_pokemon = cast(dict[str, Any], read_json(pokemon_path))
        if isinstance(loaded_pokemon, dict):
            pokemon_profiles = {
                str(key): cast(dict[str, Any], value)
                for key, value in loaded_pokemon.items()
                if isinstance(value, dict)
            }

    if move_path.exists():
        loaded_moves = cast(dict[str, Any], read_json(move_path))
        if isinstance(loaded_moves, dict):
            for key, value in loaded_moves.items():
                if not isinstance(value, dict):
                    continue
                move_profiles[str(key)] = {
                    "name": str(value.get("name", key)),
                    "type": str(value.get("type", "Normal")),
                    "power": int(value.get("power", 0) or 0),
                    "damage_class": str(value.get("damage_class", "physical")),
                    "level_learned_at": int(value.get("level_learned_at", 0) or 0),
                    "version_group": str(value.get("version_group", "")),
                    "degraded_data": bool(value.get("degraded_data", False)),
                }

    _install_lookup_cache(pokemon_profiles=pokemon_profiles, move_profiles=move_profiles)
    logger.info(
        "[type_matchups] loaded lookup cache pokemon=%s moves=%s",
        len(_LOCAL_POKEMON_PROFILES),
        len(_LOCAL_MOVE_PROFILES),
    )


def _persist_lookup_cache_to_disk() -> None:
    if _LOCAL_CACHE_PATHS is None:
        return
    pokemon_path, move_path = _LOCAL_CACHE_PATHS
    write_json(pokemon_path, _LOCAL_POKEMON_PROFILES)
    write_json(move_path, _LOCAL_MOVE_PROFILES)
    logger.info(
        "[type_matchups] persisted lookup cache pokemon=%s moves=%s",
        len(_LOCAL_POKEMON_PROFILES),
        len(_LOCAL_MOVE_PROFILES),
    )


def _fetch_pokemon_profile_from_api(species_id: str) -> tuple[dict[str, Any], bool, str | None]:
    try:
        poke = pb.pokemon(species_id)
    except Exception as exc:  # pragma: no cover - network/api failure path
        warning = f"Pokemon lookup failed for '{species_id}': {exc}"
        return (
            {
                "name": species_id,
                "types": ["Normal"],
                "stats": _default_stats(),
                "moves": [],
            },
            True,
            warning,
        )

    stats: dict[str, int] = _default_stats()
    for stat in getattr(poke, "stats", []):
        stat_name = (getattr(getattr(stat, "stat", None), "name", "") or "").replace("-", "_")
        if stat_name == "special_attack":
            stat_name = "sp_attack"
        elif stat_name == "special_defense":
            stat_name = "sp_defense"
        base_stat = getattr(stat, "base_stat", None)
        if stat_name and isinstance(base_stat, int):
            stats[stat_name] = base_stat

    moves: list[dict[str, Any]] = []
    for move_slot in getattr(poke, "moves", []):
        move_name = getattr(getattr(move_slot, "move", None), "name", "") or ""
        if not move_name:
            continue

        version_group_details: list[dict[str, Any]] = []
        for detail in getattr(move_slot, "version_group_details", []):
            version_group_details.append(
                {
                    "version_group": getattr(getattr(detail, "version_group", None), "name", "") or "",
                    "learn_method": getattr(getattr(detail, "move_learn_method", None), "name", "") or "",
                    "level_learned_at": int(getattr(detail, "level_learned_at", 0) or 0),
                }
            )

        moves.append(
            {
                "move_name": move_name,
                "version_group_details": version_group_details,
            }
        )

    return (
        {
            "name": getattr(poke, "name", species_id),
            "types": [getattr(t.type, "name", "Normal").title() for t in getattr(poke, "types", [])] or ["Normal"],
            "stats": stats,
            "moves": moves,
        },
        False,
        None,
    )


def _fetch_move_profile_from_api(move_name: str) -> tuple[MoveProfile, bool, str | None]:
    try:
        move = pb.move(move_name)
    except Exception as exc:  # pragma: no cover - network/api failure path
        warning = f"Move lookup failed for '{move_name}': {exc}"
        fallback_move: MoveProfile = {
            "name": move_name,
            "type": "Normal",
            "power": 40,
            "damage_class": "physical",
            "level_learned_at": 0,
            "version_group": "fallback",
            "degraded_data": True,
        }
        return (
            fallback_move,
            True,
            warning,
        )

    power = int(getattr(move, "power", 0) or 0)
    damage_class = getattr(getattr(move, "damage_class", None), "name", "physical") or "physical"
    move_type = getattr(getattr(move, "type", None), "name", "Normal") or "Normal"
    resolved_move: MoveProfile = {
        "name": getattr(move, "name", move_name),
        "type": move_type.title(),
        "power": power,
        "damage_class": damage_class,
        "level_learned_at": 0,
        "version_group": "",
        "degraded_data": False,
    }
    return (
        resolved_move,
        False,
        None,
    )


def _warm_lookup_cache_for_teams(teams_data: list[dict[str, Any]]) -> None:
    species_ids: set[str] = set()
    for team in teams_data:
        for member in _team_members(team):
            species = str(member.get("species", "") or "")
            species_id = normalize_species_name(species)
            if species_id:
                species_ids.add(species_id)

    logger.info("[type_matchups] warming lookup cache species=%s", len(species_ids))
    move_names: set[str] = set()

    for species_id in sorted(species_ids):
        if species_id in _LOCAL_POKEMON_PROFILES:
            profile = _LOCAL_POKEMON_PROFILES[species_id]
        else:
            profile, degraded, _ = _fetch_pokemon_profile_from_api(species_id)
            if not degraded:
                _LOCAL_POKEMON_PROFILES[species_id] = profile

        profile = _LOCAL_POKEMON_PROFILES.get(species_id, profile if "profile" in locals() else {})
        for move_slot in cast(list[dict[str, Any]], profile.get("moves", [])):
            move_name = str(move_slot.get("move_name", "") or "")
            if move_name:
                move_names.add(move_name)

    logger.info("[type_matchups] warming move cache moves=%s", len(move_names))
    for move_name in sorted(move_names):
        if move_name in _LOCAL_MOVE_PROFILES:
            continue
        move_profile, degraded, _ = _fetch_move_profile_from_api(move_name)
        if not degraded:
            _LOCAL_MOVE_PROFILES[move_name] = move_profile

    _install_lookup_cache(_LOCAL_POKEMON_PROFILES, _LOCAL_MOVE_PROFILES)


# PokéAPI version_group names must match actual learnset group identifiers.
_GAME_TO_VERSION_GROUP: dict[str, str] = {
    "red": "red-blue",
    "blue": "red-blue",
    "yellow": "yellow",
    "gold": "gold-silver",
    "silver": "gold-silver",
    "crystal": "crystal",
    "ruby": "ruby-sapphire",
    "sapphire": "ruby-sapphire",
    "emerald": "emerald",
    "firered": "firered-leafgreen",
    "leafgreen": "firered-leafgreen",
    "diamond": "diamond-pearl",
    "pearl": "diamond-pearl",
    "platinum": "platinum",
    "heartgold": "heartgold-soulsilver",
    "soulsilver": "heartgold-soulsilver",
    "black": "black-white",
    "white": "black-white",
    "black-2": "black-2-white-2",
    "white-2": "black-2-white-2",
    "x": "x-y",
    "y": "x-y",
}

_SPECIAL_SPECIES_ALIASES: dict[str, str] = {
    "mr mime": "mr-mime",
    "mr-mime": "mr-mime",
    "mr. mime": "mr-mime",
    "mime jr": "mime-jr",
    "mime-jr": "mime-jr",
    "ho oh": "ho-oh",
    "ho-oh": "ho-oh",
    "farfetchd": "farfetchd",
    "farfetch'd": "farfetchd",
    "nidoran f": "nidoran-f",
    "nidoran-f": "nidoran-f",
    "nidoran m": "nidoran-m",
    "nidoran-m": "nidoran-m",
}

_STRUGGLE_MOVE: MoveProfile = {
    "name": "struggle",
    "type": "Normal",
    "power": 50,
    "damage_class": "physical",
    "level_learned_at": 0,
    "version_group": "fallback",
    "degraded_data": True,
}


def _default_stats() -> dict[str, int]:
    return {
        "hp": 50,
        "attack": 50,
        "defense": 50,
        "sp_attack": 50,
        "sp_defense": 50,
        "speed": 50,
    }


def normalize_species_name(name: str) -> str:
    """Normalize user/data species names into a PokéAPI-friendly id."""
    normalized = " ".join(name.strip().lower().replace(".", " ").replace("_", " ").split())
    if not normalized:
        return ""

    if normalized in _SPECIAL_SPECIES_ALIASES:
        return _SPECIAL_SPECIES_ALIASES[normalized]

    normalized = normalized.replace("'", "")
    normalized = normalized.replace(" ", "-")
    return _SPECIAL_SPECIES_ALIASES.get(normalized, normalized)


def get_pokemon_types(pokemon_id: str) -> list[str]:
    """Get types for a Pokemon, returning a default type only if lookup fails."""
    warnings = WarningCollector()
    profile, _, _ = _get_pokemon_profile(normalize_species_name(pokemon_id), pokemon_id, warnings)
    return list(profile.get("types", ["Normal"]))


@lru_cache(maxsize=2048)
def _get_pokemon_profile_cached(species_id: str) -> tuple[dict[str, Any], bool, str | None]:
    cached = _LOCAL_POKEMON_PROFILES.get(species_id)
    if cached is not None:
        return cached, False, None

    profile, degraded, warning = _fetch_pokemon_profile_from_api(species_id)
    if not degraded:
        _LOCAL_POKEMON_PROFILES[species_id] = profile
    return profile, degraded, warning


def _get_pokemon_profile(
    species_id: str,
    original_species: str,
    warnings: WarningCollector,
) -> tuple[dict[str, Any], bool, str | None]:
    profile, degraded, warning = _get_pokemon_profile_cached(species_id)
    if warning:
        warnings.warn(
            f"Pokemon lookup failed (original='{original_species}', normalized='{species_id}') -> fallback defaults"
        )
    return profile, degraded, warning


@lru_cache(maxsize=4096)
def _get_move_profile_cached(move_name: str) -> tuple[MoveProfile, bool, str | None]:
    cached = _LOCAL_MOVE_PROFILES.get(move_name)
    if cached is not None:
        return cached, False, None

    move_profile, degraded, warning = _fetch_move_profile_from_api(move_name)
    if not degraded:
        _LOCAL_MOVE_PROFILES[move_name] = move_profile
    return move_profile, degraded, warning


def _get_move_profile(move_name: str, warnings: WarningCollector) -> tuple[MoveProfile, bool]:
    profile, degraded, warning = _get_move_profile_cached(move_name)
    if warning:
        warnings.warn(f"Move lookup failed (move='{move_name}') -> fallback move profile")
    return profile, degraded


def _team_members(team: dict[str, Any]) -> list[dict[str, Any]]:
    def _to_iterable(value: Any) -> list[Any]:
        if isinstance(value, list):
            return value
        if isinstance(value, tuple):
            return list(value)
        if hasattr(value, "tolist") and not isinstance(value, (str, bytes, dict)):
            converted = value.tolist()
            if isinstance(converted, list):
                return converted
        return []

    details = team.get("details")
    members: list[dict[str, Any]] = []

    details_entries = _to_iterable(details)
    if details_entries:
        for entry in details_entries:
            if not isinstance(entry, dict):
                continue
            species = entry.get("name") or entry.get("species")
            if not isinstance(species, str) or not species:
                continue
            members.append(
                {
                    "species": species,
                    "level": int(entry.get("level", team.get("avg_level", 20)) or team.get("avg_level", 20) or 20),
                }
            )
        if members:
            return members[:6]

    pokemon_entries = _to_iterable(team.get("pokemon", []))
    if pokemon_entries:
        for entry in pokemon_entries[:6]:
            if isinstance(entry, dict):
                species = entry.get("name") or entry.get("species")
                if not isinstance(species, str) or not species:
                    continue
                members.append(
                    {
                        "species": species,
                        "level": int(entry.get("level", team.get("avg_level", 20)) or team.get("avg_level", 20) or 20),
                    }
                )
            elif isinstance(entry, str) and entry:
                members.append(
                    {
                        "species": entry,
                        "level": int(team.get("avg_level", 20) or 20),
                    }
                )

    return members[:6]


def _version_group_for_game(game_version: str | None) -> str | None:
    if not isinstance(game_version, str):
        return None
    normalized = game_version.strip().lower()
    return _GAME_TO_VERSION_GROUP.get(normalized, normalized)


def _calculate_max_hp(base_hp: int, level: int) -> int:
    # Deterministic simplified HP formula; IV/EV/nature intentionally omitted.
    return max(1, int(((2 * base_hp) * level) / 100) + level + 10)


def _legal_moves_for_pokemon(
    species_id: str,
    original_species: str,
    level: int,
    game_version: str | None,
    warnings: WarningCollector,
) -> tuple[list[MoveProfile], bool]:
    profile, pokemon_degraded, _ = _get_pokemon_profile(species_id, original_species, warnings)
    version_group = _version_group_for_game(game_version)
    legal_moves: list[MoveProfile] = []
    seen_moves: set[str] = set()
    degraded = pokemon_degraded

    def _collect(allow_any_version: bool) -> None:
        nonlocal degraded
        for move_slot in profile.get("moves", []):
            move_name = move_slot.get("move_name")
            if not isinstance(move_name, str) or not move_name or move_name in seen_moves:
                continue

            for detail in move_slot.get("version_group_details", []):
                if not isinstance(detail, dict):
                    continue
                if detail.get("learn_method") != "level-up":
                    continue

                learned_at = int(detail.get("level_learned_at", 0) or 0)
                if learned_at > level:
                    continue

                detail_group = str(detail.get("version_group", "") or "")
                if not allow_any_version and version_group and detail_group and detail_group != version_group:
                    continue

                move_profile, move_degraded = _get_move_profile(move_name, warnings)
                if move_degraded:
                    degraded = True

                if int(move_profile.get("power", 0) or 0) <= 0:
                    continue

                legal_move: MoveProfile = {
                    "name": move_profile["name"],
                    "type": move_profile["type"],
                    "power": int(move_profile["power"]),
                    "damage_class": move_profile["damage_class"],
                    "level_learned_at": learned_at,
                    "version_group": detail_group,
                    "degraded_data": bool(move_profile.get("degraded_data", False) or move_degraded),
                }
                legal_moves.append(legal_move)
                seen_moves.add(move_name)
                break

    _collect(allow_any_version=False)

    if not legal_moves:
        _collect(allow_any_version=True)
        if legal_moves:
            degraded = True
            warnings.warn(
                f"No level-up moves in version group '{version_group}' for '{species_id}' at level {level}; used cross-version fallback"
            )

    if not legal_moves:
        degraded = True
        warnings.warn(f"No damaging legal moves for '{species_id}' at level {level}; using struggle")
        legal_moves.append(cast(MoveProfile, cast(object, dict(_STRUGGLE_MOVE))))

    return legal_moves, degraded


def _type_multiplier(move_type: str, defender_types: list[str], type_chart: dict[str, dict[str, float]]) -> float:
    multiplier = 1.0
    attacking_type = move_type.title()
    for defending_type in defender_types or ["Normal"]:
        multiplier *= float(type_chart.get(attacking_type, {}).get(defending_type.title(), 1.0))
    return multiplier


def get_pokemon_combat_profile(
    species: str,
    level: int,
    game_version: str | None,
    warnings: WarningCollector,
) -> CombatProfile:
    species_id = normalize_species_name(species)
    profile, pokemon_degraded, _ = _get_pokemon_profile(species_id, species, warnings)
    legal_moves, moves_degraded = _legal_moves_for_pokemon(species_id, species, level, game_version, warnings)

    stats = cast(dict[str, int], profile.get("stats", _default_stats()))
    max_hp = _calculate_max_hp(int(stats.get("hp", 50) or 50), max(1, level))

    return {
        "species": species,
        "species_id": species_id,
        "level": max(1, int(level or 1)),
        "types": [str(t).title() for t in profile.get("types", ["Normal"])],
        "stats": stats,
        "max_hp": max_hp,
        "current_hp": max_hp,
        "legal_moves": legal_moves,
        "degraded_data": bool(pokemon_degraded or moves_degraded),
    }


def calculate_expected_damage(
    attacker: CombatProfile,
    defender: CombatProfile,
    move_profile: MoveProfile,
    type_chart: dict[str, dict[str, float]],
) -> int:
    """Deterministic simplified damage model.

    This intentionally ignores status, items, abilities, weather, priority,
    critical hits, accuracy, recoil, and secondary effects.
    """
    power = int(move_profile.get("power", 0) or 0)
    if power <= 0:
        return 0

    move_type = str(move_profile.get("type", "Normal")).title()
    type_effectiveness = _type_multiplier(move_type, defender.get("types", ["Normal"]), type_chart)
    if type_effectiveness == 0:
        return 0

    damage_class = str(move_profile.get("damage_class", "physical")).lower()
    attacker_stats = attacker.get("stats", _default_stats())
    defender_stats = defender.get("stats", _default_stats())

    if damage_class == "special":
        attack_stat = int(attacker_stats.get("sp_attack", attacker_stats.get("attack", 50)) or 50)
        defense_stat = int(defender_stats.get("sp_defense", defender_stats.get("defense", 50)) or 50)
    else:
        attack_stat = int(attacker_stats.get("attack", 50) or 50)
        defense_stat = int(defender_stats.get("defense", 50) or 50)

    level = max(1, int(attacker.get("level", 1) or 1))
    base_damage = (((2 * level / 5 + 2) * power * max(attack_stat, 1)) / max(defense_stat, 1) / 50) + 2
    stab = 1.5 if move_type in {str(t).title() for t in attacker.get("types", ["Normal"])} else 1.0

    dealt = int(round(base_damage * stab * type_effectiveness))
    return max(1, dealt)


def choose_best_move(
    attacker: CombatProfile,
    defender: CombatProfile,
    type_chart: dict[str, dict[str, float]],
) -> MoveProfile:
    legal_moves = attacker.get("legal_moves", [])
    if not legal_moves:
        return cast(MoveProfile, cast(object, dict(_STRUGGLE_MOVE)))

    return max(
        legal_moves,
        key=lambda move: calculate_expected_damage(attacker, defender, move, type_chart),
    )


def simulate_one_vs_one(
    attacker: CombatProfile,
    defender: CombatProfile,
    type_chart: dict[str, dict[str, float]],
    attacker_side_wins_speed_ties: bool = True,
) -> DuelResult:
    turns = 0
    attacker_last_move = ""
    defender_last_move = ""
    degraded = bool(attacker.get("degraded_data") or defender.get("degraded_data"))

    while attacker["current_hp"] > 0 and defender["current_hp"] > 0:
        turns += 1

        attacker_move = choose_best_move(attacker, defender, type_chart)
        defender_move = choose_best_move(defender, attacker, type_chart)
        attacker_last_move = attacker_move["name"]
        defender_last_move = defender_move["name"]
        degraded = bool(degraded or attacker_move.get("degraded_data") or defender_move.get("degraded_data"))

        attacker_speed = int(attacker["stats"].get("speed", 50) or 50)
        defender_speed = int(defender["stats"].get("speed", 50) or 50)

        attacker_goes_first = (
            attacker_speed > defender_speed
            or (attacker_speed == defender_speed and attacker_side_wins_speed_ties)
        )

        if attacker_goes_first:
            damage = calculate_expected_damage(attacker, defender, attacker_move, type_chart)
            defender["current_hp"] = max(0, defender["current_hp"] - damage)
            if defender["current_hp"] == 0:
                break

            counter_damage = calculate_expected_damage(defender, attacker, defender_move, type_chart)
            attacker["current_hp"] = max(0, attacker["current_hp"] - counter_damage)
        else:
            damage = calculate_expected_damage(defender, attacker, defender_move, type_chart)
            attacker["current_hp"] = max(0, attacker["current_hp"] - damage)
            if attacker["current_hp"] == 0:
                break

            counter_damage = calculate_expected_damage(attacker, defender, attacker_move, type_chart)
            defender["current_hp"] = max(0, defender["current_hp"] - counter_damage)

    winner = "attacker" if attacker["current_hp"] > 0 else "defender"
    return {
        "winner": winner,
        "attacker_remaining_hp": attacker["current_hp"],
        "defender_remaining_hp": defender["current_hp"],
        "turns": turns,
        "attacker_move_used": attacker_last_move,
        "defender_move_used": defender_last_move,
        "degraded_data": degraded,
    }


def _first_alive_index(team: list[CombatProfile]) -> int | None:
    for i, pokemon in enumerate(team):
        if pokemon["current_hp"] > 0:
            return i
    return None


def _simulation_score(
    attacker_win: bool,
    attacker_remaining_pokemon: int,
    defender_remaining_pokemon: int,
    attacker_total_remaining_hp: int,
    defender_total_remaining_hp: int,
) -> float:
    sign = 1.0 if attacker_win else -1.0
    pokemon_delta = attacker_remaining_pokemon - defender_remaining_pokemon
    hp_delta = attacker_total_remaining_hp - defender_total_remaining_hp
    return round(sign * (100.0 + 20.0 * abs(pokemon_delta) + 0.1 * abs(hp_delta)), 3)


def simulate_team_battle(
    attacker_team: dict[str, Any],
    defender_team: dict[str, Any],
    type_chart: dict[str, dict[str, float]],
    attacker_game_version: str | None,
    defender_game_version: str | None,
) -> TeamBattleResult:
    """Simulate deterministic sequential team battle in input order.

    Simplifications: no status, no switching, no items/abilities/weather/terrain,
    no priority/accuracy/secondary effects, no PP depletion, only level-up damaging moves.
    """
    warnings = WarningCollector()

    attacker_members = _team_members(attacker_team)
    defender_members = _team_members(defender_team)

    attacker_profiles: list[CombatProfile] = [
        get_pokemon_combat_profile(
            species=str(member.get("species", "")),
            level=max(1, int(member.get("level", 20) or 20)),
            game_version=attacker_game_version,
            warnings=warnings,
        )
        for member in attacker_members
    ]
    defender_profiles: list[CombatProfile] = [
        get_pokemon_combat_profile(
            species=str(member.get("species", "")),
            level=max(1, int(member.get("level", 20) or 20)),
            game_version=defender_game_version,
            warnings=warnings,
        )
        for member in defender_members
    ]

    battle_turns = 0
    duel_summaries: list[dict[str, Any]] = []
    degraded_data = warnings.has_warnings() or any(p["degraded_data"] for p in attacker_profiles + defender_profiles)

    while True:
        atk_idx = _first_alive_index(attacker_profiles)
        def_idx = _first_alive_index(defender_profiles)

        if atk_idx is None or def_idx is None:
            break

        attacker_active = attacker_profiles[atk_idx]
        defender_active = defender_profiles[def_idx]

        duel = simulate_one_vs_one(attacker_active, defender_active, type_chart)
        battle_turns += duel["turns"]
        degraded_data = bool(degraded_data or duel["degraded_data"])

        duel_summaries.append(
            {
                "attacker_slot": atk_idx,
                "defender_slot": def_idx,
                "attacker_species": attacker_active["species"],
                "defender_species": defender_active["species"],
                "winner": duel["winner"],
                "turns": duel["turns"],
                "attacker_remaining_hp": duel["attacker_remaining_hp"],
                "defender_remaining_hp": duel["defender_remaining_hp"],
                "attacker_move_used": duel["attacker_move_used"],
                "defender_move_used": duel["defender_move_used"],
            }
        )

    attacker_remaining_pokemon = sum(1 for p in attacker_profiles if p["current_hp"] > 0)
    defender_remaining_pokemon = sum(1 for p in defender_profiles if p["current_hp"] > 0)
    attacker_total_remaining_hp = sum(p["current_hp"] for p in attacker_profiles)
    defender_total_remaining_hp = sum(p["current_hp"] for p in defender_profiles)

    attacker_win = attacker_remaining_pokemon > 0 and defender_remaining_pokemon == 0
    winner_team_id = attacker_team.get("team_id") if attacker_win else defender_team.get("team_id")

    return {
        "team_id_attacker": attacker_team.get("team_id"),
        "team_id_defender": defender_team.get("team_id"),
        "attacker_win": attacker_win,
        "winner_team_id": winner_team_id,
        "attacker_remaining_pokemon": attacker_remaining_pokemon,
        "defender_remaining_pokemon": defender_remaining_pokemon,
        "attacker_total_remaining_hp": attacker_total_remaining_hp,
        "defender_total_remaining_hp": defender_total_remaining_hp,
        "battle_turns": battle_turns,
        "simulation_score": _simulation_score(
            attacker_win,
            attacker_remaining_pokemon,
            defender_remaining_pokemon,
            attacker_total_remaining_hp,
            defender_total_remaining_hp,
        ),
        "degraded_data": bool(degraded_data or warnings.has_warnings()),
        "warnings": warnings.all(),
        "duel_summaries": duel_summaries,
    }


def load_type_chart(bronze_dir: Path = BRONZE_DIR) -> dict[str, dict[str, float]]:
    """Load type chart and fail loudly if missing to avoid silent neutral simulations."""
    type_chart_path = bronze_dir / "type_chart.json"
    if not type_chart_path.exists():
        raise FileNotFoundError(
            f"Type chart is required for battle simulation but was not found: {type_chart_path}"
        )

    type_chart = cast(dict[str, dict[str, float]], read_json(type_chart_path))
    if not isinstance(type_chart, dict) or not type_chart:
        raise ValueError(f"Type chart at {type_chart_path} is empty or invalid")
    return type_chart


def build_team_battle_simulations(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> None:
    """Build deterministic sequential team-vs-team battle simulations.

    Uses PySpark when available/enabled and falls back to local Python execution.
    """
    started_at = time.perf_counter()
    type_chart = load_type_chart(bronze_dir)
    simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME
    simulation_dir.mkdir(parents=True, exist_ok=True)

    _load_lookup_cache_from_disk(silver_dir)
    _warm_lookup_cache_for_teams(teams_data)
    _persist_lookup_cache_to_disk()

    if _should_use_spark():
        try:
            logger.info("[type_matchups] engine=spark requested")
            simulation_rows = _run_spark_simulations(
                teams_data=teams_data,
                type_chart=type_chart,
                pokemon_profiles=_LOCAL_POKEMON_PROFILES,
                move_profiles=_LOCAL_MOVE_PROFILES,
            )
        except Exception as exc:
            logger.exception("[type_matchups] spark simulation failed, falling back to local engine: %s", exc)
            simulation_rows = _run_local_simulations(teams_data=teams_data, type_chart=type_chart)
    else:
        logger.info("[type_matchups] engine=local (spark disabled via env)")
        simulation_rows = _run_local_simulations(teams_data=teams_data, type_chart=type_chart)

    write_parquet(simulation_dir / "team_battle_simulations.parquet", simulation_rows)

    elapsed_total = time.perf_counter() - started_at
    _persist_lookup_cache_to_disk()
    logger.info(
        "[type_matchups] completed sequential team battles rows=%s elapsed=%.2fs",
        len(simulation_rows),
        elapsed_total,
    )


def _should_use_spark() -> bool:
    value = os.getenv("PIPELINE_USE_PYSPARK", "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def _run_local_simulations(
    teams_data: list[dict[str, Any]],
    type_chart: dict[str, dict[str, float]],
) -> list[dict[str, Any]]:
    started_at = time.perf_counter()
    simulations: list[dict[str, Any]] = []

    teams_with_id = [team for team in teams_data if team.get("team_id") is not None]
    total_teams = len(teams_with_id)
    total_pairs = total_teams * max(total_teams - 1, 0)
    progress_interval = max(1, total_pairs // 100)  # about 1% progress cadence
    pairs_done = 0

    logger.info(
        "[type_matchups] starting sequential team battles teams=%s pairs=%s",
        total_teams,
        total_pairs,
    )

    for attacker_index, attacker in enumerate(teams_data, start=1):
        attacker_id = attacker.get("team_id")
        if attacker_id is None:
            continue

        logger.info(
            "[type_matchups] attacker progress (%s/%s) attacker_team_id=%s",
            attacker_index,
            len(teams_data),
            attacker_id,
        )

        for defender in teams_data:
            defender_id = defender.get("team_id")
            if defender_id is None or attacker_id == defender_id:
                continue

            simulation_result = simulate_team_battle(
                attacker_team=attacker,
                defender_team=defender,
                type_chart=type_chart,
                attacker_game_version=cast(str | None, attacker.get("game_version")),
                defender_game_version=cast(str | None, defender.get("game_version")),
            )
            simulations.append(cast(dict[str, Any], cast(object, simulation_result)))

            pairs_done += 1
            if pairs_done % progress_interval == 0 or pairs_done == total_pairs:
                elapsed = max(1e-9, time.perf_counter() - started_at)
                rate = pairs_done / elapsed
                percent = (pairs_done / total_pairs * 100.0) if total_pairs else 100.0
                logger.info(
                    "[type_matchups] pair progress %s/%s (%.1f%%) rate=%.2f pairs/s",
                    pairs_done,
                    total_pairs,
                    percent,
                    rate,
                )

    elapsed_total = time.perf_counter() - started_at
    logger.info("[type_matchups] local engine done rows=%s elapsed=%.2fs", len(simulations), elapsed_total)
    return simulations


def _run_spark_simulations(
    teams_data: list[dict[str, Any]],
    type_chart: dict[str, dict[str, float]],
    pokemon_profiles: dict[str, dict[str, Any]],
    move_profiles: dict[str, MoveProfile],
) -> list[dict[str, Any]]:
    pyspark_sql = importlib.import_module("pyspark.sql")
    pyspark_functions = importlib.import_module("pyspark.sql.functions")
    pyspark_types = importlib.import_module("pyspark.sql.types")

    Row = cast(Any, getattr(pyspark_sql, "Row"))
    SparkSession = cast(Any, getattr(pyspark_sql, "SparkSession"))
    F = cast(Any, pyspark_functions)
    T = cast(Any, pyspark_types)

    spark = (
        SparkSession.builder
        .appName("pokemon-team-battle-sim")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    teams_with_id = [team for team in teams_data if team.get("team_id") is not None]
    total_teams = len(teams_with_id)
    total_pairs = total_teams * max(total_teams - 1, 0)
    logger.info("[type_matchups] spark engine start teams=%s pairs=%s", total_teams, total_pairs)

    if total_pairs == 0:
        return []

    team_lookup: dict[str, dict[str, Any]] = {
        str(team.get("team_id")): team
        for team in teams_with_id
    }

    teams_rows = [
        {
            "team_id": str(team.get("team_id")),
            "game_version": str(team.get("game_version") or ""),
        }
        for team in teams_with_id
    ]

    teams_schema = T.StructType(
        [
            T.StructField("team_id", T.StringType(), False),
            T.StructField("game_version", T.StringType(), True),
        ]
    )
    teams_df = spark.createDataFrame(teams_rows, schema=teams_schema)

    pairs_df = (
        teams_df.alias("a")
        .crossJoin(teams_df.alias("d"))
        .where(F.col("a.team_id") != F.col("d.team_id"))
        .select(
            F.col("a.team_id").alias("attacker_id"),
            F.col("d.team_id").alias("defender_id"),
        )
    )

    logger.info("[type_matchups] spark pair dataframe created")

    broadcast_teams = spark.sparkContext.broadcast(team_lookup)
    broadcast_chart = spark.sparkContext.broadcast(type_chart)
    broadcast_pokemon = spark.sparkContext.broadcast(pokemon_profiles)
    broadcast_moves = spark.sparkContext.broadcast(move_profiles)

    def _simulate_partition(rows: Any) -> Any:
        teams_map = cast(dict[str, dict[str, Any]], broadcast_teams.value)
        chart = cast(dict[str, dict[str, float]], broadcast_chart.value)
        _install_lookup_cache(
            pokemon_profiles=cast(dict[str, dict[str, Any]], broadcast_pokemon.value),
            move_profiles=cast(dict[str, MoveProfile], broadcast_moves.value),
        )
        for row in rows:
            attacker_id = str(row.attacker_id)
            defender_id = str(row.defender_id)
            attacker_team = teams_map[attacker_id]
            defender_team = teams_map[defender_id]

            result = simulate_team_battle(
                attacker_team=attacker_team,
                defender_team=defender_team,
                type_chart=chart,
                attacker_game_version=cast(str | None, attacker_team.get("game_version")),
                defender_game_version=cast(str | None, defender_team.get("game_version")),
            )

            duel_rows = [
                {
                    "attacker_slot": int(duel.get("attacker_slot", 0) or 0),
                    "defender_slot": int(duel.get("defender_slot", 0) or 0),
                    "attacker_species": str(duel.get("attacker_species", "") or ""),
                    "defender_species": str(duel.get("defender_species", "") or ""),
                    "winner": str(duel.get("winner", "") or ""),
                    "turns": int(duel.get("turns", 0) or 0),
                    "attacker_remaining_hp": int(duel.get("attacker_remaining_hp", 0) or 0),
                    "defender_remaining_hp": int(duel.get("defender_remaining_hp", 0) or 0),
                    "attacker_move_used": str(duel.get("attacker_move_used", "") or ""),
                    "defender_move_used": str(duel.get("defender_move_used", "") or ""),
                }
                for duel in cast(list[dict[str, Any]], result.get("duel_summaries", []))
            ]

            yield Row(
                team_id_attacker=str(result.get("team_id_attacker", attacker_id)),
                team_id_defender=str(result.get("team_id_defender", defender_id)),
                attacker_win=bool(result.get("attacker_win", False)),
                winner_team_id=str(result.get("winner_team_id", "") or ""),
                attacker_remaining_pokemon=int(result.get("attacker_remaining_pokemon", 0) or 0),
                defender_remaining_pokemon=int(result.get("defender_remaining_pokemon", 0) or 0),
                attacker_total_remaining_hp=int(result.get("attacker_total_remaining_hp", 0) or 0),
                defender_total_remaining_hp=int(result.get("defender_total_remaining_hp", 0) or 0),
                battle_turns=int(result.get("battle_turns", 0) or 0),
                simulation_score=float(result.get("simulation_score", 0.0) or 0.0),
                degraded_data=bool(result.get("degraded_data", False)),
                warnings=[str(w) for w in cast(list[str], result.get("warnings", []))],
                duel_summaries=duel_rows,
            )

    duel_schema = T.StructType(
        [
            T.StructField("attacker_slot", T.IntegerType(), False),
            T.StructField("defender_slot", T.IntegerType(), False),
            T.StructField("attacker_species", T.StringType(), True),
            T.StructField("defender_species", T.StringType(), True),
            T.StructField("winner", T.StringType(), True),
            T.StructField("turns", T.IntegerType(), False),
            T.StructField("attacker_remaining_hp", T.IntegerType(), False),
            T.StructField("defender_remaining_hp", T.IntegerType(), False),
            T.StructField("attacker_move_used", T.StringType(), True),
            T.StructField("defender_move_used", T.StringType(), True),
        ]
    )

    result_schema = T.StructType(
        [
            T.StructField("team_id_attacker", T.StringType(), False),
            T.StructField("team_id_defender", T.StringType(), False),
            T.StructField("attacker_win", T.BooleanType(), False),
            T.StructField("winner_team_id", T.StringType(), True),
            T.StructField("attacker_remaining_pokemon", T.IntegerType(), False),
            T.StructField("defender_remaining_pokemon", T.IntegerType(), False),
            T.StructField("attacker_total_remaining_hp", T.IntegerType(), False),
            T.StructField("defender_total_remaining_hp", T.IntegerType(), False),
            T.StructField("battle_turns", T.IntegerType(), False),
            T.StructField("simulation_score", T.DoubleType(), False),
            T.StructField("degraded_data", T.BooleanType(), False),
            T.StructField("warnings", T.ArrayType(T.StringType(), containsNull=False), False),
            T.StructField("duel_summaries", T.ArrayType(duel_schema, containsNull=False), False),
        ]
    )

    partitions = max(4, min(256, total_pairs // 2000 + 1))
    pair_rdd = pairs_df.repartition(partitions).rdd.mapPartitions(_simulate_partition)
    result_df = spark.createDataFrame(pair_rdd, schema=result_schema)

    logger.info("[type_matchups] spark simulation dataframe materialized partitions=%s", partitions)

    result_df = result_df.orderBy("team_id_attacker", "team_id_defender")
    result_rows = [row.asDict(recursive=True) for row in result_df.toLocalIterator()]
    logger.info("[type_matchups] spark engine done rows=%s", len(result_rows))
    return result_rows


def build_type_matchups(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> None:
    """Backward-compatible wrapper; now delegates to sequential battle simulations."""
    build_team_battle_simulations(teams_data=teams_data, silver_dir=silver_dir, bronze_dir=bronze_dir)

