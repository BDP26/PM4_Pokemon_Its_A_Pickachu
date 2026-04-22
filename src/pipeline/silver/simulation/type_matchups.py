from __future__ import annotations

import importlib
import logging
import math
import os
import random
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, TypedDict, cast

import pokebase as pb

from src.pipeline.common.io import read_json, write_json, write_parquet
from src.pipeline.settings import (
    BRONZE_DIR,
    SILVER_DIR,
    SILVER_SIMULATION_DIRNAME,
    SIMULATION_CONFIG,
)

logger = logging.getLogger(__name__)

_LOOKUP_CACHE_DIRNAME = "lookup_cache"
_POKEMON_CACHE_FILENAME = "pokemon_profiles.json"
_MOVE_CACHE_FILENAME = "move_profiles.json"


class MoveProfile(TypedDict):
    name: str
    type: str
    power: int
    damage_class: str
    accuracy: int | None
    pp: int | None
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
    predicted_player_win_chance: float
    n_trials: int


@dataclass
class BattleSimulationConfig:
    max_overlevel: int = int(SIMULATION_CONFIG["max_overlevel"])
    max_underlevel: int = int(SIMULATION_CONFIG["max_underlevel"])
    n_battle_trials: int = int(SIMULATION_CONFIG["default_trials"])
    damage_randomness_min: float = float(SIMULATION_CONFIG["damage_randomness_min"])
    damage_randomness_max: float = float(SIMULATION_CONFIG["damage_randomness_max"])
    crit_chance: float = float(SIMULATION_CONFIG["crit_chance"])
    max_turns_per_duel: int = int(SIMULATION_CONFIG["max_turns_per_duel"])
    rng_seed: int = 42


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


_LOCAL_POKEMON_PROFILES: dict[str, dict[str, Any]] = {}
_LOCAL_MOVE_PROFILES: dict[str, MoveProfile] = {}
_LOCAL_CACHE_PATHS: tuple[Path, Path] | None = None

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

_STRUGGLE_MOVE: MoveProfile = {
    "name": "struggle",
    "type": "Normal",
    "power": 50,
    "damage_class": "physical",
    "accuracy": 100,
    "pp": 1,
    "level_learned_at": 0,
    "version_group": "fallback",
    "degraded_data": True,
}


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
                    "accuracy": value.get("accuracy"),
                    "pp": value.get("pp"),
                    "level_learned_at": int(value.get("level_learned_at", 0) or 0),
                    "version_group": str(value.get("version_group", "")),
                    "degraded_data": bool(value.get("degraded_data", False)),
                }

    _install_lookup_cache(pokemon_profiles, move_profiles)
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
    normalized = " ".join(name.strip().lower().replace(".", " ").replace("_", " ").split())
    normalized = normalized.replace("'", "").replace(" ", "-")
    return normalized


def _normalize_move_name(name: str) -> str:
    normalized = " ".join(name.strip().lower().replace(".", " ").replace("_", " ").split())
    normalized = normalized.replace("'", "").replace(" ", "-")
    return normalized


def _fetch_pokemon_profile_from_api(species_id: str) -> tuple[dict[str, Any], bool, str | None]:
    try:
        poke = pb.pokemon(species_id)
    except Exception as exc:
        return (
            {
                "name": species_id,
                "types": ["Normal"],
                "stats": _default_stats(),
                "moves": [],
            },
            True,
            f"Pokemon lookup failed for '{species_id}': {exc}",
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
    except Exception as exc:
        return (
            {
                "name": move_name,
                "type": "Normal",
                "power": 40,
                "damage_class": "physical",
                "accuracy": 100,
                "pp": 1,
                "level_learned_at": 0,
                "version_group": "fallback",
                "degraded_data": True,
            },
            True,
            f"Move lookup failed for '{move_name}': {exc}",
        )

    return (
        {
            "name": getattr(move, "name", move_name),
            "type": str(getattr(getattr(move, "type", None), "name", "Normal") or "Normal").title(),
            "power": int(getattr(move, "power", 0) or 0),
            "damage_class": str(getattr(getattr(move, "damage_class", None), "name", "physical") or "physical"),
            "accuracy": getattr(move, "accuracy", None),
            "pp": getattr(move, "pp", None),
            "level_learned_at": 0,
            "version_group": "",
            "degraded_data": False,
        },
        False,
        None,
    )


def _get_pokemon_profile(species_id: str, warnings: WarningCollector) -> tuple[dict[str, Any], bool]:
    cached = _LOCAL_POKEMON_PROFILES.get(species_id)
    if cached is not None:
        return cached, False
    profile, degraded, warning = _fetch_pokemon_profile_from_api(species_id)
    if warning:
        warnings.warn(warning)
    if not degraded:
        _LOCAL_POKEMON_PROFILES[species_id] = profile
    return profile, degraded


def _get_move_profile(move_name: str, warnings: WarningCollector) -> tuple[MoveProfile, bool]:
    cached = _LOCAL_MOVE_PROFILES.get(move_name)
    if cached is not None:
        return cached, False
    profile, degraded, warning = _fetch_move_profile_from_api(move_name)
    if warning:
        warnings.warn(warning)
    if not degraded:
        _LOCAL_MOVE_PROFILES[move_name] = profile
    return profile, degraded


def _team_members(team: dict[str, Any]) -> list[dict[str, Any]]:
    pokemon_entries = team.get("pokemon", [])
    level_entries = team.get("levels", [])
    move_entries = team.get("moves", [])
    instance_entries = team.get("pokemon_instance_ids", [])

    members: list[dict[str, Any]] = []
    if not isinstance(pokemon_entries, list):
        return members

    for slot_idx, entry in enumerate(pokemon_entries[:6]):
        species = entry.get("name") if isinstance(entry, dict) else entry
        if not isinstance(species, str) or not species:
            continue
        raw_level = level_entries[slot_idx] if isinstance(level_entries, list) and slot_idx < len(level_entries) else team.get("avg_level", 20)
        raw_moves = move_entries[slot_idx] if isinstance(move_entries, list) and slot_idx < len(move_entries) else []
        moves = [
            _normalize_move_name(str(move))
            for move in raw_moves if isinstance(raw_moves, list)
            for _ in [0]
            if str(move).strip()
        ]
        members.append(
            {
                "species": species,
                "level": int(raw_level or team.get("avg_level", 20) or 20),
                "moves": moves,
                "pokemon_instance_id": instance_entries[slot_idx] if isinstance(instance_entries, list) and slot_idx < len(instance_entries) else None,
            }
        )
    return members


def _version_group_for_game(game_version: str | None) -> str | None:
    if not isinstance(game_version, str):
        return None
    normalized = game_version.strip().lower()
    return _GAME_TO_VERSION_GROUP.get(normalized, normalized)


def _calculate_max_hp(base_hp: int, level: int) -> int:
    return max(1, int(((2 * base_hp) * level) / 100) + level + 10)


def _type_multiplier(move_type: str, defender_types: list[str], type_chart: dict[str, dict[str, float]]) -> float:
    multiplier = 1.0
    attacking_type = move_type.title()
    for defending_type in defender_types or ["Normal"]:
        multiplier *= float(type_chart.get(attacking_type, {}).get(defending_type.title(), 1.0))
    return multiplier


def _legal_moves_for_pokemon(
    species_id: str,
    level: int,
    game_version: str | None,
    warnings: WarningCollector,
    preferred_moves: list[str] | None = None,
) -> tuple[list[MoveProfile], bool]:
    profile, pokemon_degraded = _get_pokemon_profile(species_id, warnings)
    version_group = _version_group_for_game(game_version)
    legal_moves: list[MoveProfile] = []
    seen_moves: set[str] = set()
    degraded = pokemon_degraded

    if preferred_moves:
        for move_name in preferred_moves:
            normalized_move = _normalize_move_name(move_name)
            if not normalized_move or normalized_move in seen_moves:
                continue
            move_profile, move_degraded = _get_move_profile(normalized_move, warnings)
            if move_degraded:
                degraded = True
            if int(move_profile.get("power", 0) or 0) <= 0:
                continue
            legal_moves.append(move_profile)
            seen_moves.add(normalized_move)
        if legal_moves:
            return legal_moves, degraded

    for move_slot in profile.get("moves", []):
        move_name = _normalize_move_name(str(move_slot.get("move_name") or ""))
        if not move_name or move_name in seen_moves:
            continue

        learned_ok = False
        learned_level = 0
        detail_group = ""

        for detail in move_slot.get("version_group_details", []):
            detail_group = str(detail.get("version_group") or "")
            learn_method = str(detail.get("learn_method") or "")
            learned_level = int(detail.get("level_learned_at", 0) or 0)

            if version_group and detail_group != version_group:
                continue
            if learn_method != "level-up":
                continue
            if learned_level > level:
                continue

            learned_ok = True
            break

        if not learned_ok:
            continue

        move_profile, move_degraded = _get_move_profile(move_name, warnings)
        if move_degraded:
            degraded = True
        if int(move_profile.get("power", 0) or 0) <= 0:
            continue

        legal_moves.append(
            {
                **move_profile,
                "level_learned_at": learned_level,
                "version_group": detail_group,
                "degraded_data": bool(move_profile.get("degraded_data", False) or move_degraded),
            }
        )
        seen_moves.add(move_name)

    if not legal_moves:
        degraded = True
        warnings.warn(f"No damaging legal moves for '{species_id}' at level {level}; using struggle")
        legal_moves.append(dict(_STRUGGLE_MOVE))

    return legal_moves, degraded


def get_pokemon_combat_profile(
    species: str,
    level: int,
    game_version: str | None,
    warnings: WarningCollector,
    moves: list[str] | None = None,
) -> CombatProfile:
    species_id = normalize_species_name(species)
    profile, pokemon_degraded = _get_pokemon_profile(species_id, warnings)
    legal_moves, moves_degraded = _legal_moves_for_pokemon(
        species_id,
        level,
        game_version,
        warnings,
        preferred_moves=moves,
    )

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


def _choose_best_move(attacker: CombatProfile, defender: CombatProfile, type_chart: dict[str, dict[str, float]]) -> MoveProfile:
    best_move = attacker["legal_moves"][0]
    best_score = -1.0

    for move in attacker["legal_moves"]:
        power = int(move.get("power", 0) or 0)
        multiplier = _type_multiplier(move["type"], defender["types"], type_chart)
        stab = 1.5 if move["type"].title() in attacker["types"] else 1.0
        attack_stat = attacker["stats"].get("attack", 50) if move["damage_class"] == "physical" else attacker["stats"].get("sp_attack", 50)
        defense_stat = defender["stats"].get("defense", 50) if move["damage_class"] == "physical" else defender["stats"].get("sp_defense", 50)
        score = power * multiplier * stab * max(1.0, attack_stat / max(1, defense_stat))
        if score > best_score:
            best_score = score
            best_move = move

    return best_move


def _calculate_damage(
    attacker: CombatProfile,
    defender: CombatProfile,
    move: MoveProfile,
    type_chart: dict[str, dict[str, float]],
    rng: random.Random,
    config: BattleSimulationConfig,
) -> int:
    power = int(move.get("power", 0) or 0)
    if power <= 0:
        return 0

    attack_stat = attacker["stats"].get("attack", 50) if move["damage_class"] == "physical" else attacker["stats"].get("sp_attack", 50)
    defense_stat = defender["stats"].get("defense", 50) if move["damage_class"] == "physical" else defender["stats"].get("sp_defense", 50)
    base = (((2 * attacker["level"] / 5) + 2) * power * max(1, attack_stat) / max(1, defense_stat)) / 50 + 2

    stab = 1.5 if move["type"].title() in attacker["types"] else 1.0
    type_effectiveness = _type_multiplier(move["type"], defender["types"], type_chart)
    crit = 1.5 if rng.random() < config.crit_chance else 1.0
    randomness = rng.uniform(config.damage_randomness_min, config.damage_randomness_max)

    damage = int(max(1, base * stab * type_effectiveness * crit * randomness))
    return damage


def _attempt_move_hit(move: MoveProfile, rng: random.Random) -> bool:
    accuracy = move.get("accuracy")
    if accuracy is None:
        return True
    try:
        accuracy_value = int(accuracy)
    except (TypeError, ValueError):
        return True
    return rng.random() <= (max(1, min(100, accuracy_value)) / 100.0)


def simulate_one_vs_one(
    attacker: CombatProfile,
    defender: CombatProfile,
    type_chart: dict[str, dict[str, float]],
    rng: random.Random,
    config: BattleSimulationConfig,
) -> DuelResult:
    attacker_last_move = ""
    defender_last_move = ""
    degraded = bool(attacker["degraded_data"] or defender["degraded_data"])

    for turn in range(1, config.max_turns_per_duel + 1):
        attacker_move = _choose_best_move(attacker, defender, type_chart)
        defender_move = _choose_best_move(defender, attacker, type_chart)
        attacker_last_move = attacker_move["name"]
        defender_last_move = defender_move["name"]

        attacker_speed = int(attacker["stats"].get("speed", 50) or 50)
        defender_speed = int(defender["stats"].get("speed", 50) or 50)
        attacker_goes_first = attacker_speed >= defender_speed

        def _take_hit(active: CombatProfile, target: CombatProfile, move: MoveProfile) -> None:
            if not _attempt_move_hit(move, rng):
                return
            damage = _calculate_damage(active, target, move, type_chart, rng, config)
            target["current_hp"] = max(0, target["current_hp"] - damage)

        if attacker_goes_first:
            _take_hit(attacker, defender, attacker_move)
            if defender["current_hp"] == 0:
                return {
                    "winner": "attacker",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                    "degraded_data": degraded,
                }
            _take_hit(defender, attacker, defender_move)
            if attacker["current_hp"] == 0:
                return {
                    "winner": "defender",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                    "degraded_data": degraded,
                }
        else:
            _take_hit(defender, attacker, defender_move)
            if attacker["current_hp"] == 0:
                return {
                    "winner": "defender",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                    "degraded_data": degraded,
                }
            _take_hit(attacker, defender, attacker_move)
            if defender["current_hp"] == 0:
                return {
                    "winner": "attacker",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                    "degraded_data": degraded,
                }

    winner = "attacker" if attacker["current_hp"] >= defender["current_hp"] else "defender"
    return {
        "winner": winner,
        "attacker_remaining_hp": attacker["current_hp"],
        "defender_remaining_hp": defender["current_hp"],
        "turns": config.max_turns_per_duel,
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


def _is_player_candidate_team(team: dict[str, Any]) -> bool:
    if bool(team.get("is_player_candidate")):
        return True
    team_id = str(team.get("team_id") or "")
    return team_id.startswith("STARTER_")


def _is_boss_team(team: dict[str, Any]) -> bool:
    boss_name = team.get("boss_name")
    return isinstance(boss_name, str) and bool(boss_name.strip())


def _boss_level_cap(team: dict[str, Any]) -> int | None:
    levels: list[int] = []
    for member in _team_members(team):
        try:
            levels.append(max(1, int(member.get("level", 1) or 1)))
        except Exception:
            continue
    if levels:
        return max(levels)
    avg = team.get("avg_level")
    if isinstance(avg, (int, float)):
        return max(1, int(avg))
    return None


def _apply_level_plausibility_filter(attacker_team: dict[str, Any], defender_team: dict[str, Any], config: BattleSimulationConfig) -> bool:
    player_avg = attacker_team.get("avg_level")
    boss_avg = defender_team.get("avg_level")
    if not isinstance(player_avg, (int, float)) or not isinstance(boss_avg, (int, float)):
        return True
    return (
        player_avg <= boss_avg + config.max_overlevel
        and player_avg >= boss_avg - config.max_underlevel
    )


def load_type_chart(bronze_dir: Path = BRONZE_DIR) -> dict[str, dict[str, float]]:
    type_chart_path = bronze_dir / "type_chart.json"
    if not type_chart_path.exists():
        raise FileNotFoundError(f"Type chart missing: {type_chart_path}")
    type_chart = cast(dict[str, dict[str, float]], read_json(type_chart_path))
    if not isinstance(type_chart, dict) or not type_chart:
        raise ValueError(f"Type chart at {type_chart_path} is empty or invalid")
    return type_chart


def simulate_team_battle_once(
    attacker_team: dict[str, Any],
    defender_team: dict[str, Any],
    type_chart: dict[str, dict[str, float]],
    attacker_game_version: str | None,
    defender_game_version: str | None,
    rng: random.Random,
    config: BattleSimulationConfig,
) -> TeamBattleResult:
    warnings = WarningCollector()

    attacker_profiles = [
        get_pokemon_combat_profile(
            species=member["species"],
            level=int(member["level"]),
            game_version=attacker_game_version,
            warnings=warnings,
            moves=cast(list[str], member.get("moves", [])),
        )
        for member in _team_members(attacker_team)
    ]
    defender_profiles = [
        get_pokemon_combat_profile(
            species=member["species"],
            level=int(member["level"]),
            game_version=defender_game_version,
            warnings=warnings,
            moves=cast(list[str], member.get("moves", [])),
        )
        for member in _team_members(defender_team)
    ]

    duel_summaries: list[dict[str, Any]] = []
    battle_turns = 0

    while True:
        attacker_idx = _first_alive_index(attacker_profiles)
        defender_idx = _first_alive_index(defender_profiles)

        if attacker_idx is None or defender_idx is None:
            break

        duel = simulate_one_vs_one(
            attacker_profiles[attacker_idx],
            defender_profiles[defender_idx],
            type_chart,
            rng,
            config,
        )
        battle_turns += int(duel["turns"])

        duel_summaries.append(
            {
                "attacker_slot": attacker_idx + 1,
                "defender_slot": defender_idx + 1,
                "attacker_species": attacker_profiles[attacker_idx]["species"],
                "defender_species": defender_profiles[defender_idx]["species"],
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
        "degraded_data": warnings.has_warnings(),
        "warnings": warnings.all(),
        "duel_summaries": duel_summaries,
        "predicted_player_win_chance": 1.0 if attacker_win else 0.0,
        "n_trials": 1,
    }


def simulate_team_battle(
    attacker_team: dict[str, Any],
    defender_team: dict[str, Any],
    type_chart: dict[str, dict[str, float]],
    attacker_game_version: str | None,
    defender_game_version: str | None,
    n_trials: int,
    rng_seed: int,
    config: BattleSimulationConfig,
) -> TeamBattleResult:
    if not _apply_level_plausibility_filter(attacker_team, defender_team, config):
        return {
            "team_id_attacker": attacker_team.get("team_id"),
            "team_id_defender": defender_team.get("team_id"),
            "attacker_win": False,
            "winner_team_id": defender_team.get("team_id"),
            "attacker_remaining_pokemon": 0,
            "defender_remaining_pokemon": 0,
            "attacker_total_remaining_hp": 0,
            "defender_total_remaining_hp": 0,
            "battle_turns": 0,
            "simulation_score": -999.0,
            "degraded_data": False,
            "warnings": ["level_plausibility_filter_failed"],
            "duel_summaries": [],
            "predicted_player_win_chance": 0.0,
            "n_trials": n_trials,
        }

    trial_results: list[TeamBattleResult] = []
    wins = 0

    for trial_idx in range(n_trials):
        rng = random.Random(rng_seed + trial_idx)
        result = simulate_team_battle_once(
            attacker_team=attacker_team,
            defender_team=defender_team,
            type_chart=type_chart,
            attacker_game_version=attacker_game_version,
            defender_game_version=defender_game_version,
            rng=rng,
            config=config,
        )
        trial_results.append(result)
        if result["attacker_win"]:
            wins += 1

    best_result = max(trial_results, key=lambda r: r["simulation_score"])
    best_result["predicted_player_win_chance"] = round(wins / max(1, n_trials), 4)
    best_result["n_trials"] = n_trials
    return best_result


def _run_local_simulations(
    teams_data: list[dict[str, Any]],
    type_chart: dict[str, dict[str, float]],
    config: BattleSimulationConfig,
) -> list[dict[str, Any]]:
    teams_with_id = [team for team in teams_data if team.get("team_id") is not None]
    attackers = [team for team in teams_with_id if _is_player_candidate_team(team)]
    defenders = [team for team in teams_with_id if _is_boss_team(team)]

    total_pairs = len(attackers) * len(defenders)
    logger.info(
        "[type_matchups] local engine start teams=%s attackers=%s defenders=%s pairs=%s",
        len(teams_with_id),
        len(attackers),
        len(defenders),
        total_pairs,
    )
    if total_pairs == 0:
        return []

    simulations: list[dict[str, Any]] = []
    started_at = time.perf_counter()
    pairs_done = 0
    progress_interval = max(1, total_pairs // 20)

    for attacker_team in attackers:
        for defender_team in defenders:
            result = simulate_team_battle(
                attacker_team=attacker_team,
                defender_team=defender_team,
                type_chart=type_chart,
                attacker_game_version=cast(str | None, attacker_team.get("game_version")),
                defender_game_version=cast(str | None, defender_team.get("game_version")),
                n_trials=config.n_battle_trials,
                rng_seed=(hash((attacker_team.get("team_id"), defender_team.get("team_id"), config.rng_seed)) & 0xFFFFFFFF),
                config=config,
            )
            simulations.append(result)
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

    logger.info("[type_matchups] local engine done rows=%s elapsed=%.2fs", len(simulations), time.perf_counter() - started_at)
    return simulations


def _should_use_spark() -> bool:
    return os.environ.get("PIPELINE_USE_PYSPARK", "0").strip() in {"1", "true", "True"}


def _run_spark_simulations(
    teams_data: list[dict[str, Any]],
    type_chart: dict[str, dict[str, float]],
    config: BattleSimulationConfig,
) -> list[dict[str, Any]]:
    try:
        pyspark_sql = importlib.import_module("pyspark.sql")
        pyspark_functions = importlib.import_module("pyspark.sql.functions")
        pyspark_types = importlib.import_module("pyspark.sql.types")
    except ImportError:
        logger.warning("[type_matchups] pyspark not installed; falling back to local engine")
        return _run_local_simulations(teams_data, type_chart, config)

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
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        teams_with_id = [team for team in teams_data if team.get("team_id") is not None]
        attacker_count = sum(1 for team in teams_with_id if _is_player_candidate_team(team))
        defender_count = sum(1 for team in teams_with_id if _is_boss_team(team))
        total_pairs = attacker_count * defender_count
        logger.info(
            "[type_matchups] spark engine start teams=%s attackers=%s defenders=%s pairs=%s",
            len(teams_with_id),
            attacker_count,
            defender_count,
            total_pairs,
        )
        if total_pairs == 0:
            return []

        team_lookup: dict[str, dict[str, Any]] = {str(team.get("team_id")): team for team in teams_with_id}

        team_rows = [
            {
                "team_id": str(team.get("team_id")),
                "is_player_candidate": bool(_is_player_candidate_team(team)),
                "is_boss": bool(_is_boss_team(team)),
            }
            for team in teams_with_id
        ]
        schema = T.StructType(
            [
                T.StructField("team_id", T.StringType(), False),
                T.StructField("is_player_candidate", T.BooleanType(), False),
                T.StructField("is_boss", T.BooleanType(), False),
            ]
        )

        teams_df = spark.createDataFrame(team_rows, schema=schema)
        attackers_df = teams_df.where(F.col("is_player_candidate") == F.lit(True)).select("team_id")
        defenders_df = teams_df.where(F.col("is_boss") == F.lit(True)).select("team_id")
        pairs_df = attackers_df.alias("a").crossJoin(defenders_df.alias("d")).select(
            F.col("a.team_id").alias("attacker_id"),
            F.col("d.team_id").alias("defender_id"),
        )

        team_lookup_bc = spark.sparkContext.broadcast(team_lookup)
        chart_bc = spark.sparkContext.broadcast(type_chart)
        config_bc = spark.sparkContext.broadcast(asdict(config))

        def _simulate_partition(rows: Any) -> Any:
            local_teams = cast(dict[str, dict[str, Any]], team_lookup_bc.value)
            local_chart = cast(dict[str, dict[str, float]], chart_bc.value)
            local_config = BattleSimulationConfig(**cast(dict[str, Any], config_bc.value))
            for row in rows:
                attacker_team = local_teams[str(row.attacker_id)]
                defender_team = local_teams[str(row.defender_id)]
                result = simulate_team_battle(
                    attacker_team=attacker_team,
                    defender_team=defender_team,
                    type_chart=local_chart,
                    attacker_game_version=cast(str | None, attacker_team.get("game_version")),
                    defender_game_version=cast(str | None, defender_team.get("game_version")),
                    n_trials=local_config.n_battle_trials,
                    rng_seed=(hash((attacker_team.get("team_id"), defender_team.get("team_id"), local_config.rng_seed)) & 0xFFFFFFFF),
                    config=local_config,
                )
                yield Row(**result)

        partitions = max(4, min(256, total_pairs // 2000 + 1))
        result_rdd = pairs_df.repartition(partitions).rdd.mapPartitions(_simulate_partition)
        result_df = spark.createDataFrame(result_rdd)
        result_df = result_df.orderBy("team_id_attacker", "team_id_defender")
        return [cast(dict[str, Any], row.asDict(recursive=True)) for row in result_df.toLocalIterator()]
    finally:
        spark.stop()


def _load_move_and_pokemon_profiles_from_disk(silver_dir: Path) -> None:
    _load_lookup_cache_from_disk(silver_dir)


def build_team_battle_simulations(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
    force_spark: bool | None = None,
) -> None:
    started_at = time.perf_counter()
    type_chart = load_type_chart(bronze_dir)
    simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME
    simulation_dir.mkdir(parents=True, exist_ok=True)

    _load_move_and_pokemon_profiles_from_disk(silver_dir)
    _persist_lookup_cache_to_disk()

    config = BattleSimulationConfig()
    use_spark = _should_use_spark() if force_spark is None else bool(force_spark)
    logger.info("[type_matchups] engine=%s", "spark" if use_spark else "local")

    if use_spark:
        simulations = _run_spark_simulations(teams_data, type_chart, config)
    else:
        simulations = _run_local_simulations(teams_data, type_chart, config)

    write_parquet(simulation_dir / "team_battle_simulations.parquet", simulations)
    logger.info(
        "[type_matchups] wrote team_battle_simulations rows=%s elapsed_s=%.2f",
        len(simulations),
        time.perf_counter() - started_at,
    )


def build_type_matchups(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> None:
    build_team_battle_simulations(teams_data=teams_data, silver_dir=silver_dir, bronze_dir=bronze_dir)
