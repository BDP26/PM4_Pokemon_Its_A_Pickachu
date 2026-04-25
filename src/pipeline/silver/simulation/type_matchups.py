from __future__ import annotations

import importlib
import hashlib
import logging
import math
import os
import random
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, TypedDict, cast

from src.pipeline.common.io import read_json, read_parquet, write_parquet
from src.pipeline.silver.config.team_config import DEFAULT_TEAM_MEMBER_LIMIT
from src.pipeline.silver.move_power import resolve_effective_power
from src.pipeline.settings import (
    BRONZE_DIR,
    SILVER_DIR,
    SILVER_SIMULATION_DIRNAME,
    SIMULATION_CONFIG,
)

logger = logging.getLogger(__name__)

class MoveProfile(TypedDict):
    name: str
    type: str
    power: int | float | None
    raw_power: int | float | None
    effective_power: float
    power_handling: str
    is_status_move: bool
    is_damage_move: bool
    is_null_power: bool
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
    attacker_remaining_pokemon: float
    defender_remaining_pokemon: float
    attacker_total_remaining_hp: float
    defender_total_remaining_hp: float
    battle_turns: float
    simulation_score: float
    degraded_data: bool
    warnings: list[str]
    duel_summaries: list[dict[str, Any]]
    predicted_player_win_chance: float
    attacker_wins: int
    attacker_losses: int
    n_trials: int
    attacker_game_version: str | None
    defender_game_version: str | None
    is_compatible_version: bool
    representative_simulation_score: float
    representative_duel_summaries: list[dict[str, Any]]
    representative_warnings: list[str]


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
    require_exact_version_match: bool = True
    fail_on_degraded_data: bool = False


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

_POKEMON_REQUIRED_STATS = [
    "base_hp",
    "base_attack",
    "base_defense",
    "base_special_attack",
    "base_special_defense",
    "base_speed",
]


def _allow_simulation_fallbacks() -> bool:
    return os.environ.get("PM4_ALLOW_SIMULATION_FALLBACKS", "0").strip() == "1"


def _safe_int(value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, float) and math.isnan(value):
        return default
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


def _install_reference_profiles(
    pokemon_profiles: dict[str, dict[str, Any]],
    move_profiles: dict[str, MoveProfile],
) -> None:
    global _LOCAL_POKEMON_PROFILES, _LOCAL_MOVE_PROFILES
    _LOCAL_POKEMON_PROFILES = dict(pokemon_profiles)
    _LOCAL_MOVE_PROFILES = dict(move_profiles)


def _is_nullish(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _normalize_profile_key(value: Any) -> str:
    if _is_nullish(value):
        return ""
    normalized = str(value).strip().lower().replace(" ", "-").replace("_", "-")
    return normalized


def _require_non_null(row: dict[str, Any], column: str, context: str) -> Any:
    value = row.get(column)
    if _is_nullish(value):
        raise ValueError(f"{context}: required column '{column}' is null")
    return value


def load_pokemon_profiles_from_silver(silver_dir: Path) -> dict[str, dict[str, Any]]:
    pokemon_data_path = silver_dir / "references" / "pokemon_data.parquet"
    if not pokemon_data_path.exists():
        raise FileNotFoundError(f"Pokemon reference parquet missing: {pokemon_data_path}")
    pokemon_data_df = read_parquet(pokemon_data_path)
    profiles: dict[str, dict[str, Any]] = {}
    strong_name_keys: set[str] = set()

    alias_columns = [
        "name",
        "pokemon_species",
        "requested_pokemon_name",
        "normalized_requested_name",
        "normalized_species",
        "resolved_pokemon_name",
    ]
    for row in pokemon_data_df.to_dict(orient="records"):
        name = _normalize_profile_key(_require_non_null(row, "name", "pokemon_data.parquet"))
        _require_non_null(row, "type_1", f"pokemon_data.parquet[{name}]")
        for stat_column in _POKEMON_REQUIRED_STATS:
            _require_non_null(row, stat_column, f"pokemon_data.parquet[{name}]")

        profile = {
            "name": name,
            "species": _normalize_profile_key(row.get("pokemon_species") or row.get("name")),
            "pokeapi_id": row.get("pokeapi_id"),
            "source_url": row.get("source_url"),
            "types": [
                str(value).title()
                for value in [row.get("type_1"), row.get("type_2")]
                if isinstance(value, str) and value.strip()
            ],
            "type_1": str(row.get("type_1") or "").title(),
            "type_2": str(row.get("type_2")).title() if isinstance(row.get("type_2"), str) and str(row.get("type_2")).strip() else None,
            "hp": int(row.get("base_hp")),
            "attack": int(row.get("base_attack")),
            "defense": int(row.get("base_defense")),
            "special_attack": int(row.get("base_special_attack")),
            "special_defense": int(row.get("base_special_defense")),
            "speed": int(row.get("base_speed")),
            "height": row.get("height"),
            "weight": row.get("weight"),
            "base_experience": row.get("base_experience"),
            "is_default": row.get("is_default"),
            "resolved_pokemon_name": row.get("resolved_pokemon_name"),
            "resolved_pokeapi_id": row.get("resolved_pokeapi_id"),
            "resolution_method": row.get("resolution_method"),
            "resolution_warning": row.get("resolution_warning"),
            "moves": [],
            "stats": {
                "hp": int(row.get("base_hp")),
                "attack": int(row.get("base_attack")),
                "defense": int(row.get("base_defense")),
                "sp_attack": int(row.get("base_special_attack")),
                "sp_defense": int(row.get("base_special_defense")),
                "speed": int(row.get("base_speed")),
            },
        }
        for column in alias_columns:
            key = _normalize_profile_key(row.get(column))
            if not key:
                continue
            if column == "name":
                profiles[key] = profile
                strong_name_keys.add(key)
                continue
            if key in strong_name_keys or key in profiles:
                continue
            profiles[key] = profile

    return profiles


def load_move_profiles_from_silver(silver_dir: Path) -> dict[str, MoveProfile]:
    move_reference_path = silver_dir / "references" / "move_reference.parquet"
    if not move_reference_path.exists():
        raise FileNotFoundError(f"Move reference parquet missing: {move_reference_path}")
    move_ref_df = read_parquet(move_reference_path)
    profiles: dict[str, MoveProfile] = {}
    for row in move_ref_df.to_dict(orient="records"):
        move_name = _normalize_profile_key(_require_non_null(row, "move_name", "move_reference.parquet"))
        move_type = row.get("type")
        if _is_nullish(move_type):
            raise ValueError(f"move_reference.parquet[{move_name}]: required column 'type' is null")
        is_status_move = bool(row.get("is_status_move"))
        damage_class_value = row.get("damage_class")
        if _is_nullish(damage_class_value):
            if is_status_move:
                damage_class = "status"
            else:
                raise ValueError(f"move_reference.parquet[{move_name}]: damage_class is null for non-status move")
        else:
            damage_class = str(damage_class_value).strip().lower()
        effective_power = row.get("effective_power")
        if _is_nullish(effective_power):
            effective_power = 0.0
        raw_power = row.get("raw_power")
        if _is_nullish(raw_power):
            raw_power = row.get("power")
        if _is_nullish(raw_power):
            raw_power = 0.0
        accuracy = row.get("accuracy")
        if isinstance(accuracy, float) and math.isnan(accuracy):
            accuracy = None

        profiles[move_name] = {
            "name": move_name,
            "type": str(move_type).strip().title(),
            "power": float(effective_power),
            "raw_power": float(raw_power),
            "effective_power": float(effective_power),
            "power_handling": str(row.get("power_handling") or ""),
            "is_status_move": is_status_move,
            "is_damage_move": bool(row.get("is_damage_move")),
            "is_null_power": bool(row.get("is_null_power")),
            "damage_class": damage_class,
            "accuracy": None if accuracy is None else _safe_int(accuracy, 100),
            "pp": _safe_int(row.get("pp"), 0),
            "level_learned_at": 0,
            "version_group": "reference",
            "degraded_data": False,
        }
    return profiles


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
    return _normalize_profile_key(name)


_MISSING_MOVE_MARKERS = {"", "nan", "none", "null", "<na>", "na"}


def _is_missing_move_value(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    try:
        if value != value:
            return True
    except Exception:
        pass
    if isinstance(value, str):
        return value.strip().lower() in _MISSING_MOVE_MARKERS
    return False


def _normalize_move_name(name: Any) -> str:
    if _is_missing_move_value(name):
        return ""
    return _normalize_profile_key(name)


def _stable_pair_seed(attacker_team_id: Any, defender_team_id: Any, base_seed: int) -> int:
    payload = f"{attacker_team_id}|{defender_team_id}|{int(base_seed)}".encode("utf-8")
    return int(hashlib.sha256(payload).hexdigest()[:8], 16)


def _get_pokemon_profile(species_id: str, warnings: WarningCollector) -> tuple[dict[str, Any], bool]:
    cached = _LOCAL_POKEMON_PROFILES.get(species_id)
    if cached is not None:
        return cached, False
    if not _allow_simulation_fallbacks():
        raise ValueError(f"Missing Pokemon profile in pokemon_data.parquet for '{species_id}'")
    warnings.warn(f"Missing Pokemon profile for '{species_id}'; using deterministic fallback profile")
    return (
        {
            "name": species_id,
            "species": species_id,
            "types": ["Normal"],
            "type_1": "Normal",
            "type_2": None,
            "stats": _default_stats(),
            "moves": [],
        },
        True,
    )


def _get_move_profile(move_name: str, warnings: WarningCollector) -> tuple[MoveProfile, bool]:
    cached = _LOCAL_MOVE_PROFILES.get(move_name)
    if cached is not None:
        return cached, False
    if not _allow_simulation_fallbacks():
        raise ValueError(f"Missing move profile in move_reference.parquet for '{move_name}'")
    effective_power, power_handling = resolve_effective_power(move_name, 40, "physical")
    warnings.warn(f"Missing move profile for '{move_name}'; using deterministic fallback profile")
    return (
        {
            "name": move_name,
            "type": "Normal",
            "power": 40,
            "raw_power": 40,
            "effective_power": effective_power,
            "power_handling": power_handling,
            "is_status_move": False,
            "is_damage_move": True,
            "is_null_power": False,
            "damage_class": "physical",
            "accuracy": 100,
            "pp": 1,
            "level_learned_at": 0,
            "version_group": "fallback",
            "degraded_data": True,
        },
        True,
    )


def _validate_profile_coverage(teams_data: list[dict[str, Any]]) -> None:
    required_species: set[str] = set()
    required_moves: set[str] = set()
    for team in teams_data:
        for member in _team_members(team):
            species_id = normalize_species_name(str(member.get("species") or ""))
            if species_id:
                required_species.add(species_id)
            for move in cast(list[str], member.get("moves", [])):
                normalized_move = _normalize_move_name(move)
                if normalized_move:
                    required_moves.add(normalized_move)

    missing_species = sorted(species for species in required_species if species not in _LOCAL_POKEMON_PROFILES)
    missing_moves = sorted(move for move in required_moves if move not in _LOCAL_MOVE_PROFILES)
    if missing_species:
        raise ValueError(
            "Missing Pokemon profiles in pokemon_data.parquet: "
            f"total={len(missing_species)}, examples={','.join(missing_species[:50])}"
        )
    if missing_moves:
        raise ValueError(
            "Missing move profiles in move_reference.parquet: "
            f"total={len(missing_moves)}, examples={','.join(missing_moves[:50])}"
        )


def _team_members(team: dict[str, Any]) -> list[dict[str, Any]]:
    pokemon_entries = team.get("pokemon", [])
    level_entries = team.get("levels", [])
    move_entries = team.get("moves", [])
    instance_entries = team.get("pokemon_instance_ids", [])

    members: list[dict[str, Any]] = []
    if not isinstance(pokemon_entries, list):
        return members

    for slot_idx, entry in enumerate(pokemon_entries[:DEFAULT_TEAM_MEMBER_LIMIT]):
        species = entry.get("name") if isinstance(entry, dict) else entry
        if not isinstance(species, str) or not species:
            continue
        raw_level = level_entries[slot_idx] if isinstance(level_entries, list) and slot_idx < len(level_entries) else team.get("avg_level", 20)
        raw_moves = move_entries[slot_idx] if isinstance(move_entries, list) and slot_idx < len(move_entries) else []
        filtered_placeholders = 0
        moves: list[str] = []
        seen_moves: set[str] = set()
        if isinstance(raw_moves, list):
            for move in raw_moves:
                normalized_move = _normalize_move_name(move)
                if not normalized_move:
                    filtered_placeholders += 1
                    continue
                if normalized_move in seen_moves:
                    continue
                seen_moves.add(normalized_move)
                moves.append(normalized_move)
        if filtered_placeholders > 0:
            logger.info(
                "[type_matchups] filtered invalid move placeholders team_id=%s slot=%s count=%s",
                team.get("team_id"),
                slot_idx + 1,
                filtered_placeholders,
            )
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
            if float(move_profile.get("effective_power", 0.0) or 0.0) <= 0:
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
        if float(move_profile.get("effective_power", 0.0) or 0.0) <= 0:
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

    return legal_moves, degraded


def filter_simulation_teams(teams_data: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int, int]:
    filtered_teams: list[dict[str, Any]] = []
    skipped_members = 0
    dropped_teams = 0

    for team in teams_data:
        team_id = team.get("team_id")
        game_version = cast(str | None, team.get("game_version"))
        valid_members: list[dict[str, Any]] = []

        for member in _team_members(team):
            species = str(member.get("species") or "")
            level = int(member.get("level") or 1)
            warnings = WarningCollector()
            legal_moves, _ = _legal_moves_for_pokemon(
                normalize_species_name(species),
                level,
                game_version,
                warnings,
                preferred_moves=cast(list[str], member.get("moves", [])),
            )
            legal_move_names = [_normalize_move_name(move.get("name")) for move in legal_moves]
            legal_move_names = [name for name in legal_move_names if name]
            if not legal_move_names:
                skipped_members += 1
                logger.warning(
                    "[simulation] skipping member with no legal damaging moves pokemon=%s level=%s team_id=%s",
                    species or None,
                    level,
                    team_id,
                )
                continue
            valid_members.append(
                {
                    **member,
                    "moves": legal_move_names,
                }
            )

        if not valid_members:
            dropped_teams += 1
            logger.warning("[simulation] dropping team with no valid battle members team_id=%s", team_id)
            continue

        filtered_team = dict(team)
        filtered_team["pokemon"] = [member["species"] for member in valid_members]
        filtered_team["levels"] = [int(member.get("level") or 1) for member in valid_members]
        filtered_team["moves"] = [cast(list[str], member.get("moves", [])) for member in valid_members]
        if "pokemon_instance_ids" in team:
            filtered_team["pokemon_instance_ids"] = [member.get("pokemon_instance_id") for member in valid_members]
        filtered_teams.append(filtered_team)

    return filtered_teams, skipped_members, dropped_teams


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
        power = float(move.get("effective_power", 0.0) or 0.0)
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
    power = float(move.get("effective_power", 0.0) or 0.0)
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
    return bool(team.get("is_player_candidate"))


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


def _normalized_game_version(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def _normalized_boss_label(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.strip().lower().replace("_", " ").replace("-", " ").split())
    return cleaned or None


def _is_intended_boss_matchup(attacker_team: dict[str, Any], defender_team: dict[str, Any]) -> bool:
    attacker_target = _normalized_boss_label(attacker_team.get("gym"))
    if attacker_target is None:
        return False
    defender_targets = {
        label
        for label in (
            _normalized_boss_label(defender_team.get("gym")),
            _normalized_boss_label(defender_team.get("boss_name")),
        )
        if label is not None
    }
    if not defender_targets:
        return False
    return attacker_target in defender_targets


def _is_version_compatible(
    attacker_game_version: str | None,
    defender_game_version: str | None,
    config: BattleSimulationConfig,
) -> bool:
    attacker = _normalized_game_version(attacker_game_version)
    defender = _normalized_game_version(defender_game_version)
    if not config.require_exact_version_match:
        return True
    return attacker is not None and defender is not None and attacker == defender


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
        "attacker_wins": 1 if attacker_win else 0,
        "attacker_losses": 0 if attacker_win else 1,
        "n_trials": 1,
        "attacker_game_version": _normalized_game_version(attacker_game_version),
        "defender_game_version": _normalized_game_version(defender_game_version),
        "is_compatible_version": _is_version_compatible(attacker_game_version, defender_game_version, config),
        "representative_simulation_score": _simulation_score(
            attacker_win,
            attacker_remaining_pokemon,
            defender_remaining_pokemon,
            attacker_total_remaining_hp,
            defender_total_remaining_hp,
        ),
        "representative_duel_summaries": duel_summaries,
        "representative_warnings": warnings.all(),
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
    if not _is_version_compatible(attacker_game_version, defender_game_version, config):
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
            "warnings": ["incompatible_game_versions"],
            "duel_summaries": [],
            "predicted_player_win_chance": 0.0,
            "attacker_wins": 0,
            "attacker_losses": n_trials,
            "n_trials": n_trials,
            "attacker_game_version": _normalized_game_version(attacker_game_version),
            "defender_game_version": _normalized_game_version(defender_game_version),
            "is_compatible_version": False,
            "representative_simulation_score": -999.0,
            "representative_duel_summaries": [],
            "representative_warnings": ["incompatible_game_versions"],
        }

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
            "attacker_wins": 0,
            "attacker_losses": n_trials,
            "n_trials": n_trials,
            "attacker_game_version": _normalized_game_version(attacker_game_version),
            "defender_game_version": _normalized_game_version(defender_game_version),
            "is_compatible_version": True,
            "representative_simulation_score": -999.0,
            "representative_duel_summaries": [],
            "representative_warnings": ["level_plausibility_filter_failed"],
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

    representative_result = max(trial_results, key=lambda r: r["simulation_score"])
    losses = max(0, n_trials - wins)
    avg_attacker_remaining_pokemon = round(
        sum(row["attacker_remaining_pokemon"] for row in trial_results) / max(1, n_trials),
        3,
    )
    avg_defender_remaining_pokemon = round(
        sum(row["defender_remaining_pokemon"] for row in trial_results) / max(1, n_trials),
        3,
    )
    avg_attacker_remaining_hp = round(
        sum(row["attacker_total_remaining_hp"] for row in trial_results) / max(1, n_trials),
        3,
    )
    avg_defender_remaining_hp = round(
        sum(row["defender_total_remaining_hp"] for row in trial_results) / max(1, n_trials),
        3,
    )
    avg_turns = round(sum(row["battle_turns"] for row in trial_results) / max(1, n_trials), 3)
    avg_sim_score = round(sum(row["simulation_score"] for row in trial_results) / max(1, n_trials), 3)
    attacker_win = wins >= losses

    return {
        "team_id_attacker": attacker_team.get("team_id"),
        "team_id_defender": defender_team.get("team_id"),
        "attacker_win": attacker_win,
        "winner_team_id": attacker_team.get("team_id") if attacker_win else defender_team.get("team_id"),
        "attacker_remaining_pokemon": avg_attacker_remaining_pokemon,
        "defender_remaining_pokemon": avg_defender_remaining_pokemon,
        "attacker_total_remaining_hp": avg_attacker_remaining_hp,
        "defender_total_remaining_hp": avg_defender_remaining_hp,
        "battle_turns": avg_turns,
        "simulation_score": avg_sim_score,
        "degraded_data": any(bool(row["degraded_data"]) for row in trial_results),
        "warnings": sorted({warning for row in trial_results for warning in row["warnings"]}),
        "duel_summaries": [],
        "predicted_player_win_chance": round(wins / max(1, n_trials), 4),
        "attacker_wins": wins,
        "attacker_losses": losses,
        "n_trials": n_trials,
        "attacker_game_version": _normalized_game_version(attacker_game_version),
        "defender_game_version": _normalized_game_version(defender_game_version),
        "is_compatible_version": True,
        "representative_simulation_score": float(representative_result["simulation_score"]),
        "representative_duel_summaries": cast(list[dict[str, Any]], representative_result.get("duel_summaries", [])),
        "representative_warnings": cast(list[str], representative_result.get("warnings", [])),
    }


def _run_local_simulations(
    teams_data: list[dict[str, Any]],
    type_chart: dict[str, dict[str, float]],
    config: BattleSimulationConfig,
) -> list[dict[str, Any]]:
    teams_with_id = [team for team in teams_data if team.get("team_id") is not None]
    attackers = [team for team in teams_with_id if _is_player_candidate_team(team)]
    defenders = [team for team in teams_with_id if _is_boss_team(team)]

    total_pairs = sum(1 for attacker_team in attackers for defender_team in defenders if _is_intended_boss_matchup(attacker_team, defender_team))
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
            if not _is_intended_boss_matchup(attacker_team, defender_team):
                continue
            attacker_game_version = cast(str | None, attacker_team.get("game_version"))
            defender_game_version = cast(str | None, defender_team.get("game_version"))
            if not _is_version_compatible(attacker_game_version, defender_game_version, config):
                continue
            result = simulate_team_battle(
                attacker_team=attacker_team,
                defender_team=defender_team,
                type_chart=type_chart,
                attacker_game_version=attacker_game_version,
                defender_game_version=defender_game_version,
                n_trials=config.n_battle_trials,
                rng_seed=_stable_pair_seed(attacker_team.get("team_id"), defender_team.get("team_id"), config.rng_seed),
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
    return os.environ.get("PIPELINE_USE_PYSPARK", "1").strip().lower() in {"1", "true", "yes", "on"}


def _result_schema(T: Any) -> Any:
    return T.StructType(
        [
            T.StructField("team_id_attacker", T.StringType(), False),
            T.StructField("team_id_defender", T.StringType(), False),
            T.StructField("attacker_win", T.BooleanType(), False),
            T.StructField("winner_team_id", T.StringType(), True),
            T.StructField("attacker_remaining_pokemon", T.DoubleType(), False),
            T.StructField("defender_remaining_pokemon", T.DoubleType(), False),
            T.StructField("attacker_total_remaining_hp", T.DoubleType(), False),
            T.StructField("defender_total_remaining_hp", T.DoubleType(), False),
            T.StructField("battle_turns", T.DoubleType(), False),
            T.StructField("simulation_score", T.DoubleType(), False),
            T.StructField("degraded_data", T.BooleanType(), False),
            T.StructField("warnings", T.ArrayType(T.StringType(), containsNull=False), False),
            T.StructField("duel_summaries", T.ArrayType(T.MapType(T.StringType(), T.StringType(), valueContainsNull=False), containsNull=False), False),
            T.StructField("predicted_player_win_chance", T.DoubleType(), False),
            T.StructField("attacker_wins", T.IntegerType(), False),
            T.StructField("attacker_losses", T.IntegerType(), False),
            T.StructField("n_trials", T.IntegerType(), False),
            T.StructField("attacker_game_version", T.StringType(), True),
            T.StructField("defender_game_version", T.StringType(), True),
            T.StructField("is_compatible_version", T.BooleanType(), False),
            T.StructField("representative_simulation_score", T.DoubleType(), False),
            T.StructField("representative_duel_summaries", T.ArrayType(T.MapType(T.StringType(), T.StringType(), valueContainsNull=False), containsNull=False), False),
            T.StructField("representative_warnings", T.ArrayType(T.StringType(), containsNull=False), False),
        ]
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        parsed = float(value)
        if math.isnan(parsed):
            return default
        return parsed
    except (TypeError, ValueError):
        return default


def _safe_bool(value: Any) -> bool:
    return bool(value)


def _safe_string(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_summary_entries(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        return []
    normalized: list[dict[str, str]] = []
    for entry in value:
        if not isinstance(entry, dict):
            continue
        normalized_entry: dict[str, str] = {}
        for key, entry_value in entry.items():
            normalized_key = str(key).strip()
            if not normalized_key:
                continue
            normalized_entry[normalized_key] = "" if entry_value is None else str(entry_value)
        if normalized_entry:
            normalized.append(normalized_entry)
    return normalized


def _normalize_result_row(row: dict[str, Any]) -> dict[str, Any] | None:
    attacker_id = _safe_string(row.get("team_id_attacker"))
    defender_id = _safe_string(row.get("team_id_defender"))
    if attacker_id is None or defender_id is None:
        return None
    warnings = [str(item) for item in cast(list[Any], row.get("warnings", [])) if item is not None and str(item).strip()]
    rep_warnings = [
        str(item)
        for item in cast(list[Any], row.get("representative_warnings", []))
        if item is not None and str(item).strip()
    ]
    return {
        "team_id_attacker": attacker_id,
        "team_id_defender": defender_id,
        "attacker_win": _safe_bool(row.get("attacker_win")),
        "winner_team_id": _safe_string(row.get("winner_team_id")),
        "attacker_remaining_pokemon": _safe_float(row.get("attacker_remaining_pokemon")),
        "defender_remaining_pokemon": _safe_float(row.get("defender_remaining_pokemon")),
        "attacker_total_remaining_hp": _safe_float(row.get("attacker_total_remaining_hp")),
        "defender_total_remaining_hp": _safe_float(row.get("defender_total_remaining_hp")),
        "battle_turns": _safe_float(row.get("battle_turns")),
        "simulation_score": _safe_float(row.get("simulation_score")),
        "degraded_data": _safe_bool(row.get("degraded_data")),
        "warnings": warnings,
        "duel_summaries": _normalize_summary_entries(row.get("duel_summaries", [])),
        "predicted_player_win_chance": _safe_float(row.get("predicted_player_win_chance")),
        "attacker_wins": int(_safe_float(row.get("attacker_wins"))),
        "attacker_losses": int(_safe_float(row.get("attacker_losses"))),
        "n_trials": int(_safe_float(row.get("n_trials"))),
        "attacker_game_version": _safe_string(row.get("attacker_game_version")),
        "defender_game_version": _safe_string(row.get("defender_game_version")),
        "is_compatible_version": _safe_bool(row.get("is_compatible_version")),
        "representative_simulation_score": _safe_float(row.get("representative_simulation_score")),
        "representative_duel_summaries": _normalize_summary_entries(row.get("representative_duel_summaries", [])),
        "representative_warnings": rep_warnings,
    }


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

    SparkSession = cast(Any, getattr(pyspark_sql, "SparkSession"))
    F = cast(Any, pyspark_functions)
    T = cast(Any, pyspark_types)

    spark = (
        SparkSession.builder
        .appName("pokemon-team-battle-sim")
        .master("local[*]")
        .config("spark.ui.enabled", "true")
        .config("spark.ui.port", "4040")
        .config("spark.ui.bindAddress", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        teams_with_id = [team for team in teams_data if team.get("team_id") is not None]
        attacker_count = sum(1 for team in teams_with_id if _is_player_candidate_team(team))
        defender_count = sum(1 for team in teams_with_id if _is_boss_team(team))
        logger.info(
            "[type_matchups] spark engine preparing teams=%s attackers=%s defenders=%s",
            len(teams_with_id),
            attacker_count,
            defender_count,
        )
        if attacker_count == 0 or defender_count == 0:
            return []

        team_lookup: dict[str, dict[str, Any]] = {str(team.get("team_id")): team for team in teams_with_id}

        team_rows = [
            {
                "team_id": str(team.get("team_id")),
                "is_player_candidate": bool(_is_player_candidate_team(team)),
                "is_boss": bool(_is_boss_team(team)),
                "game_version": _normalized_game_version(cast(str | None, team.get("game_version"))),
                "gym_target": _normalized_boss_label(team.get("gym")),
                "boss_target": _normalized_boss_label(team.get("gym")) or _normalized_boss_label(team.get("boss_name")),
            }
            for team in teams_with_id
        ]
        schema = T.StructType(
            [
                T.StructField("team_id", T.StringType(), False),
                T.StructField("is_player_candidate", T.BooleanType(), False),
                T.StructField("is_boss", T.BooleanType(), False),
                T.StructField("game_version", T.StringType(), True),
                T.StructField("gym_target", T.StringType(), True),
                T.StructField("boss_target", T.StringType(), True),
            ]
        )

        teams_df = spark.createDataFrame(team_rows, schema=schema)
        attackers_df = teams_df.where(F.col("is_player_candidate") == F.lit(True)).select(
            F.col("team_id").alias("attacker_id"),
            F.col("game_version").alias("game_version"),
            F.col("gym_target").alias("target"),
        )
        defenders_df = teams_df.where(F.col("is_boss") == F.lit(True)).select(
            F.col("team_id").alias("defender_id"),
            F.col("game_version").alias("game_version"),
            F.col("boss_target").alias("target"),
        )
        pairs_df = attackers_df.join(
            defenders_df,
            on=["game_version", "target"],
            how="inner",
        )
        pairs_df = pairs_df.where(
            F.col("game_version").isNotNull()
            & F.col("target").isNotNull()
            & F.col("attacker_id").isNotNull()
            & F.col("defender_id").isNotNull()
        )
        pairs_df = pairs_df.persist()
        total_pairs = int(pairs_df.count())
        if total_pairs == 0:
            pairs_df.unpersist()
            return []
        invalid_pairs = int(
            pairs_df.where(
                F.col("game_version").isNull()
                | F.col("target").isNull()
                | F.col("attacker_id").isNull()
                | F.col("defender_id").isNull()
            ).count()
        )
        if invalid_pairs:
            pairs_df.unpersist()
            raise ValueError(f"[type_matchups] spark pairing produced invalid rows count={invalid_pairs}")

        group_counts_df = pairs_df.groupBy("game_version", "target").count()
        group_count = int(group_counts_df.count())
        top_groups = [
            (
                str(row["game_version"] or ""),
                str(row["target"] or ""),
                int(row["count"] or 0),
            )
            for row in group_counts_df.orderBy(F.desc("count"), F.asc("game_version"), F.asc("target")).limit(5).toLocalIterator()
        ]
        partitions = max(4, min(128, total_pairs // 1000 + 1))
        pairs_df = pairs_df.repartition(partitions, "game_version", "target")
        logger.info(
            "[type_matchups] spark groups game_boss_groups=%s eligible_pairs=%s partitions=%s attackers=%s defenders=%s",
            group_count,
            total_pairs,
            partitions,
            attacker_count,
            defender_count,
        )
        if top_groups:
            logger.info("[type_matchups] spark largest groups top5=%s", top_groups)

        team_lookup_bc = spark.sparkContext.broadcast(team_lookup)
        pokemon_profiles_bc = spark.sparkContext.broadcast(_LOCAL_POKEMON_PROFILES)
        move_profiles_bc = spark.sparkContext.broadcast(_LOCAL_MOVE_PROFILES)
        chart_bc = spark.sparkContext.broadcast(type_chart)
        config_bc = spark.sparkContext.broadcast(asdict(config))

        def _simulate_partition(rows: Any) -> Any:
            local_teams = cast(dict[str, dict[str, Any]], team_lookup_bc.value)
            local_pokemon_profiles = cast(dict[str, dict[str, Any]], pokemon_profiles_bc.value)
            local_move_profiles = cast(dict[str, MoveProfile], move_profiles_bc.value)
            local_chart = cast(dict[str, dict[str, float]], chart_bc.value)
            local_config = BattleSimulationConfig(**cast(dict[str, Any], config_bc.value))
            _install_reference_profiles(local_pokemon_profiles, local_move_profiles)
            for row in rows:
                attacker_team = local_teams[str(row.attacker_id)]
                defender_team = local_teams[str(row.defender_id)]
                game_version = cast(str | None, row.game_version)
                if not _is_version_compatible(game_version, game_version, local_config):
                    continue
                result = simulate_team_battle(
                    attacker_team=attacker_team,
                    defender_team=defender_team,
                    type_chart=local_chart,
                    attacker_game_version=game_version,
                    defender_game_version=game_version,
                    n_trials=local_config.n_battle_trials,
                    rng_seed=_stable_pair_seed(
                        attacker_team.get("team_id"),
                        defender_team.get("team_id"),
                        local_config.rng_seed,
                    ),
                    config=local_config,
                )
                normalized = _normalize_result_row(result)
                if normalized is not None:
                    yield normalized

        result_rdd = pairs_df.rdd.mapPartitions(_simulate_partition)
        result_rows = [row for row in result_rdd.collect() if row]
        pairs_df.unpersist()
        if not result_rows:
            return []
        result_df = spark.createDataFrame(result_rows, schema=_result_schema(T))
        result_df = result_df.orderBy("team_id_attacker", "team_id_defender")
        return [cast(dict[str, Any], row.asDict(recursive=True)) for row in result_df.toLocalIterator()]
    finally:
        spark.stop()


def _load_move_and_pokemon_profiles_from_disk(silver_dir: Path) -> None:
    pokemon_profiles = load_pokemon_profiles_from_silver(silver_dir)
    move_profiles = load_move_profiles_from_silver(silver_dir)
    _install_reference_profiles(pokemon_profiles, move_profiles)
    logger.info(
        "[type_matchups] loaded parquet reference profiles pokemon=%s moves=%s",
        len(_LOCAL_POKEMON_PROFILES),
        len(_LOCAL_MOVE_PROFILES),
    )


def build_team_battle_simulations(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
    force_spark: bool | None = None,
    runtime_config: BattleSimulationConfig | None = None,
) -> None:
    started_at = time.perf_counter()
    type_chart = load_type_chart(bronze_dir)
    simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME
    simulation_dir.mkdir(parents=True, exist_ok=True)

    _load_move_and_pokemon_profiles_from_disk(silver_dir)

    config = runtime_config or BattleSimulationConfig()
    if not _allow_simulation_fallbacks():
        _validate_profile_coverage(teams_data)
    filtered_teams, skipped_members, dropped_teams = filter_simulation_teams(teams_data)
    logger.info(
        "[simulation] filtered teams original=%s remaining=%s skipped_members=%s dropped_teams=%s",
        len(teams_data),
        len(filtered_teams),
        skipped_members,
        dropped_teams,
    )
    if not filtered_teams:
        logger.warning("[type_matchups] no valid teams remain after filtering; writing empty simulation outputs")
        write_parquet(simulation_dir / "team_battle_simulations.parquet", [])
        write_parquet(simulation_dir / "team_battle_simulation_diagnostics.parquet", [])
        return
    use_spark = _should_use_spark() if force_spark is None else bool(force_spark)
    logger.info("[type_matchups] engine=%s", "spark" if use_spark else "local")

    if use_spark:
        simulations = _run_spark_simulations(filtered_teams, type_chart, config)
    else:
        simulations = _run_local_simulations(filtered_teams, type_chart, config)

    incompatible_rows = [row for row in simulations if not bool(row.get("is_compatible_version", False))]
    if incompatible_rows:
        raise ValueError("Incompatible cross-version simulation rows detected; strict compatibility contract violated")
    if config.fail_on_degraded_data and any(bool(row.get("degraded_data", False)) for row in simulations):
        raise ValueError("Degraded simulation rows detected in strict mode; aborting output write")

    aggregate_rows: list[dict[str, Any]] = []
    diagnostic_rows: list[dict[str, Any]] = []
    for row in simulations:
        aggregate_rows.append(
            {
                key: value
                for key, value in row.items()
                if key not in {"representative_simulation_score", "representative_duel_summaries", "representative_warnings"}
            }
        )
        diagnostic_rows.append(
            {
                "team_id_attacker": row.get("team_id_attacker"),
                "team_id_defender": row.get("team_id_defender"),
                "representative_simulation_score": row.get("representative_simulation_score"),
                "representative_duel_summaries": row.get("representative_duel_summaries", []),
                "representative_warnings": row.get("representative_warnings", []),
            }
        )

    write_parquet(simulation_dir / "team_battle_simulations.parquet", aggregate_rows)
    write_parquet(simulation_dir / "team_battle_simulation_diagnostics.parquet", diagnostic_rows)
    logger.info(
        "[type_matchups] wrote team_battle_simulations rows=%s and diagnostics rows=%s elapsed_s=%.2f",
        len(aggregate_rows),
        len(diagnostic_rows),
        time.perf_counter() - started_at,
    )


def build_type_matchups(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> None:
    build_team_battle_simulations(teams_data=teams_data, silver_dir=silver_dir, bronze_dir=bronze_dir)
