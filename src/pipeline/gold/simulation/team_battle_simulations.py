from __future__ import annotations

import copy
import importlib
import hashlib
import logging
import math
import os
import random
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, TypedDict, cast

from tqdm.auto import tqdm

from src.pipeline.common.cast import is_nullish, to_bool, to_float, to_int, to_list
from src.pipeline.common.io import read_json, read_parquet, write_parquet
from src.pipeline.common.normalize import normalize_optional_text, normalize_text
from src.pipeline.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    GOLD_SIMULATION_DIRNAME,
    SILVER_DIR,
    SIMULATION_CONFIG,
)
from src.pipeline.gold.simulation.progression_balancing import clamp_progression_depth, dynamic_level_gap_limits
from src.pipeline.silver.config.game_config import get_starter_type
from src.pipeline.silver.config.team_config import DEFAULT_TEAM_MEMBER_LIMIT
from src.pipeline.silver.move_power import resolve_effective_power

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


class CombatProfile(TypedDict):
    species: str
    species_id: str
    level: int
    types: list[str]
    stats: dict[str, int]
    max_hp: int
    current_hp: int
    legal_moves: list[MoveProfile]


class DuelResult(TypedDict):
    winner: str
    attacker_remaining_hp: int
    defender_remaining_hp: int
    turns: int
    attacker_move_used: str
    defender_move_used: str


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
    boss_sequence_id: str | None
    sequence_position: int | None
    remaining_team_state: list[dict[str, Any]]
    gauntlet_success: bool
    gauntlet_success_rate: float | None
    simulation_mode: str


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


_STALEMATE_RESOLVED_WARNING = "stalemate_resolved_single_battle"


_LOCAL_POKEMON_PROFILES: dict[str, dict[str, Any]] = {}
_LOCAL_MOVE_PROFILES: dict[str, MoveProfile] = {}
_LOCAL_TEAM_PROFILE_CACHE: dict[tuple[str, str | None], dict[str, Any]] = {}

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
    return int(to_int(value, default=default) or default)


def _install_reference_profiles(
    pokemon_profiles: dict[str, dict[str, Any]],
    move_profiles: dict[str, MoveProfile],
) -> None:
    global _LOCAL_POKEMON_PROFILES, _LOCAL_MOVE_PROFILES, _LOCAL_TEAM_PROFILE_CACHE
    _LOCAL_POKEMON_PROFILES = dict(pokemon_profiles)
    _LOCAL_MOVE_PROFILES = dict(move_profiles)
    _LOCAL_TEAM_PROFILE_CACHE = {}


def _is_nullish(value: Any) -> bool:
    return is_nullish(value, include_pandas_na=False)


def _normalize_profile_key(value: Any) -> str:
    if _is_nullish(value):
        return ""
    return str(value).strip().lower().replace(" ", "-").replace("_", "-")


def _normalized_text(value: Any) -> str:
    return normalize_text(value)


def _is_truthy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(value, float) and math.isnan(value):
            return False
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


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
            "type_2": (
                str(row.get("type_2")).title()
                if isinstance(row.get("type_2"), str) and str(row.get("type_2")).strip()
                else None
            ),
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


def _stable_sequence_seed(player_team_id: Any, boss_sequence_id: str, base_seed: int) -> int:
    payload = f"{player_team_id}|{boss_sequence_id}|{int(base_seed)}".encode("utf-8")
    return int(hashlib.sha256(payload).hexdigest()[:8], 16)


def _get_pokemon_profile(species_id: str, warnings: WarningCollector) -> dict[str, Any]:
    cached = _LOCAL_POKEMON_PROFILES.get(species_id)
    if cached is not None:
        return cached

    if not _allow_simulation_fallbacks():
        raise ValueError(f"Missing Pokemon profile in pokemon_data.parquet for '{species_id}'")

    warnings.warn(f"Missing Pokemon profile for '{species_id}'; using deterministic fallback profile")
    return {
        "name": species_id,
        "species": species_id,
        "types": ["Normal"],
        "type_1": "Normal",
        "type_2": None,
        "stats": _default_stats(),
        "moves": [],
    }


def _get_move_profile(move_name: str, warnings: WarningCollector) -> MoveProfile:
    cached = _LOCAL_MOVE_PROFILES.get(move_name)
    if cached is not None:
        return cached

    if not _allow_simulation_fallbacks():
        raise ValueError(f"Missing move profile in move_reference.parquet for '{move_name}'")

    effective_power, power_handling = resolve_effective_power(move_name, 40, "physical")
    warnings.warn(f"Missing move profile for '{move_name}'; using deterministic fallback profile")
    return {
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
    }


def _as_sequence(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    tolist = getattr(value, "tolist", None)
    if callable(tolist):
        converted = tolist()
        if isinstance(converted, list):
            return converted
        if isinstance(converted, tuple):
            return list(converted)
    return []


def _team_members(team: dict[str, Any]) -> list[dict[str, Any]]:
    pokemon_entries = _as_sequence(team.get("pokemon", []))
    level_entries = _as_sequence(team.get("levels", []))
    move_entries = _as_sequence(team.get("moves", []))
    instance_entries = _as_sequence(team.get("pokemon_instance_ids", []))

    members: list[dict[str, Any]] = []
    if not pokemon_entries:
        return members

    for slot_idx, entry in enumerate(pokemon_entries[:DEFAULT_TEAM_MEMBER_LIMIT]):
        species = entry.get("name") if isinstance(entry, dict) else entry
        if not isinstance(species, str) or not species:
            continue

        raw_level = (
            level_entries[slot_idx]
            if slot_idx < len(level_entries)
            else team.get("avg_level", 20)
        )
        raw_moves = move_entries[slot_idx] if slot_idx < len(move_entries) else []

        filtered_placeholders = 0
        moves: list[str] = []
        seen_moves: set[str] = set()

        for move in _as_sequence(raw_moves):
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
                "pokemon_instance_id": (
                    instance_entries[slot_idx]
                    if slot_idx < len(instance_entries)
                    else None
                ),
            }
        )

    return members


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
) -> list[MoveProfile]:
    profile = _get_pokemon_profile(species_id, warnings)
    version_group = _version_group_for_game(game_version)
    legal_moves: list[MoveProfile] = []
    seen_moves: set[str] = set()

    if preferred_moves:
        for move_name in preferred_moves:
            normalized_move = _normalize_move_name(move_name)
            if not normalized_move or normalized_move in seen_moves:
                continue

            move_profile = _get_move_profile(normalized_move, warnings)

            if float(move_profile.get("effective_power", 0.0) or 0.0) <= 0:
                continue

            legal_moves.append(move_profile)
            seen_moves.add(normalized_move)

        if legal_moves:
            return legal_moves

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

        move_profile = _get_move_profile(move_name, warnings)

        if float(move_profile.get("effective_power", 0.0) or 0.0) <= 0:
            continue

        legal_moves.append(
            {
                **move_profile,
                "level_learned_at": learned_level,
                "version_group": detail_group,
            }
        )
        seen_moves.add(move_name)

    return legal_moves


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

            legal_moves = _legal_moves_for_pokemon(
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
    profile = _get_pokemon_profile(species_id, warnings)
    legal_moves = _legal_moves_for_pokemon(
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
    }


def _prepared_team_bundle(
    team: dict[str, Any],
    game_version: str | None,
) -> dict[str, Any]:
    team_id = _safe_string(team.get("team_id"))
    if team_id is None:
        raise ValueError("Simulation team is missing team_id during combat profile preparation")

    cache_key = (team_id, _normalized_game_version(game_version))
    cached = _LOCAL_TEAM_PROFILE_CACHE.get(cache_key)
    if cached is not None:
        return cached

    warnings = WarningCollector()
    profiles = [
        get_pokemon_combat_profile(
            species=member["species"],
            level=int(member["level"]),
            game_version=game_version,
            warnings=warnings,
            moves=cast(list[str], member.get("moves", [])),
        )
        for member in _team_members(team)
    ]
    bundle = {
        "profiles": profiles,
        "warnings": warnings.all(),}
    _LOCAL_TEAM_PROFILE_CACHE[cache_key] = bundle
    return bundle


def _clone_team_profiles(bundle: dict[str, Any]) -> list[CombatProfile]:
    return cast(list[CombatProfile], copy.deepcopy(bundle["profiles"]))


def _serialize_team_state(team_profiles: list[CombatProfile]) -> list[dict[str, Any]]:
    return [
        {
            "slot": str(index + 1),
            "species": str(profile["species"]),
            "level": str(profile["level"]),
            "current_hp": str(profile["current_hp"]),
            "max_hp": str(profile["max_hp"]),
            "is_fainted": "true" if int(profile["current_hp"]) <= 0 else "false",
        }
        for index, profile in enumerate(team_profiles)
    ]


def _choose_best_move(attacker: CombatProfile, defender: CombatProfile, type_chart: dict[str, dict[str, float]]) -> MoveProfile:
    damaging_moves = [
        move
        for move in attacker["legal_moves"]
        if float(move.get("effective_power", 0.0) or 0.0) > 0
        and _type_multiplier(move["type"], defender["types"], type_chart) > 0
    ]
    candidate_moves = damaging_moves or attacker["legal_moves"]

    best_move = candidate_moves[0]
    best_score = -1.0

    for move in candidate_moves:
        power = float(move.get("effective_power", 0.0) or 0.0)
        multiplier = _type_multiplier(move["type"], defender["types"], type_chart)
        stab = 1.5 if move["type"].title() in attacker["types"] else 1.0
        accuracy = int(move.get("accuracy") or 100)
        hit_chance = max(1, min(100, accuracy)) / 100.0

        attack_stat = (
            attacker["stats"].get("attack", 50)
            if move["damage_class"] == "physical"
            else attacker["stats"].get("sp_attack", 50)
        )
        defense_stat = (
            defender["stats"].get("defense", 50)
            if move["damage_class"] == "physical"
            else defender["stats"].get("sp_defense", 50)
        )

        score = power * multiplier * stab * max(1.0, attack_stat / max(1, defense_stat)) * hit_chance
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

    attack_stat = (
        attacker["stats"].get("attack", 50)
        if move["damage_class"] == "physical"
        else attacker["stats"].get("sp_attack", 50)
    )
    defense_stat = (
        defender["stats"].get("defense", 50)
        if move["damage_class"] == "physical"
        else defender["stats"].get("sp_defense", 50)
    )

    base = (((2 * attacker["level"] / 5) + 2) * power * max(1, attack_stat) / max(1, defense_stat)) / 50 + 2

    stab = 1.5 if move["type"].title() in attacker["types"] else 1.0
    type_effectiveness = _type_multiplier(move["type"], defender["types"], type_chart)
    if type_effectiveness <= 0:
        return 0
    crit = 1.5 if rng.random() < config.crit_chance else 1.0
    randomness = rng.uniform(config.damage_randomness_min, config.damage_randomness_max)

    return int(max(1, base * stab * type_effectiveness * crit * randomness))


def _attempt_move_hit(move: MoveProfile, rng: random.Random) -> bool:
    accuracy = move.get("accuracy")
    if accuracy is None:
        return True

    try:
        accuracy_value = int(accuracy)
    except (TypeError, ValueError):
        return True

    return rng.random() <= (max(1, min(100, accuracy_value)) / 100.0)


_STATUS_MOVE_TRIGGERS: dict[str, dict[str, Any]] = {
    "will-o-wisp": {"status": "burn", "chance": 1.0},
    "ember": {"status": "burn", "chance": 0.1},
    "flamethrower": {"status": "burn", "chance": 0.1},
    "fire-blast": {"status": "burn", "chance": 0.1},
    "thunder-wave": {"status": "paralyze", "chance": 1.0},
    "thunderbolt": {"status": "paralyze", "chance": 0.1},
    "thunder": {"status": "paralyze", "chance": 0.3},
    "body-slam": {"status": "paralyze", "chance": 0.3},
    "spore": {"status": "sleep", "chance": 1.0, "sleep_turns": (1, 2)},
    "sleep-powder": {"status": "sleep", "chance": 1.0, "sleep_turns": (1, 2)},
    "hypnosis": {"status": "sleep", "chance": 1.0, "sleep_turns": (1, 2)},
    "sing": {"status": "sleep", "chance": 1.0, "sleep_turns": (1, 2)},
}


def _empty_status_state() -> dict[str, Any]:
    return {"status": "", "sleep_turns": 0}


def _can_act_with_status(status_state: dict[str, Any], rng: random.Random) -> bool:
    status = str(status_state.get("status") or "")
    if status == "sleep":
        turns_remaining = int(status_state.get("sleep_turns") or 0)
        if turns_remaining > 0:
            status_state["sleep_turns"] = turns_remaining - 1
            return False
        status_state["status"] = ""
        return True
    if status == "paralyze":
        return rng.random() >= 0.25
    return True


def _apply_end_of_turn_status_damage(profile: CombatProfile, status_state: dict[str, Any]) -> None:
    if int(profile["current_hp"]) <= 0:
        return
    if str(status_state.get("status") or "") != "burn":
        return
    burn_damage = max(1, int(profile["max_hp"]) // 16)
    profile["current_hp"] = max(0, int(profile["current_hp"]) - burn_damage)


def _try_inflict_status_from_move(
    move: MoveProfile,
    target_profile: CombatProfile,
    target_status: dict[str, Any],
    rng: random.Random,
) -> None:
    if int(target_profile["current_hp"]) <= 0:
        return
    if str(target_status.get("status") or ""):
        return

    trigger = _STATUS_MOVE_TRIGGERS.get(_normalize_move_name(move.get("name")))
    if not trigger:
        return
    if rng.random() > float(trigger.get("chance") or 0.0):
        return

    status_name = str(trigger.get("status") or "")
    if status_name == "sleep":
        low, high = cast(tuple[int, int], trigger.get("sleep_turns", (1, 2)))
        target_status["status"] = "sleep"
        target_status["sleep_turns"] = rng.randint(max(1, low), max(1, high))
        return

    target_status["status"] = status_name


def simulate_one_vs_one(
    attacker: CombatProfile,
    defender: CombatProfile,
    type_chart: dict[str, dict[str, float]],
    rng: random.Random,
    config: BattleSimulationConfig,
) -> DuelResult:
    attacker_last_move = ""
    defender_last_move = ""
    attacker_status = _empty_status_state()
    defender_status = _empty_status_state()

    for turn in range(1, config.max_turns_per_duel + 1):
        attacker_move = _choose_best_move(attacker, defender, type_chart)
        defender_move = _choose_best_move(defender, attacker, type_chart)
        attacker_last_move = attacker_move["name"]
        defender_last_move = defender_move["name"]

        attacker_speed = int(attacker["stats"].get("speed", 50) or 50)
        defender_speed = int(defender["stats"].get("speed", 50) or 50)
        attacker_goes_first = attacker_speed >= defender_speed

        def _take_hit(
            active: CombatProfile,
            target: CombatProfile,
            move: MoveProfile,
            active_status: dict[str, Any],
            target_status: dict[str, Any],
        ) -> None:
            if not _can_act_with_status(active_status, rng):
                return
            if not _attempt_move_hit(move, rng):
                return
            damage = _calculate_damage(active, target, move, type_chart, rng, config)
            target["current_hp"] = max(0, target["current_hp"] - damage)
            _try_inflict_status_from_move(move, target, target_status, rng)

        if attacker_goes_first:
            _take_hit(attacker, defender, attacker_move, attacker_status, defender_status)
            if defender["current_hp"] == 0:
                return {
                    "winner": "attacker",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                }

            _take_hit(defender, attacker, defender_move, defender_status, attacker_status)
            if attacker["current_hp"] == 0:
                return {
                    "winner": "defender",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                }
        else:
            _take_hit(defender, attacker, defender_move, defender_status, attacker_status)
            if attacker["current_hp"] == 0:
                return {
                    "winner": "defender",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                }

            _take_hit(attacker, defender, attacker_move, attacker_status, defender_status)
            if defender["current_hp"] == 0:
                return {
                    "winner": "attacker",
                    "attacker_remaining_hp": attacker["current_hp"],
                    "defender_remaining_hp": defender["current_hp"],
                    "turns": turn,
                    "attacker_move_used": attacker_last_move,
                    "defender_move_used": defender_last_move,
                }

        _apply_end_of_turn_status_damage(attacker, attacker_status)
        _apply_end_of_turn_status_damage(defender, defender_status)
        if defender["current_hp"] == 0:
            return {
                "winner": "attacker",
                "attacker_remaining_hp": attacker["current_hp"],
                "defender_remaining_hp": defender["current_hp"],
                "turns": turn,
                "attacker_move_used": attacker_last_move,
                "defender_move_used": defender_last_move,
            }
        if attacker["current_hp"] == 0:
            return {
                "winner": "defender",
                "attacker_remaining_hp": attacker["current_hp"],
                "defender_remaining_hp": defender["current_hp"],
                "turns": turn,
                "attacker_move_used": attacker_last_move,
                "defender_move_used": defender_last_move,
            }

    winner = "attacker" if attacker["current_hp"] >= defender["current_hp"] else "defender"
    return {
        "winner": winner,
        "attacker_remaining_hp": attacker["current_hp"],
        "defender_remaining_hp": defender["current_hp"],
        "turns": config.max_turns_per_duel,
        "attacker_move_used": attacker_last_move,
        "defender_move_used": defender_last_move,
    }


def _first_alive_index(team: list[CombatProfile]) -> int | None:
    for i, pokemon in enumerate(team):
        if pokemon["current_hp"] > 0:
            return i
    return None


def _first_two_alive_indices(team: list[CombatProfile]) -> list[int]:
    return [i for i, pokemon in enumerate(team) if int(pokemon["current_hp"]) > 0][:2]


def _team_battle_type(team: dict[str, Any]) -> str:
    normalized = _normalized_text(team.get("battle_type") or team.get("target_battle_type"))
    if normalized == "double":
        return "double"
    return "single"


def _simulate_double_battle_once_from_profiles(
    attacker_team: dict[str, Any],
    defender_team: dict[str, Any],
    attacker_profiles: list[CombatProfile],
    defender_profiles: list[CombatProfile],
    type_chart: dict[str, dict[str, float]],
    attacker_game_version: str | None,
    defender_game_version: str | None,
    rng: random.Random,
    config: BattleSimulationConfig,
    warnings: WarningCollector,
) -> TeamBattleResult:
    duel_summaries: list[dict[str, Any]] = []
    battle_turns = 0
    max_rounds = max(1, config.max_turns_per_duel * DEFAULT_TEAM_MEMBER_LIMIT)

    for round_idx in range(1, max_rounds + 1):
        attacker_active = _first_two_alive_indices(attacker_profiles)
        defender_active = _first_two_alive_indices(defender_profiles)
        if not attacker_active or not defender_active:
            break

        battle_turns += 1
        actions: list[dict[str, Any]] = []

        for attacker_idx in attacker_active:
            if not defender_active:
                break
            best_target_idx = max(
                defender_active,
                key=lambda target_idx: float(
                    _choose_best_move(attacker_profiles[attacker_idx], defender_profiles[target_idx], type_chart).get(
                        "effective_power", 0.0
                    )
                ),
            )
            move = _choose_best_move(attacker_profiles[attacker_idx], defender_profiles[best_target_idx], type_chart)
            speed = int(attacker_profiles[attacker_idx]["stats"].get("speed", 50) or 50)
            actions.append({"side": "attacker", "actor_idx": attacker_idx, "target_idx": best_target_idx, "move": move, "speed": speed})

        for defender_idx in defender_active:
            if not attacker_active:
                break
            best_target_idx = max(
                attacker_active,
                key=lambda target_idx: float(
                    _choose_best_move(defender_profiles[defender_idx], attacker_profiles[target_idx], type_chart).get(
                        "effective_power", 0.0
                    )
                ),
            )
            move = _choose_best_move(defender_profiles[defender_idx], attacker_profiles[best_target_idx], type_chart)
            speed = int(defender_profiles[defender_idx]["stats"].get("speed", 50) or 50)
            actions.append({"side": "defender", "actor_idx": defender_idx, "target_idx": best_target_idx, "move": move, "speed": speed})

        actions.sort(key=lambda row: (-int(row["speed"]), 0 if row["side"] == "attacker" else 1))
        round_events: list[dict[str, Any]] = []

        for action in actions:
            move = cast(MoveProfile, action["move"])
            actor_team = attacker_profiles if action["side"] == "attacker" else defender_profiles
            target_team = defender_profiles if action["side"] == "attacker" else attacker_profiles
            actor_idx = int(action["actor_idx"])
            target_idx = int(action["target_idx"])
            actor = actor_team[actor_idx]
            target = target_team[target_idx]

            if int(actor["current_hp"]) <= 0 or int(target["current_hp"]) <= 0:
                continue

            hit = _attempt_move_hit(move, rng)
            damage = 0
            if hit:
                damage = _calculate_damage(actor, target, move, type_chart, rng, config)
                target["current_hp"] = max(0, int(target["current_hp"]) - int(damage))

            round_events.append(
                {
                    "side": str(action["side"]),
                    "actor_slot": str(actor_idx + 1),
                    "actor_species": str(actor["species"]),
                    "target_slot": str(target_idx + 1),
                    "target_species": str(target["species"]),
                    "move": str(move["name"]),
                    "hit": "true" if hit else "false",
                    "damage": str(damage),
                    "target_hp_after": str(target["current_hp"]),
                }
            )

            if not _first_two_alive_indices(attacker_profiles) or not _first_two_alive_indices(defender_profiles):
                break

        duel_summaries.append(
            {
                "round": round_idx,
                "active_attackers": [idx + 1 for idx in attacker_active],
                "active_defenders": [idx + 1 for idx in defender_active],
                "events": round_events,
            }
        )

        if not _first_two_alive_indices(attacker_profiles) or not _first_two_alive_indices(defender_profiles):
            break

    attacker_remaining_pokemon = sum(1 for p in attacker_profiles if int(p["current_hp"]) > 0)
    defender_remaining_pokemon = sum(1 for p in defender_profiles if int(p["current_hp"]) > 0)
    attacker_total_remaining_hp = sum(int(p["current_hp"]) for p in attacker_profiles)
    defender_total_remaining_hp = sum(int(p["current_hp"]) for p in defender_profiles)
    attacker_win = attacker_remaining_pokemon > 0 and defender_remaining_pokemon == 0
    winner_team_id = attacker_team.get("team_id") if attacker_win else defender_team.get("team_id")
    simulation_score = _simulation_score(
        attacker_win,
        attacker_remaining_pokemon,
        defender_remaining_pokemon,
        attacker_total_remaining_hp,
        defender_total_remaining_hp,
    )

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
        "simulation_score": simulation_score,
        "warnings": warnings.all(),
        "duel_summaries": duel_summaries,
        "predicted_player_win_chance": 1.0 if attacker_win else 0.0,
        "attacker_wins": 1 if attacker_win else 0,
        "attacker_losses": 0 if attacker_win else 1,
        "n_trials": 1,
        "attacker_game_version": _normalized_game_version(attacker_game_version),
        "defender_game_version": _normalized_game_version(defender_game_version),
        "is_compatible_version": _is_version_compatible(attacker_game_version, defender_game_version, config),
        "representative_simulation_score": simulation_score,
        "representative_duel_summaries": duel_summaries,
        "representative_warnings": warnings.all(),
        "boss_sequence_id": None,
        "sequence_position": None,
        "remaining_team_state": _serialize_team_state(attacker_profiles),
        "gauntlet_success": attacker_win,
        "simulation_mode": "gym",
    }


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
    team_role = _normalized_text(team.get("team_role"))
    origin = _normalized_text(team.get("origin"))
    team_id = str(team.get("team_id") or "").strip().lower()

    if team_role == "boss":
        return False
    if _is_truthy_flag(team.get("is_boss_team")):
        return False
    if team_id.startswith("boss-team:"):
        return False

    return _is_truthy_flag(team.get("is_player_candidate")) and origin != "kaggle"


def _is_boss_team(team: dict[str, Any]) -> bool:
    team_role = _normalized_text(team.get("team_role"))
    origin = _normalized_text(team.get("origin"))
    boss_name = _normalized_boss_label(team.get("boss_name"))

    if team_role == "boss":
        return True
    if _is_truthy_flag(team.get("is_boss_team")):
        return True

    # Backward-compatible fallback for real Kaggle boss rows only.
    if origin == "kaggle" and not _is_truthy_flag(team.get("is_player_candidate")):
        return True

    if boss_name is not None and not _is_truthy_flag(team.get("is_player_candidate")):
        return True

    return False


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


def _apply_level_plausibility_filter(
    attacker_team: dict[str, Any],
    defender_team: dict[str, Any],
    config: BattleSimulationConfig,
) -> bool:
    player_avg = attacker_team.get("avg_level")
    boss_avg = defender_team.get("avg_level")

    if not isinstance(player_avg, (int, float)) or not isinstance(boss_avg, (int, float)):
        return True

    progression_depth = clamp_progression_depth(
        attacker_team.get("progression_depth", defender_team.get("progression_depth"))
    )
    max_overlevel, max_underlevel = dynamic_level_gap_limits(
        progression_depth,
        base_max_overlevel=config.max_overlevel,
        base_max_underlevel=config.max_underlevel,
    )
    boss_upper_reference = _boss_level_cap(defender_team)
    boss_upper_level = float(boss_upper_reference) if isinstance(boss_upper_reference, int) else float(boss_avg)
    return player_avg <= boss_upper_level + max_overlevel and player_avg >= boss_avg - max_underlevel


def _normalized_game_version(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    return normalize_optional_text(value)


def _normalized_boss_label(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    cleaned = " ".join(value.strip().lower().replace("_", " ").replace("-", " ").split())
    return cleaned or None


def _coerce_boss_alias_values(value: Any) -> list[Any]:
    return to_list(value)


def _normalized_starter_type(value: Any) -> str | None:
    normalized = _normalized_text(value)
    return get_starter_type(normalized)


def _resolve_starter_type(*values: Any) -> str | None:
    for value in values:
        if _is_nullish(value):
            continue
        resolved = _normalized_starter_type(value)
        if resolved is not None:
            return resolved
    return None


def _sequence_id_for_game_version(game_version: str) -> str:
    return f"{game_version}:elite_four_champion"


def _load_boss_reference_rows(silver_dir: Path) -> list[dict[str, Any]]:
    bosses_path = silver_dir / "references" / "bosses.parquet"
    if not bosses_path.exists():
        raise FileNotFoundError(f"Boss reference parquet missing: {bosses_path}")

    bosses_df = read_parquet(bosses_path)
    required_columns = {"game_version", "boss_name_canonical", "boss_order", "boss_role"}
    missing_columns = sorted(required_columns - set(bosses_df.columns))
    if missing_columns:
        raise ValueError(f"bosses.parquet missing required columns: {missing_columns}")

    normalized_rows: list[dict[str, Any]] = []
    for row in bosses_df.to_dict(orient="records"):
        game_version = _normalized_game_version(cast(str | None, row.get("game_version")))
        boss_name = _normalized_boss_label(row.get("boss_name_canonical"))
        boss_name_kaggle = _normalized_boss_label(row.get("boss_name_kaggle"))
        alias_values = _coerce_boss_alias_values(row.get("boss_name_aliases"))
        boss_role = _normalized_text(row.get("boss_role"))
        boss_order = _safe_int(row.get("boss_order"), 0)
        gym_index = _safe_int(row.get("gym_index"), boss_order)
        starter_condition = _normalized_text(row.get("starter_condition")) or None
        starter_type = _resolve_starter_type(row.get("starter_type"), starter_condition)
        starter_dependency_type = _normalized_text(row.get("starter_dependency_type")) or "none"
        has_team_variants = bool(row.get("has_team_variants", False))
        is_optional = _is_truthy_flag(row.get("is_optional"))
        is_simulatable = bool(row.get("is_simulatable", True))

        if game_version is None or boss_name is None or boss_role == "" or boss_order <= 0:
            raise ValueError(
                "bosses.parquet contains invalid boss metadata: "
                f"game_version={row.get('game_version')} boss_name={row.get('boss_name_canonical')} "
                f"boss_role={row.get('boss_role')} boss_order={row.get('boss_order')}"
            )

        boss_name_aliases = sorted(
            {
                alias
                for alias in [
                    boss_name,
                    boss_name_kaggle,
                    *(_normalized_boss_label(alias) for alias in alias_values),
                ]
                if alias
            }
        )
        normalized_rows.append(
            {
                "game_version": game_version,
                "boss_name": boss_name,
                "boss_name_aliases": boss_name_aliases,
                "boss_role": boss_role,
                "boss_order": boss_order,
                "boss_id": _safe_string(row.get("boss_id")),
                "gym_index": gym_index,
                "starter_condition": starter_condition,
                "starter_type": starter_type,
                "starter_dependency_type": starter_dependency_type,
                "has_team_variants": has_team_variants,
                "is_optional": is_optional,
                "is_simulatable": is_simulatable,
            }
        )

    return normalized_rows


def _enrich_teams_with_boss_context(
    teams_data: list[dict[str, Any]],
    silver_dir: Path,
) -> list[dict[str, Any]]:
    teams_with_context = [dict(team) for team in teams_data]
    boss_reference_rows = _load_boss_reference_rows(silver_dir)

    boss_reference_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    boss_reference_by_canonical_key: dict[tuple[str, str], dict[str, Any]] = {}
    boss_reference_by_id: dict[str, dict[str, Any]] = {}
    reference_rows_by_game: dict[str, list[dict[str, Any]]] = {}

    for row in boss_reference_rows:
        if not bool(row.get("is_simulatable", True)):
            continue
        canonical_key = (str(row["game_version"]), str(row["boss_name"]))
        if canonical_key in boss_reference_by_canonical_key:
            raise ValueError(
                f"bosses.parquet contains duplicate boss reference rows for game_version={canonical_key[0]} boss_name={canonical_key[1]}"
            )
        boss_reference_by_canonical_key[canonical_key] = row
        reference_rows_by_game.setdefault(str(row["game_version"]), []).append(row)
        boss_id = _safe_string(row.get("boss_id"))
        if boss_id:
            boss_reference_by_id[boss_id] = row

        for alias in cast(list[str], row.get("boss_name_aliases", [])):
            key = (str(row["game_version"]), alias)
            existing = boss_reference_by_key.get(key)
            if existing is not None and existing != row:
                raise ValueError(
                    "bosses.parquet contains ambiguous boss aliases for the same game_version: "
                    f"game_version={key[0]} boss_name={key[1]}"
                )
            boss_reference_by_key[key] = row

    boss_team_presence_by_key: set[tuple[str, str]] = set()
    boss_team_variant_keys_by_boss: dict[tuple[str, str], set[tuple[str, str | None]]] = {}
    player_sequence_counts: dict[tuple[str, str | None], int] = {}

    for team in teams_with_context:
        game_version = _normalized_game_version(cast(str | None, team.get("game_version")))
        if game_version is None:
            raise ValueError(f"Simulation team is missing game_version: team_id={team.get('team_id')}")

        team["game_version"] = game_version

        if _is_boss_team(team):
            boss_id = _target_boss_id_for_defender(team)
            ref_row = boss_reference_by_id.get(boss_id) if boss_id else None
            if ref_row is None:
                boss_name = _target_label_for_defender(team)
                if boss_name is None:
                    raise ValueError(f"Boss team missing canonical label after normalization: team_id={team.get('team_id')}")
                ref_key = (game_version, boss_name)
                ref_row = boss_reference_by_key.get(ref_key)
            if ref_row is None:
                raise ValueError(
                    "Boss team was loaded without a matching Silver boss reference row: "
                    f"team_id={team.get('team_id')} game_version={game_version} boss_id={boss_id or '<missing>'}"
                )
            canonical_ref_key = (str(ref_row["game_version"]), str(ref_row["boss_name"]))
            team_variant = _normalized_text(team.get("team_variant")) or "default"
            team_starter_type = _resolve_starter_type(team.get("starter_type"), team.get("starter_condition"))
            variant_key = (team_variant, team_starter_type)
            variant_keys = boss_team_variant_keys_by_boss.setdefault(canonical_ref_key, set())
            if variant_key in variant_keys:
                raise ValueError(
                    "Duplicate boss teams detected for the same reference boss variant: "
                    f"game_version={game_version} boss_name={ref_row['boss_name']} "
                    f"variant={team_variant} starter_type={team_starter_type}"
                )
            variant_keys.add(variant_key)
            team["boss_role"] = ref_row["boss_role"]
            team["boss_order"] = ref_row["boss_order"]
            team["gym_index"] = ref_row.get("gym_index")
            team["starter_condition"] = ref_row.get("starter_condition")
            team["starter_type"] = team_starter_type or ref_row.get("starter_type")
            team["starter_dependency_type"] = ref_row.get("starter_dependency_type")
            team["has_team_variants"] = ref_row.get("has_team_variants")
            team["is_optional"] = _is_truthy_flag(ref_row.get("is_optional"))
            team["boss_id"] = ref_row.get("boss_id")
            team["boss_name"] = ref_row["boss_name"]
            team["gym"] = team.get("gym") or ref_row["boss_name"]
            if str(ref_row["boss_role"]) in {"elite_four", "champion"}:
                team["boss_sequence_id"] = _sequence_id_for_game_version(game_version)
            boss_team_presence_by_key.add(canonical_ref_key)

        if _is_player_candidate_team(team):
            target_boss_id = _target_boss_id_for_attacker(team)
            ref_row = boss_reference_by_id.get(target_boss_id) if target_boss_id else None
            if ref_row is None:
                target_boss = _target_label_for_attacker(team)
                if target_boss is None:
                    raise ValueError(f"Player team missing canonical target label after normalization: team_id={team.get('team_id')}")
                ref_key = (game_version, target_boss)
                ref_row = boss_reference_by_key.get(ref_key)
            if ref_row is None:
                raise ValueError(
                    "Player team targets a boss missing from Silver reference data: "
                    f"team_id={team.get('team_id')} game_version={game_version} target_boss_id={target_boss_id or '<missing>'}"
                )
            starter_base = _normalized_text(team.get("starter_base")) or None
            starter_type = _resolve_starter_type(team.get("starter_type"), starter_base)
            gym_index = _safe_int(ref_row.get("gym_index"), _safe_int(ref_row.get("boss_order"), 0))
            if starter_type and (
                str(ref_row.get("starter_dependency_type") or "none") == "branching"
                or ref_row.get("starter_type") is not None
                or ref_row.get("starter_condition") is not None
            ):
                conditional_group = [
                    row
                    for row in reference_rows_by_game.get(str(ref_row["game_version"]), [])
                    if _safe_int(row.get("gym_index"), _safe_int(row.get("boss_order"), 0)) == gym_index
                    and (
                        str(row.get("starter_dependency_type") or "none") == "branching"
                        or row.get("starter_type") is not None
                        or row.get("starter_condition") is not None
                    )
                ]
                matching = [
                    row
                    for row in conditional_group
                    if _resolve_starter_type(row.get("starter_type"), row.get("starter_condition")) == starter_type
                ]
                if len(matching) != 1:
                    raise ValueError(
                        "Exactly one conditional Striaton boss must match the player starter before simulation: "
                        f"team_id={team.get('team_id')} game_version={game_version} starter_type={starter_type} match_count={len(matching)}"
                    )
                selected = matching[0]
                if _safe_string(ref_row.get("boss_id")) != _safe_string(selected.get("boss_id")):
                    raise ValueError(
                        "Player team resolved to the wrong conditional boss before simulation: "
                        f"team_id={team.get('team_id')} starter_type={starter_type} "
                        f"expected_boss_id={selected.get('boss_id')} actual_boss_id={ref_row.get('boss_id')}"
                    )
                logger.info(
                    "[simulation] selected conditional gym leader team_id=%s starter_type=%s boss_id=%s boss_name=%s",
                    team.get("team_id"),
                    starter_type,
                    selected.get("boss_id"),
                    selected.get("boss_name"),
                )
            team["target_boss_role"] = ref_row["boss_role"]
            team["target_boss_order"] = ref_row["boss_order"]
            team["target_gym_index"] = ref_row.get("gym_index")
            team["target_starter_condition"] = ref_row.get("starter_condition")
            team["target_starter_type"] = ref_row.get("starter_type")
            team["target_is_optional"] = _is_truthy_flag(ref_row.get("is_optional"))
            team["starter_type"] = starter_type
            team["boss_id"] = ref_row.get("boss_id")
            team["boss_name"] = ref_row.get("boss_name")
            team["gym"] = ref_row.get("boss_name")
            if str(ref_row["boss_role"]) in {"elite_four", "champion"}:
                team["boss_sequence_id"] = _sequence_id_for_game_version(game_version)
                if not _is_truthy_flag(ref_row.get("is_optional")):
                    player_sequence_counts[(game_version, starter_type)] = player_sequence_counts.get((game_version, starter_type), 0) + 1

    missing_boss_teams = [
        f"{game_version}:{boss_name}"
        for (game_version, boss_name), row in sorted(boss_reference_by_canonical_key.items())
        if not _is_truthy_flag(row.get("is_optional"))
        if (game_version, boss_name) not in boss_team_presence_by_key
    ]
    if missing_boss_teams:
        raise ValueError(
            "Missing boss teams for Silver boss references: "
            f"count={len(missing_boss_teams)} examples={missing_boss_teams[:20]}"
        )

    for game_version, reference_rows in sorted(reference_rows_by_game.items()):
        game_rows = sorted(
            [row for row in reference_rows if not _is_truthy_flag(row.get("is_optional"))],
            key=lambda row: (int(row["boss_order"]), str(row["boss_name"])),
        )
        sequence_rows = [row for row in game_rows if str(row["boss_role"]) in {"elite_four", "champion"}]
        if not sequence_rows:
            continue

        champion_rows = [row for row in sequence_rows if str(row["boss_role"]) == "champion"]
        if not champion_rows:
            # Optional champions are intentionally ignored for gauntlet simulation.
            continue
        if len(champion_rows) != 1:
            raise ValueError(
                "Expected exactly one champion row in bosses.parquet for gauntlet simulation: "
                f"game_version={game_version} champion_count={len(champion_rows)}"
            )

        sequence_orders = [int(row["boss_order"]) for row in sequence_rows]
        sequence_unique_orders = sorted(set(sequence_orders))
        expected_sequence_unique_orders = list(
            range(sequence_unique_orders[0], sequence_unique_orders[-1] + 1)
        )
        if sequence_unique_orders != expected_sequence_unique_orders:
            raise ValueError(
                "Elite Four / Champion order is not contiguous in bosses.parquet (unique order blocks): "
                f"game_version={game_version} observed={sequence_unique_orders} expected={expected_sequence_unique_orders}"
            )

        full_unique_orders = sorted({int(row["boss_order"]) for row in game_rows})
        expected_tail = full_unique_orders[-len(sequence_unique_orders):]
        if sequence_unique_orders != expected_tail:
            raise ValueError(
                "Elite Four / Champion sequence must be the final ordered boss block for gauntlet simulation: "
                f"game_version={game_version} observed={sequence_unique_orders} expected_tail={expected_tail}"
            )

        max_order = max(sequence_orders)
        max_order_roles = {
            str(row.get("boss_role") or "")
            for row in sequence_rows
            if int(row.get("boss_order") or 0) == max_order
        }
        if max_order_roles != {"champion"}:
            raise ValueError(
                "Champion must be the final boss order block in the gauntlet sequence: "
                f"game_version={game_version} final_block_roles={sorted(max_order_roles)}"
            )

        missing_sequence_team_ids = [
            f"{game_version}:{row['boss_name']}"
            for row in sequence_rows
            if (game_version, str(row["boss_name"])) not in boss_team_presence_by_key
        ]
        if missing_sequence_team_ids:
            raise ValueError(
                "Elite Four / Champion boss teams missing after Silver reconstruction: "
                f"game_version={game_version} missing={missing_sequence_team_ids}"
            )

        if sum(
            count
            for (sequence_game_version, _starter_type), count in player_sequence_counts.items()
            if sequence_game_version == game_version
        ) == 0:
            raise ValueError(
                "No player teams available for Elite Four / Champion gauntlet simulation: "
                f"game_version={game_version}"
            )

    return teams_with_context


def _gauntlet_sequences_by_version(
    teams_with_id: list[dict[str, Any]],
) -> dict[tuple[str, str | None], list[dict[str, Any]]]:
    boss_rows_by_game: dict[str, list[dict[str, Any]]] = {}
    player_starter_types_by_game: dict[str, set[str | None]] = {}

    for team in teams_with_id:
        game_version = _normalized_game_version(cast(str | None, team.get("game_version")))
        if game_version is None:
            continue

        if _is_player_candidate_team(team) and _normalized_text(team.get("target_boss_role")) in {"elite_four", "champion"}:
            if _is_truthy_flag(team.get("target_is_optional")):
                continue
            player_starter_types_by_game.setdefault(game_version, set()).add(
                _resolve_starter_type(team.get("starter_type"), team.get("starter_base"))
            )

        boss_role = _normalized_text(team.get("boss_role"))
        if _is_boss_team(team) and boss_role in {"elite_four", "champion"} and not _is_truthy_flag(team.get("is_optional")):
            boss_rows_by_game.setdefault(game_version, []).append(team)

    sequences: dict[tuple[str, str | None], list[dict[str, Any]]] = {}
    for game_version, teams in boss_rows_by_game.items():
        if not any(_normalized_text(team.get("boss_role")) == "champion" for team in teams):
            # No non-optional champion means no mandatory gauntlet for this version.
            continue

        teams_by_boss_id: dict[str, list[dict[str, Any]]] = {}
        for team in teams:
            boss_id = _safe_string(team.get("boss_id"))
            if not boss_id:
                raise ValueError(
                    "Gauntlet boss team is missing boss_id after Silver enrichment: "
                    f"team_id={team.get('team_id')} game_version={game_version}"
                )
            teams_by_boss_id.setdefault(boss_id, []).append(team)

        starter_types = player_starter_types_by_game.get(game_version) or {None}
        for starter_type in starter_types:
            selected_sequence: list[dict[str, Any]] = []
            for boss_id, boss_teams in sorted(teams_by_boss_id.items()):
                variant_teams = [
                    team
                    for team in boss_teams
                    if _normalized_text(team.get("variant_dimension")) == "starter_type"
                ]
                if variant_teams:
                    if starter_type is None:
                        raise ValueError(
                            "Starter-dependent gauntlet boss variants require player starter_type resolution: "
                            f"game_version={game_version} boss_id={boss_id}"
                        )
                    matching_variant_teams = [
                        team
                        for team in variant_teams
                        if _resolve_starter_type(team.get("starter_type"), team.get("starter_condition")) == starter_type
                    ]
                    if len(matching_variant_teams) != 1:
                        raise ValueError(
                            "Exactly one gauntlet boss team variant must match the player starter_type: "
                            f"game_version={game_version} boss_id={boss_id} starter_type={starter_type} "
                            f"match_count={len(matching_variant_teams)}"
                        )
                    selected_sequence.append(matching_variant_teams[0])
                    continue

                if len(boss_teams) != 1:
                    raise ValueError(
                        "Non-variant gauntlet boss unexpectedly resolved to multiple teams: "
                        f"game_version={game_version} boss_id={boss_id} team_count={len(boss_teams)}"
                    )
                selected_sequence.append(boss_teams[0])

            selected_sequence.sort(key=lambda row: (_safe_int(row.get("boss_order"), 0), str(row.get("team_id") or "")))
            sequence_roles = [_normalized_text(team.get("boss_role")) for team in selected_sequence]
            if not selected_sequence or sequence_roles[-1] != "champion":
                raise ValueError(
                    "Resolved gauntlet sequence does not end with the champion boss team: "
                    f"game_version={game_version} starter_type={starter_type} roles={sequence_roles}"
                )
            sequences[(game_version, starter_type)] = selected_sequence

    return sequences


def _target_label_for_attacker(team: dict[str, Any]) -> str | None:
    return (
        _normalized_boss_label(team.get("gym"))
        or _normalized_boss_label(team.get("target_boss"))
        or _normalized_boss_label(team.get("boss_name"))
    )


def _target_label_for_defender(team: dict[str, Any]) -> str | None:
    return (
        _normalized_boss_label(team.get("gym"))
        or _normalized_boss_label(team.get("boss_name"))
        or _normalized_boss_label(team.get("target_boss"))
    )


def _target_boss_id_for_attacker(team: dict[str, Any]) -> str | None:
    boss_id = _safe_string(team.get("boss_id"))
    return boss_id or None


def _target_boss_id_for_defender(team: dict[str, Any]) -> str | None:
    boss_id = _safe_string(team.get("boss_id"))
    return boss_id or None


def _target_key_for_attacker(team: dict[str, Any]) -> str | None:
    return _target_boss_id_for_attacker(team) or _target_label_for_attacker(team)


def _target_key_for_defender(team: dict[str, Any]) -> str | None:
    return _target_boss_id_for_defender(team) or _target_label_for_defender(team)


def _validate_team_roles_for_simulation(teams_with_id: list[dict[str, Any]]) -> None:
    overlap = [
        team
        for team in teams_with_id
        if _is_player_candidate_team(team) and _is_boss_team(team)
    ]

    if overlap:
        examples = [str(team.get("team_id")) for team in overlap[:10]]
        raise ValueError(
            "Teams cannot be both player candidates and boss teams: "
            f"count={len(overlap)}, examples={examples}"
        )

    player_with_missing_target = [
        team
        for team in teams_with_id
        if _is_player_candidate_team(team) and _target_label_for_attacker(team) is None
    ]

    if player_with_missing_target:
        examples = [str(team.get("team_id")) for team in player_with_missing_target[:10]]
        raise ValueError(
            "Player candidate teams are missing simulation target labels: "
            f"count={len(player_with_missing_target)}, examples={examples}"
        )

    boss_with_missing_target = [
        team
        for team in teams_with_id
        if _is_boss_team(team) and _target_label_for_defender(team) is None
    ]

    if boss_with_missing_target:
        examples = [str(team.get("team_id")) for team in boss_with_missing_target[:10]]
        raise ValueError(
            "Boss teams are missing simulation target labels: "
            f"count={len(boss_with_missing_target)}, examples={examples}"
        )


def _log_team_role_diagnostics(teams_with_id: list[dict[str, Any]]) -> None:
    role_counts: dict[str, int] = {}
    origin_counts: dict[str, int] = {}

    for team in teams_with_id:
        role = _normalized_text(team.get("team_role")) or "<missing>"
        origin = _normalized_text(team.get("origin")) or "<missing>"
        role_counts[role] = role_counts.get(role, 0) + 1
        origin_counts[origin] = origin_counts.get(origin, 0) + 1

    overlap_count = sum(
        1 for team in teams_with_id if _is_player_candidate_team(team) and _is_boss_team(team)
    )

    logger.info(
        "[type_matchups] team role diagnostics roles=%s origins=%s overlap=%s",
        role_counts,
        origin_counts,
        overlap_count,
    )


def _is_intended_boss_matchup(attacker_team: dict[str, Any], defender_team: dict[str, Any]) -> bool:
    attacker_boss_id = _safe_string(attacker_team.get("boss_id"))
    defender_boss_id = _safe_string(defender_team.get("boss_id"))
    if attacker_boss_id and defender_boss_id and attacker_boss_id == defender_boss_id:
        variant_dimension = _normalized_text(defender_team.get("variant_dimension"))
        if variant_dimension == "starter_type":
            attacker_starter_type = _resolve_starter_type(attacker_team.get("starter_type"), attacker_team.get("starter_base"))
            defender_starter_type = _resolve_starter_type(defender_team.get("starter_type"), defender_team.get("starter_condition"))
            return attacker_starter_type == defender_starter_type
        return True

    attacker_target = _target_key_for_attacker(attacker_team)
    defender_target = _target_key_for_defender(defender_team)

    if attacker_target is None or defender_target is None:
        return False

    return attacker_target == defender_target


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
    attacker_bundle = _prepared_team_bundle(attacker_team, attacker_game_version)
    defender_bundle = _prepared_team_bundle(defender_team, defender_game_version)

    warnings = WarningCollector()
    for message in cast(list[str], attacker_bundle.get("warnings", [])) + cast(list[str], defender_bundle.get("warnings", [])):
        warnings.warn(message)

    attacker_profiles = _clone_team_profiles(attacker_bundle)
    defender_profiles = _clone_team_profiles(defender_bundle)

    return _simulate_team_battle_once_from_profiles(
        attacker_team=attacker_team,
        defender_team=defender_team,
        attacker_profiles=attacker_profiles,
        defender_profiles=defender_profiles,
        type_chart=type_chart,
        attacker_game_version=attacker_game_version,
        defender_game_version=defender_game_version,
        rng=rng,
        config=config,
        warnings=warnings,
    )


def _simulate_team_battle_once_from_profiles(
    attacker_team: dict[str, Any],
    defender_team: dict[str, Any],
    attacker_profiles: list[CombatProfile],
    defender_profiles: list[CombatProfile],
    type_chart: dict[str, dict[str, float]],
    attacker_game_version: str | None,
    defender_game_version: str | None,
    rng: random.Random,
    config: BattleSimulationConfig,
    warnings: WarningCollector,
) -> TeamBattleResult:
    if _team_battle_type(attacker_team) == "double" or _team_battle_type(defender_team) == "double":
        return _simulate_double_battle_once_from_profiles(
            attacker_team=attacker_team,
            defender_team=defender_team,
            attacker_profiles=attacker_profiles,
            defender_profiles=defender_profiles,
            type_chart=type_chart,
            attacker_game_version=attacker_game_version,
            defender_game_version=defender_game_version,
            rng=rng,
            config=config,
            warnings=warnings,
        )

    duel_summaries: list[dict[str, Any]] = []
    battle_turns = 0

    while True:
        attacker_idx = _first_alive_index(attacker_profiles)
        defender_idx = _first_alive_index(defender_profiles)

        if attacker_idx is None or defender_idx is None:
            break

        attacker_hp_before = int(attacker_profiles[attacker_idx]["current_hp"])
        defender_hp_before = int(defender_profiles[defender_idx]["current_hp"])

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

        attacker_hp_after = int(attacker_profiles[attacker_idx]["current_hp"])
        defender_hp_after = int(defender_profiles[defender_idx]["current_hp"])
        if attacker_hp_before == attacker_hp_after and defender_hp_before == defender_hp_after:
            duel_winner = str(duel.get("winner") or "attacker")
            if duel_winner == "attacker":
                defender_profiles[defender_idx]["current_hp"] = 0
            else:
                attacker_profiles[attacker_idx]["current_hp"] = 0
            warnings.warn(
                f"[simulation] {_STALEMATE_RESOLVED_WARNING} "
                f"attacker_team_id={attacker_team.get('team_id')} defender_team_id={defender_team.get('team_id')} "
                f"attacker_slot={attacker_idx + 1} defender_slot={defender_idx + 1} winner={duel_winner}"
            )

    attacker_remaining_pokemon = sum(1 for p in attacker_profiles if p["current_hp"] > 0)
    defender_remaining_pokemon = sum(1 for p in defender_profiles if p["current_hp"] > 0)
    attacker_total_remaining_hp = sum(p["current_hp"] for p in attacker_profiles)
    defender_total_remaining_hp = sum(p["current_hp"] for p in defender_profiles)

    attacker_win = attacker_remaining_pokemon > 0 and defender_remaining_pokemon == 0
    winner_team_id = attacker_team.get("team_id") if attacker_win else defender_team.get("team_id")

    simulation_score = _simulation_score(
        attacker_win,
        attacker_remaining_pokemon,
        defender_remaining_pokemon,
        attacker_total_remaining_hp,
        defender_total_remaining_hp,
    )

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
        "simulation_score": simulation_score,
        "warnings": warnings.all(),
        "duel_summaries": duel_summaries,
        "predicted_player_win_chance": 1.0 if attacker_win else 0.0,
        "attacker_wins": 1 if attacker_win else 0,
        "attacker_losses": 0 if attacker_win else 1,
        "n_trials": 1,
        "attacker_game_version": _normalized_game_version(attacker_game_version),
        "defender_game_version": _normalized_game_version(defender_game_version),
        "is_compatible_version": _is_version_compatible(attacker_game_version, defender_game_version, config),
        "representative_simulation_score": simulation_score,
        "representative_duel_summaries": duel_summaries,
        "representative_warnings": warnings.all(),
        "boss_sequence_id": None,
        "sequence_position": None,
        "remaining_team_state": _serialize_team_state(attacker_profiles),
        "gauntlet_success": attacker_win,
        "gauntlet_success_rate": None,
        "simulation_mode": "gym",
    }


def _filtered_result(
    attacker_team: dict[str, Any],
    defender_team: dict[str, Any],
    attacker_game_version: str | None,
    defender_game_version: str | None,
    warning: str,
    n_trials: int,
    compatible: bool,
) -> TeamBattleResult:
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
        "warnings": [warning],
        "duel_summaries": [],
        "predicted_player_win_chance": 0.0,
        "attacker_wins": 0,
        "attacker_losses": n_trials,
        "n_trials": n_trials,
        "attacker_game_version": _normalized_game_version(attacker_game_version),
        "defender_game_version": _normalized_game_version(defender_game_version),
        "is_compatible_version": compatible,
        "representative_simulation_score": -999.0,
        "representative_duel_summaries": [],
        "representative_warnings": [warning],
        "boss_sequence_id": None,
        "sequence_position": None,
        "remaining_team_state": [],
        "gauntlet_success": False,
        "gauntlet_success_rate": None,
        "simulation_mode": "gym",
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
        return _filtered_result(
            attacker_team,
            defender_team,
            attacker_game_version,
            defender_game_version,
            "incompatible_game_versions",
            n_trials,
            compatible=False,
        )

    if not _apply_level_plausibility_filter(attacker_team, defender_team, config):
        return _filtered_result(
            attacker_team,
            defender_team,
            attacker_game_version,
            defender_game_version,
            "level_plausibility_filter_failed",
            n_trials,
            compatible=True,
        )

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
        "boss_sequence_id": None,
        "sequence_position": None,
        "remaining_team_state": cast(list[dict[str, Any]], representative_result.get("remaining_team_state", [])),
        "gauntlet_success": attacker_win,
        "gauntlet_success_rate": None,
        "simulation_mode": "gym",
    }


def _untouched_defender_state(team: dict[str, Any], game_version: str | None) -> tuple[int, int]:
    bundle = _prepared_team_bundle(team, game_version)
    profiles = cast(list[CombatProfile], bundle["profiles"])
    return (
        sum(1 for profile in profiles if int(profile["current_hp"]) > 0),
        sum(int(profile["current_hp"]) for profile in profiles),
    )


def _gauntlet_placeholder_result(
    player_team: dict[str, Any],
    boss_team: dict[str, Any],
    attacker_game_version: str | None,
    defender_game_version: str | None,
    boss_sequence_id: str,
    sequence_position: int,
    n_trials: int,
    remaining_team_state: list[dict[str, Any]],
) -> TeamBattleResult:
    defender_remaining_pokemon, defender_remaining_hp = _untouched_defender_state(boss_team, defender_game_version)
    return {
        "team_id_attacker": player_team.get("team_id"),
        "team_id_defender": boss_team.get("team_id"),
        "attacker_win": False,
        "winner_team_id": boss_team.get("team_id"),
        "attacker_remaining_pokemon": 0,
        "defender_remaining_pokemon": defender_remaining_pokemon,
        "attacker_total_remaining_hp": 0,
        "defender_total_remaining_hp": defender_remaining_hp,
        "battle_turns": 0,
        "simulation_score": -999.0,
        "warnings": ["gauntlet_ended_before_sequence_position"],
        "duel_summaries": [],
        "predicted_player_win_chance": 0.0,
        "attacker_wins": 0,
        "attacker_losses": n_trials,
        "n_trials": n_trials,
        "attacker_game_version": _normalized_game_version(attacker_game_version),
        "defender_game_version": _normalized_game_version(defender_game_version),
        "is_compatible_version": _is_version_compatible(attacker_game_version, defender_game_version, config=BattleSimulationConfig()),
        "representative_simulation_score": -999.0,
        "representative_duel_summaries": [],
        "representative_warnings": ["gauntlet_ended_before_sequence_position"],
        "boss_sequence_id": boss_sequence_id,
        "sequence_position": sequence_position,
        "remaining_team_state": remaining_team_state,
        "gauntlet_success": False,
        "gauntlet_success_rate": 0.0,
        "simulation_mode": "gauntlet",
    }


def simulate_gauntlet(
    player_team: dict[str, Any],
    boss_teams: list[dict[str, Any]],
    type_chart: dict[str, dict[str, float]],
    attacker_game_version: str | None,
    n_trials: int,
    rng_seed: int,
    config: BattleSimulationConfig,
    boss_sequence_id: str,
) -> dict[str, Any]:
    if not boss_teams:
        raise ValueError("simulate_gauntlet requires at least one boss team")
    if not _team_members(player_team):
        raise ValueError(f"simulate_gauntlet requires a non-empty player team: team_id={player_team.get('team_id')}")

    player_version = _normalized_game_version(attacker_game_version)
    if player_version is None:
        raise ValueError(f"simulate_gauntlet requires attacker_game_version: team_id={player_team.get('team_id')}")

    player_bundle = _prepared_team_bundle(player_team, player_version)
    boss_bundles = [_prepared_team_bundle(team, player_version) for team in boss_teams]
    shared_warning_messages = cast(list[str], player_bundle.get("warnings", []))

    per_trial_rows: list[list[TeamBattleResult]] = []
    gauntlet_successes = 0

    for trial_idx in range(n_trials):
        rng = random.Random(rng_seed + trial_idx)
        player_profiles = _clone_team_profiles(player_bundle)
        trial_rows: list[TeamBattleResult] = []
        last_team_state = _serialize_team_state(player_profiles)

        for position, boss_team in enumerate(boss_teams, start=1):
            warnings = WarningCollector()
            for message in shared_warning_messages + cast(list[str], boss_bundles[position - 1].get("warnings", [])):
                warnings.warn(message)

            boss_profiles = _clone_team_profiles(boss_bundles[position - 1])
            boss_result = _simulate_team_battle_once_from_profiles(
                attacker_team=player_team,
                defender_team=boss_team,
                attacker_profiles=player_profiles,
                defender_profiles=boss_profiles,
                type_chart=type_chart,
                attacker_game_version=player_version,
                defender_game_version=player_version,
                rng=rng,
                config=config,
                warnings=warnings,
            )
            boss_result["boss_sequence_id"] = boss_sequence_id
            boss_result["sequence_position"] = position
            boss_result["remaining_team_state"] = _serialize_team_state(player_profiles)
            boss_result["gauntlet_success"] = position == len(boss_teams) and bool(boss_result["attacker_win"])
            boss_result["simulation_mode"] = "gauntlet"
            trial_rows.append(boss_result)
            last_team_state = cast(list[dict[str, Any]], boss_result["remaining_team_state"])

            if not boss_result["attacker_win"]:
                break

        if len(trial_rows) == len(boss_teams) and bool(trial_rows[-1]["attacker_win"]):
            gauntlet_successes += 1

        if len(trial_rows) < len(boss_teams):
            for position in range(len(trial_rows) + 1, len(boss_teams) + 1):
                trial_rows.append(
                    _gauntlet_placeholder_result(
                        player_team=player_team,
                        boss_team=boss_teams[position - 1],
                        attacker_game_version=player_version,
                        defender_game_version=player_version,
                        boss_sequence_id=boss_sequence_id,
                        sequence_position=position,
                        n_trials=1,
                        remaining_team_state=last_team_state,
                    )
                )

        per_trial_rows.append(trial_rows)

    aggregated_rows: list[TeamBattleResult] = []
    gauntlet_success_rate = round(gauntlet_successes / max(1, n_trials), 4)
    for position in range(len(boss_teams)):
        position_rows = [trial_rows[position] for trial_rows in per_trial_rows]
        wins = sum(1 for row in position_rows if bool(row["attacker_win"]))
        losses = max(0, n_trials - wins)
        representative_result = max(
            position_rows,
            key=lambda row: (
                1 if bool(row["attacker_win"]) else 0,
                float(row["simulation_score"]),
                float(row["attacker_total_remaining_hp"]),
            ),
        )
        boss_team = boss_teams[position]
        attacker_win = wins >= losses

        aggregated_rows.append(
            {
                "team_id_attacker": player_team.get("team_id"),
                "team_id_defender": boss_team.get("team_id"),
                "attacker_win": attacker_win,
                "winner_team_id": player_team.get("team_id") if attacker_win else boss_team.get("team_id"),
                "attacker_remaining_pokemon": round(sum(float(row["attacker_remaining_pokemon"]) for row in position_rows) / max(1, n_trials), 3),
                "defender_remaining_pokemon": round(sum(float(row["defender_remaining_pokemon"]) for row in position_rows) / max(1, n_trials), 3),
                "attacker_total_remaining_hp": round(sum(float(row["attacker_total_remaining_hp"]) for row in position_rows) / max(1, n_trials), 3),
                "defender_total_remaining_hp": round(sum(float(row["defender_total_remaining_hp"]) for row in position_rows) / max(1, n_trials), 3),
                "battle_turns": round(sum(float(row["battle_turns"]) for row in position_rows) / max(1, n_trials), 3),
                "simulation_score": round(sum(float(row["simulation_score"]) for row in position_rows) / max(1, n_trials), 3),
                "warnings": sorted({warning for row in position_rows for warning in cast(list[str], row.get("warnings", []))}),
                "duel_summaries": [],
                "predicted_player_win_chance": round(wins / max(1, n_trials), 4),
                "attacker_wins": wins,
                "attacker_losses": losses,
                "n_trials": n_trials,
                "attacker_game_version": player_version,
                "defender_game_version": player_version,
                "is_compatible_version": True,
                "representative_simulation_score": float(representative_result["simulation_score"]),
                "representative_duel_summaries": cast(list[dict[str, Any]], representative_result.get("duel_summaries", [])),
                "representative_warnings": cast(list[str], representative_result.get("warnings", [])),
                "boss_sequence_id": boss_sequence_id,
                "sequence_position": position + 1,
                "remaining_team_state": cast(list[dict[str, Any]], representative_result.get("remaining_team_state", [])),
                "gauntlet_success": position == len(boss_teams) - 1 and gauntlet_successes > (n_trials - gauntlet_successes),
                "gauntlet_success_rate": gauntlet_success_rate,
                "simulation_mode": "gauntlet",
            }
        )

    return {
        "boss_sequence_id": boss_sequence_id,
        "battle_rows": aggregated_rows,
        "gauntlet_success_rate": round(gauntlet_successes / max(1, n_trials), 4),
        "gauntlet_successes": gauntlet_successes,
        "n_trials": n_trials,
    }


def _run_local_simulations(
    teams_data: list[dict[str, Any]],
    type_chart: dict[str, dict[str, float]],
    config: BattleSimulationConfig,
) -> list[dict[str, Any]]:
    teams_with_id = [team for team in teams_data if team.get("team_id") is not None]
    _validate_team_roles_for_simulation(teams_with_id)
    _log_team_role_diagnostics(teams_with_id)

    attackers = [team for team in teams_with_id if _is_player_candidate_team(team)]
    single_attackers = [
        team
        for team in attackers
        if _normalized_text(team.get("target_boss_role")) in {"gym", "elite_four", "champion"}
        and not _is_truthy_flag(team.get("target_is_optional"))
    ]
    gauntlet_attackers = [
        team
        for team in attackers
        if _normalized_text(team.get("target_boss_role")) in {"elite_four", "champion"}
        and not _is_truthy_flag(team.get("target_is_optional"))
    ]
    single_defenders = [
        team
        for team in teams_with_id
        if _is_boss_team(team)
        and _normalized_text(team.get("boss_role")) in {"gym", "elite_four", "champion"}
        and not _is_truthy_flag(team.get("is_optional"))
    ]
    gauntlet_sequences = _gauntlet_sequences_by_version(teams_with_id)

    total_work_items = sum(
        1
        for attacker_team in single_attackers
        for defender_team in single_defenders
        if _is_intended_boss_matchup(attacker_team, defender_team)
        and _is_version_compatible(
            cast(str | None, attacker_team.get("game_version")),
            cast(str | None, defender_team.get("game_version")),
            config,
        )
    ) + len(gauntlet_attackers)

    logger.info(
        "[type_matchups] local engine start teams=%s single_attackers=%s single_defenders=%s gauntlet_attackers=%s work_items=%s",
        len(teams_with_id),
        len(single_attackers),
        len(single_defenders),
        len(gauntlet_attackers),
        total_work_items,
    )

    if total_work_items == 0:
        return []

    simulations: list[dict[str, Any]] = []
    started_at = time.perf_counter()
    items_done = 0
    progress = tqdm(total=total_work_items, desc="Local simulation", unit="match")

    for attacker_team in single_attackers:
        target_role = _normalized_text(attacker_team.get("target_boss_role")) or "gym"
        pair_mode = "gym" if target_role == "gym" else "boss"
        for defender_team in single_defenders:
            if not _is_intended_boss_matchup(attacker_team, defender_team):
                continue

            attacker_game_version = cast(str | None, attacker_team.get("game_version"))
            defender_game_version = cast(str | None, defender_team.get("game_version"))

            if not _is_version_compatible(attacker_game_version, defender_game_version, config):
                continue

            current_boss = _target_label_for_defender(defender_team) or _target_boss_id_for_defender(defender_team) or "unknown"
            progress.set_description(f"Local simulation [{current_boss}]", refresh=False)
            progress.set_postfix_str(f"attacker={attacker_team.get('team_id')}", refresh=False)

            result = simulate_team_battle(
                attacker_team=attacker_team,
                defender_team=defender_team,
                type_chart=type_chart,
                attacker_game_version=attacker_game_version,
                defender_game_version=defender_game_version,
                n_trials=config.n_battle_trials,
                rng_seed=_stable_pair_seed(
                    attacker_team.get("team_id"),
                    defender_team.get("team_id"),
                    config.rng_seed,
                ),
                config=config,
            )
            result["simulation_mode"] = pair_mode
            simulations.append(result)

            items_done += 1
            progress.update(1)

    for attacker_team in gauntlet_attackers:
        attacker_game_version = cast(str | None, attacker_team.get("game_version"))
        normalized_version = _normalized_game_version(attacker_game_version)
        if normalized_version is None:
            raise ValueError(f"Gauntlet attacker missing game_version: team_id={attacker_team.get('team_id')}")
        starter_type = _resolve_starter_type(attacker_team.get("starter_type"), attacker_team.get("starter_base"))
        boss_sequence = gauntlet_sequences.get((normalized_version, starter_type))
        if not boss_sequence:
            continue

        sequence_tail = _target_label_for_defender(boss_sequence[-1]) or _target_boss_id_for_defender(boss_sequence[-1]) or "champion"
        progress.set_description(f"Local gauntlet [{sequence_tail}]", refresh=False)
        progress.set_postfix_str(f"attacker={attacker_team.get('team_id')}", refresh=False)

        gauntlet_result = simulate_gauntlet(
            player_team=attacker_team,
            boss_teams=boss_sequence,
            type_chart=type_chart,
            attacker_game_version=normalized_version,
            n_trials=config.n_battle_trials,
            rng_seed=_stable_sequence_seed(
                attacker_team.get("team_id"),
                _sequence_id_for_game_version(normalized_version),
                config.rng_seed,
            ),
            config=config,
            boss_sequence_id=_sequence_id_for_game_version(normalized_version),
        )
        simulations.extend(cast(list[dict[str, Any]], gauntlet_result["battle_rows"]))

        items_done += 1
        progress.update(1)

    progress.close()

    logger.info(
        "[type_matchups] local engine done rows=%s items_done=%s/%s elapsed=%.2fs",
        len(simulations),
        items_done,
        total_work_items,
        time.perf_counter() - started_at,
    )
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
            T.StructField("boss_sequence_id", T.StringType(), True),
            T.StructField("sequence_position", T.IntegerType(), True),
            T.StructField("remaining_team_state", T.ArrayType(T.MapType(T.StringType(), T.StringType(), valueContainsNull=False), containsNull=False), False),
            T.StructField("gauntlet_success", T.BooleanType(), False),
            T.StructField("gauntlet_success_rate", T.DoubleType(), True),
            T.StructField("simulation_mode", T.StringType(), False),
        ]
    )


def _safe_float(value: Any, default: float = 0.0) -> float:
    return float(to_float(value, default=default, finite_only=True) or default)


def _safe_bool(value: Any) -> bool:
    return to_bool(value, default=False)


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


def _classify_outcome_cause(row: dict[str, Any]) -> str:
    mode = (_safe_string(row.get("simulation_mode")) or "gym").strip().lower()
    warnings = {
        str(item).strip().lower()
        for item in cast(list[Any], row.get("warnings", []))
        if item is not None and str(item).strip()
    }
    predicted_win = _safe_float(row.get("predicted_player_win_chance"), default=0.0)

    if predicted_win > 0.0:
        return "simulated_win"
    if "incompatible_game_versions" in warnings:
        return "version_filter"
    if "level_plausibility_filter_failed" in warnings:
        return "level_filter"
    if mode == "gauntlet" and "gauntlet_ended_before_sequence_position" in warnings:
        return "gauntlet_placeholder"
    return "simulated_loss"


def _normalize_result_row(row: dict[str, Any]) -> dict[str, Any] | None:
    attacker_id = _safe_string(row.get("team_id_attacker"))
    defender_id = _safe_string(row.get("team_id_defender"))

    if attacker_id is None or defender_id is None:
        return None

    warnings = [
        str(item)
        for item in cast(list[Any], row.get("warnings", []))
        if item is not None and str(item).strip()
    ]
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
        "boss_sequence_id": _safe_string(row.get("boss_sequence_id")),
        "sequence_position": int(_safe_float(row.get("sequence_position"), default=-1)) if row.get("sequence_position") is not None else None,
        "remaining_team_state": _normalize_summary_entries(row.get("remaining_team_state", [])),
        "gauntlet_success": _safe_bool(row.get("gauntlet_success")),
        "gauntlet_success_rate": _safe_float(row.get("gauntlet_success_rate"), default=0.0) if row.get("gauntlet_success_rate") is not None else None,
        "simulation_mode": _safe_string(row.get("simulation_mode")) or "gym",
        "outcome_cause": _safe_string(row.get("outcome_cause")) or _classify_outcome_cause(row),
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

        _validate_team_roles_for_simulation(teams_with_id)
        _log_team_role_diagnostics(teams_with_id)

        attacker_count = sum(1 for team in teams_with_id if _is_player_candidate_team(team))
        defender_count = sum(1 for team in teams_with_id if _is_boss_team(team))
        gauntlet_sequences = _gauntlet_sequences_by_version(teams_with_id)

        logger.info(
            "[type_matchups] spark engine preparing teams=%s attackers=%s defenders=%s",
            len(teams_with_id),
            attacker_count,
            defender_count,
        )

        if attacker_count == 0 or defender_count == 0:
            return []

        team_lookup: dict[str, dict[str, Any]] = {
            str(team.get("team_id")): team
            for team in teams_with_id
        }

        team_rows = [
            {
                "team_id": str(team.get("team_id")),
                "is_player_candidate": bool(_is_player_candidate_team(team)),
                "is_boss": bool(_is_boss_team(team)),
                "game_version": _normalized_game_version(cast(str | None, team.get("game_version"))),
                "starter_type": _resolve_starter_type(team.get("starter_type"), team.get("starter_base")),
                "is_optional": bool(_is_truthy_flag(team.get("is_optional"))),
                "target_is_optional": bool(_is_truthy_flag(team.get("target_is_optional"))),
                "attacker_target": _target_key_for_attacker(team),
                "defender_target": _target_key_for_defender(team),
                "target_boss_id": _safe_string(team.get("boss_id")),
                "boss_id": _safe_string(team.get("boss_id")),
                "target_boss_role": _normalized_text(team.get("target_boss_role")) or None,
                "boss_role": _normalized_text(team.get("boss_role")) or None,
                "variant_dimension": _normalized_text(team.get("variant_dimension")) or None,
                "team_role": _normalized_text(team.get("team_role")) or None,
                "origin": _normalized_text(team.get("origin")) or None,
            }
            for team in teams_with_id
        ]

        schema = T.StructType(
            [
                T.StructField("team_id", T.StringType(), False),
                T.StructField("is_player_candidate", T.BooleanType(), False),
                T.StructField("is_boss", T.BooleanType(), False),
                T.StructField("game_version", T.StringType(), True),
                T.StructField("starter_type", T.StringType(), True),
                T.StructField("is_optional", T.BooleanType(), False),
                T.StructField("target_is_optional", T.BooleanType(), False),
                T.StructField("attacker_target", T.StringType(), True),
                T.StructField("defender_target", T.StringType(), True),
                T.StructField("target_boss_id", T.StringType(), True),
                T.StructField("boss_id", T.StringType(), True),
                T.StructField("target_boss_role", T.StringType(), True),
                T.StructField("boss_role", T.StringType(), True),
                T.StructField("variant_dimension", T.StringType(), True),
                T.StructField("team_role", T.StringType(), True),
                T.StructField("origin", T.StringType(), True),
            ]
        )

        teams_df = spark.createDataFrame(team_rows, schema=schema)

        attackers_df = teams_df.where(F.col("is_player_candidate") == F.lit(True)).select(
            F.col("team_id").alias("attacker_id"),
            F.col("game_version").alias("game_version"),
            F.col("starter_type").alias("starter_type"),
            F.col("target_is_optional").alias("target_is_optional"),
            F.col("attacker_target").alias("target"),
            F.col("target_boss_id").alias("target_boss_id"),
            F.col("target_boss_role").alias("target_boss_role"),
        )

        defenders_df = teams_df.where(F.col("is_boss") == F.lit(True)).select(
            F.col("team_id").alias("defender_id"),
            F.col("game_version").alias("game_version"),
            F.col("is_optional").alias("is_optional"),
            F.col("defender_target").alias("target"),
            F.col("boss_id").alias("target_boss_id"),
            F.col("boss_role").alias("boss_role"),
            F.col("variant_dimension").alias("variant_dimension"),
            F.col("starter_type").alias("defender_starter_type"),
        )

        single_pairs_df = attackers_df.where(
            F.col("target_boss_role").isin(["gym", "elite_four", "champion"])
            & (F.col("target_is_optional") == F.lit(False))
        ).join(
            defenders_df.where(
                F.col("boss_role").isin(["gym", "elite_four", "champion"])
                & (F.col("is_optional") == F.lit(False))
            ),
            on=["game_version", "target_boss_id"],
            how="inner",
        )
        single_pairs_df = single_pairs_df.where(
            (
                F.col("variant_dimension").isNull()
                | (F.col("variant_dimension") != F.lit("starter_type"))
                | (F.col("starter_type") == F.col("defender_starter_type"))
            )
        ).withColumn(
            "simulation_mode",
            F.when(F.col("target_boss_role") == F.lit("gym"), F.lit("gym")).otherwise(F.lit("boss")),
        )

        single_pairs_df = single_pairs_df.where(
            F.col("game_version").isNotNull()
            & F.col("target_boss_id").isNotNull()
            & F.col("attacker_id").isNotNull()
            & F.col("defender_id").isNotNull()
        )
        pair_bucket_count = 8
        single_pairs_df = single_pairs_df.withColumn(
            "pair_bucket",
            F.pmod(F.hash(F.col("attacker_id")), F.lit(pair_bucket_count)),
        )
        gauntlet_attackers_df = (
            attackers_df.where(
                F.col("target_boss_role").isin(["elite_four", "champion"])
                & (F.col("target_is_optional") == F.lit(False))
            )
            .select("attacker_id", "game_version", "starter_type")
            .where(F.col("game_version").isNotNull() & F.col("attacker_id").isNotNull())
            .dropDuplicates(["attacker_id", "starter_type"])
        )

        if gauntlet_sequences:
            sequence_key_rows = [
                {
                    "game_version": version,
                    "starter_type": starter_type,
                }
                for (version, starter_type) in gauntlet_sequences.keys()
            ]
            sequence_key_schema = T.StructType(
                [
                    T.StructField("game_version", T.StringType(), False),
                    T.StructField("starter_type", T.StringType(), True),
                ]
            )
            sequence_keys_df = spark.createDataFrame(sequence_key_rows, schema=sequence_key_schema)
            gauntlet_attackers_df = gauntlet_attackers_df.join(
                sequence_keys_df,
                on=["game_version", "starter_type"],
                how="inner",
            )
        else:
            gauntlet_attackers_df = gauntlet_attackers_df.limit(0)

        single_pairs_df = single_pairs_df.persist()
        gauntlet_attackers_df = gauntlet_attackers_df.persist()
        total_pairs = int(single_pairs_df.count())
        total_gauntlet_attackers = int(gauntlet_attackers_df.count())

        if total_pairs == 0 and total_gauntlet_attackers == 0:
            single_pairs_df.unpersist()
            gauntlet_attackers_df.unpersist()
            return []

        group_counts_df = single_pairs_df.groupBy("game_version", "target_boss_id").count()
        group_count = int(group_counts_df.count()) if total_pairs > 0 else 0


        gym_partitions = max(4, min(128, total_pairs // 1000 + 1)) if total_pairs > 0 else 1
        gauntlet_partitions = max(4, min(128, total_gauntlet_attackers // 250 + 1)) if total_gauntlet_attackers > 0 else 1
        if total_pairs > 0:
            single_pairs_df = single_pairs_df.repartition(
                gym_partitions,
                "game_version",
                "target_boss_id",
                "starter_type",
                "pair_bucket",
            )
        if total_gauntlet_attackers > 0:
            gauntlet_attackers_df = gauntlet_attackers_df.repartition(gauntlet_partitions, "game_version")

        logger.info(
            "[type_matchups] spark groups game_boss_groups=%s eligible_single_pairs=%s gauntlet_attackers=%s single_partitions=%s gauntlet_partitions=%s pair_bucket_count=%s attackers=%s defenders=%s",
            group_count,
            total_pairs,
            total_gauntlet_attackers,
            gym_partitions,
            gauntlet_partitions,
            pair_bucket_count,
            attacker_count,
            defender_count,
        )

        team_lookup_bc = spark.sparkContext.broadcast(team_lookup)
        pokemon_profiles_bc = spark.sparkContext.broadcast(_LOCAL_POKEMON_PROFILES)
        move_profiles_bc = spark.sparkContext.broadcast(_LOCAL_MOVE_PROFILES)
        chart_bc = spark.sparkContext.broadcast(type_chart)
        config_bc = spark.sparkContext.broadcast(asdict(config))
        gauntlet_sequences_bc = spark.sparkContext.broadcast(
            {
                (version, starter_type): {
                    "boss_sequence_id": _sequence_id_for_game_version(version),
                    "boss_team_ids": [str(team.get("team_id")) for team in teams],
                }
                for (version, starter_type), teams in gauntlet_sequences.items()
            }
        )

        def _simulate_single_partition(rows: Any) -> Any:
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
                    normalized["simulation_mode"] = str(row.simulation_mode or "gym")
                    yield normalized

        def _simulate_gauntlet_partition(rows: Any) -> Any:
            local_teams = cast(dict[str, dict[str, Any]], team_lookup_bc.value)
            local_pokemon_profiles = cast(dict[str, dict[str, Any]], pokemon_profiles_bc.value)
            local_move_profiles = cast(dict[str, MoveProfile], move_profiles_bc.value)
            local_chart = cast(dict[str, dict[str, float]], chart_bc.value)
            local_config = BattleSimulationConfig(**cast(dict[str, Any], config_bc.value))
            local_sequences = cast(dict[str, dict[str, Any]], gauntlet_sequences_bc.value)

            _install_reference_profiles(local_pokemon_profiles, local_move_profiles)

            for row in rows:
                attacker_team = local_teams[str(row.attacker_id)]
                game_version = cast(str | None, row.game_version)
                if game_version is None:
                    raise ValueError(f"Gauntlet partition row missing game_version for attacker_id={row.attacker_id}")
                sequence_payload = local_sequences.get((game_version, cast(str | None, row.starter_type)))
                if sequence_payload is None:
                    raise ValueError(
                        "Missing gauntlet sequence payload for game_version/starter_type: "
                        f"game_version={game_version} starter_type={row.starter_type}"
                    )

                boss_teams = [local_teams[team_id] for team_id in cast(list[str], sequence_payload["boss_team_ids"])]
                result = simulate_gauntlet(
                    player_team=attacker_team,
                    boss_teams=boss_teams,
                    type_chart=local_chart,
                    attacker_game_version=game_version,
                    n_trials=local_config.n_battle_trials,
                    rng_seed=_stable_sequence_seed(
                        attacker_team.get("team_id"),
                        str(sequence_payload["boss_sequence_id"]),
                        local_config.rng_seed,
                    ),
                    config=local_config,
                    boss_sequence_id=str(sequence_payload["boss_sequence_id"]),
                )
                for battle_row in cast(list[dict[str, Any]], result["battle_rows"]):
                    normalized = _normalize_result_row(battle_row)
                    if normalized is not None:
                        yield normalized

        gym_rows = [row for row in single_pairs_df.rdd.mapPartitions(_simulate_single_partition).collect() if row] if total_pairs > 0 else []
        gauntlet_rows = [
            row for row in gauntlet_attackers_df.rdd.mapPartitions(_simulate_gauntlet_partition).collect() if row
        ] if total_gauntlet_attackers > 0 else []
        result_rows = gym_rows + gauntlet_rows
        single_pairs_df.unpersist()
        gauntlet_attackers_df.unpersist()

        if not result_rows:
            return []

        return sorted(
            result_rows,
            key=lambda row: (
                str(row.get("team_id_attacker") or ""),
                str(row.get("team_id_defender") or ""),
            ),
        )

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
    output_dir: Path = GOLD_DIR,
    bronze_dir: Path = BRONZE_DIR,
    simulation_dirname: str = GOLD_SIMULATION_DIRNAME,
    force_spark: bool | None = None,
    runtime_config: BattleSimulationConfig | None = None,
) -> None:
    started_at = time.perf_counter()
    type_chart = load_type_chart(bronze_dir)
    simulation_dir = output_dir / simulation_dirname
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
        write_parquet(simulation_dir / "boss_fight_counts.parquet", [])
        return

    filtered_teams = _enrich_teams_with_boss_context(filtered_teams, silver_dir)

    use_spark = _should_use_spark() if force_spark is None else bool(force_spark)
    logger.info("[type_matchups] engine=%s", "spark" if use_spark else "local")

    if use_spark:
        simulations = _run_spark_simulations(filtered_teams, type_chart, config)
    else:
        simulations = _run_local_simulations(filtered_teams, type_chart, config)

    incompatible_rows = [row for row in simulations if not bool(row.get("is_compatible_version", False))]
    if incompatible_rows:
        raise ValueError("Incompatible cross-version simulation rows detected; strict compatibility contract violated")

    aggregate_rows: list[dict[str, Any]] = []
    diagnostic_rows: list[dict[str, Any]] = []
    boss_fight_counts: dict[str, int] = {}
    boss_team_context: dict[str, dict[str, Any]] = {
        str(team.get("team_id")): {
            "game_version": _normalized_game_version(cast(str | None, team.get("game_version"))),
            "boss_id": _safe_string(team.get("boss_id")),
            "boss_name": _safe_string(team.get("boss_name")) or _safe_string(team.get("gym")),
            "boss_role": _safe_string(team.get("boss_role")),
        }
        for team in filtered_teams
        if _is_boss_team(team) and team.get("team_id") is not None
    }

    for row in simulations:
        outcome_cause = _classify_outcome_cause(row)
        aggregate_rows.append(
            {
                key: value
                for key, value in row.items()
                if key
                not in {
                    "representative_simulation_score",
                    "representative_duel_summaries",
                    "representative_warnings",
                }
            }
        )
        aggregate_rows[-1]["outcome_cause"] = outcome_cause
        diagnostic_rows.append(
            {
                "team_id_attacker": row.get("team_id_attacker"),
                "team_id_defender": row.get("team_id_defender"),
                "boss_sequence_id": row.get("boss_sequence_id"),
                "sequence_position": row.get("sequence_position"),
                "remaining_team_state": row.get("remaining_team_state", []),
                "gauntlet_success": row.get("gauntlet_success"),
                "simulation_mode": row.get("simulation_mode"),
                "outcome_cause": outcome_cause,
                "representative_simulation_score": row.get("representative_simulation_score"),
                "representative_duel_summaries": row.get("representative_duel_summaries", []),
                "representative_warnings": row.get("representative_warnings", []),
            }
        )
        defender_id = _safe_string(row.get("team_id_defender"))
        if defender_id is not None:
            boss_fight_counts[defender_id] = boss_fight_counts.get(defender_id, 0) + 1

    boss_fight_count_rows = [
        {
            "team_id_defender": defender_id,
            "game_version": (boss_team_context.get(defender_id) or {}).get("game_version"),
            "boss_id": (boss_team_context.get(defender_id) or {}).get("boss_id"),
            "boss_name": (boss_team_context.get(defender_id) or {}).get("boss_name"),
            "boss_role": (boss_team_context.get(defender_id) or {}).get("boss_role"),
            "fight_count": fight_count,
        }
        for defender_id, fight_count in sorted(boss_fight_counts.items(), key=lambda item: (-item[1], item[0]))
    ]

    write_parquet(simulation_dir / "team_battle_simulations.parquet", aggregate_rows)
    write_parquet(simulation_dir / "team_battle_simulation_diagnostics.parquet", diagnostic_rows)
    write_parquet(simulation_dir / "boss_fight_counts.parquet", boss_fight_count_rows)

    logger.info(
        "[type_matchups] wrote team_battle_simulations rows=%s diagnostics rows=%s boss_fight_counts rows=%s elapsed_s=%.2f",
        len(aggregate_rows),
        len(diagnostic_rows),
        len(boss_fight_count_rows),
        time.perf_counter() - started_at,
    )


def build_type_matchups(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    output_dir: Path = GOLD_DIR,
    bronze_dir: Path = BRONZE_DIR,
    simulation_dirname: str = GOLD_SIMULATION_DIRNAME,
) -> None:
    build_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=silver_dir,
        output_dir=output_dir,
        bronze_dir=bronze_dir,
        simulation_dirname=simulation_dirname,
    )
