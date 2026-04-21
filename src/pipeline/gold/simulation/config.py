from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


@dataclass(frozen=True)
class BattleSimulationConfig:
    enable_accuracy: bool = True
    enable_damage_randomness: bool = True
    enable_critical_hits: bool = True
    enable_basic_switching: bool = True

    strict_legal_moves_only: bool = True
    allow_cross_version_fallback: bool = False
    allow_non_level_learn_fallback: bool = False
    allow_struggle_fallback: bool = True

    damage_random_min: float = 0.85
    damage_random_max: float = 1.00
    crit_chance_default: float = 1 / 24

    switch_if_damage_ratio_below: float = 0.20
    switch_if_incoming_ko_likely: bool = True

    n_battle_trials: int = 15
    rng_seed: int = 42


def load_battle_simulation_config() -> BattleSimulationConfig:
    return BattleSimulationConfig(
        enable_accuracy=_env_bool("PM4_SIM_ENABLE_ACCURACY", True),
        enable_damage_randomness=_env_bool("PM4_SIM_ENABLE_DAMAGE_RANDOMNESS", True),
        enable_critical_hits=_env_bool("PM4_SIM_ENABLE_CRITICAL_HITS", True),
        enable_basic_switching=_env_bool("PM4_SIM_ENABLE_BASIC_SWITCHING", True),
        strict_legal_moves_only=_env_bool("PM4_SIM_STRICT_LEGAL_MOVES_ONLY", True),
        allow_cross_version_fallback=_env_bool("PM4_SIM_ALLOW_CROSS_VERSION_FALLBACK", False),
        allow_non_level_learn_fallback=_env_bool("PM4_SIM_ALLOW_NON_LEVEL_LEARN_FALLBACK", False),
        allow_struggle_fallback=_env_bool("PM4_SIM_ALLOW_STRUGGLE_FALLBACK", True),
        damage_random_min=_env_float("PM4_SIM_DAMAGE_RANDOM_MIN", 0.85),
        damage_random_max=_env_float("PM4_SIM_DAMAGE_RANDOM_MAX", 1.00),
        crit_chance_default=_env_float("PM4_SIM_CRIT_CHANCE_DEFAULT", 1 / 24),
        switch_if_damage_ratio_below=_env_float("PM4_SIM_SWITCH_IF_DAMAGE_RATIO_BELOW", 0.20),
        switch_if_incoming_ko_likely=_env_bool("PM4_SIM_SWITCH_IF_INCOMING_KO_LIKELY", True),
        n_battle_trials=_env_int("PM4_SIM_BATTLE_TRIALS", 15),
        rng_seed=_env_int("PM4_SIM_RNG_SEED", 42),
    )