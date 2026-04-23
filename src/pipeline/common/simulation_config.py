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
class RuntimeBattlePolicyConfig:
    allow_cross_version_fallback: bool = False
    damage_random_min: float = 0.85
    damage_random_max: float = 1.00
    crit_chance_default: float = 1 / 24
    n_battle_trials: int = 15
    rng_seed: int = 42
    fail_on_degraded_data: bool = True


def load_runtime_battle_policy_config() -> RuntimeBattlePolicyConfig:
    return RuntimeBattlePolicyConfig(
        allow_cross_version_fallback=_env_bool("PM4_SIM_ALLOW_CROSS_VERSION_FALLBACK", False),
        damage_random_min=_env_float("PM4_SIM_DAMAGE_RANDOM_MIN", 0.85),
        damage_random_max=_env_float("PM4_SIM_DAMAGE_RANDOM_MAX", 1.00),
        crit_chance_default=_env_float("PM4_SIM_CRIT_CHANCE_DEFAULT", 1 / 24),
        n_battle_trials=_env_int("PM4_SIM_BATTLE_TRIALS", 15),
        rng_seed=_env_int("PM4_SIM_RNG_SEED", 42),
        fail_on_degraded_data=_env_bool("PM4_SIM_FAIL_ON_DEGRADED_DATA", True),
    )
