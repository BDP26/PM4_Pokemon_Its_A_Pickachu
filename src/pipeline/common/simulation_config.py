from __future__ import annotations

from dataclasses import dataclass
from src.pipeline.common.env import env_bool, env_float, env_int, env_text


_STRICT_DEFAULTS: dict[str, float | int | bool] = {
    "allow_cross_version_fallback": False,
    "damage_random_min": 0.85,
    "damage_random_max": 1.00,
    "crit_chance_default": 1 / 24,
    "n_battle_trials": 15,
    "rng_seed": 42,
    "fail_on_degraded_data": True,
}


@dataclass(frozen=True)
class RuntimeBattlePolicyConfig:
    profile: str = "strict"
    allow_cross_version_fallback: bool = False
    damage_random_min: float = 0.85
    damage_random_max: float = 1.00
    crit_chance_default: float = 1 / 24
    n_battle_trials: int = 15
    rng_seed: int = 42
    fail_on_degraded_data: bool = True


def load_runtime_battle_policy_config() -> RuntimeBattlePolicyConfig:
    profile = env_text("PM4_SIM_POLICY_PROFILE", "strict")
    if profile != "strict":
        profile = "strict"
    defaults = _STRICT_DEFAULTS
    return RuntimeBattlePolicyConfig(
        profile=profile,
        allow_cross_version_fallback=env_bool(
            "PM4_SIM_ALLOW_CROSS_VERSION_FALLBACK",
            bool(defaults["allow_cross_version_fallback"]),
        ),
        damage_random_min=env_float("PM4_SIM_DAMAGE_RANDOM_MIN", float(defaults["damage_random_min"])),
        damage_random_max=env_float("PM4_SIM_DAMAGE_RANDOM_MAX", float(defaults["damage_random_max"])),
        crit_chance_default=env_float("PM4_SIM_CRIT_CHANCE_DEFAULT", float(defaults["crit_chance_default"])),
        n_battle_trials=env_int("PM4_SIM_BATTLE_TRIALS", int(defaults["n_battle_trials"])),
        rng_seed=env_int("PM4_SIM_RNG_SEED", int(defaults["rng_seed"])),
        fail_on_degraded_data=env_bool("PM4_SIM_FAIL_ON_DEGRADED_DATA", bool(defaults["fail_on_degraded_data"])),
    )
