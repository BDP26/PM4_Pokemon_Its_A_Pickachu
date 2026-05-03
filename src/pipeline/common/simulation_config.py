from __future__ import annotations

from dataclasses import dataclass
from src.pipeline.common.env import env_float, env_int, env_text


@dataclass(frozen=True)
class RuntimeBattlePolicyConfig:
    profile: str = "strict"
    damage_random_min: float = 0.85
    damage_random_max: float = 1.00
    crit_chance_default: float = 1 / 24
    n_battle_trials: int = 500
    rng_seed: int = 42


def load_runtime_battle_policy_config() -> RuntimeBattlePolicyConfig:
    defaults = RuntimeBattlePolicyConfig()
    profile = env_text("PM4_SIM_POLICY_PROFILE", defaults.profile)
    if profile != defaults.profile:
        profile = defaults.profile

    return RuntimeBattlePolicyConfig(
        profile=profile,
        damage_random_min=env_float("PM4_SIM_DAMAGE_RANDOM_MIN", defaults.damage_random_min),
        damage_random_max=env_float("PM4_SIM_DAMAGE_RANDOM_MAX", defaults.damage_random_max),
        crit_chance_default=env_float("PM4_SIM_CRIT_CHANCE_DEFAULT", defaults.crit_chance_default),
        n_battle_trials=env_int("PM4_SIM_BATTLE_TRIALS", defaults.n_battle_trials),
        rng_seed=env_int("PM4_SIM_RNG_SEED", defaults.rng_seed),
    )
