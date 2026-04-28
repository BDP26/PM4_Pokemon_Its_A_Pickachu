"""Compatibility wrapper for the relocated Monte Carlo optimizer module."""

from __future__ import annotations

from pathlib import Path

from src.pipeline.gold.simulation import monte_carlo_optimizer as _impl
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


def run_monte_carlo_team_optimizer(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    n_trials: int = 500,
    rng_seed: int = 42,
    gold_dir: Path | None = None,
) -> int:
    return _impl.run_monte_carlo_team_optimizer(
        gold_dir=silver_dir if gold_dir is None else gold_dir,
        simulation_dirname=simulation_dirname,
        silver_dir=silver_dir,
        n_trials=n_trials,
        rng_seed=rng_seed,
    )
