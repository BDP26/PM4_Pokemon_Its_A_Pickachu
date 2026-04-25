"""Compatibility shim for moved Gold Monte Carlo optimizer."""

from __future__ import annotations

import logging
from pathlib import Path

from src.pipeline.gold.simulation.monte_carlo_optimizer import run_monte_carlo_team_optimizer as _gold_run
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME

logger = logging.getLogger(__name__)


def run_monte_carlo_team_optimizer(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> int:
    logger.warning("[gold/simulation] deprecated silver simulation path used")
    return _gold_run(
        gold_dir=silver_dir,
        simulation_dirname=simulation_dirname,
        silver_dir=silver_dir,
        n_trials=n_trials,
        rng_seed=rng_seed,
    )
