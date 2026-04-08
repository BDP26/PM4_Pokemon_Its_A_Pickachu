from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.gold.inputs.team_tables import load_reconstructed_teams_from_silver
from src.pipeline.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    GOLD_SIMULATION_DIRNAME,
    SILVER_DIR,
    SILVER_SIMULATION_DIRNAME,
)
from src.pipeline.silver.simulation.battle_seeds import build_battle_seeds
from src.pipeline.silver.simulation.monte_carlo_optimizer import run_monte_carlo_team_optimizer
from src.pipeline.silver.simulation.type_matchups import build_team_battle_simulations


logger = logging.getLogger(__name__)


def run_gold_simulation_from_silver(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    bronze_dir: Path = BRONZE_DIR,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> None:
    """Run full battle simulation pipeline in gold using silver prepared team inputs."""
    started_at = time.perf_counter()
    silver_simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME
    gold_simulation_dir = gold_dir / GOLD_SIMULATION_DIRNAME
    gold_simulation_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[gold/simulation] start silver_dir=%s gold_dir=%s", silver_dir, gold_dir)

    reconstructed_teams = load_reconstructed_teams_from_silver(
        silver_dir=silver_dir,
        simulation_dirname=SILVER_SIMULATION_DIRNAME,
    )
    teams_df = pd.DataFrame(reconstructed_teams)
    if teams_df.empty:
        logger.warning("[gold/simulation] reconstructed team dataset is empty; skipping simulation")
        return

    teams_data = cast(list[dict[str, Any]], teams_df.to_dict(orient="records"))
    write_parquet(gold_simulation_dir / "teams.parquet", teams_data)
    logger.info("[gold/simulation] loaded teams count=%s", len(teams_data))

    sims_started_at = time.perf_counter()
    logger.info("[gold/simulation] running team battle simulations with pyspark")
    build_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=gold_dir,
        bronze_dir=bronze_dir,
        force_spark=True,
    )
    logger.info("[gold/simulation] team battle simulations done elapsed_s=%.2f", time.perf_counter() - sims_started_at)

    seeds_started_at = time.perf_counter()
    logger.info("[gold/simulation] building battle seeds")
    build_battle_seeds(
        silver_dir=gold_dir,
        simulation_dirname=GOLD_SIMULATION_DIRNAME,
    )
    logger.info("[gold/simulation] battle seeds done elapsed_s=%.2f", time.perf_counter() - seeds_started_at)

    mc_started_at = time.perf_counter()
    logger.info("[gold/simulation] running monte carlo optimizer trials=%s seed=%s", n_trials, rng_seed)
    run_monte_carlo_team_optimizer(
        silver_dir=gold_dir,
        simulation_dirname=GOLD_SIMULATION_DIRNAME,
        n_trials=n_trials,
        rng_seed=rng_seed,
    )
    logger.info("[gold/simulation] monte carlo optimizer done elapsed_s=%.2f", time.perf_counter() - mc_started_at)
    logger.info("[gold/simulation] finished elapsed_s=%.2f", time.perf_counter() - started_at)


