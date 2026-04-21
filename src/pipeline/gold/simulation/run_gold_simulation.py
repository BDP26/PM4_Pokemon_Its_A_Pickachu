from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.gold.inputs.team_tables import load_reconstructed_teams_from_silver
from src.pipeline.gold.simulation.config import BattleSimulationConfig, load_battle_simulation_config
from src.pipeline.settings import (
    BRONZE_DIR,
    GOLD_DIR,
    GOLD_SIMULATION_DIRNAME,
    SILVER_DIR,
    SILVER_SIMULATION_DIRNAME,
)
from src.pipeline.silver.simulation.battle_seeds import build_battle_seeds
from src.pipeline.silver.writers.outputs import write_simulation_run_metadata
from src.pipeline.silver.simulation.monte_carlo_optimizer import run_monte_carlo_team_optimizer
from src.pipeline.silver.simulation.type_matchups import build_team_battle_simulations
from src.pipeline.silver.validation.simulation_outputs import validate_team_battle_simulations
from src.pipeline.silver.validation.team_tables import validate_reconstructed_teams


logger = logging.getLogger(__name__)


def _run_gold_team_battle_simulations(
    *,
    teams_data: list[dict[str, Any]],
    gold_dir: Path,
    bronze_dir: Path,
    config: BattleSimulationConfig,
) -> None:
    build_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=gold_dir,
        bronze_dir=bronze_dir,
        force_spark=True,
        config=config,
    )


def _build_gold_battle_seeds(*, gold_dir: Path) -> None:
    build_battle_seeds(
        silver_dir=gold_dir,
        simulation_dirname=GOLD_SIMULATION_DIRNAME,
    )


def _run_gold_monte_carlo_optimizer(*, gold_dir: Path, n_trials: int, rng_seed: int) -> None:
    run_monte_carlo_team_optimizer(
        silver_dir=gold_dir,
        simulation_dirname=GOLD_SIMULATION_DIRNAME,
        n_trials=n_trials,
        rng_seed=rng_seed,
    )


def run_gold_simulation_from_silver(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
    bronze_dir: Path = BRONZE_DIR,
    required_input_files: dict[str, Path] | None = None,
    n_trials: int = 500,
    rng_seed: int = 42,
    sim_config: BattleSimulationConfig | None = None,
) -> None:
    started_at = time.perf_counter()
    silver_simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME
    gold_simulation_dir = gold_dir / GOLD_SIMULATION_DIRNAME
    gold_simulation_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[gold/simulation] start silver_dir=%s gold_dir=%s", silver_dir, gold_dir)

    config = sim_config or load_battle_simulation_config()

    teams_path = required_input_files.get("teams") if required_input_files else (silver_simulation_dir / "teams.parquet")
    team_members_path = (
        required_input_files.get("team_members") if required_input_files else (silver_simulation_dir / "team_members.parquet")
    )
    team_member_moves_path = (
        required_input_files.get("team_member_moves")
        if required_input_files
        else (silver_simulation_dir / "team_member_moves.parquet")
    )

    loader_kwargs: dict[str, Any] = {
        "silver_dir": silver_dir,
        "simulation_dirname": SILVER_SIMULATION_DIRNAME,
        "teams_path": teams_path,
        "team_members_path": team_members_path,
        "team_member_moves_path": team_member_moves_path,
    }
    reconstructed_teams = load_reconstructed_teams_from_silver(**loader_kwargs)
    teams_df = pd.DataFrame(reconstructed_teams)
    if teams_df.empty:
        logger.warning("[gold/simulation] reconstructed team dataset is empty; skipping simulation")
        return

    teams_data = cast(list[dict[str, Any]], teams_df.to_dict(orient="records"))

    if config.validate_reconstructed_teams:
        validate_reconstructed_teams(
            teams=teams_data,
            strict=config.fail_on_validation_errors,
        )

    write_parquet(gold_simulation_dir / "teams.parquet", teams_data)
    logger.info("[gold/simulation] loaded teams count=%s", len(teams_data))

    write_simulation_run_metadata(
        gold_simulation_dir,
        engine="spark",
        config=config,
        extra={
            "stage": "gold_simulation",
            "input_team_count": len(teams_data),
            "mc_trials": n_trials,
            "mc_rng_seed": rng_seed,
        },
    )

    sims_started_at = time.perf_counter()
    logger.info(
        "[gold/simulation] running team battle simulations with pyspark battle_trials=%s sim_seed=%s",
        config.n_battle_trials,
        config.rng_seed,
    )
    _run_gold_team_battle_simulations(
        teams_data=teams_data,
        gold_dir=gold_dir,
        bronze_dir=bronze_dir,
        config=config,
    )
    logger.info("[gold/simulation] team battle simulations done elapsed_s=%.2f", time.perf_counter() - sims_started_at)

    simulations_path = gold_simulation_dir / "team_battle_simulations.parquet"
    if simulations_path.exists() and config.validate_simulation_outputs:
        simulation_rows = read_parquet(simulations_path).to_dict(orient="records")
        validate_team_battle_simulations(
            rows=cast(list[dict[str, Any]], simulation_rows),
            strict=config.fail_on_validation_errors,
        )
        if config.fail_on_degraded_data:
            degraded_count = sum(bool(row.get("degraded_data", False)) for row in simulation_rows)
            if degraded_count > 0:
                raise ValueError(f"Simulation output contains degraded rows count={degraded_count}")

    seeds_started_at = time.perf_counter()
    logger.info("[gold/simulation] building battle seeds")
    _build_gold_battle_seeds(gold_dir=gold_dir)
    logger.info("[gold/simulation] battle seeds done elapsed_s=%.2f", time.perf_counter() - seeds_started_at)

    mc_started_at = time.perf_counter()
    logger.info("[gold/simulation] running monte carlo resampling trials=%s seed=%s", n_trials, rng_seed)
    _run_gold_monte_carlo_optimizer(
        gold_dir=gold_dir,
        n_trials=n_trials,
        rng_seed=rng_seed,
    )
    logger.info("[gold/simulation] monte carlo resampling done elapsed_s=%.2f", time.perf_counter() - mc_started_at)
    logger.info("[gold/simulation] finished elapsed_s=%.2f", time.perf_counter() - started_at)