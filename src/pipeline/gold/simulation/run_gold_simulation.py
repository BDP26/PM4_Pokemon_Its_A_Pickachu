from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any, cast

import pandas as pd

from src.pipeline.common.io import write_parquet
from src.pipeline.common.simulation_config import load_runtime_battle_policy_config
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
from src.pipeline.silver.simulation.type_matchups import BattleSimulationConfig, build_team_battle_simulations


logger = logging.getLogger(__name__)


def _run_gold_team_battle_simulations(
    *,
    teams_data: list[dict[str, Any]],
    silver_dir: Path,
    bronze_dir: Path,
    runtime_config: BattleSimulationConfig,
) -> None:
    build_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
        force_spark=None,
        runtime_config=runtime_config,
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
    required_input_files: dict[str, Path | list[Path]] | None = None,
    n_trials: int = 500,
    rng_seed: int = 42,
) -> None:
    started_at = time.perf_counter()
    silver_simulation_dir = silver_dir / SILVER_SIMULATION_DIRNAME
    gold_simulation_dir = gold_dir / GOLD_SIMULATION_DIRNAME
    gold_simulation_dir.mkdir(parents=True, exist_ok=True)
    logger.info("[gold/simulation] start silver_dir=%s gold_dir=%s", silver_dir, gold_dir)

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
    write_parquet(gold_simulation_dir / "teams.parquet", teams_data)
    logger.info("[gold/simulation] loaded teams count=%s", len(teams_data))
    runtime_policy = load_runtime_battle_policy_config()
    base_config = BattleSimulationConfig()
    runtime_config = BattleSimulationConfig(
        max_overlevel=base_config.max_overlevel,
        max_underlevel=base_config.max_underlevel,
        n_battle_trials=int(n_trials or runtime_policy.n_battle_trials),
        damage_randomness_min=runtime_policy.damage_random_min,
        damage_randomness_max=runtime_policy.damage_random_max,
        crit_chance=runtime_policy.crit_chance_default,
        max_turns_per_duel=base_config.max_turns_per_duel,
        rng_seed=int(rng_seed or runtime_policy.rng_seed),
        require_exact_version_match=not bool(runtime_policy.allow_cross_version_fallback),
        fail_on_degraded_data=bool(runtime_policy.fail_on_degraded_data),
    )

    sims_started_at = time.perf_counter()
    logger.info("[gold/simulation] running round-based team battle simulations")
    _run_gold_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=silver_dir,
        bronze_dir=bronze_dir,
        runtime_config=runtime_config,
    )
    logger.info("[gold/simulation] team battle simulations done elapsed_s=%.2f", time.perf_counter() - sims_started_at)

    seeds_started_at = time.perf_counter()
    logger.info("[gold/simulation] building battle seeds")
    _build_gold_battle_seeds(gold_dir=gold_dir)
    logger.info("[gold/simulation] battle seeds done elapsed_s=%.2f", time.perf_counter() - seeds_started_at)

    mc_started_at = time.perf_counter()
    logger.info("[gold/simulation] summarizing simulation results trials=%s seed=%s", n_trials, rng_seed)
    _run_gold_monte_carlo_optimizer(
        gold_dir=gold_dir,
        n_trials=n_trials,
        rng_seed=rng_seed,
    )
    logger.info("[gold/simulation] simulation summary done elapsed_s=%.2f", time.perf_counter() - mc_started_at)
    logger.info("[gold/simulation] finished elapsed_s=%.2f", time.perf_counter() - started_at)
