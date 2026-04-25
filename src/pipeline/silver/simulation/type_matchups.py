"""Compatibility shim for moved Gold simulation engine."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.pipeline.gold.simulation.team_battle_simulations import *  # noqa: F401,F403
from src.pipeline.gold.simulation.team_battle_simulations import (
    BattleSimulationConfig,
    build_team_battle_simulations as _gold_build_team_battle_simulations,
    build_type_matchups as _gold_build_type_matchups,
)
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR

from src.pipeline.gold.simulation.team_battle_simulations import (
    _install_reference_profiles,
    _is_version_compatible,
    _load_move_and_pokemon_profiles_from_disk,
    _run_local_simulations,
    _run_spark_simulations,
    _should_use_spark,
    _stable_pair_seed,
)

logger = logging.getLogger(__name__)


def build_team_battle_simulations(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
    force_spark: bool | None = None,
    runtime_config: BattleSimulationConfig | None = None,
) -> None:
    logger.warning("[gold/simulation] deprecated silver simulation path used")
    _gold_build_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=silver_dir,
        output_dir=silver_dir,
        bronze_dir=bronze_dir,
        simulation_dirname="simulation",
        force_spark=False if force_spark is None else force_spark,
        runtime_config=runtime_config,
    )


def build_type_matchups(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    bronze_dir: Path = BRONZE_DIR,
) -> None:
    logger.warning("[gold/simulation] deprecated silver simulation path used")
    _gold_build_type_matchups(
        teams_data=teams_data,
        silver_dir=silver_dir,
        output_dir=silver_dir,
        bronze_dir=bronze_dir,
        simulation_dirname="simulation",
    )
