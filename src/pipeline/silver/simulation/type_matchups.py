"""Compatibility wrapper for the relocated team battle simulation module."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.pipeline.gold.simulation import team_battle_simulations as _impl
from src.pipeline.settings import BRONZE_DIR, SILVER_DIR, SILVER_SIMULATION_DIRNAME

BattleSimulationConfig = _impl.BattleSimulationConfig
WarningCollector = _impl.WarningCollector

_stable_pair_seed = _impl._stable_pair_seed
_stable_sequence_seed = _impl._stable_sequence_seed
_is_version_compatible = _impl._is_version_compatible
_should_use_spark = _impl._should_use_spark
_install_reference_profiles = _impl._install_reference_profiles
_validate_profile_coverage = _impl._validate_profile_coverage
_get_pokemon_profile = _impl._get_pokemon_profile
filter_simulation_teams = _impl.filter_simulation_teams
load_pokemon_profiles_from_silver = _impl.load_pokemon_profiles_from_silver
load_move_profiles_from_silver = _impl.load_move_profiles_from_silver

_run_local_simulations = _impl._run_local_simulations
_run_spark_simulations = _impl._run_spark_simulations


def _sync_overrides_to_impl() -> None:
    _impl._run_local_simulations = _run_local_simulations
    _impl._run_spark_simulations = _run_spark_simulations


def build_team_battle_simulations(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    output_dir: Path | None = None,
    bronze_dir: Path = BRONZE_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    force_spark: bool | None = None,
    runtime_config: BattleSimulationConfig | None = None,
) -> None:
    _sync_overrides_to_impl()
    _impl.build_team_battle_simulations(
        teams_data=teams_data,
        silver_dir=silver_dir,
        output_dir=silver_dir if output_dir is None else output_dir,
        bronze_dir=bronze_dir,
        simulation_dirname=simulation_dirname,
        force_spark=False if force_spark is None else force_spark,
        runtime_config=runtime_config,
    )


def build_type_matchups(
    teams_data: list[dict[str, Any]],
    silver_dir: Path = SILVER_DIR,
    output_dir: Path | None = None,
    bronze_dir: Path = BRONZE_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    force_spark: bool | None = None,
    runtime_config: BattleSimulationConfig | None = None,
) -> None:
    _sync_overrides_to_impl()
    _impl.build_type_matchups(
        teams_data=teams_data,
        silver_dir=silver_dir,
        output_dir=silver_dir if output_dir is None else output_dir,
        bronze_dir=bronze_dir,
        simulation_dirname=simulation_dirname,
        force_spark=False if force_spark is None else force_spark,
        runtime_config=runtime_config,
    )


def __getattr__(name: str) -> Any:
    return getattr(_impl, name)
