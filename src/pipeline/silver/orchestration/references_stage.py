"""Reference-stage helpers for Silver orchestration."""

from __future__ import annotations

from pathlib import Path


def cleanup_simulation_shards(simulation_dir: Path) -> None:
    for pattern in [
        "source_teams_*.parquet",
        "source_team_members_*.parquet",
        "member_moveset_combos_*.parquet",
        "member_move_options_*.parquet",
        "pokemon_moveset_options_*.parquet",
        "simulation_sampling_plan_*.parquet",
        "pokemon_combat_pool_*.parquet",
    ]:
        for old_file in simulation_dir.glob(pattern):
            old_file.unlink()

