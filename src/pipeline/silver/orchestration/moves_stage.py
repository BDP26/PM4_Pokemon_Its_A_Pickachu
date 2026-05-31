"""Move-reference stage helpers for Silver orchestration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.pipeline.silver.writers.outputs import write_validated_move_data


def write_move_data_snapshot(
    *,
    simulation_dir: Path,
    all_move_data: dict[str, Any],
    chunk_threshold: int = 500_000,
    chunk_size: int = 40_000,
) -> None:
    write_validated_move_data(
        simulation_dir / "move_data.parquet",
        all_move_data,
        chunk_threshold=chunk_threshold,
        chunk_size=chunk_size,
    )

