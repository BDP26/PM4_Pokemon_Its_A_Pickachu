from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def count_rows(parquet_path: Path) -> int:
    dataframe = pd.read_parquet(parquet_path)
    return len(dataframe)


def main() -> None:
    parser = argparse.ArgumentParser(description="Print row counts for parquet files in a simulation directory.")
    parser.add_argument(
        "--simulation-dir",
        type=Path,
        default=Path("data/silver/simulation"),
        help="Path to simulation directory (default: data/silver/simulation)",
    )
    args = parser.parse_args()

    simulation_dir = args.simulation_dir
    if not simulation_dir.exists():
        print(f"Simulation directory does not exist: {simulation_dir}")
        return

    parquet_paths = sorted(simulation_dir.glob("*.parquet"))
    if not parquet_paths:
        print(f"No parquet files found in: {simulation_dir}")
        return

    print(f"Parquet row counts in {simulation_dir}:")
    for parquet_path in parquet_paths:
        try:
            row_count = count_rows(parquet_path)
            print(f"- {parquet_path.name}: {row_count}")
        except Exception as exc:  # pragma: no cover - utility script output only
            print(f"- {parquet_path.name}: ERROR ({exc})")


if __name__ == "__main__":
    main()
