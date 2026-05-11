#!/usr/bin/env python3
"""Diagnose null/empty team IDs in simulation artifacts."""

from pathlib import Path
from src.pipeline.common.io import read_parquet
from src.pipeline.settings import GOLD_DIR, GOLD_SIMULATION_DIRNAME

def diagnose_team_ids(gold_dir: Path = GOLD_DIR, simulation_dirname: str = GOLD_SIMULATION_DIRNAME) -> None:
    simulation_dir = gold_dir / simulation_dirname

    if not simulation_dir.exists():
        print(f"[ERROR] Simulation directory not found: {simulation_dir}")
        return

    files_to_check = [
        ("team_battle_simulations.parquet", ["team_id_attacker", "team_id_defender"]),
        ("battle_seeds.parquet", ["player_team_id", "boss_team_id"]),
        ("monte_carlo_results.parquet", ["player_team_id", "boss_team_id"]),
    ]

    for filename, team_id_cols in files_to_check:
        filepath = simulation_dir / filename
        if not filepath.exists():
            print(f"[SKIP] {filename} not found")
            continue

        print(f"\n[CHECK] {filename}")
        df = read_parquet(filepath)
        print(f"  Total rows: {len(df)}")

        for col in team_id_cols:
            if col not in df.columns:
                print(f"  WARNING: Column '{col}' not found")
                continue

            null_count = df[col].isna().sum()
            empty_count = (df[col].astype(str).str.strip() == "").sum()
            valid_count = len(df) - null_count - empty_count

            print(f"  {col}:")
            print(f"    Null/NaN: {null_count}")
            print(f"    Empty strings: {empty_count}")
            print(f"    Valid: {valid_count}")

            if null_count > 0 or empty_count > 0:
                print(f"    Examples of null/empty:")
                invalid_rows = df[(df[col].isna()) | (df[col].astype(str).str.strip() == "")].head(3)
                for idx, row in invalid_rows.iterrows():
                    print(f"      Row {idx}: {row[team_id_cols].to_dict()}")

if __name__ == "__main__":
    diagnose_team_ids()

