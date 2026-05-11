#!/usr/bin/env python3
"""Validate consistency between battle_seeds and monte_carlo_results."""

from pathlib import Path
from src.pipeline.common.io import read_parquet
from src.pipeline.settings import GOLD_DIR, GOLD_SIMULATION_DIRNAME

def validate_simulation_consistency(gold_dir: Path = GOLD_DIR, simulation_dirname: str = GOLD_SIMULATION_DIRNAME) -> bool:
    simulation_dir = gold_dir / simulation_dirname

    if not simulation_dir.exists():
        print(f"[ERROR] Simulation directory not found: {simulation_dir}")
        return False

    seeds_path = simulation_dir / "battle_seeds.parquet"
    monte_carlo_path = simulation_dir / "monte_carlo_results.parquet"

    if not seeds_path.exists():
        print(f"[WARNING] battle_seeds.parquet not found")
        return False

    if not monte_carlo_path.exists():
        print(f"[WARNING] monte_carlo_results.parquet not found")
        return False

    seeds_df = read_parquet(seeds_path)
    monte_carlo_df = read_parquet(monte_carlo_path)

    print(f"\n[CONSISTENCY CHECK]")
    print(f"  battle_seeds rows: {len(seeds_df)}")
    print(f"  monte_carlo_results rows: {len(monte_carlo_df)}")

    issues = []

    # Check row counts
    if len(seeds_df) != len(monte_carlo_df):
        issues.append(f"Row count mismatch: seeds={len(seeds_df)}, monte_carlo={len(monte_carlo_df)}")

    # Check scenario_ids match
    if "scenario_id" in seeds_df.columns and "scenario_id" in monte_carlo_df.columns:
        seed_ids = set(seeds_df["scenario_id"].dropna().astype(str))
        mc_ids = set(monte_carlo_df["scenario_id"].dropna().astype(str))

        missing_in_mc = sorted(seed_ids - mc_ids)[:5]
        missing_in_seeds = sorted(mc_ids - seed_ids)[:5]

        if missing_in_mc:
            issues.append(f"Scenario IDs in seeds but missing in monte_carlo: {missing_in_mc}")
        if missing_in_seeds:
            issues.append(f"Scenario IDs in monte_carlo but missing in seeds: {missing_in_seeds}")

    # Check null team IDs
    for col in ["player_team_id", "boss_team_id"]:
        if col in seeds_df.columns:
            null_count = seeds_df[col].isna().sum() + (seeds_df[col].astype(str).str.strip() == "").sum()
            if null_count > 0:
                issues.append(f"Null/empty {col} in battle_seeds: {null_count} rows")

        if col in monte_carlo_df.columns:
            null_count = monte_carlo_df[col].isna().sum() + (monte_carlo_df[col].astype(str).str.strip() == "").sum()
            if null_count > 0:
                issues.append(f"Null/empty {col} in monte_carlo_results: {null_count} rows")

    if issues:
        print(f"\n[ISSUES FOUND]")
        for issue in issues:
            print(f"  - {issue}")
        return False
    else:
        print(f"\n[OK] All consistency checks passed!")
        return True

if __name__ == "__main__":
    success = validate_simulation_consistency()
    exit(0 if success else 1)

