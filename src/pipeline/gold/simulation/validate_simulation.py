"""Smoke checks for simulation artifacts in data/gold/simulation."""
from __future__ import annotations

import sys

from src.pipeline.settings import GOLD_DIR
from src.pipeline.silver.simulation.validate_simulation import validate_simulation_artifacts


def main() -> int:
    issues = validate_simulation_artifacts(silver_dir=GOLD_DIR)
    if issues:
        print("[validate_simulation_gold] FAILED")
        for issue in issues[:200]:
            print(f"  - {issue}")
        if len(issues) > 200:
            print(f"  ... and {len(issues) - 200} more")
        return 1

    print("[validate_simulation_gold] OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())


