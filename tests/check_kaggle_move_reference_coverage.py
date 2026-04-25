from __future__ import annotations

import csv
from pathlib import Path

from src.pipeline.common.io import read_parquet
from src.pipeline.silver.inputs.reference_context import normalize_move_name


def main() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    kaggle_path = repo_root / "data" / "bronze" / "kagglehub" / "gym_leaders_elite_four.csv"
    move_reference_path = repo_root / "data" / "silver" / "references" / "move_reference.parquet"

    kaggle_moves: set[str] = set()
    with open(kaggle_path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle, delimiter=",")
        for row in reader:
            for column in ("Move 1", "Move 2", "Move 3", "Move 4"):
                move_name = normalize_move_name(row.get(column) or "")
                if move_name:
                    kaggle_moves.add(move_name)

    move_reference_df = read_parquet(move_reference_path)
    reference_moves = {
        normalize_move_name(row.get("move_name"))
        for row in move_reference_df.to_dict(orient="records")
        if normalize_move_name(row.get("move_name"))
    }

    missing = sorted(kaggle_moves - reference_moves)
    print(f"kaggle_distinct_moves={len(kaggle_moves)}")
    print(f"move_reference_moves={len(reference_moves)}")
    print(f"missing={len(missing)}")
    if missing:
        print("missing_moves_sample=", ",".join(missing[:30]))
        raise SystemExit(1)


if __name__ == "__main__":
    main()
