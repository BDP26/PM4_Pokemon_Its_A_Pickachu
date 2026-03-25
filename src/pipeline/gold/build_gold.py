from pathlib import Path

import pandas as pd

from src.pipeline.common.io import read_jsonl, write_json, write_jsonl
from src.pipeline.settings import GOLD_DIR, SILVER_DIR, ensure_medallion_dirs, get_silver_subdirs


def build_gold_from_silver(silver_dir: Path = SILVER_DIR, gold_dir: Path = GOLD_DIR) -> None:
    ensure_medallion_dirs()
    gold_dir.mkdir(parents=True, exist_ok=True)

    snapshots_dir = get_silver_subdirs(silver_dir)["snapshots"]
    game_files = sorted(snapshots_dir.glob("*_boss_snapshots.jsonl"))
    if not game_files:
        game_files = sorted(silver_dir.glob("*_boss_snapshots.jsonl"))
    if not game_files:
        raise FileNotFoundError(f"No silver files found in {silver_dir}")

    frames: list[pd.DataFrame] = []
    for file_path in game_files:
        frames.append(read_jsonl(file_path))

    silver_df = pd.concat(frames, ignore_index=True)

    progression = (
        silver_df.sort_values(["game", "part"])
        .groupby("game", as_index=False)
        .agg(
            boss_steps=("boss_name", "count"),
            final_reachable_locations=("reachable_location_count", "last"),
            max_reachable_locations=("reachable_location_count", "max"),
        )
        .sort_values("final_reachable_locations", ascending=False)
    )
    progression.to_csv(gold_dir / "game_progression_summary.csv", index=False)

    exploded = silver_df[["game", "reachable_locations"]].explode("reachable_locations")
    exploded = exploded.rename(columns={"reachable_locations": "location_slug"}).dropna()

    location_popularity = (
        exploded.groupby("location_slug", as_index=False)
        .agg(game_count=("game", "nunique"), total_mentions=("game", "count"))
        .sort_values(["game_count", "total_mentions"], ascending=False)
    )
    write_jsonl(gold_dir / "location_popularity.jsonl", location_popularity.to_dict(orient="records"))

    manifest = {
        "silver_game_files": [path.name for path in game_files],
        "silver_records": int(len(silver_df)),
        "gold_outputs": ["game_progression_summary.csv", "location_popularity.jsonl"],
    }
    write_json(gold_dir / "manifest.json", manifest)

    print(f"[gold] wrote {len(manifest['gold_outputs'])} datasets from {len(game_files)} silver files")

if __name__ == "__main__":
    build_gold_from_silver()

