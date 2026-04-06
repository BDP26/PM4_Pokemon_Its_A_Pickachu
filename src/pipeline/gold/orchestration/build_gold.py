from pathlib import Path
import importlib
import logging

import pandas as pd

from src.pipeline.common.io import read_jsonl, read_parquet, write_json, write_parquet
from src.pipeline.gold.reporting.build_walkthrough_web import build_walkthrough_best_teams_payload
from src.pipeline.gold.simulation.run_gold_simulation import run_gold_simulation_from_silver
from src.pipeline.settings import (
    GOLD_DIR,
    SILVER_DIR,
    ensure_medallion_dirs,
    get_gold_subdirs,
    get_silver_subdirs,
)


logger = logging.getLogger(__name__)


def _build_core_aggregations_with_spark(
    silver_df: pd.DataFrame,
    gold_dir: Path,
) -> bool:
    """Compute core gold aggregations with PySpark if available."""
    try:
        pyspark_sql = importlib.import_module("pyspark.sql")
        pyspark_functions = importlib.import_module("pyspark.sql.functions")
    except Exception:
        logger.warning("[gold] pyspark not available; core aggregations will use pandas fallback")
        return False

    SparkSession = getattr(pyspark_sql, "SparkSession")
    F = pyspark_functions

    spark = (
        SparkSession.builder
        .appName("pokemon-gold-aggregations")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    spark_df = spark.createDataFrame(silver_df)

    progression_df = (
        spark_df.orderBy("game", "part")
        .groupBy("game")
        .agg(
            F.count("boss_name").alias("boss_steps"),
            F.max("reachable_location_count").alias("max_reachable_locations"),
            F.max(F.struct(F.col("part"), F.col("reachable_location_count"))).alias("_last_reachable"),
        )
        .select(
            "game",
            "boss_steps",
            F.col("_last_reachable.reachable_location_count").alias("final_reachable_locations"),
            "max_reachable_locations",
        )
        .orderBy(F.col("final_reachable_locations").desc())
    )
    progression_pdf = progression_df.toPandas()
    progression_pdf.to_csv(gold_dir / "game_progression_summary.csv", index=False)
    logger.info("[gold] wrote game_progression_summary.csv rows=%s (spark)", len(progression_pdf))

    location_popularity_df = (
        spark_df.select("game", F.explode_outer("reachable_locations").alias("location_slug"))
        .where(F.col("location_slug").isNotNull())
        .groupBy("location_slug")
        .agg(
            F.countDistinct("game").alias("game_count"),
            F.count("game").alias("total_mentions"),
        )
        .orderBy(F.col("game_count").desc(), F.col("total_mentions").desc())
    )
    write_parquet(gold_dir / "location_popularity.parquet", location_popularity_df.toPandas())
    logger.info(
        "[gold] wrote location_popularity.parquet rows=%s (spark)",
        location_popularity_df.count(),
    )
    return True


def build_gold_from_silver(silver_dir: Path = SILVER_DIR, gold_dir: Path = GOLD_DIR) -> None:
    ensure_medallion_dirs()
    logger.info("[gold] build start silver_dir=%s gold_dir=%s", silver_dir, gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)
    silver_subdirs = get_silver_subdirs(silver_dir)
    gold_subdirs = get_gold_subdirs(gold_dir)
    gold_simulation_dir = gold_subdirs["simulation"]
    gold_simulation_dir.mkdir(parents=True, exist_ok=True)

    run_gold_simulation_from_silver(silver_dir=silver_dir, gold_dir=gold_dir)

    snapshots_dir = silver_subdirs["snapshots"]
    game_files = sorted(snapshots_dir.glob("*_boss_snapshots.jsonl"))
    if not game_files:
        game_files = sorted(silver_dir.glob("*_boss_snapshots.jsonl"))
    if not game_files:
        raise FileNotFoundError(f"No silver files found in {silver_dir}")

    logger.info("[gold] loading %s silver snapshot files", len(game_files))

    frames: list[pd.DataFrame] = []
    for file_path in game_files:
        logger.info("[gold] reading %s", file_path.name)
        frames.append(read_jsonl(file_path))

    silver_df = pd.concat(frames, ignore_index=True)
    logger.info("[gold] loaded silver rows=%s", len(silver_df))

    used_spark_for_core = _build_core_aggregations_with_spark(silver_df=silver_df, gold_dir=gold_dir)
    if not used_spark_for_core:
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
        logger.info("[gold] wrote game_progression_summary.csv rows=%s (pandas fallback)", len(progression))

        exploded = silver_df[["game", "reachable_locations"]].explode("reachable_locations")
        exploded = exploded.rename(columns={"reachable_locations": "location_slug"}).dropna()

        location_popularity = (
            exploded.groupby("location_slug", as_index=False)
            .agg(game_count=("game", "nunique"), total_mentions=("game", "count"))
            .sort_values(["game_count", "total_mentions"], ascending=False)
        )
        write_parquet(gold_dir / "location_popularity.parquet", location_popularity)
        logger.info("[gold] wrote location_popularity.parquet rows=%s (pandas fallback)", len(location_popularity))

    gold_outputs = ["game_progression_summary.csv", "location_popularity.parquet"]
    simulation_outputs = [
        "teams.parquet",
        "team_battle_simulations.parquet",
        "battle_seeds.parquet",
        "monte_carlo_results.parquet",
    ]
    gold_outputs.extend([name for name in simulation_outputs if (gold_simulation_dir / name).exists()])

    monte_carlo_path = gold_simulation_dir / "monte_carlo_results.parquet"
    if monte_carlo_path.exists():
        logger.info("[gold] monte_carlo_results found, building recommendation outputs")
        monte_carlo_df = read_parquet(monte_carlo_path)
        if not monte_carlo_df.empty:
            teams_path = gold_simulation_dir / "teams.parquet"
            if teams_path.exists():
                teams_df = read_parquet(teams_path)[["team_id", "game_version", "avg_level"]]
                player_teams_df = teams_df.rename(
                    columns={
                        "team_id": "player_team_id",
                        "game_version": "player_game_version",
                        "avg_level": "player_avg_level",
                    }
                )
                boss_teams_df = teams_df.rename(
                    columns={
                        "team_id": "boss_team_id",
                        "avg_level": "boss_avg_level",
                    }
                )[["boss_team_id", "boss_avg_level"]]

                monte_carlo_df = monte_carlo_df.merge(player_teams_df, on="player_team_id", how="left")
                monte_carlo_df = monte_carlo_df.merge(boss_teams_df, on="boss_team_id", how="left")

            team_recommendations = (
                monte_carlo_df.groupby("player_team_id", as_index=False)
                .agg(
                    avg_win_rate=("mc_win_rate", "mean"),
                    scenarios=("scenario_id", "count"),
                    unique_bosses=("boss_team_id", "nunique"),
                    avg_expected_wins=("wins", "mean"),
                    avg_trials=("n_trials", "mean"),
                )
                .sort_values(["avg_win_rate", "avg_expected_wins"], ascending=False)
            )
            team_recommendations["rank"] = range(1, len(team_recommendations) + 1)
            write_parquet(gold_dir / "team_recommendations.parquet", team_recommendations)
            logger.info("[gold] wrote team_recommendations.parquet rows=%s", len(team_recommendations))
            gold_outputs.append("team_recommendations.parquet")

            if "player_game_version" in monte_carlo_df.columns:
                same_version_df = monte_carlo_df[
                    monte_carlo_df["player_game_version"] == monte_carlo_df["game_version"]
                ].copy()
                if not same_version_df.empty:
                    # Keep recommendations level-plausible for the current boss.
                    # Allow slightly higher teams (+3) but avoid extreme overlevel picks.
                    if {"player_avg_level", "boss_avg_level"}.issubset(same_version_df.columns):
                        level_mask = (
                            same_version_df["player_avg_level"].notna()
                            & same_version_df["boss_avg_level"].notna()
                            & (same_version_df["player_avg_level"] <= same_version_df["boss_avg_level"] + 3)
                            & (same_version_df["player_avg_level"] >= same_version_df["boss_avg_level"] - 10)
                        )
                        level_reasonable_df = same_version_df[level_mask].copy()
                        # Fallback to unconstrained version-only view if filtering removes everything.
                        if not level_reasonable_df.empty:
                            same_version_df = level_reasonable_df

                    rankings = same_version_df.sort_values(
                        ["game_version", "boss_name", "boss_team_id", "mc_win_rate", "wins"],
                        ascending=[True, True, True, False, False],
                    )
                    rankings["rank_in_boss_version"] = rankings.groupby("boss_team_id").cumcount() + 1

                    ranking_cols = [
                        "boss_team_id",
                        "boss_name",
                        "game_version",
                        "player_team_id",
                        "mc_win_rate",
                        "wins",
                        "losses",
                        "n_trials",
                        "player_avg_level",
                        "boss_avg_level",
                        "rank_in_boss_version",
                    ]
                    rankings_export = rankings[ranking_cols]
                    write_parquet(gold_dir / "team_rankings_by_boss_version.parquet", rankings_export)
                    logger.info("[gold] wrote team_rankings_by_boss_version.parquet rows=%s", len(rankings))
                    gold_outputs.append("team_rankings_by_boss_version.parquet")

                    best_same_version = rankings[rankings["rank_in_boss_version"] == 1][ranking_cols]
                    write_parquet(gold_dir / "best_team_by_boss_version.parquet", best_same_version)
                    logger.info("[gold] wrote best_team_by_boss_version.parquet rows=%s", len(best_same_version))
                    gold_outputs.append("best_team_by_boss_version.parquet")

                    # Presentation-friendly tabular export
                    best_same_version.to_csv(
                        gold_dir / "best_team_by_boss_version.csv",
                        index=False,
                    )
                    logger.info("[gold] wrote best_team_by_boss_version.csv rows=%s", len(best_same_version))
                    gold_outputs.append("best_team_by_boss_version.csv")

            best_idx = monte_carlo_df.groupby("boss_team_id")["mc_win_rate"].idxmax()
            best_team_by_boss = (
                monte_carlo_df.loc[best_idx, [
                    "boss_team_id",
                    "boss_name",
                    "game_version",
                    "player_team_id",
                    "mc_win_rate",
                    "wins",
                    "losses",
                    "n_trials",
                ]]
                .sort_values(["game_version", "boss_name", "mc_win_rate"], ascending=[True, True, False])
            )
            write_parquet(gold_dir / "best_team_by_boss.parquet", best_team_by_boss)
            logger.info("[gold] wrote best_team_by_boss.parquet rows=%s", len(best_team_by_boss))
            gold_outputs.append("best_team_by_boss.parquet")
        else:
            logger.warning("[gold] monte_carlo_results.parquet is empty; skipping recommendation outputs")
    else:
        logger.warning("[gold] monte_carlo_results.parquet missing; skipping recommendation outputs")

    web_payload_path = build_walkthrough_best_teams_payload(silver_dir=silver_dir, gold_dir=gold_dir)
    if web_payload_path is not None:
        gold_outputs.append(web_payload_path.name)

    manifest = {
        "silver_game_files": [path.name for path in game_files],
        "silver_records": int(len(silver_df)),
        "gold_simulation_dir": str(gold_simulation_dir.relative_to(gold_dir)),
        "gold_outputs": gold_outputs,
    }
    write_json(gold_dir / "manifest.json", manifest)

    logger.info("[gold] wrote %s datasets from %s silver files", len(gold_outputs), len(game_files))

if __name__ == "__main__":
    build_gold_from_silver()



