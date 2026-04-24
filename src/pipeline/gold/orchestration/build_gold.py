from pathlib import Path
import importlib
import logging
import math
from typing import Any, NoReturn, cast

import pandas as pd

from src.pipeline.common.io import read_json, read_jsonl, read_parquet, write_json, write_parquet
from src.pipeline.gold.reporting.build_walkthrough_web import build_walkthrough_best_teams_payload
from src.pipeline.gold.simulation.run_gold_simulation import run_gold_simulation_from_silver
from src.pipeline.silver.config.game_config import get_games_config
from src.pipeline.settings import (
    GOLD_DIR,
    SILVER_DIR,
    ensure_medallion_dirs,
    get_gold_subdirs,
)


logger = logging.getLogger(__name__)

# Keep recommendations plausible relative to the current boss level.
MAX_PLAYER_OVERLEVEL_GAP = 2
MAX_PLAYER_UNDERLEVEL_GAP = 10


class GoldContractError(ValueError):
    """Raised when Silver->Gold manifest contract validation fails."""


_REQUIRED_MANIFEST_DATASET_FILES = (
    "simulation_inputs_teams",
    "source_team_members",
    "member_move_options",
)
_OPTIONAL_MANIFEST_DATASET_FILES = (
    "pokemon_reference",
    "snapshot_available_pokemon",
    "encounters",
)
_STRICT_SHARDED_DATASET_KEYS = {
    "simulation_inputs_teams",
    "source_team_members",
    "member_move_options",
}


def _raise_contract_error(code: str, message: str, *, dataset: str | None = None, path: Path | None = None) -> NoReturn:
    details: list[str] = [f"[gold.contract] {code}"]
    if dataset:
        details.append(f"dataset={dataset}")
    if path is not None:
        details.append(f"path={path}")
    details.append(f"action=\"{message}\"")
    raise GoldContractError(" ".join(details))


def _apply_level_plausibility_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    required = {"player_avg_level", "boss_avg_level"}
    if not required.issubset(df.columns):
        return df

    level_mask = (
        df["player_avg_level"].notna()
        & df["boss_avg_level"].notna()
        & (df["player_avg_level"] <= df["boss_avg_level"] + MAX_PLAYER_OVERLEVEL_GAP)
        & (df["player_avg_level"] >= df["boss_avg_level"] - MAX_PLAYER_UNDERLEVEL_GAP)
    )
    filtered = df[level_mask].copy()
    # Fallback to the original frame if constraints remove all rows.
    return filtered if not filtered.empty else df


def _load_silver_manifest(silver_dir: Path) -> dict[str, Any]:
    manifest_path = silver_dir / "manifest.json"
    if not manifest_path.exists():
        _raise_contract_error(
            "missing_manifest",
            "Run Silver first to generate manifest.json.",
            path=manifest_path,
        )
    try:
        payload = read_json(manifest_path)
    except Exception as exc:
        _raise_contract_error(
            "invalid_manifest_json",
            f"manifest.json is unreadable ({exc}). Rebuild Silver outputs.",
            path=manifest_path,
        )
    if not isinstance(payload, dict):
        _raise_contract_error(
            "invalid_manifest_shape",
            "manifest.json must be a JSON object.",
            path=manifest_path,
        )
    return cast(dict[str, Any], payload)


def _manifest_datasets(manifest: dict[str, Any]) -> dict[str, Any]:
    datasets = manifest.get("datasets")
    if not isinstance(datasets, dict):
        _raise_contract_error(
            "missing_manifest_datasets",
            "manifest.json requires a top-level datasets object.",
        )
    return cast(dict[str, Any], datasets)


def _resolve_required_manifest_file(silver_dir: Path, manifest: dict[str, Any], dataset_key: str) -> Path | list[Path]:
    datasets = _manifest_datasets(manifest)
    dataset_entry = datasets.get(dataset_key)
    if not isinstance(dataset_entry, dict):
        _raise_contract_error(
            "missing_dataset_entry",
            f"Add datasets.{dataset_key} to silver/manifest.json.",
            dataset=dataset_key,
        )
    files_entry = dataset_entry.get("files")
    if dataset_key in _STRICT_SHARDED_DATASET_KEYS:
        if isinstance(files_entry, list) and files_entry:
            resolved_files: list[Path] = []
            for index, rel in enumerate(cast(list[Any], files_entry)):
                if not isinstance(rel, str) or not rel.strip():
                    _raise_contract_error(
                        "invalid_dataset_files_entry",
                        f"datasets.{dataset_key}.files[{index}] must be a non-empty string path.",
                        dataset=dataset_key,
                    )
                candidate = silver_dir / rel
                if not candidate.exists() or not candidate.is_file():
                    _raise_contract_error(
                        "missing_dataset_file",
                        "Regenerate Silver outputs so all strict contract files exist.",
                        dataset=dataset_key,
                        path=candidate,
                    )
                resolved_files.append(candidate)
            return sorted(set(resolved_files))
        if dataset_entry.get("file"):
            _raise_contract_error(
                "strict_sharded_contract_violation",
                f"datasets.{dataset_key} must use files[]; directory/file-only entries are not allowed.",
                dataset=dataset_key,
            )
        _raise_contract_error(
            "missing_dataset_files",
            f"Set datasets.{dataset_key}.files in silver/manifest.json.",
            dataset=dataset_key,
        )

    if isinstance(files_entry, list) and files_entry:
        resolved_files: list[Path] = []
        for index, rel in enumerate(cast(list[Any], files_entry)):
            if not isinstance(rel, str) or not rel.strip():
                _raise_contract_error(
                    "invalid_dataset_files_entry",
                    f"datasets.{dataset_key}.files[{index}] must be a non-empty string path.",
                    dataset=dataset_key,
                )
            candidate = silver_dir / rel
            if not candidate.exists():
                _raise_contract_error(
                    "missing_dataset_file",
                    "Regenerate Silver outputs so all contract files or partition folders exist.",
                    dataset=dataset_key,
                    path=candidate,
                )
            resolved_files.append(candidate)
        return sorted(set(resolved_files))

    rel_path = dataset_entry.get("file")
    if not isinstance(rel_path, str) or not rel_path.strip():
        _raise_contract_error(
            "missing_dataset_file_path",
            f"Set datasets.{dataset_key}.file or datasets.{dataset_key}.files in silver/manifest.json.",
            dataset=dataset_key,
        )
    path = silver_dir / cast(str, rel_path)
    if not path.exists():
        _raise_contract_error(
            "missing_dataset_file",
            "Regenerate Silver outputs so all contract files or partition folders exist.",
            dataset=dataset_key,
            path=path,
        )
    return path


def _resolve_snapshot_files_from_manifest(silver_dir: Path, manifest: dict[str, Any]) -> list[Path]:
    datasets = _manifest_datasets(manifest)
    boss_records = datasets.get("boss_records")
    if not isinstance(boss_records, dict):
        _raise_contract_error(
            "missing_dataset_entry",
            "Add datasets.boss_records with files[] in silver/manifest.json.",
            dataset="boss_records",
        )
    files = boss_records.get("files")
    if not isinstance(files, list) or not files:
        _raise_contract_error(
            "missing_snapshot_files",
            "Populate datasets.boss_records.files with snapshot JSONL inputs.",
            dataset="boss_records",
        )

    resolved: list[Path] = []
    files_list = cast(list[Any], files)
    for index, rel in enumerate(files_list):
        if not isinstance(rel, str) or not rel.strip():
            _raise_contract_error(
                "invalid_snapshot_entry",
                f"datasets.boss_records.files[{index}] must be a non-empty string path.",
                dataset="boss_records",
            )
        path = silver_dir / rel
        if not path.exists() or not path.is_file():
            _raise_contract_error(
                "missing_snapshot_file",
                "Regenerate Silver snapshots and refresh manifest entries.",
                dataset="boss_records",
                path=path,
            )
        resolved.append(path)
    snapshot_files = sorted(set(resolved))
    if not snapshot_files:
        _raise_contract_error(
            "missing_snapshot_files",
            "No valid snapshot files resolved from datasets.boss_records.files.",
            dataset="boss_records",
        )
    return snapshot_files


def _load_and_validate_gold_contract(silver_dir: Path) -> dict[str, Any]:
    manifest = _load_silver_manifest(silver_dir)
    snapshot_files = _resolve_snapshot_files_from_manifest(silver_dir, manifest)

    required_files: dict[str, Path | list[Path]] = {}
    for dataset_key in _REQUIRED_MANIFEST_DATASET_FILES:
        required_files[dataset_key] = _resolve_required_manifest_file(silver_dir, manifest, dataset_key)

    datasets = _manifest_datasets(manifest)
    for dataset_key in _OPTIONAL_MANIFEST_DATASET_FILES:
        if dataset_key not in datasets:
            logger.warning(
                "[gold.contract] optional_dataset_missing dataset=%s action=\"Rebuild Silver to include this dataset if required by downstream analyses.\"",
                dataset_key,
            )
            continue
        try:
            required_files[dataset_key] = _resolve_required_manifest_file(silver_dir, manifest, dataset_key)
        except GoldContractError as exc:
            logger.warning("%s", str(exc))
            continue

    logger.info(
        "[gold.contract] validated snapshots=%s required_datasets=%s",
        len(snapshot_files),
        ",".join(sorted(required_files.keys())),
    )
    return {
        "manifest": manifest,
        "snapshot_files": snapshot_files,
        "required_files": required_files,
    }


def _normalize_game_key_to_game_version(dataframe: pd.DataFrame, *, source_name: str) -> pd.DataFrame:
    frame = dataframe.copy()
    if "game_version" not in frame.columns and "game" in frame.columns:
        frame = frame.rename(columns={"game": "game_version"})
    if "game_version" not in frame.columns:
        _raise_contract_error(
            "missing_game_version_column",
            f"{source_name} must contain a game_version (or legacy game) column.",
            dataset="boss_records",
        )

    frame["game_version"] = frame["game_version"].astype(str).str.strip().str.lower()
    empty_mask = frame["game_version"].eq("")
    if bool(empty_mask.any()):
        _raise_contract_error(
            "invalid_game_version_values",
            f"{source_name} contains empty game_version rows.",
            dataset="boss_records",
        )
    return frame


def _boss_order_lookup() -> dict[tuple[str, str], tuple[int, int]]:
    lookup: dict[tuple[str, str], tuple[int, int]] = {}
    for game in get_games_config():
        version = str(game.get("game_key") or "").strip().lower()
        bosses = [str(name).strip() for name in game.get("bosses", []) if str(name).strip()]
        total = len(bosses)
        for idx, boss in enumerate(bosses, start=1):
            lookup[(version, boss.lower())] = (idx, total)
    return lookup


_STARTER_BOSS_GROUP_COLS = [
    "effective_game_version",
    "effective_boss_name",
    "boss_team_id",
    "starter_base",
    "starter_evolved_species",
    "player_team_id",
]


def _write_starter_boss_outputs(gold_dir: Path, boss_rank: pd.DataFrame) -> list[str]:
    outputs: list[str] = []
    write_parquet(gold_dir / "team_rankings_by_boss_version_starter.parquet", boss_rank)
    outputs.append("team_rankings_by_boss_version_starter.parquet")
    best = boss_rank[boss_rank["rank_in_boss_starter"] == 1].copy()
    write_parquet(gold_dir / "best_team_by_boss_version_starter.parquet", best)
    outputs.append("best_team_by_boss_version_starter.parquet")
    return outputs


def _write_starter_sequence_outputs(gold_dir: Path, sequence_rank: pd.DataFrame) -> list[str]:
    outputs: list[str] = []
    write_parquet(gold_dir / "team_rankings_e4_champion_sequence_by_version_starter.parquet", sequence_rank)
    outputs.append("team_rankings_e4_champion_sequence_by_version_starter.parquet")
    sequence_best = sequence_rank[sequence_rank["rank_in_sequence"] == 1].copy()
    write_parquet(gold_dir / "best_team_by_e4_champion_sequence_version_starter.parquet", sequence_best)
    outputs.append("best_team_by_e4_champion_sequence_version_starter.parquet")
    return outputs


def _build_starter_rankings_from_monte_carlo(gold_dir: Path, gold_simulation_dir: Path) -> list[str]:
    monte_carlo_path = gold_simulation_dir / "monte_carlo_results.parquet"
    teams_path = gold_simulation_dir / "teams.parquet"
    if not monte_carlo_path.exists() or not teams_path.exists():
        return []

    monte_carlo_df = read_parquet(monte_carlo_path)
    teams_df = read_parquet(teams_path)
    if monte_carlo_df.empty or teams_df.empty:
        return []

    player_context = teams_df[["team_id", "game_version", "starter_base", "starter_evolved_species", "avg_level"]].rename(
        columns={"team_id": "player_team_id", "game_version": "player_game_version"}
    )
    boss_context = teams_df[["team_id", "boss_name", "game_version", "avg_level"]].rename(
        columns={"team_id": "boss_team_id", "game_version": "boss_game_version"}
    )

    player_context = player_context.rename(columns={"avg_level": "player_avg_level"})
    boss_context = boss_context.rename(columns={"avg_level": "boss_avg_level"})

    joined = monte_carlo_df.merge(player_context, on="player_team_id", how="left")
    joined = joined.merge(boss_context, on="boss_team_id", how="left", suffixes=("", "_team"))
    joined["effective_game_version"] = joined["boss_game_version"].fillna(joined.get("game_version")).fillna(joined["player_game_version"])
    joined["effective_boss_name"] = joined["boss_name_team"].fillna(joined.get("boss_name"))

    lookup = _boss_order_lookup()

    def _resolve_stage(row: pd.Series) -> str:
        version = str(row.get("effective_game_version") or "").strip().lower()
        boss_name = str(row.get("effective_boss_name") or "").strip().lower()
        order_meta = lookup.get((version, boss_name))
        if order_meta is None:
            return "boss"
        order, total = order_meta
        elite_cutoff = max(1, total - 4)
        if order >= elite_cutoff:
            return "champion" if order == total else "elite_four"
        return "boss"

    joined["boss_stage"] = joined.apply(_resolve_stage, axis=1)
    joined = joined[joined["starter_base"].notna() & joined["effective_game_version"].notna()].copy()
    joined = _apply_level_plausibility_filter(joined)
    if joined.empty:
        return []

    outputs: list[str] = []

    try:
        pyspark_sql = importlib.import_module("pyspark.sql")
        pyspark_functions = importlib.import_module("pyspark.sql.functions")
        pyspark_window = importlib.import_module("pyspark.sql.window")

        SparkSession = getattr(pyspark_sql, "SparkSession")
        F = pyspark_functions
        Window = getattr(pyspark_window, "Window")

        spark = (
            SparkSession.builder
            .appName("pokemon-starter-rankings")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")
        try:
            sdf = spark.createDataFrame(joined)

            boss_rank = (
                sdf.groupBy(*_STARTER_BOSS_GROUP_COLS)
                .agg(
                    F.avg("mc_win_rate").alias("avg_mc_win_rate"),
                    F.avg("wins").alias("avg_wins"),
                    F.avg("losses").alias("avg_losses"),
                    F.avg("n_trials").alias("avg_n_trials"),
                    F.count("scenario_id").alias("scenario_rows"),
                    F.avg("player_avg_level").alias("player_avg_level"),
                    F.avg("boss_avg_level").alias("boss_avg_level"),
                )
            )
            boss_window = Window.partitionBy("effective_game_version", "effective_boss_name", "starter_base").orderBy(
                F.desc("avg_mc_win_rate"),
                F.desc("avg_wins"),
                F.asc("player_team_id"),
            )
            boss_rank = boss_rank.withColumn("rank_in_boss_starter", F.row_number().over(boss_window))
            boss_rank_pdf = boss_rank.toPandas()
            outputs.extend(_write_starter_boss_outputs(gold_dir, boss_rank_pdf))

            sequence = sdf.where(F.col("boss_stage").isin(["elite_four", "champion"]))
            sequence = sequence.withColumn("safe_rate", F.when(F.col("mc_win_rate") < 1e-6, F.lit(1e-6)).otherwise(F.col("mc_win_rate")))
            sequence = (
                sequence.groupBy("effective_game_version", "starter_base", "player_team_id")
                .agg(
                    F.exp(F.avg(F.log("safe_rate"))).alias("sequence_win_rate"),
                    F.avg("mc_win_rate").alias("mean_mc_win_rate"),
                    F.countDistinct("effective_boss_name").alias("bosses_covered"),
                    F.avg(F.col("degraded_data").cast("double")).alias("degraded_ratio"),
                )
                .withColumn("sequence_score", F.col("sequence_win_rate") * (F.lit(1.0) - F.coalesce(F.col("degraded_ratio"), F.lit(0.0)) * F.lit(0.2)))
            )
            seq_window = Window.partitionBy("effective_game_version", "starter_base").orderBy(
                F.desc("sequence_score"),
                F.desc("sequence_win_rate"),
                F.asc("player_team_id"),
            )
            sequence = sequence.withColumn("rank_in_sequence", F.row_number().over(seq_window))
            sequence_pdf = sequence.toPandas()
            outputs.extend(_write_starter_sequence_outputs(gold_dir, sequence_pdf))
            return outputs
        finally:
            spark.stop()
    except Exception as exc:
        logger.warning("[gold] starter ranking pyspark path failed; fallback to pandas: %s", exc)

    boss_rank = (
        joined.groupby(
            _STARTER_BOSS_GROUP_COLS,
            as_index=False,
        )
        .agg(
            avg_mc_win_rate=("mc_win_rate", "mean"),
            avg_wins=("wins", "mean"),
            avg_losses=("losses", "mean"),
            avg_n_trials=("n_trials", "mean"),
            scenario_rows=("scenario_id", "count"),
            player_avg_level=("player_avg_level", "mean"),
            boss_avg_level=("boss_avg_level", "mean"),
        )
        .sort_values(
            ["effective_game_version", "effective_boss_name", "starter_base", "avg_mc_win_rate", "avg_wins"],
            ascending=[True, True, True, False, False],
        )
    )
    boss_rank["rank_in_boss_starter"] = boss_rank.groupby(["effective_game_version", "effective_boss_name", "starter_base"]).cumcount() + 1
    outputs.extend(_write_starter_boss_outputs(gold_dir, boss_rank))

    sequence_df = joined[joined["boss_stage"].isin(["elite_four", "champion"])].copy()
    if not sequence_df.empty:
        sequence_df["safe_rate"] = sequence_df["mc_win_rate"].clip(lower=1e-6)
        sequence_df["log_rate"] = sequence_df["safe_rate"].map(lambda value: math.log(float(value)))
        sequence_rank = (
            sequence_df.groupby(["effective_game_version", "starter_base", "player_team_id"], as_index=False)
            .agg(
                mean_log_rate=("log_rate", "mean"),
                mean_mc_win_rate=("mc_win_rate", "mean"),
                bosses_covered=("effective_boss_name", "nunique"),
                degraded_ratio=("degraded_data", "mean"),
            )
        )
        sequence_rank["sequence_win_rate"] = sequence_rank["mean_log_rate"].map(lambda value: math.exp(float(value)))
        sequence_rank["degraded_ratio"] = sequence_rank["degraded_ratio"].fillna(0.0)
        sequence_rank["sequence_score"] = sequence_rank["sequence_win_rate"] * (1.0 - sequence_rank["degraded_ratio"] * 0.2)
        sequence_rank = sequence_rank.sort_values(
            ["effective_game_version", "starter_base", "sequence_score", "sequence_win_rate"],
            ascending=[True, True, False, False],
        )
        sequence_rank["rank_in_sequence"] = sequence_rank.groupby(["effective_game_version", "starter_base"]).cumcount() + 1
        outputs.extend(_write_starter_sequence_outputs(gold_dir, sequence_rank))

    return outputs


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

    spark = None
    try:
        spark = (
            SparkSession.builder
            .appName("pokemon-gold-aggregations")
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        spark_df = spark.createDataFrame(silver_df)

        progression_df = (
            spark_df.orderBy("game_version", "part")
            .groupBy("game_version")
            .agg(
                F.count("boss_name").alias("boss_steps"),
                F.max("reachable_location_count").alias("max_reachable_locations"),
                F.max(F.struct(F.col("part"), F.col("reachable_location_count"))).alias("_last_reachable"),
            )
            .select(
                "game_version",
                "boss_steps",
                F.col("_last_reachable.reachable_location_count").alias("final_reachable_locations"),
                "max_reachable_locations",
            )
            .orderBy(F.col("final_reachable_locations").desc())
        )
        progression_pdf = progression_df.toPandas().rename(columns={"game_version": "game"})
        progression_pdf.to_csv(gold_dir / "game_progression_summary.csv", index=False)
        logger.info("[gold] wrote game_progression_summary.csv rows=%s (spark)", len(progression_pdf))

        location_popularity_df = (
            spark_df.select("game_version", F.explode_outer("reachable_locations").alias("location_slug"))
            .where(F.col("location_slug").isNotNull())
            .groupBy("location_slug")
            .agg(
                F.countDistinct("game_version").alias("game_count"),
                F.count("game_version").alias("total_mentions"),
            )
            .orderBy(F.col("game_count").desc(), F.col("total_mentions").desc())
        )
        write_parquet(gold_dir / "location_popularity.parquet", location_popularity_df.toPandas())
        logger.info(
            "[gold] wrote location_popularity.parquet rows=%s (spark)",
            location_popularity_df.count(),
        )
        return True
    except Exception as exc:
        logger.warning("[gold] pyspark aggregation failed; using pandas fallback: %s", exc)
        return False
    finally:
        if spark is not None:
            spark.stop()


def build_gold_from_silver(silver_dir: Path = SILVER_DIR, gold_dir: Path = GOLD_DIR) -> None:
    ensure_medallion_dirs()
    logger.info("[gold] build start silver_dir=%s gold_dir=%s", silver_dir, gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)
    gold_subdirs = get_gold_subdirs(gold_dir)
    gold_simulation_dir = gold_subdirs["simulation"]
    gold_simulation_dir.mkdir(parents=True, exist_ok=True)

    contract = _load_and_validate_gold_contract(silver_dir)
    manifest_snapshot_files = contract["snapshot_files"]
    required_files: dict[str, Path | list[Path]] = contract["required_files"]
    logger.info("[gold] using strict manifest snapshot inputs count=%s", len(manifest_snapshot_files))

    simulation_kwargs: dict[str, Any] = {
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
        "required_input_files": {
            "teams": required_files["simulation_inputs_teams"],
            "team_members": required_files["source_team_members"],
            "member_move_options": required_files["member_move_options"],
        },
    }
    run_gold_simulation_from_silver(**simulation_kwargs)

    game_files = manifest_snapshot_files

    logger.info("[gold] loading %s silver snapshot files", len(game_files))

    frames: list[pd.DataFrame] = []
    for file_path in game_files:
        logger.info("[gold] reading %s", file_path.name)
        frames.append(read_jsonl(file_path))

    silver_df = pd.concat(frames, ignore_index=True)
    silver_df = _normalize_game_key_to_game_version(silver_df, source_name="silver boss snapshots")
    logger.info("[gold] loaded silver rows=%s", len(silver_df))

    used_spark_for_core = _build_core_aggregations_with_spark(silver_df=silver_df, gold_dir=gold_dir)
    if not used_spark_for_core:
        progression = (
            silver_df.sort_values(["game_version", "part"])
            .groupby("game_version", as_index=False)
            .agg(
                boss_steps=("boss_name", "count"),
                final_reachable_locations=("reachable_location_count", "last"),
                max_reachable_locations=("reachable_location_count", "max"),
            )
            .sort_values("final_reachable_locations", ascending=False)
        )
        progression = progression.rename(columns={"game_version": "game"})
        progression.to_csv(gold_dir / "game_progression_summary.csv", index=False)
        logger.info("[gold] wrote game_progression_summary.csv rows=%s (pandas fallback)", len(progression))

        exploded = silver_df[["game_version", "reachable_locations"]].explode("reachable_locations")
        exploded = exploded.rename(columns={"reachable_locations": "location_slug"}).dropna()

        location_popularity = (
            exploded.groupby("location_slug", as_index=False)
            .agg(game_count=("game_version", "nunique"), total_mentions=("game_version", "count"))
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
                    same_version_df = _apply_level_plausibility_filter(same_version_df)

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

            starter_outputs = _build_starter_rankings_from_monte_carlo(
                gold_dir=gold_dir,
                gold_simulation_dir=gold_simulation_dir,
            )
            gold_outputs.extend(starter_outputs)

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
        "silver_manifest_used": True,
        "gold_simulation_dir": str(gold_simulation_dir.relative_to(gold_dir)),
        "gold_outputs": gold_outputs,
    }
    write_json(gold_dir / "manifest.json", manifest)

    logger.info("[gold] wrote %s datasets from %s silver files", len(gold_outputs), len(game_files))

if __name__ == "__main__":
    build_gold_from_silver()
