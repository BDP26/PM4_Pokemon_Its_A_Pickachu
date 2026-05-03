from pathlib import Path
import logging
from typing import Any, NoReturn, cast

import pandas as pd

from src.pipeline.common.contracts import format_contract_error
from src.pipeline.common.io import read_parquet, write_json, write_parquet
from src.pipeline.common.manifest import (
    ManifestResolutionError,
    load_manifest,
    resolve_manifest_dataset_path,
)
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

_NON_YELLOW_EXCLUDED_VERSIONS = {"yellow"}
_PLAUSIBILITY_GROUP_COLS = ["effective_game_version", "effective_boss_name", "starter_base"]
_GS_POSTGAME_EXCLUDED_BOSSES = {"lt. surge", "sabrina", "misty", "erika", "janine", "brock", "blaine", "blue"}
_STRIATON_REQUIRED_STARTER_BY_BOSS = {"chili": "snivy", "cilan": "oshawott", "cress": "tepig"}


class GoldContractError(ValueError):
    """Raised when Silver->Gold manifest contract validation fails."""


_REQUIRED_MANIFEST_DATASET_FILES = (
    "pokemon_data",
    "move_data",
    "simulation_inputs_teams",
    "source_team_members",
    "member_moveset_combos",
)
_STRICT_SHARDED_DATASET_KEYS = {
    "simulation_inputs_teams",
    "source_team_members",
    "member_moveset_combos",
}


def _raise_contract_error(code: str, message: str, *, dataset: str | None = None, path: Path | None = None) -> NoReturn:
    raise GoldContractError(
        format_contract_error(
            prefix="gold.contract",
            code=code,
            message=message,
            dataset=dataset,
            path=path,
        )
    )


def _apply_level_plausibility_filter(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if "player_avg_level" not in df.columns:
        return df

    player_avg_level = pd.to_numeric(df["player_avg_level"], errors="coerce")
    if "player_max_level" in df.columns:
        player_level_cap = pd.to_numeric(df["player_max_level"], errors="coerce")
    elif "boss_ace_level" in df.columns:
        player_level_cap = pd.to_numeric(df["boss_ace_level"], errors="coerce")
    else:
        return df

    # Silver now persists the hard player level cap directly. Gold should only
    # reject rows that exceed that cap.
    level_mask = player_avg_level.notna() & player_level_cap.notna() & (player_avg_level <= player_level_cap)
    filtered = df[level_mask].copy()
    # Fallback to the original frame if constraints remove all rows.
    return filtered if not filtered.empty else df


def _supported_non_yellow_boss_starter_groups() -> pd.DataFrame:
    records: list[dict[str, str]] = []
    for game in get_games_config():
        version = str(game.get("game_key") or "").strip().lower()
        if not version or version in _NON_YELLOW_EXCLUDED_VERSIONS:
            continue
        starters = [str(starter).strip().lower() for starter in game.get("starter_choices", []) if str(starter).strip()]
        bosses = [str(boss).strip().lower() for boss in game.get("bosses", []) if str(boss).strip()]
        for boss_name in bosses:
            if version in {"gold", "silver"} and boss_name in _GS_POSTGAME_EXCLUDED_BOSSES:
                continue

            expected_starters = starters
            if version in {"black", "white"} and boss_name in _STRIATON_REQUIRED_STARTER_BY_BOSS:
                expected_starters = [_STRIATON_REQUIRED_STARTER_BY_BOSS[boss_name]]

            for starter_base in expected_starters:
                records.append(
                    {
                        "game_version": version,
                        "boss_name": boss_name,
                        "starter_base": starter_base,
                    }
                )
    return pd.DataFrame.from_records(records).drop_duplicates(ignore_index=True)


def _build_plausibility_filter_diagnostics(
    *,
    joined_before_filter: pd.DataFrame,
    joined_after_filter: pd.DataFrame,
) -> pd.DataFrame:
    expected = _supported_non_yellow_boss_starter_groups()
    if expected.empty:
        return pd.DataFrame(
            columns=[
                "game_version",
                "boss_name",
                "starter_base",
                "rows_before_plausibility_filter",
                "rows_after_plausibility_filter",
                "rows_single_boss_mode",
                "rows_gauntlet_mode",
                "rows_removed",
                "rows_exceeding_level_cap",
                "rows_missing_level_cap_metadata",
                "status",
                "removal_reason",
            ]
        )

    before = joined_before_filter.copy()
    after = joined_after_filter.copy()
    for frame in (before, after):
        if frame.empty:
            continue
        frame["effective_game_version"] = frame["effective_game_version"].astype(str).str.strip().str.lower()
        frame["effective_boss_name"] = frame["effective_boss_name"].astype(str).str.strip().str.lower()
        frame["starter_base"] = frame["starter_base"].astype(str).str.strip().str.lower()

    before_counts = (
        before.groupby(_PLAUSIBILITY_GROUP_COLS, as_index=False)
        .size()
        .rename(columns={"size": "rows_before_plausibility_filter"})
    )
    after_counts = (
        after.groupby(_PLAUSIBILITY_GROUP_COLS, as_index=False)
        .size()
        .rename(columns={"size": "rows_after_plausibility_filter"})
    )

    exceed_counts = pd.DataFrame(columns=[*_PLAUSIBILITY_GROUP_COLS, "rows_exceeding_level_cap"])
    missing_cap_counts = pd.DataFrame(columns=[*_PLAUSIBILITY_GROUP_COLS, "rows_missing_level_cap_metadata"])
    single_mode_counts = pd.DataFrame(columns=[*_PLAUSIBILITY_GROUP_COLS, "rows_single_boss_mode"])
    gauntlet_mode_counts = pd.DataFrame(columns=[*_PLAUSIBILITY_GROUP_COLS, "rows_gauntlet_mode"])
    if not before.empty:
        player_avg_level = pd.to_numeric(before.get("player_avg_level"), errors="coerce")
        if "player_max_level" in before.columns:
            player_level_cap = pd.to_numeric(before.get("player_max_level"), errors="coerce")
        else:
            player_level_cap = pd.to_numeric(before.get("boss_ace_level"), errors="coerce")
        exceed_mask = player_avg_level.notna() & player_level_cap.notna() & (player_avg_level > player_level_cap)
        missing_cap_mask = player_avg_level.notna() & player_level_cap.isna()
        if bool(exceed_mask.any()):
            exceed_counts = (
                before.loc[exceed_mask]
                .groupby(_PLAUSIBILITY_GROUP_COLS, as_index=False)
                .size()
                .rename(columns={"size": "rows_exceeding_level_cap"})
            )
        if bool(missing_cap_mask.any()):
            missing_cap_counts = (
                before.loc[missing_cap_mask]
                .groupby(_PLAUSIBILITY_GROUP_COLS, as_index=False)
                .size()
                .rename(columns={"size": "rows_missing_level_cap_metadata"})
            )
    if not after.empty and "simulation_mode" in after.columns:
        single_mode_mask = after["simulation_mode"].fillna("gym").isin(["gym", "boss"])
        gauntlet_mode_mask = after["simulation_mode"].fillna("").eq("gauntlet")
        if bool(single_mode_mask.any()):
            single_mode_counts = (
                after.loc[single_mode_mask]
                .groupby(_PLAUSIBILITY_GROUP_COLS, as_index=False)
                .size()
                .rename(columns={"size": "rows_single_boss_mode"})
            )
        if bool(gauntlet_mode_mask.any()):
            gauntlet_mode_counts = (
                after.loc[gauntlet_mode_mask]
                .groupby(_PLAUSIBILITY_GROUP_COLS, as_index=False)
                .size()
                .rename(columns={"size": "rows_gauntlet_mode"})
            )

    diagnostics = expected.rename(
        columns={"game_version": "effective_game_version", "boss_name": "effective_boss_name"}
    )
    diagnostics = diagnostics.merge(before_counts, on=_PLAUSIBILITY_GROUP_COLS, how="left")
    diagnostics = diagnostics.merge(after_counts, on=_PLAUSIBILITY_GROUP_COLS, how="left")
    diagnostics = diagnostics.merge(single_mode_counts, on=_PLAUSIBILITY_GROUP_COLS, how="left")
    diagnostics = diagnostics.merge(gauntlet_mode_counts, on=_PLAUSIBILITY_GROUP_COLS, how="left")
    diagnostics = diagnostics.merge(exceed_counts, on=_PLAUSIBILITY_GROUP_COLS, how="left")
    diagnostics = diagnostics.merge(missing_cap_counts, on=_PLAUSIBILITY_GROUP_COLS, how="left")

    count_columns = [
        "rows_before_plausibility_filter",
        "rows_after_plausibility_filter",
        "rows_single_boss_mode",
        "rows_gauntlet_mode",
        "rows_exceeding_level_cap",
        "rows_missing_level_cap_metadata",
    ]
    for column in count_columns:
        diagnostics[column] = pd.to_numeric(diagnostics[column], errors="coerce").fillna(0).astype(int)
    diagnostics["rows_removed"] = (
        diagnostics["rows_before_plausibility_filter"] - diagnostics["rows_after_plausibility_filter"]
    ).clip(lower=0)

    diagnostics["status"] = "ok"
    diagnostics.loc[diagnostics["rows_before_plausibility_filter"] == 0, "status"] = "no_valid_team_generated"
    diagnostics.loc[
        (diagnostics["rows_before_plausibility_filter"] > 0)
        & (diagnostics["rows_after_plausibility_filter"] == 0),
        "status",
    ] = "all_rows_removed_by_level_cap"
    diagnostics.loc[
        (diagnostics["rows_after_plausibility_filter"] > 0) & (diagnostics["rows_removed"] > 0),
        "status",
    ] = "rows_removed_by_level_cap"

    diagnostics["removal_reason"] = ""
    diagnostics.loc[
        diagnostics["status"] == "no_valid_team_generated",
        "removal_reason",
    ] = "no_valid_team_generated"
    diagnostics.loc[
        diagnostics["status"] == "all_rows_removed_by_level_cap",
        "removal_reason",
    ] = "all_rows_exceeded_silver_level_cap"
    diagnostics.loc[
        diagnostics["status"] == "rows_removed_by_level_cap",
        "removal_reason",
    ] = "some_rows_exceeded_silver_level_cap"
    diagnostics.loc[
        (diagnostics["rows_missing_level_cap_metadata"] > 0) & diagnostics["removal_reason"].eq(""),
        "removal_reason",
    ] = "missing_level_cap_metadata"

    diagnostics = diagnostics.rename(
        columns={"effective_game_version": "game_version", "effective_boss_name": "boss_name"}
    ).sort_values(["game_version", "boss_name", "starter_base"]).reset_index(drop=True)
    return diagnostics


def _write_plausibility_filter_diagnostics(gold_dir: Path, diagnostics_df: pd.DataFrame) -> str:
    debug_dir = gold_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    filename = "ranking_plausibility_filter_diagnostics.parquet"
    write_parquet(debug_dir / filename, diagnostics_df)
    return str(Path("debug") / filename)


def _build_mode_coverage_diagnostics(
    *,
    joined_before_filter: pd.DataFrame,
    joined_after_filter: pd.DataFrame,
) -> pd.DataFrame:
    columns = [
        "game_version",
        "boss_name",
        "simulation_mode",
        "rows_before_filter",
        "rows_after_filter",
        "distinct_player_teams_after_filter",
        "max_mc_win_rate_after_filter",
        "mean_mc_win_rate_after_filter",
        "has_positive_win_rate_after_filter",
    ]
    if joined_before_filter.empty and joined_after_filter.empty:
        return pd.DataFrame(columns=columns)

    before = joined_before_filter.copy()
    before["game_version"] = before["effective_game_version"].astype(str).str.strip().str.lower()
    before["boss_name"] = before["effective_boss_name"].astype(str).str.strip().str.lower()
    before["simulation_mode"] = before["simulation_mode"].fillna("gym").astype(str).str.strip().str.lower()

    group_cols = ["game_version", "boss_name", "simulation_mode"]
    before_counts = (
        before.groupby(group_cols, as_index=False)
        .size()
        .rename(columns={"size": "rows_before_filter"})
    )

    if joined_after_filter.empty:
        diagnostics = before_counts.copy()
        diagnostics["rows_after_filter"] = 0
        diagnostics["distinct_player_teams_after_filter"] = 0
        diagnostics["max_mc_win_rate_after_filter"] = 0.0
        diagnostics["mean_mc_win_rate_after_filter"] = 0.0
        diagnostics["has_positive_win_rate_after_filter"] = False
        return diagnostics[columns].sort_values(group_cols).reset_index(drop=True)

    after = joined_after_filter.copy()
    after["game_version"] = after["effective_game_version"].astype(str).str.strip().str.lower()
    after["boss_name"] = after["effective_boss_name"].astype(str).str.strip().str.lower()
    after["simulation_mode"] = after["simulation_mode"].fillna("gym").astype(str).str.strip().str.lower()
    after["mc_win_rate"] = pd.to_numeric(after["mc_win_rate"], errors="coerce").fillna(0.0)

    after_metrics = (
        after.groupby(group_cols, as_index=False)
        .agg(
            rows_after_filter=("scenario_id", "count"),
            distinct_player_teams_after_filter=("player_team_id", "nunique"),
            max_mc_win_rate_after_filter=("mc_win_rate", "max"),
            mean_mc_win_rate_after_filter=("mc_win_rate", "mean"),
        )
    )
    after_metrics["has_positive_win_rate_after_filter"] = after_metrics["max_mc_win_rate_after_filter"] > 0

    diagnostics = before_counts.merge(after_metrics, on=group_cols, how="left")
    diagnostics["rows_after_filter"] = pd.to_numeric(diagnostics["rows_after_filter"], errors="coerce").fillna(0).astype(int)
    diagnostics["distinct_player_teams_after_filter"] = (
        pd.to_numeric(diagnostics["distinct_player_teams_after_filter"], errors="coerce").fillna(0).astype(int)
    )
    diagnostics["max_mc_win_rate_after_filter"] = pd.to_numeric(
        diagnostics["max_mc_win_rate_after_filter"], errors="coerce"
    ).fillna(0.0)
    diagnostics["mean_mc_win_rate_after_filter"] = pd.to_numeric(
        diagnostics["mean_mc_win_rate_after_filter"], errors="coerce"
    ).fillna(0.0)
    diagnostics["has_positive_win_rate_after_filter"] = diagnostics["has_positive_win_rate_after_filter"].fillna(False).astype(bool)
    return diagnostics[columns].sort_values(group_cols).reset_index(drop=True)


def _write_mode_coverage_diagnostics(gold_dir: Path, diagnostics_df: pd.DataFrame) -> str:
    debug_dir = gold_dir / "debug"
    debug_dir.mkdir(parents=True, exist_ok=True)
    filename = "ranking_mode_coverage_diagnostics.parquet"
    write_parquet(debug_dir / filename, diagnostics_df)
    return str(Path("debug") / filename)


def _validate_non_yellow_starter_boss_coverage(
    *,
    boss_rank: pd.DataFrame,
    diagnostics_df: pd.DataFrame,
) -> None:
    expected = _supported_non_yellow_boss_starter_groups()
    if expected.empty:
        return

    if boss_rank.empty:
        missing = expected.copy()
    else:
        actual = (
            boss_rank[["effective_game_version", "effective_boss_name", "starter_base"]]
            .drop_duplicates()
            .rename(columns={"effective_game_version": "game_version", "effective_boss_name": "boss_name"})
        )
        actual["game_version"] = actual["game_version"].astype(str).str.strip().str.lower()
        actual["boss_name"] = actual["boss_name"].astype(str).str.strip().str.lower()
        actual["starter_base"] = actual["starter_base"].astype(str).str.strip().str.lower()
        missing = expected.merge(actual, on=["game_version", "boss_name", "starter_base"], how="left", indicator=True)
        missing = missing[missing["_merge"] == "left_only"].drop(columns="_merge")

    if missing.empty:
        return

    diagnostics_enriched = diagnostics_df.copy()
    for column in ("rows_single_boss_mode", "rows_gauntlet_mode"):
        if column not in diagnostics_enriched.columns:
            diagnostics_enriched[column] = 0

    diagnostic_subset = diagnostics_enriched[
        [
            "game_version",
            "boss_name",
            "starter_base",
            "rows_before_plausibility_filter",
            "rows_after_plausibility_filter",
            "rows_single_boss_mode",
            "rows_gauntlet_mode",
            "rows_removed",
            "status",
            "removal_reason",
        ]
    ].copy()
    missing = missing.merge(diagnostic_subset, on=["game_version", "boss_name", "starter_base"], how="left")
    for column in ("rows_single_boss_mode", "rows_gauntlet_mode"):
        missing[column] = pd.to_numeric(missing[column], errors="coerce").fillna(0).astype(int)

    tolerated_statuses = {"no_valid_team_generated"}
    is_tolerated_status = missing["status"].isin(tolerated_statuses)
    is_gauntlet_only = (
        missing["status"].eq("ok")
        & missing["rows_after_plausibility_filter"].gt(0)
        & missing["rows_single_boss_mode"].eq(0)
        & missing["rows_gauntlet_mode"].gt(0)
    )
    tolerated_missing = missing[is_tolerated_status | is_gauntlet_only].copy()
    blocking_missing = missing[~(is_tolerated_status | is_gauntlet_only)].copy()

    if blocking_missing.empty:
        logger.warning(
            "[gold] starter/boss ranking coverage incomplete but tolerated missing_groups=%s statuses=%s",
            len(tolerated_missing),
            ",".join(sorted(set(tolerated_missing["status"].dropna().astype(str)))) or "unknown",
        )
        return

    sample = ", ".join(
        (
            f"{row.game_version}/{row.boss_name}/{row.starter_base}"
            f":status={row.status or 'unknown'}"
            f":before={int(row.rows_before_plausibility_filter or 0)}"
            f":after={int(row.rows_after_plausibility_filter or 0)}"
        )
        for row in blocking_missing.head(12).itertuples(index=False)
    )
    raise ValueError(
        "[gold] non-Yellow starter/boss ranking coverage failed "
        f"missing_groups={len(blocking_missing)}"
        f" tolerated_missing_groups={len(tolerated_missing)}"
        f" sample=[{sample}]"
    )


def _load_silver_manifest(silver_dir: Path) -> dict[str, Any]:
    manifest_path = silver_dir / "manifest.json"
    try:
        return load_manifest(manifest_path)
    except FileNotFoundError:
        _raise_contract_error(
            "missing_manifest",
            "Run Silver first to generate manifest.json.",
            path=manifest_path,
        )
    except Exception as exc:
        _raise_contract_error(
            "invalid_manifest_json",
            f"manifest.json is unreadable ({exc}). Rebuild Silver outputs.",
            path=manifest_path,
        )


def _resolve_required_manifest_file(silver_dir: Path, manifest: dict[str, Any], dataset_key: str) -> Path | list[Path]:
    try:
        return resolve_manifest_dataset_path(
            base_dir=silver_dir,
            manifest=manifest,
            dataset_key=dataset_key,
            strict_sharded=dataset_key in _STRICT_SHARDED_DATASET_KEYS,
        )
    except ManifestResolutionError as exc:
        if exc.code == "missing_dataset_entry":
            _raise_contract_error(
                exc.code,
                f"Add datasets.{dataset_key} to silver/manifest.json.",
                dataset=dataset_key,
            )
        if exc.code == "invalid_dataset_files_entry":
            _raise_contract_error(
                exc.code,
                f"datasets.{dataset_key}.files entry must be a non-empty string path.",
                dataset=dataset_key,
                path=exc.path,
            )
        if exc.code == "missing_dataset_files":
            _raise_contract_error(
                exc.code,
                f"Set datasets.{dataset_key}.files in silver/manifest.json.",
                dataset=dataset_key,
            )
        if exc.code == "missing_dataset_file_path":
            _raise_contract_error(
                exc.code,
                f"Set datasets.{dataset_key}.file or datasets.{dataset_key}.files in silver/manifest.json.",
                dataset=dataset_key,
            )
        _raise_contract_error(
            exc.code,
            "Regenerate Silver outputs so all contract files or partition folders exist.",
            dataset=dataset_key,
            path=exc.path,
        )


def _load_and_validate_gold_contract(silver_dir: Path) -> dict[str, Any]:
    manifest = _load_silver_manifest(silver_dir)

    required_files: dict[str, Path | list[Path]] = {}
    for dataset_key in _REQUIRED_MANIFEST_DATASET_FILES:
        required_files[dataset_key] = _resolve_required_manifest_file(silver_dir, manifest, dataset_key)

    logger.info(
        "[gold.contract] validated required_datasets=%s",
        ",".join(sorted(required_files.keys())),
    )
    return {
        "manifest": manifest,
        "required_files": required_files,
    }


def _boss_order_lookup() -> dict[tuple[str, str], tuple[int, int]]:
    lookup: dict[tuple[str, str], tuple[int, int]] = {}
    for game in get_games_config():
        version = str(game.get("game_key") or "").strip().lower()
        bosses = [str(name).strip() for name in game.get("bosses", []) if str(name).strip()]
        total = len(bosses)
        for idx, boss in enumerate(bosses, start=1):
            lookup[(version, boss.lower())] = (idx, total)
    return lookup


def _sequence_level_from_levels(value: Any) -> int | None:
    if isinstance(value, list) and value:
        return max(int(level or 0) for level in value)
    if isinstance(value, tuple) and value:
        return max(int(level or 0) for level in value)
    if hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list) and converted:
            return max(int(level or 0) for level in converted)
    return None


def _join_simulation_context(monte_carlo_df: pd.DataFrame, teams_df: pd.DataFrame) -> pd.DataFrame:
    teams = teams_df.copy()
    if "progression_depth" not in teams.columns:
        teams["progression_depth"] = None
    if "boss_ace_level" not in teams.columns:
        teams["boss_ace_level"] = None
    if "player_max_level" not in teams.columns:
        teams["player_max_level"] = teams.get("boss_ace_level")
    if "level_cap_offset" not in teams.columns:
        teams["level_cap_offset"] = None
    if "progression_pool_id" not in teams.columns:
        teams["progression_pool_id"] = None

    player_context = teams[
        [
            "team_id",
            "game_version",
            "boss_id",
            "boss_name",
            "starter_base",
            "starter_evolved_species",
            "avg_level",
            "player_max_level",
            "levels",
            "pokemon",
            "progression_depth",
            "boss_ace_level",
            "level_cap_offset",
            "source_team_id",
            "progression_pool_id",
        ]
    ].rename(
        columns={
            "team_id": "player_team_id",
            "game_version": "player_game_version",
            "boss_id": "player_source_boss_id",
            "boss_name": "player_source_boss_name",
            "avg_level": "player_avg_level",
            "levels": "player_team_levels",
            "pokemon": "player_team_species",
            "progression_depth": "player_progression_depth",
            "source_team_id": "progression_source_team_id",
        }
    )
    boss_context = teams[
        [
            "team_id",
            "boss_id",
            "boss_name",
            "game_version",
            "avg_level",
            "levels",
            "pokemon",
            "progression_depth",
        ]
    ].rename(
        columns={
            "team_id": "boss_team_id",
            "boss_id": "target_boss_id",
            "boss_name": "target_boss_name",
            "game_version": "boss_game_version",
            "avg_level": "boss_avg_level",
            "levels": "boss_team_levels",
            "pokemon": "boss_team_species",
            "progression_depth": "boss_progression_depth",
        }
    )
    boss_context["target_boss_ace_level"] = boss_context["boss_team_levels"].map(_sequence_level_from_levels)

    joined = monte_carlo_df.merge(player_context, on="player_team_id", how="left")
    joined = joined.merge(boss_context, on="boss_team_id", how="left")
    joined["effective_game_version"] = joined["boss_game_version"].fillna(joined.get("game_version")).fillna(joined["player_game_version"])
    joined["effective_boss_name"] = joined["target_boss_name"].fillna(joined.get("boss_name"))
    joined["source_target_boss_match"] = joined["player_source_boss_id"] == joined["target_boss_id"]
    return joined


def _assert_gold_simulation_artifact_consistency(gold_simulation_dir: Path) -> None:
    teams_path = gold_simulation_dir / "teams.parquet"
    team_battles_path = gold_simulation_dir / "team_battle_simulations.parquet"
    seeds_path = gold_simulation_dir / "battle_seeds.parquet"
    monte_carlo_path = gold_simulation_dir / "monte_carlo_results.parquet"

    required = [teams_path, team_battles_path, seeds_path, monte_carlo_path]
    missing = [path.name for path in required if not path.exists()]
    if missing:
        _raise_contract_error(
            "missing_gold_simulation_artifacts",
            f"Missing Gold simulation artifacts: {missing}",
            path=gold_simulation_dir,
        )

    teams_df = read_parquet(teams_path)
    team_battles_df = read_parquet(team_battles_path)
    seeds_df = read_parquet(seeds_path)
    monte_carlo_df = read_parquet(monte_carlo_path)

    team_ids = set(teams_df.get("team_id", pd.Series(dtype="object")).dropna().astype(str))
    if not team_ids:
        _raise_contract_error(
            "empty_gold_team_snapshot",
            "teams.parquet is empty or missing team_id values.",
            path=teams_path,
        )

    def _invalid_refs(frame: pd.DataFrame, column: str) -> list[str]:
        if column not in frame.columns:
            return [f"missing column {column}"]
        values = frame[column].dropna().astype(str)
        invalid = sorted(set(values) - team_ids)
        return invalid[:10]

    issues: list[str] = []
    invalid_attacker = _invalid_refs(team_battles_df, "team_id_attacker")
    if invalid_attacker:
        issues.append(f"team_battle_simulations invalid attacker ids examples={invalid_attacker}")
    invalid_defender = _invalid_refs(team_battles_df, "team_id_defender")
    if invalid_defender:
        issues.append(f"team_battle_simulations invalid defender ids examples={invalid_defender}")
    invalid_seed_players = _invalid_refs(seeds_df, "player_team_id")
    if invalid_seed_players:
        issues.append(f"battle_seeds invalid player ids examples={invalid_seed_players}")
    invalid_seed_bosses = _invalid_refs(seeds_df, "boss_team_id")
    if invalid_seed_bosses:
        issues.append(f"battle_seeds invalid boss ids examples={invalid_seed_bosses}")
    invalid_mc_players = _invalid_refs(monte_carlo_df, "player_team_id")
    if invalid_mc_players:
        issues.append(f"monte_carlo invalid player ids examples={invalid_mc_players}")
    invalid_mc_bosses = _invalid_refs(monte_carlo_df, "boss_team_id")
    if invalid_mc_bosses:
        issues.append(f"monte_carlo invalid boss ids examples={invalid_mc_bosses}")

    if "scenario_id" in seeds_df.columns and "scenario_id" in monte_carlo_df.columns:
        seed_ids = set(seeds_df["scenario_id"].dropna().astype(str))
        mc_ids = set(monte_carlo_df["scenario_id"].dropna().astype(str))
        missing_mc_ids = sorted(seed_ids - mc_ids)[:10]
        missing_seed_ids = sorted(mc_ids - seed_ids)[:10]
        if missing_mc_ids:
            issues.append(f"battle_seeds scenario_ids missing from monte_carlo examples={missing_mc_ids}")
        if missing_seed_ids:
            issues.append(f"monte_carlo scenario_ids missing from battle_seeds examples={missing_seed_ids}")

    if len(monte_carlo_df) != len(seeds_df):
        issues.append(
            f"monte_carlo_results row count mismatch monte_carlo={len(monte_carlo_df)} battle_seeds={len(seeds_df)}"
        )

    if issues:
        _raise_contract_error(
            "inconsistent_gold_simulation_snapshot",
            "Gold simulation artifacts are out of sync. Rebuild the Gold simulation outputs together. "
            + " | ".join(issues),
            path=gold_simulation_dir,
        )


def _attach_group_diagnostics(
    rows: pd.DataFrame,
    *,
    group_cols: list[str],
    win_rate_col: str,
) -> pd.DataFrame:
    if rows.empty:
        return rows

    metrics = (
        rows.groupby(group_cols, as_index=False)
        .agg(
            candidate_team_count=("player_team_id", "nunique"),
            simulated_team_count=(win_rate_col, lambda s: int(s.notna().sum())),
            viable_team_count=(win_rate_col, lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0) > 0).sum())),
            best_win_pct=(win_rate_col, "max"),
            null_win_pct_count=(win_rate_col, lambda s: int(s.isna().sum())),
            zero_win_pct_count=(win_rate_col, lambda s: int((pd.to_numeric(s, errors="coerce").fillna(0) == 0).sum())),
        )
    )
    return rows.merge(metrics, on=group_cols, how="left")


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
    joined = _join_simulation_context(monte_carlo_df, teams_df)

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
    outputs: list[str] = []
    filtered_joined = _apply_level_plausibility_filter(joined)
    diagnostics_df = _build_plausibility_filter_diagnostics(
        joined_before_filter=joined,
        joined_after_filter=filtered_joined,
    )
    outputs.append(_write_plausibility_filter_diagnostics(gold_dir, diagnostics_df))
    mode_diagnostics_df = _build_mode_coverage_diagnostics(
        joined_before_filter=joined,
        joined_after_filter=filtered_joined,
    )
    outputs.append(_write_mode_coverage_diagnostics(gold_dir, mode_diagnostics_df))

    joined = filtered_joined
    if joined.empty:
        return outputs

    single_boss_rows = joined[
        joined["source_target_boss_match"].fillna(False)
        & joined["simulation_mode"].fillna("gym").isin(["gym", "boss"])
    ].copy()
    if not single_boss_rows.empty:
        boss_rank = (
            single_boss_rows.groupby(_STARTER_BOSS_GROUP_COLS, as_index=False)
            .agg(
                avg_mc_win_rate=("mc_win_rate", "mean"),
                avg_wins=("wins", "mean"),
                avg_losses=("losses", "mean"),
                avg_n_trials=("n_trials", "mean"),
                scenario_rows=("scenario_id", "count"),
                player_avg_level=("player_avg_level", "mean"),
                player_max_level=("player_max_level", "max"),
                boss_avg_level=("boss_avg_level", "mean"),
                boss_ace_level=("target_boss_ace_level", "max"),
                progression_depth=("player_progression_depth", "max"),
                source_boss_id=("player_source_boss_id", "first"),
                source_boss_name=("player_source_boss_name", "first"),
                team_species=("player_team_species", "first"),
                team_levels=("player_team_levels", "first"),
                progression_pool_id=("progression_pool_id", "first"),
            )
        )
        boss_rank = _attach_group_diagnostics(
            boss_rank,
            group_cols=[
                "effective_game_version",
                "effective_boss_name",
                "boss_team_id",
                "starter_base",
                "starter_evolved_species",
            ],
            win_rate_col="avg_mc_win_rate",
        )
        boss_rank = boss_rank.sort_values(
            ["effective_game_version", "effective_boss_name", "starter_base", "avg_mc_win_rate", "avg_wins", "player_team_id"],
            ascending=[True, True, True, False, False, True],
        )
        boss_rank["rank_in_boss_starter"] = boss_rank.groupby(["effective_game_version", "effective_boss_name", "starter_base"]).cumcount() + 1
        _validate_non_yellow_starter_boss_coverage(boss_rank=boss_rank, diagnostics_df=diagnostics_df)
        outputs.extend(_write_starter_boss_outputs(gold_dir, boss_rank))

    sequence_df = joined[joined["simulation_mode"].fillna("").eq("gauntlet")].copy()
    if not sequence_df.empty:
        sequence_df["sequence_position"] = pd.to_numeric(sequence_df["sequence_position"], errors="coerce")
        sequence_df["mc_win_rate"] = pd.to_numeric(sequence_df["mc_win_rate"], errors="coerce").fillna(0.0)
        sequence_df["n_trials"] = pd.to_numeric(sequence_df.get("n_trials"), errors="coerce").fillna(0).astype(int)
        sequence_df["gauntlet_success_rate"] = pd.to_numeric(
            sequence_df.get("gauntlet_success_rate"),
            errors="coerce",
        ).fillna(0.0)
        sequence_df["sequence_win_rate_fallback"] = (
            sequence_df.groupby(["effective_game_version", "starter_base", "player_team_id"])["mc_win_rate"]
            .transform("prod")
            .fillna(0.0)
        )
        sequence_df["sequence_expected_wins_fallback"] = (
            sequence_df.groupby(["effective_game_version", "starter_base", "player_team_id"])["mc_win_rate"]
            .transform("sum")
            .fillna(0.0)
        )
        if "gauntlet_success_rate" in sequence_df.columns and sequence_df["gauntlet_success_rate"].notna().any():
            sequence_rank = (
                sequence_df.groupby(["effective_game_version", "starter_base", "player_team_id"], as_index=False)
                .agg(
                    strict_clear_rate=("gauntlet_success_rate", "max"),
                    mean_mc_win_rate=("mc_win_rate", "mean"),
                    bosses_covered=("sequence_position", lambda s: int(s.dropna().nunique())),
                    sequence_n_trials=("n_trials", "max"),
                    sequence_completion_prob=("sequence_win_rate_fallback", "max"),
                    sequence_expected_wins=("sequence_expected_wins_fallback", "max"),
                    player_avg_level=("player_avg_level", "mean"),
                    player_max_level=("player_max_level", "max"),
                    progression_depth=("player_progression_depth", "max"),
                    source_boss_id=("player_source_boss_id", "first"),
                    source_boss_name=("player_source_boss_name", "first"),
                    team_species=("player_team_species", "first"),
                    team_levels=("player_team_levels", "first"),
                    progression_pool_id=("progression_pool_id", "first"),
                )
            )
        else:
            sequence_rank = (
                sequence_df.groupby(["effective_game_version", "starter_base", "player_team_id"], as_index=False)
                .agg(
                    strict_clear_rate=("mc_win_rate", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).prod())),
                    mean_mc_win_rate=("mc_win_rate", "mean"),
                    bosses_covered=("sequence_position", lambda s: int(s.dropna().nunique())),
                    sequence_n_trials=("n_trials", "max"),
                    sequence_completion_prob=("mc_win_rate", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).prod())),
                    sequence_expected_wins=("mc_win_rate", lambda s: float(pd.to_numeric(s, errors="coerce").fillna(0).sum())),
                    player_avg_level=("player_avg_level", "mean"),
                    player_max_level=("player_max_level", "max"),
                    progression_depth=("player_progression_depth", "max"),
                    source_boss_id=("player_source_boss_id", "first"),
                    source_boss_name=("player_source_boss_name", "first"),
                    team_species=("player_team_species", "first"),
                    team_levels=("player_team_levels", "first"),
                    progression_pool_id=("progression_pool_id", "first"),
                )
            )
        sequence_rank["sequence_win_rate"] = pd.to_numeric(
            sequence_rank["sequence_completion_prob"],
            errors="coerce",
        ).fillna(0.0)
        sequence_rank["sequence_n_trials"] = (
            pd.to_numeric(sequence_rank["sequence_n_trials"], errors="coerce").fillna(0).astype(int)
        )
        sequence_rank["bosses_covered"] = pd.to_numeric(
            sequence_rank["bosses_covered"],
            errors="coerce",
        ).fillna(0).astype(int)
        sequence_rank["sequence_expected_wins"] = pd.to_numeric(
            sequence_rank["sequence_expected_wins"],
            errors="coerce",
        ).fillna(0.0)
        sequence_rank["sequence_expected_wins_pct"] = (
            sequence_rank["sequence_expected_wins"]
            / sequence_rank["bosses_covered"].clip(lower=1)
        )
        sequence_rank["sequence_wins"] = (
            (pd.to_numeric(sequence_rank["strict_clear_rate"], errors="coerce").fillna(0.0)
             * sequence_rank["sequence_n_trials"])
            .round()
            .astype(int)
        )
        sequence_rank = _attach_group_diagnostics(
            sequence_rank,
            group_cols=["effective_game_version", "starter_base"],
            win_rate_col="sequence_win_rate",
        )
        progression_signal = pd.to_numeric(sequence_rank["progression_depth"], errors="coerce").fillna(0.5).clip(0.0, 1.0)
        early_weight = 1.0 - progression_signal
        late_weight = progression_signal
        sequence_rank["sequence_score"] = (
            (0.35 + 0.45 * late_weight) * sequence_rank["sequence_completion_prob"]
            + (0.25 + 0.35 * early_weight) * sequence_rank["sequence_expected_wins_pct"]
            + 0.10 * pd.to_numeric(sequence_rank["strict_clear_rate"], errors="coerce").fillna(0.0)
        )
        sequence_rank = sequence_rank.sort_values(
            ["effective_game_version", "starter_base", "sequence_score", "sequence_completion_prob", "sequence_expected_wins_pct", "mean_mc_win_rate", "player_team_id"],
            ascending=[True, True, False, False, False, False, True],
        )
        sequence_rank["rank_in_sequence"] = sequence_rank.groupby(["effective_game_version", "starter_base"]).cumcount() + 1
        outputs.extend(_write_starter_sequence_outputs(gold_dir, sequence_rank))

    return outputs
def build_gold_from_silver(silver_dir: Path = SILVER_DIR, gold_dir: Path = GOLD_DIR) -> None:
    ensure_medallion_dirs()
    logger.info("[gold] build start silver_dir=%s gold_dir=%s", silver_dir, gold_dir)
    gold_dir.mkdir(parents=True, exist_ok=True)
    gold_subdirs = get_gold_subdirs(gold_dir)
    gold_simulation_dir = gold_subdirs["simulation"]
    gold_simulation_dir.mkdir(parents=True, exist_ok=True)

    contract = _load_and_validate_gold_contract(silver_dir)
    required_files: dict[str, Path | list[Path]] = contract["required_files"]

    simulation_kwargs: dict[str, Any] = {
        "silver_dir": silver_dir,
        "gold_dir": gold_dir,
        "required_input_files": {
            "teams": required_files["simulation_inputs_teams"],
            "team_members": required_files["source_team_members"],
            "member_moveset_combos": required_files["member_moveset_combos"],
        },
    }
    run_gold_simulation_from_silver(**simulation_kwargs)

    gold_outputs: list[str] = []

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
        _assert_gold_simulation_artifact_consistency(gold_simulation_dir)
        monte_carlo_df = read_parquet(monte_carlo_path)
        if not monte_carlo_df.empty:
            teams_path = gold_simulation_dir / "teams.parquet"
            joined_monte_carlo_df = monte_carlo_df.copy()
            if teams_path.exists():
                teams_df = read_parquet(teams_path)
                joined_monte_carlo_df = _join_simulation_context(monte_carlo_df, teams_df)

            team_recommendations = (
                joined_monte_carlo_df.groupby("player_team_id", as_index=False)
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

            if "player_game_version" in joined_monte_carlo_df.columns:
                same_version_df = joined_monte_carlo_df[
                    joined_monte_carlo_df["player_game_version"] == joined_monte_carlo_df["game_version"]
                ].copy()
                same_version_df = same_version_df[
                    same_version_df["source_target_boss_match"].fillna(False)
                    & same_version_df["simulation_mode"].fillna("gym").isin(["gym", "boss"])
                ].copy()
                if not same_version_df.empty:
                    same_version_df = _apply_level_plausibility_filter(same_version_df)

                    rankings = same_version_df.sort_values(
                        ["game_version", "boss_name", "boss_team_id", "mc_win_rate", "wins", "player_team_id"],
                        ascending=[True, True, True, False, False, True],
                    )
                    rankings = _attach_group_diagnostics(
                        rankings,
                        group_cols=["game_version", "boss_name", "boss_team_id"],
                        win_rate_col="mc_win_rate",
                    )
                    rankings["rank_in_boss_version"] = rankings.groupby("boss_team_id").cumcount() + 1

                    ranking_cols = [
                        "boss_team_id",
                        "target_boss_id",
                        "boss_name",
                        "game_version",
                        "player_team_id",
                        "player_source_boss_id",
                        "player_source_boss_name",
                        "mc_win_rate",
                        "wins",
                        "losses",
                        "n_trials",
                        "player_avg_level",
                        "player_max_level",
                        "boss_avg_level",
                        "target_boss_ace_level",
                        "player_progression_depth",
                        "player_team_species",
                        "player_team_levels",
                        "progression_pool_id",
                        "candidate_team_count",
                        "simulated_team_count",
                        "viable_team_count",
                        "best_win_pct",
                        "null_win_pct_count",
                        "zero_win_pct_count",
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

            best_same_version = None
            best_path = gold_dir / "best_team_by_boss_version.parquet"
            if best_path.exists():
                best_same_version = read_parquet(best_path)
            if best_same_version is not None and not best_same_version.empty:
                best_team_by_boss = best_same_version.sort_values(["game_version", "boss_name", "mc_win_rate"], ascending=[True, True, False])
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
        "silver_manifest_used": True,
        "gold_simulation_dir": str(gold_simulation_dir.relative_to(gold_dir)),
        "gold_outputs": gold_outputs,
    }
    write_json(gold_dir / "manifest.json", manifest)

    logger.info("[gold] wrote %s gold outputs", len(gold_outputs))

if __name__ == "__main__":
    build_gold_from_silver()
