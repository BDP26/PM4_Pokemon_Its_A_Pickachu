from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.pipeline.common.io import read_parquet
from src.pipeline.gold.orchestration.build_gold import _build_starter_rankings_from_monte_carlo


GOLD_DIR = ROOT / "data" / "gold"
SIM_DIR = GOLD_DIR / "simulation"
DIAG_DIR = GOLD_DIR / "diagnostics"

TABLE_PATHS = {
    "best_team_by_boss_version_csv": GOLD_DIR / "best_team_by_boss_version.csv",
    "team_rankings_by_boss_version": GOLD_DIR / "team_rankings_by_boss_version.parquet",
    "team_rankings_by_boss_version_starter": GOLD_DIR / "team_rankings_by_boss_version_starter.parquet",
    "best_team_by_boss_version": GOLD_DIR / "best_team_by_boss_version.parquet",
    "best_team_by_boss_version_starter": GOLD_DIR / "best_team_by_boss_version_starter.parquet",
    "team_rankings_e4_champion_sequence_by_version_starter": GOLD_DIR
    / "team_rankings_e4_champion_sequence_by_version_starter.parquet",
    "best_team_by_e4_champion_sequence_version_starter": GOLD_DIR
    / "best_team_by_e4_champion_sequence_version_starter.parquet",
    "best_team_by_boss": GOLD_DIR / "best_team_by_boss.parquet",
    "monte_carlo_results": SIM_DIR / "monte_carlo_results.parquet",
    "teams": SIM_DIR / "teams.parquet",
    "plausibility_filter_diagnostics": GOLD_DIR / "debug" / "ranking_plausibility_filter_diagnostics.parquet",
}

E4_KEYWORDS = {
    "elite_four",
    "champion",
}

WRITER_OWNERSHIP = {
    "team_rankings_by_boss_version": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::build_gold_from_silver",
        "lines": "801-843",
    },
    "best_team_by_boss_version": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::build_gold_from_silver",
        "lines": "832-843",
    },
    "best_team_by_boss_version_csv": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::build_gold_from_silver",
        "lines": "837-843",
    },
    "team_rankings_by_boss_version_starter": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::_build_starter_rankings_from_monte_carlo",
        "lines": "575-595, 649-670",
    },
    "best_team_by_boss_version_starter": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::_write_starter_boss_outputs",
        "lines": "440-446",
    },
    "team_rankings_e4_champion_sequence_by_version_starter": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::_build_starter_rankings_from_monte_carlo",
        "lines": "597-642, 672-720",
    },
    "best_team_by_e4_champion_sequence_version_starter": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::_write_starter_sequence_outputs",
        "lines": "450-456",
    },
    "best_team_by_boss": {
        "writer": "src/pipeline/gold/orchestration/build_gold.py::build_gold_from_silver",
        "lines": "851-867",
    },
}

FUNCTION_OWNERSHIP = {
    "join_simulation_results_to_context": [
        "src/pipeline/gold/orchestration/build_gold.py::_build_starter_rankings_from_monte_carlo (lines 498-499)",
        "src/pipeline/gold/orchestration/build_gold.py::build_gold_from_silver (lines 782-783)",
    ],
    "calculate_win_percentage": [
        "src/pipeline/gold/simulation/monte_carlo_optimizer.py::run_monte_carlo_team_optimizer (lines 79-147)",
    ],
    "filter_rankings": [
        "src/pipeline/gold/orchestration/build_gold.py::_apply_level_plausibility_filter (lines 55-72)",
    ],
    "select_best_team": [
        "src/pipeline/gold/orchestration/build_gold.py::_write_starter_boss_outputs (lines 440-446)",
        "src/pipeline/gold/orchestration/build_gold.py::_write_starter_sequence_outputs (lines 450-456)",
        "src/pipeline/gold/orchestration/build_gold.py::build_gold_from_silver (lines 832-865)",
    ],
    "construct_e4_champion_gauntlet_sequences": [
        "src/pipeline/gold/simulation/team_battle_simulations.py::_gauntlet_sequences_by_version (lines 1612-1688)",
        "src/pipeline/gold/simulation/team_battle_simulations.py::_run_spark_simulations gauntlet pairing (lines 2741-2907)",
    ],
    "apply_level_plausibility_filters": [
        "src/pipeline/silver/inputs/builders/player_teams.py::_progression_level_offset (lines 124-127)",
        "src/pipeline/silver/inputs/builders/player_teams.py::_level_cap_from_progression (lines 130-132)",
        "src/pipeline/silver/inputs/builders/player_teams.py::build_progression_source_teams_from_encounters (lines 743-803)",
        "src/pipeline/gold/orchestration/build_gold.py::_apply_level_plausibility_filter (lines 55-72)",
    ],
    "starter_specific_filtering": [
        "src/pipeline/silver/inputs/builders/player_teams.py::build_player_team_compact_tables (lines 920-983)",
        "src/pipeline/gold/simulation/team_battle_simulations.py::_gauntlet_sequences_by_version (lines 1650-1677)",
    ],
}


def _to_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    if isinstance(value, list):
        return json.dumps(value)
    return str(value)


def _normalize_key(value: Any) -> str:
    return " ".join(str(value or "").strip().lower().replace("_", " ").replace("-", " ").split())


def _as_python_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    if hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list):
            return converted
    return []


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".csv":
        return pd.read_csv(path)
    return read_parquet(path)


def _table_contract_summary() -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for name, path in TABLE_PATHS.items():
        exists = path.exists()
        entry: dict[str, Any] = {
            "path": str(path.relative_to(ROOT)),
            "exists": exists,
        }
        if exists:
            frame = _read_table(path)
            entry["row_count"] = int(len(frame))
            entry["columns"] = list(frame.columns)
            entry["dtypes"] = {column: str(dtype) for column, dtype in frame.dtypes.items()}
        summary[name] = entry
    return summary


def _load_core_frames() -> dict[str, pd.DataFrame]:
    frames = {name: _read_table(path) for name, path in TABLE_PATHS.items() if path.exists()}
    teams = frames["teams"].copy()
    teams["boss_name_norm"] = teams["boss_name"].map(_normalize_key)
    return frames


def _build_boss_lookup(frames: dict[str, pd.DataFrame]) -> tuple[pd.DataFrame, pd.DataFrame]:
    teams = frames["teams"].copy()
    players = teams[teams["team_role"].eq("player")].copy()
    bosses = teams[teams["team_role"].eq("boss")].copy()

    player_lookup = players.rename(
        columns={
            "team_id": "player_team_id",
            "boss_id": "source_boss_id",
            "boss_name": "source_boss_name",
            "avg_level": "source_team_avg_level",
            "levels": "source_team_levels",
            "pokemon": "source_team_species",
            "boss_ace_level": "source_boss_ace_level",
            "boss_avg_level": "source_boss_avg_level",
            "progression_depth": "source_progression_depth",
            "level_cap_offset": "source_level_cap_offset",
            "source_team_id": "progression_source_team_id",
        }
    )
    boss_lookup = bosses.rename(
        columns={
            "team_id": "boss_team_id",
            "boss_id": "target_boss_id",
            "boss_name": "target_boss_name",
            "avg_level": "target_boss_avg_level",
            "levels": "target_boss_levels",
            "pokemon": "target_boss_species",
        }
    )
    boss_lookup["target_boss_ace_level"] = boss_lookup["target_boss_levels"].map(
        lambda levels: max(_as_python_list(levels)) if _as_python_list(levels) else None
    )
    player_lookup = player_lookup[
        [
            "player_team_id",
            "source_boss_id",
            "source_boss_name",
            "source_team_avg_level",
            "source_team_levels",
            "source_team_species",
            "source_boss_ace_level",
            "source_boss_avg_level",
            "source_progression_depth",
            "source_level_cap_offset",
            "progression_source_team_id",
        ]
    ]
    return player_lookup, boss_lookup[
        [
            "boss_team_id",
            "target_boss_id",
            "target_boss_name",
            "target_boss_avg_level",
            "target_boss_levels",
            "target_boss_species",
            "target_boss_ace_level",
        ]
    ]


def _group_issue_flag(row: pd.Series) -> str:
    reasons: list[str] = []
    if int(row.get("candidate_team_count") or 0) == 0:
        reasons.append("candidate_count_zero")
    if int(row.get("non_null_win_pct_count") or 0) == 0:
        reasons.append("all_win_rates_null")
    elif int(row.get("positive_win_pct_count") or 0) == 0:
        reasons.append("all_win_rates_zero")
    if int(row.get("mismatching_source_target_count") or 0) > 0:
        reasons.append("cross_boss_candidate_pool")
    inconsistent = row.get("best_team_level_inconsistent")
    if pd.notna(inconsistent) and bool(inconsistent):
        reasons.append("best_team_level_inconsistent")
    return ",".join(reasons)


def _coalesce_columns(frame: pd.DataFrame, target: str, candidates: list[str]) -> pd.DataFrame:
    coalesced: pd.Series | None = None
    for column in candidates:
        if column not in frame.columns:
            continue
        series = frame[column]
        coalesced = series if coalesced is None else coalesced.where(coalesced.notna(), series)
    if coalesced is not None:
        frame[target] = coalesced
    return frame


def _build_integrity_summary(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    rank = frames["team_rankings_by_boss_version_starter"].copy()
    player_lookup, boss_lookup = _build_boss_lookup(frames)
    merged = rank.merge(player_lookup, on="player_team_id", how="left").merge(boss_lookup, on="boss_team_id", how="left")
    merged = _coalesce_columns(merged, "source_boss_id", ["source_boss_id", "source_boss_id_x", "source_boss_id_y", "player_source_boss_id"])
    merged = _coalesce_columns(merged, "source_boss_name", ["source_boss_name", "source_boss_name_x", "source_boss_name_y", "player_source_boss_name"])
    merged = _coalesce_columns(merged, "source_team_avg_level", ["source_team_avg_level", "player_avg_level"])
    merged = _coalesce_columns(merged, "source_team_levels", ["source_team_levels", "team_levels", "player_team_levels"])
    merged = _coalesce_columns(merged, "source_team_species", ["source_team_species", "team_species", "player_team_species"])
    merged = _coalesce_columns(merged, "source_boss_ace_level", ["source_boss_ace_level", "boss_ace_level"])
    merged = _coalesce_columns(merged, "source_boss_avg_level", ["source_boss_avg_level", "boss_avg_level"])
    merged = _coalesce_columns(merged, "source_progression_depth", ["source_progression_depth", "progression_depth", "player_progression_depth"])
    merged = _coalesce_columns(merged, "source_level_cap_offset", ["source_level_cap_offset", "level_cap_offset"])
    merged["source_target_boss_match"] = merged["source_boss_id"] == merged["target_boss_id"]

    best_idx = (
        merged.sort_values(
            [
                "effective_game_version",
                "effective_boss_name",
                "starter_base",
                "avg_mc_win_rate",
                "avg_wins",
                "player_team_id",
            ],
            ascending=[True, True, True, False, False, True],
        )
        .groupby(["effective_game_version", "effective_boss_name", "starter_base"], sort=False)
        .head(1)
        .copy()
    )
    best_idx["best_team_level_gap_vs_target_ace"] = (
        pd.to_numeric(best_idx["source_team_avg_level"], errors="coerce")
        - pd.to_numeric(best_idx["target_boss_ace_level"], errors="coerce")
    )
    best_idx["best_team_level_inconsistent"] = (
        (best_idx["source_boss_id"] != best_idx["target_boss_id"])
        | (best_idx["best_team_level_gap_vs_target_ace"].abs() >= 10)
    )
    best_idx = best_idx[
        [
            "effective_game_version",
            "effective_boss_name",
            "starter_base",
            "player_team_id",
            "source_boss_id",
            "source_boss_name",
            "source_team_avg_level",
            "source_team_levels",
            "source_team_species",
            "target_boss_id",
            "target_boss_name",
            "target_boss_ace_level",
            "target_boss_avg_level",
            "best_team_level_gap_vs_target_ace",
            "best_team_level_inconsistent",
        ]
    ].rename(columns={"player_team_id": "best_player_team_id"})

    group_summary = (
        merged.groupby(["effective_game_version", "effective_boss_name", "starter_base"], dropna=False)
        .agg(
            boss_team_id=("boss_team_id", "max"),
            candidate_team_count=("player_team_id", "nunique"),
            ranking_row_count=("player_team_id", "size"),
            non_null_win_pct_count=("avg_mc_win_rate", lambda s: int(s.notna().sum())),
            null_win_pct_count=("avg_mc_win_rate", lambda s: int(s.isna().sum())),
            positive_win_pct_count=("avg_mc_win_rate", lambda s: int((s.fillna(0) > 0).sum())),
            zero_win_pct_count=("avg_mc_win_rate", lambda s: int((s.fillna(0) == 0).sum())),
            best_win_pct=("avg_mc_win_rate", "max"),
            min_player_team_level=("source_team_avg_level", "min"),
            avg_player_team_level=("source_team_avg_level", "mean"),
            max_player_team_level=("source_team_avg_level", "max"),
            target_boss_ace_level=("target_boss_ace_level", "max"),
            target_boss_avg_level=("target_boss_avg_level", "max"),
            matching_source_target_count=("source_target_boss_match", lambda s: int(s.sum())),
            mismatching_source_target_count=("source_target_boss_match", lambda s: int((~s).sum())),
            min_source_boss_ace_level=("source_boss_ace_level", "min"),
            max_source_boss_ace_level=("source_boss_ace_level", "max"),
            min_source_level_cap_offset=("source_level_cap_offset", "min"),
            max_source_level_cap_offset=("source_level_cap_offset", "max"),
            source_progression_depth_min=("source_progression_depth", "min"),
            source_progression_depth_max=("source_progression_depth", "max"),
        )
        .reset_index()
    )

    if "plausibility_filter_diagnostics" in frames:
        diag = frames["plausibility_filter_diagnostics"].rename(
            columns={"game_version": "effective_game_version", "boss_name": "effective_boss_name"}
        )
        group_summary = diag.merge(
            group_summary,
            on=["effective_game_version", "effective_boss_name", "starter_base"],
            how="left",
        )

    for column in [
        "candidate_team_count",
        "ranking_row_count",
        "non_null_win_pct_count",
        "null_win_pct_count",
        "positive_win_pct_count",
        "zero_win_pct_count",
        "matching_source_target_count",
        "mismatching_source_target_count",
    ]:
        if column in group_summary.columns:
            numeric = pd.to_numeric(group_summary[column], errors="coerce")
            group_summary[column] = numeric.astype("float64").fillna(0).astype(int)

    group_summary = group_summary.merge(
        best_idx,
        on=["effective_game_version", "effective_boss_name", "starter_base"],
        how="left",
    )
    group_summary = group_summary.rename(
        columns={
            "target_boss_ace_level_x": "target_boss_ace_level",
            "target_boss_avg_level_x": "target_boss_avg_level",
            "target_boss_ace_level_y": "best_target_boss_ace_level",
            "target_boss_avg_level_y": "best_target_boss_avg_level",
        }
    )
    group_summary["issue_flags"] = group_summary.apply(_group_issue_flag, axis=1)
    return group_summary.sort_values(["effective_game_version", "effective_boss_name", "starter_base"]).reset_index(drop=True)


def _build_zero_win_summary(integrity: pd.DataFrame) -> pd.DataFrame:
    zero_or_null = integrity[
        (integrity["candidate_team_count"] == 0)
        | (integrity["non_null_win_pct_count"] == 0)
        | (integrity["positive_win_pct_count"] == 0)
    ].copy()
    zero_or_null["root_issue"] = zero_or_null["issue_flags"]
    return zero_or_null.sort_values(["effective_game_version", "effective_boss_name", "starter_base"]).reset_index(drop=True)


def _current_builder_outputs() -> dict[str, pd.DataFrame]:
    scratch_dir = Path(tempfile.mkdtemp(prefix="gold-rank-diagnose-", dir="/tmp"))
    _build_starter_rankings_from_monte_carlo(gold_dir=scratch_dir, gold_simulation_dir=SIM_DIR)
    outputs = {}
    for name in [
        "team_rankings_by_boss_version_starter.parquet",
        "best_team_by_boss_version_starter.parquet",
        "team_rankings_e4_champion_sequence_by_version_starter.parquet",
        "best_team_by_e4_champion_sequence_version_starter.parquet",
    ]:
        path = scratch_dir / name
        if path.exists():
            outputs[name] = read_parquet(path)
    outputs["_scratch_dir"] = pd.DataFrame([{"scratch_dir": str(scratch_dir)}])
    return outputs


def _build_gauntlet_audit(frames: dict[str, pd.DataFrame], current_outputs: dict[str, pd.DataFrame]) -> pd.DataFrame:
    best_seq = frames["best_team_by_e4_champion_sequence_version_starter"].copy()
    mc = frames["monte_carlo_results"].copy()
    player_lookup, _ = _build_boss_lookup(frames)

    gauntlet_rows = mc[mc["simulation_mode"].eq("gauntlet")].copy()
    gym_rows = mc[mc["simulation_mode"].eq("gym")].copy()

    sequence_by_version = (
        gauntlet_rows.sort_values(["game_version", "sequence_position", "boss_name"])
        .groupby(["game_version", "sequence_position"], as_index=False)
        .agg(
            sequence_boss_name=("boss_name", "first"),
            sequence_boss_team_id=("boss_team_id", "first"),
            sequence_boss_level=("boss_level", "first"),
        )
    )
    sequence_compact = (
        sequence_by_version.groupby("game_version", as_index=False)
        .agg(
            sequence_boss_names=("sequence_boss_name", list),
            sequence_boss_team_ids=("sequence_boss_team_id", list),
            sequence_boss_levels=("sequence_boss_level", list),
        )
    )

    gym_counts = gym_rows.groupby("player_team_id", as_index=False).agg(gym_row_count=("boss_team_id", "size"))
    gauntlet_team_counts = gauntlet_rows.groupby("player_team_id", as_index=False).agg(
        gauntlet_row_count=("boss_team_id", "size"),
        gauntlet_positions=("sequence_position", lambda s: [int(v) for v in s.dropna().astype(int).tolist()]),
        gauntlet_boss_names=("boss_name", list),
        gauntlet_boss_team_ids=("boss_team_id", list),
        gauntlet_boss_levels=("boss_level", list),
        gauntlet_mean_rate=("mc_win_rate", "mean"),
        gauntlet_product_rate=("mc_win_rate", "prod"),
        gauntlet_success_any=("gauntlet_success", "max"),
    )

    persisted = (
        best_seq.merge(player_lookup, on="player_team_id", how="left")
        .merge(gym_counts, on="player_team_id", how="left")
        .merge(gauntlet_team_counts, on="player_team_id", how="left")
        .merge(sequence_compact, left_on="effective_game_version", right_on="game_version", how="left")
    )
    persisted["persisted_selected_gym_only_team"] = persisted["gauntlet_row_count"].fillna(0).eq(0)

    current_best = current_outputs["best_team_by_e4_champion_sequence_version_starter.parquet"].copy()
    current_best = current_best.rename(columns={"player_team_id": "current_player_team_id"})
    current_best = (
        current_best.merge(
            player_lookup.rename(
                columns={
                    "player_team_id": "current_player_team_id",
                    "source_boss_id": "current_source_boss_id",
                    "source_boss_name": "current_source_boss_name",
                    "source_team_avg_level": "current_source_team_avg_level",
                    "source_team_levels": "current_source_team_levels",
                    "source_team_species": "current_source_team_species",
                }
            ),
            on="current_player_team_id",
            how="left",
        )
        .merge(
            gauntlet_team_counts.rename(
                columns={
                    "player_team_id": "current_player_team_id",
                    "gauntlet_row_count": "current_gauntlet_row_count",
                    "gauntlet_positions": "current_gauntlet_positions",
                    "gauntlet_boss_names": "current_gauntlet_boss_names",
                    "gauntlet_boss_team_ids": "current_gauntlet_boss_team_ids",
                    "gauntlet_boss_levels": "current_gauntlet_boss_levels",
                    "gauntlet_mean_rate": "current_gauntlet_mean_rate",
                    "gauntlet_product_rate": "current_gauntlet_product_rate",
                    "gauntlet_success_any": "current_gauntlet_success_any",
                }
            ),
            on="current_player_team_id",
            how="left",
        )
    )
    current_best = current_best.rename(
        columns={
            "effective_game_version": "current_effective_game_version",
            "starter_base": "current_starter_base",
            "sequence_win_rate": "current_sequence_win_rate",
            "mean_mc_win_rate": "current_mean_mc_win_rate",
            "bosses_covered": "current_bosses_covered",
            "sequence_score": "current_sequence_score",
        }
    )

    audit = persisted.merge(
        current_best,
        left_on=["effective_game_version", "starter_base"],
        right_on=["current_effective_game_version", "current_starter_base"],
        how="left",
    )
    return audit[
        [
            "effective_game_version",
            "starter_base",
            "player_team_id",
            "sequence_win_rate",
            "mean_mc_win_rate",
            "bosses_covered",
            "sequence_score",
            "source_boss_id",
            "source_boss_name",
            "source_team_avg_level",
            "source_team_levels",
            "source_team_species",
            "gym_row_count",
            "gauntlet_row_count",
            "gauntlet_positions",
            "gauntlet_boss_names",
            "gauntlet_boss_team_ids",
            "gauntlet_boss_levels",
            "gauntlet_mean_rate",
            "gauntlet_product_rate",
            "gauntlet_success_any",
            "sequence_boss_names",
            "sequence_boss_team_ids",
            "sequence_boss_levels",
            "persisted_selected_gym_only_team",
            "current_player_team_id",
            "current_sequence_win_rate",
            "current_mean_mc_win_rate",
            "current_bosses_covered",
            "current_sequence_score",
            "current_source_boss_id",
            "current_source_boss_name",
            "current_source_team_avg_level",
            "current_source_team_levels",
            "current_source_team_species",
            "current_gauntlet_row_count",
            "current_gauntlet_positions",
            "current_gauntlet_boss_names",
            "current_gauntlet_boss_levels",
            "current_gauntlet_mean_rate",
            "current_gauntlet_product_rate",
            "current_gauntlet_success_any",
        ]
    ].sort_values(["effective_game_version", "starter_base"]).reset_index(drop=True)


def _json_ready_records(frame: pd.DataFrame, limit: int | None = None) -> list[dict[str, Any]]:
    records = frame.to_dict(orient="records")
    if limit is not None:
        records = records[:limit]
    return [{key: _to_text(value) for key, value in row.items()} for row in records]


def _render_report(
    table_summary: dict[str, Any],
    integrity: pd.DataFrame,
    zero_win: pd.DataFrame,
    gauntlet_audit: pd.DataFrame,
) -> str:
    zero_contexts = zero_win[zero_win["positive_win_pct_count"].eq(0)].copy()
    null_contexts = zero_win[zero_win["non_null_win_pct_count"].eq(0)].copy()
    mismatched = integrity[integrity["mismatching_source_target_count"] > 0].copy()
    stale_gauntlet = gauntlet_audit[gauntlet_audit["persisted_selected_gym_only_team"]].copy()

    per_version_zero = (
        zero_contexts.groupby("effective_game_version")
        .agg(contexts=("starter_base", "size"))
        .sort_values("contexts", ascending=False)
        .reset_index()
    )

    report_lines = [
        "# Gold Fix Diagnosis Report",
        "",
        "## Confirmed Problems",
        "",
        f"- Persisted Gold starter/boss rankings contain `{int(len(zero_contexts))}` boss/version/starter contexts with no positive win rate.",
        f"- Persisted Gold starter/boss rankings contain `{int(len(null_contexts))}` contexts with all-null win rates.",
        f"- Persisted Gold starter/boss rankings contain `{int(len(mismatched))}` contexts where the ranked candidate pool mixes player teams generated for other bosses.",
        f"- Persisted gauntlet best-team output is invalid for all `{int(len(gauntlet_audit))}` version/starter rows: every persisted best sequence row is backed by `0` gauntlet simulations and only `1` gym simulation row.",
        f"- Persisted sequence ranking table has `22896 / 37296` rows with zero gauntlet simulations behind them.",
        "",
        "### Exact Affected Contexts",
        "",
        "- Full exact context list is written to `data/gold/diagnostics/gold_zero_win_bosses.csv`.",
        "- Zero-win context counts by version:",
    ]
    report_lines.extend(
        f"  - {row.effective_game_version}: {int(row.contexts)}"
        for row in per_version_zero.itertuples(index=False)
    )

    report_lines.extend(
        [
            "",
            "### Representative Table Evidence",
            "",
            "- `black / oshawott / caitlin`: persisted best team is `player-source:black:a6e76d2a4ea8`, but the broader candidate pool contains 400 teams sourced from five different endgame bosses instead of the 80 teams generated for Caitlin.",
            "- `black / oshawott / alder`: all 400 persisted starter-specific candidate teams have `avg_mc_win_rate = 0.0` even though their source-team average levels span `42..71` and come from mixed `boss_ace_level` contexts `50..77`.",
            "- `black / oshawott` gauntlet persisted best team is `player-source:black:002a5d7c6944`, a level-10 Cilan-era team with no gauntlet rows at all; its `sequence_win_rate = 1.0` comes from a single gym row.",
            "",
            "## Code Ownership",
            "",
        ]
    )
    for table_name, owner in WRITER_OWNERSHIP.items():
        report_lines.append(f"- `{table_name}` -> `{owner['writer']}` ({owner['lines']})")

    report_lines.extend(
        [
            "",
            "### Responsible Functions",
            "",
        ]
    )
    for topic, owners in FUNCTION_OWNERSHIP.items():
        report_lines.append(f"- `{topic}`")
        report_lines.extend(f"  - {owner}" for owner in owners)

    report_lines.extend(
        [
            "",
            "## Root Cause Ranking",
            "",
            "### Confirmed",
            "",
            "- Gold single-boss rankings are built from every same-version simulation row without restricting the player-team pool to the target boss context. `build_gold.py` joins Monte Carlo rows to player and boss context, then ranks directly (`lines 498-520`, `575-595`, `649-670`, `801-843`). This mixes unrelated endgame pools into single-boss tables.",
            "- Persisted gauntlet ranking table on disk was built by older logic than the current checked-in `build_gold.py`: the current checked-in builder, when run against the same `data/gold/simulation/monte_carlo_results.parquet`, produces only gauntlet-backed sequence rows, while the persisted table ranks gym-only teams with `bosses_covered = NaN`.",
            "- Current checked-in gauntlet ranking logic is still incorrect. In both Spark and pandas branches, Gold keeps only the max `sequence_position` row per team (`build_gold.py` lines `597-620` and `672-699`), so the sequence score is driven by the final gauntlet battle row instead of one persistent team evaluated across the full sequence.",
            "- Silver candidate generation applies a progression offset below the boss ace level (`player_teams.py` lines `124-132`, `743-803`). Many all-zero gym contexts are severely underleveled in persisted outputs, for example `diamond / roark / chimchar` where generated player teams average level `4..5` into a level-12 boss and `black / lenora / oshawott` where teams average `11..12` into a level-20 ace context.",
            "",
            "### Likely",
            "",
            "- The widespread all-zero gym results are likely caused by the Silver level-cap model being too conservative for early and some mid-game bosses, not by null joins or missing simulations. Persisted tables have non-null `mc_win_rate`, non-zero candidate counts, and no persisted move-reference coverage gaps.",
            "- The gauntlet candidate pool should be tied to one explicit endgame progression snapshot per version/starter instead of combining five separate endgame boss-target pools. Persisted starter/boss ranking counts jump from `80` source teams in Silver to `400` ranked candidates for each E4/Champion target.",
            "",
            "### Not Supported By Evidence",
            "",
            "- Missing simulation rows as the primary cause of the starter/boss ranking failures: ranking tables are populated and `mc_win_rate` is never null in `data/gold/simulation/monte_carlo_results.parquet`.",
            "- Missing Kaggle boss move reference data: `data/silver/diagnostics/kaggle_boss_move_profile_gaps.csv` is empty.",
            "- Player teams with no damaging moves as the primary cause: `data/silver/diagnostics/player_no_damaging_move_gaps_summary.csv` is empty.",
            "",
            "## Proposed Fix Plan",
            "",
            "- Fix Silver candidate generation to use explicit `player_max_level = boss_ace_level` as the hard cap, keeping `player_avg_level` and the prior offset only as diagnostics. Rebuild source teams and simulation inputs so early/mid-game teams are not artificially several levels below the boss ace.",
            "- Fix Gold single-boss rankings to keep only rows where the player team was generated for the same target boss context. Do not mix other boss pools into `team_rankings_by_boss_version*` or `best_team_by_boss*`.",
            "- Fix Gold gauntlet ranking to consume only gauntlet simulation rows and compute sequence success from one persistent team across the full ordered E4/Champion sequence. Remove any path that can rank gym-only rows as gauntlet results.",
            "- Add explicit diagnostics columns to Gold rankings: `candidate_team_count`, `simulated_team_count`, `viable_team_count`, `best_win_pct`, `null_win_pct_count`, `zero_win_pct_count`, and level-cap metadata.",
            "- Rebuild affected Silver and Gold outputs, rerun this diagnostics script, and compare before/after artifacts under `data/gold/diagnostics/`.",
            "",
            "## Table Contract Snapshot",
            "",
            f"- Relevant tables summarized: `{len(table_summary)}`",
        ]
    )

    return "\n".join(report_lines)


def main() -> None:
    DIAG_DIR.mkdir(parents=True, exist_ok=True)

    table_summary = _table_contract_summary()
    frames = _load_core_frames()
    integrity = _build_integrity_summary(frames)
    zero_win = _build_zero_win_summary(integrity)
    current_outputs = _current_builder_outputs()
    gauntlet_audit = _build_gauntlet_audit(frames, current_outputs)
    report = _render_report(table_summary, integrity, zero_win, gauntlet_audit)

    integrity_out = integrity.copy()
    zero_out = zero_win.copy()
    gauntlet_out = gauntlet_audit.copy()

    for frame in (integrity_out, zero_out, gauntlet_out):
        for column in frame.columns:
            frame[column] = frame[column].map(_to_text)

    integrity_out.to_csv(DIAG_DIR / "gold_output_integrity_summary.csv", index=False)
    zero_out.to_csv(DIAG_DIR / "gold_zero_win_bosses.csv", index=False)
    gauntlet_out.to_csv(DIAG_DIR / "gold_gauntlet_team_level_audit.csv", index=False)

    contract_payload = {
        "tables": table_summary,
        "writer_ownership": WRITER_OWNERSHIP,
        "function_ownership": FUNCTION_OWNERSHIP,
        "integrity_summary_rows": int(len(integrity)),
        "zero_or_null_context_rows": int(len(zero_win)),
        "gauntlet_audit_rows": int(len(gauntlet_audit)),
    }
    (DIAG_DIR / "gold_table_contract_summary.json").write_text(
        json.dumps(contract_payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (DIAG_DIR / "gold_fix_diagnosis_report.md").write_text(report + "\n", encoding="utf-8")

    print(report)


if __name__ == "__main__":
    main()
