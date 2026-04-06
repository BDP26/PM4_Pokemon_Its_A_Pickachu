"""Build a web-friendly payload: best team per walkthrough boss and version."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import numpy as np

from src.pipeline.common.io import read_jsonl, read_parquet, write_json
from src.pipeline.silver.game_config import get_games_config, get_starter_family_members
from src.pipeline.settings import SILVER_DIR, GOLD_DIR, get_silver_subdirs


def _norm_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if isinstance(value, np.ndarray):
        return [_json_safe(v) for v in value.tolist()]
    if isinstance(value, (np.integer, np.floating)):
        return value.item()
    if isinstance(value, np.bool_):
        return bool(value)
    return value


def build_walkthrough_best_teams_payload(
    silver_dir: Path = SILVER_DIR,
    gold_dir: Path = GOLD_DIR,
) -> Path | None:
    silver_subdirs = get_silver_subdirs(silver_dir)
    snapshots_dir = silver_subdirs["snapshots"]
    simulation_dir = silver_subdirs["simulation"]

    best_by_boss_file = gold_dir / "best_team_by_boss_version.parquet"
    rankings_file = gold_dir / "team_rankings_by_boss_version.parquet"
    teams_file = simulation_dir / "teams.parquet"

    if not best_by_boss_file.exists() or not teams_file.exists():
        return None

    best_df = read_parquet(best_by_boss_file)
    teams_df = read_parquet(teams_file)
    rankings_df = (
        read_parquet(rankings_file)
        if rankings_file.exists()
        else None
    )

    if best_df.empty or teams_df.empty:
        return None

    team_by_id: dict[str, dict[str, Any]] = {}
    for row in teams_df.to_dict(orient="records"):
        team_id = row.get("team_id")
        if isinstance(team_id, str):
            team_by_id[team_id] = row

    best_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in best_df.to_dict(orient="records"):
        version = row.get("game_version")
        boss_name = row.get("boss_name")
        if isinstance(version, str) and isinstance(boss_name, str):
            best_by_key[(version, _norm_name(boss_name))] = row

    rankings_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    if rankings_df is not None and not rankings_df.empty:
        ranking_rows = rankings_df.sort_values(
            ["game_version", "boss_name", "rank_in_boss_version", "mc_win_rate"],
            ascending=[True, True, True, False],
        ).to_dict(orient="records")
        for row in ranking_rows:
            version = row.get("game_version")
            boss_name = row.get("boss_name")
            if not isinstance(version, str) or not isinstance(boss_name, str):
                continue
            key = (version, _norm_name(boss_name))
            rankings_by_key.setdefault(key, []).append(row)

    def _team_payload_from_row(ranking_row: dict[str, Any]) -> dict[str, Any] | None:
        team_id = ranking_row.get("player_team_id")
        team_details = team_by_id.get(team_id) if isinstance(team_id, str) else None
        if not isinstance(team_details, dict):
            return None
        return {
            "team_id": team_id,
            "mc_win_rate": ranking_row.get("mc_win_rate"),
            "wins": ranking_row.get("wins"),
            "losses": ranking_row.get("losses"),
            "n_trials": ranking_row.get("n_trials"),
            "avg_level": team_details.get("avg_level"),
            "pokemon": team_details.get("details", []),
            "rank_in_boss_version": ranking_row.get("rank_in_boss_version"),
        }

    walkthroughs: dict[str, list[dict[str, Any]]] = {}

    for snapshot_path in sorted(snapshots_dir.glob("*_boss_snapshots.jsonl")):
        version = snapshot_path.stem.replace("_boss_snapshots", "")
        snapshot_df = read_jsonl(snapshot_path)
        if snapshot_df.empty:
            continue

        rows_by_key: dict[str, dict[str, Any]] = {}
        for snap in snapshot_df.sort_values(["boss_order", "part"]).to_dict(orient="records"):
            boss_name = snap.get("boss_name")
            if not isinstance(boss_name, str):
                continue

            boss_order = snap.get("boss_order")
            boss_key = f"{version}:{boss_order}:{_norm_name(boss_name)}"

            best = best_by_key.get((version, _norm_name(boss_name)))
            recommended_team = None
            if best is not None:
                recommended_team = _team_payload_from_row(best)

            top_rankings = rankings_by_key.get((version, _norm_name(boss_name)), [])
            top_teams: list[dict[str, Any]] = []
            for ranking_row in top_rankings[:5]:
                payload = _team_payload_from_row(ranking_row)
                if payload is not None:
                    top_teams.append(payload)

            row = {
                "boss_key": boss_key,
                "boss_id": snap.get("boss_id"),
                "boss_slug": snap.get("boss_slug"),
                "boss_order": boss_order,
                "part": snap.get("part"),
                "boss_name": boss_name,
                "location_count": snap.get("reachable_location_count"),
                "recommended_team": recommended_team,
                "top_teams": top_teams,
            }

            existing = rows_by_key.get(boss_key)
            if existing is None or (row.get("part") or 0) < (existing.get("part") or 0):
                rows_by_key[boss_key] = row

        walkthroughs[version] = sorted(
            rows_by_key.values(),
            key=lambda item: (
                item.get("boss_order") or 0,
                item.get("part") or 0,
                str(item.get("boss_name") or ""),
            ),
        )

    starter_choices_by_version = {
        row["game_key"]: row.get("starter_choices", [])
        for row in get_games_config()
    }
    starter_family_members_by_version = {
        version: {
            starter: get_starter_family_members(starter)
            for starter in starters
        }
        for version, starters in starter_choices_by_version.items()
    }

    output = {
        "versions": sorted(walkthroughs.keys()),
        "starter_choices_by_version": starter_choices_by_version,
        "starter_family_members_by_version": starter_family_members_by_version,
        "walkthroughs": walkthroughs,
    }

    output_path = gold_dir / "walkthrough_best_teams.json"
    write_json(output_path, _json_safe(output))
    return output_path

