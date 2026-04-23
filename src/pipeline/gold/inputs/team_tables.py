"""Load normalized team tables from Silver and reconstruct simulation team records.

Aligned with sharded Silver outputs:
- teams_<game>.parquet
- team_members_<game>.parquet
- team_member_moves_<game>.parquet
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.common.io import read_many_parquet, read_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME


_REQUIRED_MEMBER_COLUMNS = {"team_member_id", "team_id", "game_version", "slot", "pokemon_species", "level"}
_REQUIRED_MEMBER_MOVE_COLUMNS = {"team_member_id", "team_id", "game_version", "move_slot", "move_name"}


def _validate_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{name} missing required columns: {sorted(missing)}")


def _load_sharded_frames(
    simulation_dir: Path,
    teams_path: Path | list[Path] | None,
    team_members_path: Path | list[Path] | None,
    team_member_moves_path: Path | list[Path] | None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    def _load_frame(path_or_paths: Path | list[Path] | None, glob_pattern: str) -> pd.DataFrame:
        if isinstance(path_or_paths, list):
            if not path_or_paths:
                raise ValueError(f"Strict contract supplied an empty file list for {glob_pattern}")
            return read_many_parquet(path_or_paths)
        if isinstance(path_or_paths, Path):
            return read_parquet(path_or_paths)
        return read_many_parquet(sorted(simulation_dir.glob(glob_pattern)))

    if teams_path is not None:
        teams_df = _load_frame(teams_path, "teams_*.parquet")
    else:
        teams_df = read_many_parquet(sorted(simulation_dir.glob("teams_*.parquet")))

    if team_members_path is not None:
        members_df = _load_frame(team_members_path, "team_members_*.parquet")
    else:
        members_df = read_many_parquet(sorted(simulation_dir.glob("team_members_*.parquet")))

    if team_member_moves_path is not None:
        member_moves_df = _load_frame(team_member_moves_path, "team_member_moves_*.parquet")
    else:
        member_moves_df = read_many_parquet(sorted(simulation_dir.glob("team_member_moves_*.parquet")))

    return teams_df, members_df, member_moves_df


def load_reconstructed_teams_from_silver(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    teams_path: Path | list[Path] | None = None,
    team_members_path: Path | list[Path] | None = None,
    team_member_moves_path: Path | list[Path] | None = None,
) -> list[dict[str, Any]]:
    simulation_dir = silver_dir / simulation_dirname

    teams_df, members_df, member_moves_df = _load_sharded_frames(
        simulation_dir=simulation_dir,
        teams_path=teams_path,
        team_members_path=team_members_path,
        team_member_moves_path=team_member_moves_path,
    )

    if members_df.empty or member_moves_df.empty:
        raise FileNotFoundError(
            "Required normalized team tables are missing. Expected sharded team_members_*.parquet and team_member_moves_*.parquet."
        )

    _validate_columns(members_df, _REQUIRED_MEMBER_COLUMNS, "team_members parquet")
    _validate_columns(member_moves_df, _REQUIRED_MEMBER_MOVE_COLUMNS, "team_member_moves parquet")

    team_meta_by_id: dict[str, dict[str, Any]] = {}
    if not teams_df.empty and "team_id" in teams_df.columns:
        for row in teams_df.to_dict(orient="records"):
            team_id = str(row.get("team_id") or "").strip()
            if team_id:
                team_meta_by_id[team_id] = row

    moves_by_member: dict[str, dict[int, str]] = {}
    for row in member_moves_df.to_dict(orient="records"):
        member_id = str(row.get("team_member_id") or "").strip()
        if not member_id:
            continue
        try:
            move_slot = int(row.get("move_slot") or 0)
        except (TypeError, ValueError):
            move_slot = 0
        move_name = str(row.get("move_name") or "").strip().lower()
        if move_slot <= 0 or not move_name:
            continue
        moves_by_member.setdefault(member_id, {})[move_slot] = move_name

    members_by_team: dict[str, list[dict[str, Any]]] = {}
    for row in members_df.to_dict(orient="records"):
        team_id = str(row.get("team_id") or "").strip()
        if not team_id:
            continue
        members_by_team.setdefault(team_id, []).append(row)

    reconstructed: list[dict[str, Any]] = []

    for team_id, members in members_by_team.items():
        members_sorted = sorted(members, key=lambda item: int(item.get("slot") or 0))

        pokemon: list[str] = []
        levels: list[int] = []
        moves: list[list[str]] = []
        instance_ids: list[str] = []

        for member in members_sorted:
            species = str(member.get("pokemon_species") or "").strip().lower()
            if not species:
                continue

            try:
                level = int(member.get("level") or 0)
            except (TypeError, ValueError):
                level = 0

            member_id = str(member.get("team_member_id") or "").strip()
            member_moves = [
                move_name
                for _, move_name in sorted(moves_by_member.get(member_id, {}).items())
            ][:4]

            pokemon.append(species)
            levels.append(level)
            moves.append(member_moves)
            instance_ids.append(member_id)

        if not pokemon:
            continue

        meta = team_meta_by_id.get(team_id, {})
        avg_level = int(sum(levels) / len(levels)) if levels else int(meta.get("avg_level") or 0)

        reconstructed.append(
            {
                "team_id": team_id,
                "game_version": str(meta.get("game_version") or members_sorted[0].get("game_version") or "").strip().lower(),
                "team_role": meta.get("team_role"),
                "boss_name": meta.get("boss_name"),
                "gym": meta.get("gym"),
                "is_player_candidate": bool(meta.get("is_player_candidate", False)),
                "starter_base": meta.get("starter_base"),
                "starter_evolved_species": meta.get("starter_evolved_species"),
                "source_team_id": meta.get("source_team_id"),
                "pokemon": pokemon,
                "levels": levels,
                "moves": moves,
                "pokemon_instance_ids": instance_ids,
                "avg_level": avg_level,
            }
        )

    return reconstructed
