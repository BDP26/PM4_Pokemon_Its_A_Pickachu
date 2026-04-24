"""Load compact team tables from Silver and reconstruct deterministic simulation teams."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.common.io import read_many_parquet, read_parquet
from src.pipeline.settings import SILVER_DIR, SILVER_SIMULATION_DIRNAME

_REQUIRED_MEMBER_COLUMNS = {"team_member_id", "source_team_id", "game_version", "slot", "pokemon_species", "level"}
_REQUIRED_MEMBER_MOVE_OPTION_COLUMNS = {
    "team_member_id",
    "source_team_id",
    "game_version",
    "move_name",
    "option_rank",
}


def _validate_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{name} missing required columns: {sorted(missing)}")


def _load_frame(path_or_paths: Path | list[Path] | None, simulation_dir: Path, glob_pattern: str) -> pd.DataFrame:
    if isinstance(path_or_paths, list):
        if not path_or_paths:
            raise ValueError(f"Strict contract supplied an empty file list for {glob_pattern}")
        return read_many_parquet(path_or_paths)
    if isinstance(path_or_paths, Path):
        return read_parquet(path_or_paths)
    return read_many_parquet(sorted(simulation_dir.glob(glob_pattern)))


def load_reconstructed_teams_from_silver(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    teams_path: Path | list[Path] | None = None,
    team_members_path: Path | list[Path] | None = None,
    member_move_options_path: Path | list[Path] | None = None,
) -> list[dict[str, Any]]:
    simulation_dir = silver_dir / simulation_dirname

    teams_df = _load_frame(teams_path, simulation_dir, "source_teams_*.parquet")
    members_df = _load_frame(team_members_path, simulation_dir, "source_team_members_*.parquet")
    move_options_df = _load_frame(member_move_options_path, simulation_dir, "member_move_options_*.parquet")

    if members_df.empty:
        raise FileNotFoundError("Required source_team_members parquet is missing.")

    _validate_columns(members_df, _REQUIRED_MEMBER_COLUMNS, "source_team_members parquet")
    if not move_options_df.empty:
        _validate_columns(move_options_df, _REQUIRED_MEMBER_MOVE_OPTION_COLUMNS, "member_move_options parquet")

    meta_by_id: dict[str, dict[str, Any]] = {}
    if not teams_df.empty and "source_team_id" in teams_df.columns:
        for row in teams_df.to_dict(orient="records"):
            team_id = str(row.get("source_team_id") or "").strip()
            if team_id:
                meta_by_id[team_id] = row

    moves_by_member: dict[str, list[str]] = {}
    if not move_options_df.empty:
        sorted_options = move_options_df.sort_values(["team_member_id", "option_rank", "move_name"])
        for row in sorted_options.to_dict(orient="records"):
            member_id = str(row.get("team_member_id") or "").strip()
            move_name = str(row.get("move_name") or "").strip().lower()
            if not member_id or not move_name:
                continue
            slot = moves_by_member.setdefault(member_id, [])
            if move_name not in slot and len(slot) < 4:
                slot.append(move_name)

    members_by_team: dict[str, list[dict[str, Any]]] = {}
    for row in members_df.to_dict(orient="records"):
        team_id = str(row.get("source_team_id") or "").strip()
        if team_id:
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
            level = int(member.get("level") or 0)
            member_id = str(member.get("team_member_id") or "").strip()
            pokemon.append(species)
            levels.append(level)
            moves.append(list(moves_by_member.get(member_id, []))[:4])
            instance_ids.append(member_id)

        if not pokemon:
            continue

        meta = meta_by_id.get(team_id, {})
        avg_level = int(sum(levels) / len(levels)) if levels else int(meta.get("avg_level") or 0)
        reconstructed.append(
            {
                "team_id": team_id,
                "game_version": str(meta.get("game_version") or members_sorted[0].get("game_version") or "").strip().lower(),
                "team_role": meta.get("team_role", "player_source"),
                "boss_name": meta.get("boss_name"),
                "gym": meta.get("gym") or meta.get("boss_name"),
                "is_player_candidate": bool(meta.get("is_player_candidate", True)),
                "starter_base": meta.get("starter_base"),
                "starter_evolved_species": meta.get("starter_evolved_species"),
                "source_team_id": meta.get("progression_source_team_id"),
                "pokemon": pokemon,
                "levels": levels,
                "moves": moves,
                "pokemon_instance_ids": instance_ids,
                "avg_level": avg_level,
            }
        )

    return reconstructed
