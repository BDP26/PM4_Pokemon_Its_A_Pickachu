"""Load compact team tables from Silver and reconstruct deterministic simulation teams."""

from __future__ import annotations

import json
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
_REQUIRED_MEMBER_MOVESET_COMBO_COLUMNS = {
    "moveset_combo_id",
    "team_id",
    "pokemon_instance_id",
    "slot_index",
}
_INVALID_MOVE_VALUES = {"", "nan", "none", "null", "<na>", "na"}
def _normalize_move_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value != value:
        return ""
    text = str(value).strip().lower()
    if text in _INVALID_MOVE_VALUES:
        return ""
    return text

def _validate_columns(frame: pd.DataFrame, required: set[str], name: str) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{name} missing required columns: {sorted(missing)}")


def _coerce_fixed_moves(value: Any) -> list[str]:
    if isinstance(value, list):
        return [_normalize_move_value(move) for move in value if _normalize_move_value(move)]
    if isinstance(value, tuple):
        return [_normalize_move_value(move) for move in value if _normalize_move_value(move)]
    if hasattr(value, "tolist"):
        converted = value.tolist()
        if isinstance(converted, list):
            return [_normalize_move_value(move) for move in converted if _normalize_move_value(move)]
    if isinstance(value, str):
        text = value.strip()
        if text.startswith("[") and text.endswith("]"):
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if isinstance(parsed, list):
                return [_normalize_move_value(move) for move in parsed if _normalize_move_value(move)]
        normalized = _normalize_move_value(text)
        return [normalized] if normalized else []
    return []


def _load_frame(path_or_paths: Path | list[Path] | None, simulation_dir: Path, glob_pattern: str) -> pd.DataFrame:
    if isinstance(path_or_paths, list):
        if not path_or_paths:
            raise ValueError(f"Strict contract supplied an empty file list for {glob_pattern}")
        return read_many_parquet(path_or_paths)
    if isinstance(path_or_paths, Path):
        return read_parquet(path_or_paths)
    return read_many_parquet(sorted(simulation_dir.glob(glob_pattern)))




def _select_diverse_moves(ordered_moves: list[str], *, width: int = 4) -> list[str]:
    """Select a bounded, deterministic, slightly-diverse moveset from ranked options."""
    unique_moves = []
    for move in ordered_moves:
        move_norm = _normalize_move_value(move)
        if move_norm and move_norm not in unique_moves:
            unique_moves.append(move_norm)

    if len(unique_moves) <= width:
        return unique_moves

    selected = unique_moves[:2]
    tail = unique_moves[2:]
    middle = tail[len(tail) // 2]
    last = tail[-1]
    for candidate in (middle, last):
        if candidate not in selected:
            selected.append(candidate)
    if len(selected) < width:
        for move in unique_moves[2:]:
            if move not in selected:
                selected.append(move)
            if len(selected) >= width:
                break
    return selected[:width]
def load_reconstructed_teams_from_silver(
    silver_dir: Path = SILVER_DIR,
    simulation_dirname: str = SILVER_SIMULATION_DIRNAME,
    teams_path: Path | list[Path] | None = None,
    team_members_path: Path | list[Path] | None = None,
    member_moveset_combos_path: Path | list[Path] | None = None,
    member_move_options_path: Path | list[Path] | None = None,
) -> list[dict[str, Any]]:
    simulation_dir = silver_dir / simulation_dirname

    teams_df = _load_frame(teams_path, simulation_dir, "source_teams_*.parquet")
    members_df = _load_frame(team_members_path, simulation_dir, "source_team_members_*.parquet")
    moveset_combos_df = _load_frame(member_moveset_combos_path, simulation_dir, "member_moveset_combos_*.parquet")
    move_options_df = _load_frame(member_move_options_path, simulation_dir, "member_move_options_*.parquet")

    if members_df.empty:
        raise FileNotFoundError("Required source_team_members parquet is missing.")

    _validate_columns(members_df, _REQUIRED_MEMBER_COLUMNS, "source_team_members parquet")
    if not moveset_combos_df.empty:
        _validate_columns(moveset_combos_df, _REQUIRED_MEMBER_MOVESET_COMBO_COLUMNS, "member_moveset_combos parquet")
    if not move_options_df.empty:
        _validate_columns(move_options_df, _REQUIRED_MEMBER_MOVE_OPTION_COLUMNS, "member_move_options parquet")

    meta_by_id: dict[str, dict[str, Any]] = {}
    if not teams_df.empty and "source_team_id" in teams_df.columns:
        for row in teams_df.to_dict(orient="records"):
            team_id = str(row.get("source_team_id") or "").strip()
            if team_id:
                meta_by_id[team_id] = row

    def _is_boss_or_kaggle_team(team_id: str) -> bool:
        meta = meta_by_id.get(team_id, {})
        team_role = str(meta.get("team_role") or "").strip().lower()
        origin = str(meta.get("origin") or "").strip().lower()
        return team_role == "boss" or origin == "kaggle"

    moves_by_member: dict[str, list[str]] = {}
    combos_by_member: dict[str, list[list[str]]] = {}
    if not moveset_combos_df.empty:
        sorted_combos = moveset_combos_df.sort_values(["pokemon_instance_id", "combo_rank", "moveset_combo_id"])
        for row in sorted_combos.to_dict(orient="records"):
            team_id = str(row.get("team_id") or "").strip()
            if team_id and _is_boss_or_kaggle_team(team_id):
                continue
            member_id = str(row.get("pokemon_instance_id") or "").strip()
            if not member_id:
                continue
            if isinstance(row.get("moves"), list):
                combo = []
                for move in row.get("moves", []):
                    normalized_move = _normalize_move_value(move)
                    if normalized_move:
                        combo.append(normalized_move)
            else:
                combo = []
                for idx in range(1, 5):
                    normalized_move = _normalize_move_value(row.get(f"move_{idx}"))
                    if normalized_move:
                        combo.append(normalized_move)
            if combo:
                combos_by_member.setdefault(member_id, []).append(combo)

    if not move_options_df.empty:
        sorted_options = move_options_df.sort_values(["team_member_id", "option_rank", "move_name"])
        for row in sorted_options.to_dict(orient="records"):
            team_id = str(row.get("source_team_id") or "").strip()
            if team_id and _is_boss_or_kaggle_team(team_id):
                continue
            member_id = str(row.get("team_member_id") or "").strip()
            move_name = _normalize_move_value(row.get("move_name"))
            if not member_id or not move_name:
                continue
            slot = moves_by_member.setdefault(member_id, [])
            if move_name not in slot:
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
            combos = combos_by_member.get(member_id, [])
            if combos:
                # Deterministic bounded choice: use top-ranked combo only in base reconstruction.
                moves.append(list(combos[0])[:4])
            else:
                fixed_moves = _coerce_fixed_moves(member.get("fixed_moves"))
                if fixed_moves:
                    moves.append(fixed_moves[:4])
                else:
                    moves.append(_select_diverse_moves(list(moves_by_member.get(member_id, [])), width=4))
            instance_ids.append(member_id)

        if not pokemon:
            continue

        meta = meta_by_id.get(team_id, {})
        avg_level = int(sum(levels) / len(levels)) if levels else int(meta.get("avg_level") or 0)
        reconstructed.append(
            {
                "team_id": team_id,
                "game_version": str(meta.get("game_version") or members_sorted[0].get("game_version") or "").strip().lower(),
                "team_role": meta.get("team_role", "player"),
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
