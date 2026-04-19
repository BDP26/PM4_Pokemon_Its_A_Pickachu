"""Relational integrity checks for normalized Silver tables."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

import pandas as pd


@dataclass(frozen=True)
class ValidationIssue:
    level: str
    code: str
    table: str
    detail: str
    count: int | None = None


@dataclass(frozen=True)
class ValidationReport:
    is_valid: bool
    issues: list[ValidationIssue]

    def as_dict(self) -> dict[str, Any]:
        return {
            "is_valid": self.is_valid,
            "issues": [asdict(issue) for issue in self.issues],
        }


def _series_set(df: pd.DataFrame, column: str) -> set[str]:
    if column not in df.columns:
        return set()
    return {str(value).strip().lower() for value in df[column].dropna().tolist() if str(value).strip()}


def _append_issue(issues: list[ValidationIssue], *, level: str, code: str, table: str, detail: str, count: int | None = None) -> None:
    issues.append(ValidationIssue(level=level, code=code, table=table, detail=detail, count=count))


def validate_normalized_silver_tables(tables: dict[str, pd.DataFrame]) -> ValidationReport:
    issues: list[ValidationIssue] = []

    games = tables.get("games", pd.DataFrame())
    bosses = tables.get("bosses", pd.DataFrame())
    locations = tables.get("locations", pd.DataFrame())
    encounters = tables.get("encounters", pd.DataFrame())
    teams = tables.get("teams", pd.DataFrame())
    team_members = tables.get("team_members", pd.DataFrame())
    team_member_moves = tables.get("team_member_moves", pd.DataFrame())
    move_reference = tables.get("move_reference", pd.DataFrame())
    learnable_moves = tables.get("learnable_moves", pd.DataFrame())

    if games.empty:
        _append_issue(issues, level="error", code="EMPTY_GAMES", table="games", detail="games table is empty")
    if teams.empty:
        _append_issue(issues, level="error", code="EMPTY_TEAMS", table="teams", detail="teams table is empty")

    game_versions = _series_set(games, "game_version")
    boss_ids = _series_set(bosses, "boss_id")
    location_ids = _series_set(locations, "location_id")
    team_ids = _series_set(teams, "team_id")
    team_member_ids = _series_set(team_members, "team_member_id")
    move_names = _series_set(move_reference, "move_name")

    if not bosses.empty:
        invalid = [v for v in _series_set(bosses, "game_version") if v and v not in game_versions]
        if invalid:
            _append_issue(
                issues,
                level="error",
                code="FK_BOSSES_GAME",
                table="bosses",
                detail="bosses.game_version references unknown games.game_version",
                count=len(invalid),
            )

    if not locations.empty:
        invalid = [v for v in _series_set(locations, "game_version") if v and v != "unknown" and v not in game_versions]
        if invalid:
            _append_issue(
                issues,
                level="warning",
                code="FK_LOCATIONS_GAME",
                table="locations",
                detail="locations.game_version contains unknown values",
                count=len(invalid),
            )

    if not encounters.empty:
        invalid = [v for v in _series_set(encounters, "game") if v and v not in game_versions]
        if invalid:
            _append_issue(
                issues,
                level="warning",
                code="FK_ENCOUNTERS_GAME",
                table="encounters",
                detail="encounters.game references unknown games.game_version",
                count=len(invalid),
            )

    if not teams.empty:
        invalid = [v for v in _series_set(teams, "game_version") if v and v not in game_versions]
        if invalid:
            _append_issue(issues, level="error", code="FK_TEAMS_GAME", table="teams", detail="teams.game_version references unknown games", count=len(invalid))

    if not team_members.empty:
        invalid_team_ids = [v for v in _series_set(team_members, "team_id") if v and v not in team_ids]
        if invalid_team_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBERS_TEAM", table="team_members", detail="team_members.team_id references unknown teams", count=len(invalid_team_ids))
        invalid_game_ids = [v for v in _series_set(team_members, "game_version") if v and v not in game_versions]
        if invalid_game_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBERS_GAME", table="team_members", detail="team_members.game_version references unknown games", count=len(invalid_game_ids))

    if not team_member_moves.empty:
        invalid_member_ids = [v for v in _series_set(team_member_moves, "team_member_id") if v and v not in team_member_ids]
        if invalid_member_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBER_MOVES_MEMBER", table="team_member_moves", detail="team_member_moves.team_member_id references unknown team_members", count=len(invalid_member_ids))
        invalid_team_ids = [v for v in _series_set(team_member_moves, "team_id") if v and v not in team_ids]
        if invalid_team_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBER_MOVES_TEAM", table="team_member_moves", detail="team_member_moves.team_id references unknown teams", count=len(invalid_team_ids))

    if not learnable_moves.empty:
        invalid_games = [v for v in _series_set(learnable_moves, "game_version") if v and v not in game_versions]
        invalid_moves = [v for v in _series_set(learnable_moves, "move_name") if v and move_names and v not in move_names]
        if invalid_games:
            _append_issue(issues, level="error", code="FK_LEARNABLE_GAME", table="learnable_moves", detail="learnable_moves.game_version references unknown games", count=len(invalid_games))
        if invalid_moves:
            _append_issue(issues, level="warning", code="FK_LEARNABLE_MOVE", table="learnable_moves", detail="learnable_moves.move_name missing in move_reference", count=len(invalid_moves))

    is_valid = not any(issue.level == "error" for issue in issues)
    return ValidationReport(is_valid=is_valid, issues=issues)

