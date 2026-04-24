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


@dataclass(frozen=True)
class TableValidationProfile:
    columns: dict[str, set[str]]
    row_count: int = 0


def _profile_from_any(table: pd.DataFrame | TableValidationProfile | dict[str, Any] | None) -> TableValidationProfile:
    if isinstance(table, TableValidationProfile):
        return table
    if isinstance(table, dict):
        row_count = int(table.get("row_count", 0) or 0)
        columns_raw = table.get("columns", table)
        columns: dict[str, set[str]] = {}
        if isinstance(columns_raw, dict):
            for column, values in columns_raw.items():
                if isinstance(values, set):
                    norm_values = values
                elif isinstance(values, (list, tuple)):
                    norm_values = set(values)
                else:
                    norm_values = {values}
                columns[str(column)] = {str(v).strip().lower() for v in norm_values if str(v).strip()}
        return TableValidationProfile(columns=columns, row_count=row_count)
    if table is None or table.empty:
        return TableValidationProfile(columns={}, row_count=0)
    columns: dict[str, set[str]] = {}
    for column in table.columns:
        columns[str(column)] = {str(value).strip().lower() for value in table[column].dropna().tolist() if str(value).strip()}
    return TableValidationProfile(columns=columns, row_count=len(table))


def _series_set(profile: TableValidationProfile, column: str) -> set[str]:
    return profile.columns.get(column, set())


def _append_issue(issues: list[ValidationIssue], *, level: str, code: str, table: str, detail: str, count: int | None = None) -> None:
    issues.append(ValidationIssue(level=level, code=code, table=table, detail=detail, count=count))


def validate_normalized_silver_tables(tables: dict[str, pd.DataFrame | TableValidationProfile | dict[str, Any]]) -> ValidationReport:
    issues: list[ValidationIssue] = []

    games = _profile_from_any(tables.get("games"))
    bosses = _profile_from_any(tables.get("bosses"))
    locations = _profile_from_any(tables.get("locations"))
    encounters = _profile_from_any(tables.get("encounters"))
    teams = _profile_from_any(tables.get("teams"))
    team_members = _profile_from_any(tables.get("team_members"))
    team_member_moves = _profile_from_any(tables.get("team_member_moves"))
    move_reference = _profile_from_any(tables.get("move_reference"))
    learnable_moves = _profile_from_any(tables.get("learnable_moves"))
    pokemon_data = _profile_from_any(tables.get("pokemon_data"))

    if games.row_count == 0:
        _append_issue(issues, level="error", code="EMPTY_GAMES", table="games", detail="games table is empty")
    if teams.row_count == 0:
        _append_issue(issues, level="error", code="EMPTY_TEAMS", table="teams", detail="teams table is empty")

    game_versions = _series_set(games, "game_version")
    boss_ids = _series_set(bosses, "boss_id")
    location_ids = _series_set(locations, "location_id")
    team_ids = _series_set(teams, "team_id")
    team_member_ids = _series_set(team_members, "team_member_id")
    move_names = _series_set(move_reference, "move_name")
    pokemon_names = _series_set(pokemon_data, "name") | _series_set(pokemon_data, "pokemon_species")

    if bosses.row_count > 0:
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

    if locations.row_count > 0:
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

    if encounters.row_count > 0:
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

    if teams.row_count > 0:
        invalid = [v for v in _series_set(teams, "game_version") if v and v not in game_versions]
        if invalid:
            _append_issue(issues, level="error", code="FK_TEAMS_GAME", table="teams", detail="teams.game_version references unknown games", count=len(invalid))

    if team_members.row_count > 0:
        invalid_team_ids = [v for v in _series_set(team_members, "team_id") if v and v not in team_ids]
        if invalid_team_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBERS_TEAM", table="team_members", detail="team_members.team_id references unknown teams", count=len(invalid_team_ids))
        invalid_game_ids = [v for v in _series_set(team_members, "game_version") if v and v not in game_versions]
        if invalid_game_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBERS_GAME", table="team_members", detail="team_members.game_version references unknown games", count=len(invalid_game_ids))

    if team_member_moves.row_count > 0:
        invalid_member_ids = [v for v in _series_set(team_member_moves, "team_member_id") if v and v not in team_member_ids]
        if invalid_member_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBER_MOVES_MEMBER", table="team_member_moves", detail="team_member_moves.team_member_id references unknown team_members", count=len(invalid_member_ids))
        invalid_team_ids = [v for v in _series_set(team_member_moves, "team_id") if v and v not in team_ids]
        if invalid_team_ids:
            _append_issue(issues, level="error", code="FK_TEAM_MEMBER_MOVES_TEAM", table="team_member_moves", detail="team_member_moves.team_id references unknown teams", count=len(invalid_team_ids))

    if learnable_moves.row_count > 0:
        invalid_games = [v for v in _series_set(learnable_moves, "game_version") if v and v not in game_versions]
        invalid_moves = [v for v in _series_set(learnable_moves, "move_name") if v and move_names and v not in move_names]
        if invalid_games:
            _append_issue(issues, level="error", code="FK_LEARNABLE_GAME", table="learnable_moves", detail="learnable_moves.game_version references unknown games", count=len(invalid_games))
        if invalid_moves:
            _append_issue(issues, level="warning", code="FK_LEARNABLE_MOVE", table="learnable_moves", detail="learnable_moves.move_name missing in move_reference", count=len(invalid_moves))

    if pokemon_data.row_count == 0:
        _append_issue(issues, level="error", code="EMPTY_POKEMON_DATA", table="pokemon_data", detail="pokemon_data table is empty")

    if pokemon_names:
        invalid_encounter_species = [v for v in _series_set(encounters, "pokemon") if v and v not in pokemon_names]
        if invalid_encounter_species:
            _append_issue(
                issues,
                level="error",
                code="FK_ENCOUNTERS_POKEMON",
                table="encounters",
                detail="encounters.pokemon missing in pokemon_data",
                count=len(invalid_encounter_species),
            )

        invalid_team_member_species = [v for v in _series_set(team_members, "pokemon_species") if v and v not in pokemon_names]
        if invalid_team_member_species:
            _append_issue(
                issues,
                level="error",
                code="FK_TEAM_MEMBERS_POKEMON",
                table="team_members",
                detail="team_members.pokemon_species missing in pokemon_data",
                count=len(invalid_team_member_species),
            )

    member_moves: set[str] = set()
    for column in ("move_1", "move_2", "move_3", "move_4"):
        member_moves |= _series_set(team_member_moves, column)
    if move_names and member_moves:
        invalid_member_moves = [v for v in member_moves if v and v not in move_names]
        if invalid_member_moves:
            _append_issue(
                issues,
                level="error",
                code="FK_MEMBER_MOVESET_MOVE",
                table="team_member_moves",
                detail="member_moveset_combos moves missing in move_reference",
                count=len(invalid_member_moves),
            )

    is_valid = not any(issue.level == "error" for issue in issues)
    return ValidationReport(is_valid=is_valid, issues=issues)
