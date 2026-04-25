from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.silver.inputs.reference_context import MoveReferenceContext, normalize_move_name
from src.pipeline.silver.inputs.sources.boss_teams import _normalize_kaggle_row
from src.pipeline.silver.orchestration.build_silver import (
    _build_boss_compact_tables,
    _validate_kaggle_boss_move_profiles,
    _validate_kaggle_moves_in_move_reference,
)


def test_kaggle_missing_move_reference_fails_with_diagnostics(tmp_path: Path) -> None:
    kaggle_rows_by_game = {
        "crystal": [
            {"Pokemon": "Gyarados", "Move 1": "Fla", "Move 2": "Surf", "Move 3": "", "Move 4": ""},
        ]
    }
    move_reference_df = pd.DataFrame([{"move_name": "surf"}])

    with pytest.raises(ValueError, match="Kaggle move reference validation failed"):
        _validate_kaggle_moves_in_move_reference(kaggle_rows_by_game, move_reference_df, tmp_path)

    diagnostics_path = tmp_path / "kaggle_move_reference_gaps.csv"
    assert diagnostics_path.exists()
    diagnostics_df = pd.read_csv(diagnostics_path)
    assert "flail" in set(diagnostics_df["move_name"].tolist())
    assert "Move 1" in set(diagnostics_df["source_column"].tolist())


def test_kaggle_complete_move_reference_coverage_passes(tmp_path: Path) -> None:
    kaggle_rows_by_game = {
        "crystal": [
            {"Pokemon": "Gyarados", "Move 1": "Fla", "Move 2": "Surf", "Move 3": "", "Move 4": ""},
        ]
    }
    move_reference_df = pd.DataFrame([{"move_name": "flail"}, {"move_name": "surf"}])
    _validate_kaggle_moves_in_move_reference(kaggle_rows_by_game, move_reference_df, tmp_path)

    diagnostics_path = tmp_path / "kaggle_move_reference_gaps.csv"
    assert diagnostics_path.exists()
    diagnostics_df = pd.read_csv(diagnostics_path)
    assert diagnostics_df.empty


def test_alias_fla_normalizes_to_flail() -> None:
    assert normalize_move_name("Fla") == "flail"
    normalized_row = _normalize_kaggle_row({"Pokemon": "Gyarados", "Move 1": "Fla", "Move 2": ""})
    assert normalized_row["moves"] == ["flail"]


def test_build_member_moves_fails_fast_for_missing_provided_moves() -> None:
    context = MoveReferenceContext(
        move_profiles={
            "surf": {"move_name": "surf", "effective_power": 90, "damage_class": "special"},
        },
        learnable_by_game_species={
            ("crystal", "gyarados"): {"surf": 1},
        },
    )

    with pytest.raises(ValueError, match="Kaggle boss move reference validation failed"):
        context.build_member_moves(
            name="gyarados",
            level=50,
            moves=["surf", "flail"],
            game_version="crystal",
        )


def test_boss_move_data_validation_requires_profiles_for_provided_moves(tmp_path: Path) -> None:
    move_data = {
        "team:1:m1:gyarados": {
            "species": "gyarados",
            "game_version": "crystal",
            "provided_moves": ["surf", "flail"],
            "move_details": {"surf": {"move_name": "surf"}},
        }
    }
    with pytest.raises(ValueError, match="Kaggle boss move reference validation failed"):
        _validate_kaggle_boss_move_profiles(move_data, tmp_path)


def test_gold_facing_boss_move_records_are_complete() -> None:
    boss_teams = [
        {
            "team_id": "boss:crystal:lance",
            "boss_name": "Lance",
            "game_version": "crystal",
            "pokemon": ["gyarados"],
            "levels": [50],
            "moves": [["surf", "flail"]],
            "pokemon_instance_ids": ["boss:crystal:lance:m1:gyarados"],
            "avg_level": 50,
        }
    ]
    move_data = {
        "boss:crystal:lance:m1:gyarados": {
            "learnable_moves": ["surf", "flail"],
            "provided_moves": ["surf", "flail"],
            "move_details": {"surf": {"move_name": "surf"}, "flail": {"move_name": "flail"}},
        }
    }
    compact = _build_boss_compact_tables(boss_teams, move_data)
    assert len(compact["source_teams"]) == 1
    assert len(compact["source_team_members"]) == 1
    move_names = {row["move_name"] for row in compact["member_move_options"]}
    assert {"surf", "flail"}.issubset(move_names)
