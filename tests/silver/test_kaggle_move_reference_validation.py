from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.silver.inputs.reference_context import MoveReferenceContext, normalize_move_name
from src.pipeline.silver.inputs.sources.boss_teams import _normalize_kaggle_row, load_kaggle_boss_rows_by_game
from src.pipeline.silver.orchestration.build_silver import (
    _build_boss_compact_tables,
    _build_kaggle_bootstrap_entries,
    _build_boss_team_members_reference_rows,
    _validate_boss_reference_coverage,
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


def test_kaggle_bootstrap_entries_use_full_kaggle_rows(tmp_path: Path) -> None:
    kaggle_dir = tmp_path / "kagglehub"
    kaggle_dir.mkdir(parents=True, exist_ok=True)
    csv_path = kaggle_dir / "gym_leaders_elite_four.csv"
    csv_path.write_text(
        "Game;Gym leader;Gym;Pokemon;Level;Move 1;Move 2;Move 3;Move 4\n"
        "silver;Bugsy;Azalea;Scyther;17;Quick Attack;Leer;;\n",
        encoding="utf-8",
    )

    rows_by_game = load_kaggle_boss_rows_by_game(tmp_path, allowed_versions={"silver"})
    entries = _build_kaggle_bootstrap_entries(rows_by_game)
    assert ("scyther", 17, "silver", ["quick-attack", "leer"]) in entries


def test_boss_coverage_warns_on_missing_learnable_pair_only(tmp_path: Path) -> None:
    boss_teams = [
        {
            "team_id": "boss:silver:bugsy",
            "boss_name": "Bugsy",
            "game_version": "silver",
            "team_role": "boss",
            "pokemon": ["scyther"],
            "levels": [17],
            "moves": [["quick-attack"]],
        }
    ]
    boss_members = pd.DataFrame(_build_boss_team_members_reference_rows(boss_teams))
    pokemon_data_df = pd.DataFrame([{"pokemon_species": "scyther"}])
    move_reference_df = pd.DataFrame(
        [{"move_name": "quick-attack", "damage_class": "physical", "type": "normal", "power": 40}]
    )
    learnable_moves_df = pd.DataFrame(columns=["game_version", "pokemon_species", "move_name"])

    _validate_boss_reference_coverage(
        boss_team_members_df=boss_members,
        boss_teams=boss_teams,
        pokemon_data_df=pokemon_data_df,
        move_reference_df=move_reference_df,
        learnable_moves_df=learnable_moves_df,
        diagnostics_dir=tmp_path,
    )

    report = pd.read_csv(tmp_path / "boss_silver_reference_coverage.csv")
    assert set(report["severity"].tolist()) == {"WARN"}
