from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.silver.inputs.reference_context import MoveReferenceContext, normalize_move_name
from src.pipeline.silver.inputs.sources.boss_teams import _normalize_kaggle_row, load_kaggle_boss_rows_by_game
from src.pipeline.silver.orchestration.build_silver import (
    _build_boss_compact_tables,
    _dedupe_bootstrap_entries,
    _build_kaggle_bootstrap_entries,
    _canonicalize_boss_teams_to_references,
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


def test_build_kaggle_bootstrap_entries_only_keeps_delta_rows() -> None:
    kaggle_rows_by_game = {
        "crystal": [
            {"Pokemon": "Gyarados", "Level": 30, "Move 1": "Surf", "Move 2": "Flail", "Move 3": "", "Move 4": ""},
            {"Pokemon": "Dragonair", "Level": 35, "Move 1": "Slam", "Move 2": "", "Move 3": "", "Move 4": ""},
            {"Pokemon": "Lapras", "Level": 28, "Move 1": "Ice Beam", "Move 2": "", "Move 3": "", "Move 4": ""},
        ]
    }
    learnable_moves_df = pd.DataFrame(
        [
            {"game_version": "crystal", "pokemon_species": "gyarados", "move_name": "surf", "learned_level": 1},
            {"game_version": "crystal", "pokemon_species": "dragonair", "move_name": "slam", "learned_level": 1},
        ]
    )
    move_reference_df = pd.DataFrame(
        [
            {"move_name": "surf"},
            {"move_name": "flail"},
        ]
    )

    entries = _build_kaggle_bootstrap_entries(
        kaggle_rows_by_game,
        learnable_moves_df=learnable_moves_df,
        move_reference_df=move_reference_df,
    )

    assert ("gyarados", 30, "crystal", ["surf", "flail"]) not in entries
    assert ("dragonair", 35, "crystal", ["slam"]) in entries
    assert ("lapras", 28, "crystal", ["ice-beam"]) in entries


def test_dedupe_bootstrap_entries_merges_move_hints_for_same_species_level_game() -> None:
    deduped = _dedupe_bootstrap_entries(
        [
            ("alakazam", 50, "diamond", ["psychic"]),
            ("alakazam", 50, "diamond", ["focus-blast"]),
        ]
    )

    assert deduped == [("alakazam", 50, "diamond", ["psychic", "focus-blast"])]


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
    assert compact["member_move_options"] == []
    assert compact["source_team_members"][0]["fixed_moves"] == ["surf", "flail"]


def test_striaton_variants_remain_separate_reference_bosses() -> None:
    boss_teams = [
        {
            "team_id": "boss:black:chili",
            "boss_name": "Chili",
            "game_version": "black",
            "pokemon": ["lillipup", "pansear"],
            "levels": [12, 14],
            "moves": [["bite"], ["incinerate"]],
            "pokemon_instance_ids": ["boss:black:chili:m1", "boss:black:chili:m2"],
            "avg_level": 13,
        },
        {
            "team_id": "boss:black:cilan",
            "boss_name": "Cilan",
            "game_version": "black",
            "pokemon": ["lillipup", "pansage"],
            "levels": [12, 14],
            "moves": [["bite"], ["vine-whip"]],
            "pokemon_instance_ids": ["boss:black:cilan:m1", "boss:black:cilan:m2"],
            "avg_level": 13,
        },
        {
            "team_id": "boss:black:cress",
            "boss_name": "Cress",
            "game_version": "black",
            "pokemon": ["lillipup", "panpour"],
            "levels": [12, 14],
            "moves": [["bite"], ["water-gun"]],
            "pokemon_instance_ids": ["boss:black:cress:m1", "boss:black:cress:m2"],
            "avg_level": 13,
        },
        {
            "team_id": "boss:black:lenora",
            "boss_name": "Lenora",
            "game_version": "black",
            "pokemon": ["herdier"],
            "levels": [18],
            "moves": [["retaliate"]],
            "pokemon_instance_ids": ["boss:black:lenora:m1"],
            "avg_level": 18,
        },
    ]
    boss_move_data = {
        member_id: {"move_details": {}, "provided_moves": [], "learnable_moves": []}
        for team in boss_teams
        for member_id in team["pokemon_instance_ids"]
    }
    bosses_reference_df = pd.DataFrame(
        [
            {"game_version": "black-white", "boss_name_canonical": "Chili", "boss_name_kaggle": "Chili"},
            {"game_version": "black-white", "boss_name_canonical": "Cilan", "boss_name_kaggle": "Cilan"},
            {"game_version": "black-white", "boss_name_canonical": "Cress", "boss_name_kaggle": "Cress"},
            {"game_version": "black", "boss_name_canonical": "Lenora", "boss_name_kaggle": "Lenora"},
        ]
    )
    for team in boss_teams[:3]:
        team["game_version"] = "black-white"

    canonicalized_teams, filtered_move_data = _canonicalize_boss_teams_to_references(
        boss_teams,
        boss_move_data,
        bosses_reference_df,
    )

    assert [team["team_id"] for team in canonicalized_teams] == [
        "boss:black:chili",
        "boss:black:cilan",
        "boss:black:cress",
        "boss:black:lenora",
    ]
    assert [team["boss_name"] for team in canonicalized_teams] == ["chili", "cilan", "cress", "lenora"]
    assert set(filtered_move_data) == {
        "boss:black:chili:m1",
        "boss:black:chili:m2",
        "boss:black:cilan:m1",
        "boss:black:cilan:m2",
        "boss:black:cress:m1",
        "boss:black:cress:m2",
        "boss:black:lenora:m1",
    }


def test_champion_alias_variants_collapse_to_single_reference_boss() -> None:
    boss_teams = [
        {
            "team_id": "boss:blue:bulbasaur",
            "boss_id": "blue:champion-blue-bulbasaur",
            "boss_name": "Champion Blue Bulbasaur",
            "game_version": "blue",
            "pokemon": ["pidgeot"],
            "levels": [65],
            "moves": [["wing-attack"]],
            "pokemon_instance_ids": ["boss:blue:bulbasaur:m1"],
            "avg_level": 65,
        },
        {
            "team_id": "boss:blue:squirtle",
            "boss_id": "blue:champion-blue-squirtle",
            "boss_name": "Blue",
            "game_version": "blue",
            "pokemon": ["pidgeot"],
            "levels": [65],
            "moves": [["wing-attack"]],
            "pokemon_instance_ids": ["boss:blue:squirtle:m1"],
            "avg_level": 65,
        },
    ]
    boss_move_data = {
        "boss:blue:bulbasaur:m1": {"move_details": {}, "provided_moves": [], "learnable_moves": []},
        "boss:blue:squirtle:m1": {"move_details": {}, "provided_moves": [], "learnable_moves": []},
    }
    bosses_reference_df = pd.DataFrame(
        [
            {
                "game_version": "blue",
                "boss_id": "blue:blue",
                "boss_name_canonical": "Blue",
                "boss_name_kaggle": "Champion Blue Squirtle",
                "boss_name_aliases": [
                    "Blue",
                    "Champion Blue Squirtle",
                    "Champion Blue Bulbasaur",
                    "Champion Blue Charmander",
                ],
            }
        ]
    )

    canonicalized_teams, filtered_move_data = _canonicalize_boss_teams_to_references(
        boss_teams,
        boss_move_data,
        bosses_reference_df,
    )

    assert [team["team_id"] for team in canonicalized_teams] == ["boss:blue:squirtle"]
    assert [team["boss_name"] for team in canonicalized_teams] == ["blue"]
    assert [team["boss_id"] for team in canonicalized_teams] == ["blue:blue"]
    assert set(filtered_move_data) == {"boss:blue:squirtle:m1"}


def test_unmatched_boss_teams_are_dropped_during_canonicalization() -> None:
    boss_teams = [
        {
            "team_id": "boss:gold:blaine",
            "boss_id": "gold:blaine",
            "boss_name": "Blaine",
            "game_version": "gold",
            "pokemon": ["magcargo"],
            "levels": [46],
            "moves": [["flamethrower"]],
            "pokemon_instance_ids": ["boss:gold:blaine:m1"],
            "avg_level": 46,
        },
        {
            "team_id": "boss:gold:falkner",
            "boss_id": "gold:falkner",
            "boss_name": "Falkner",
            "game_version": "gold",
            "pokemon": ["pidgeotto"],
            "levels": [9],
            "moves": [["gust"]],
            "pokemon_instance_ids": ["boss:gold:falkner:m1"],
            "avg_level": 9,
        },
    ]
    boss_move_data = {
        "boss:gold:blaine:m1": {"move_details": {}, "provided_moves": [], "learnable_moves": []},
        "boss:gold:falkner:m1": {"move_details": {}, "provided_moves": [], "learnable_moves": []},
    }
    bosses_reference_df = pd.DataFrame(
        [
            {
                "game_version": "gold",
                "boss_id": "gold:falkner",
                "boss_name_canonical": "Falkner",
                "boss_name_kaggle": "Falkner",
            }
        ]
    )

    canonicalized_teams, filtered_move_data = _canonicalize_boss_teams_to_references(
        boss_teams,
        boss_move_data,
        bosses_reference_df,
    )

    assert [team["team_id"] for team in canonicalized_teams] == ["boss:gold:falkner"]
    assert [team["boss_id"] for team in canonicalized_teams] == ["gold:falkner"]
    assert set(filtered_move_data) == {"boss:gold:falkner:m1"}


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


def test_boss_coverage_accepts_kaggle_defined_move_without_learnable_pair(tmp_path: Path) -> None:
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
    boss_members = pd.DataFrame(
        [
            {
                "game_version": "silver",
                "boss_id": "boss:silver:bugsy",
                "boss_name": "bugsy",
                "slot": 1,
                "pokemon_species": "scyther",
                "level": 17,
                "move_name": "quick-attack",
            }
        ]
    )
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
    assert set(report["severity"].tolist()) == {"OK"}
    assert set(report["reason"].tolist()) == {"complete"}
    assert set(report["learnable_pair_present"].tolist()) == {False}
