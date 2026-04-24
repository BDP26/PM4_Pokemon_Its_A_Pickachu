from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.gold.inputs.team_tables import load_reconstructed_teams_from_silver
from src.pipeline.gold.simulation import run_gold_simulation as run_module


def _write_compact_inputs(simulation_dir: Path) -> None:
    write_parquet(
        simulation_dir / "source_teams_red.parquet",
        [
            {
                "source_team_id": "player_t1",
                "game_version": "red",
                "team_role": "player_source",
                "boss_name": "brock",
                "gym": "brock",
                "is_player_candidate": True,
                "avg_level": 12,
            },
            {
                "source_team_id": "boss_t1",
                "game_version": "red",
                "team_role": "boss_source",
                "boss_name": "brock",
                "gym": "brock",
                "is_player_candidate": False,
                "avg_level": 12,
            },
        ],
    )
    write_parquet(
        simulation_dir / "source_team_members_red.parquet",
        [
            {"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "slot": 1, "pokemon_species": "pikachu", "level": 12},
            {"team_member_id": "m2", "source_team_id": "boss_t1", "game_version": "red", "slot": 1, "pokemon_species": "geodude", "level": 12},
        ],
    )
    write_parquet(
        simulation_dir / "member_move_options_red.parquet",
        [
            {"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "move_name": "quick-attack", "option_rank": 2},
            {"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "move_name": "thunderbolt", "option_rank": 1},
            {"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "move_name": "slam", "option_rank": 3},
            {"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "move_name": "double-team", "option_rank": 4},
            {"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "move_name": "swift", "option_rank": 5},
            {"team_member_id": "m2", "source_team_id": "boss_t1", "game_version": "red", "move_name": "tackle", "option_rank": 1},
        ],
    )


def test_gold_loader_builds_bounded_moveset_variants(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    simulation_dir = silver_dir / "simulation"
    simulation_dir.mkdir(parents=True, exist_ok=True)
    _write_compact_inputs(simulation_dir)

    teams = load_reconstructed_teams_from_silver(silver_dir=silver_dir)
    by_id = {team["team_id"]: team for team in teams}

    assert by_id["player_t1"]["moves"][0] == ["thunderbolt", "quick-attack", "slam", "double-team"]
    assert len(by_id["player_t1"]["moves"][0]) == 4
    assert by_id["player_t1"]["is_player_candidate"] is True
    assert by_id["boss_t1"]["is_player_candidate"] is False


def test_gold_simulation_consumes_compact_inputs(monkeypatch, tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    bronze_dir = tmp_path / "bronze"
    simulation_dir = silver_dir / "simulation"
    simulation_dir.mkdir(parents=True, exist_ok=True)
    _write_compact_inputs(simulation_dir)

    monkeypatch.setattr(run_module, "_run_gold_team_battle_simulations", lambda **_: None)
    monkeypatch.setattr(run_module, "_build_gold_battle_seeds", lambda **_: None)
    monkeypatch.setattr(run_module, "_run_gold_monte_carlo_optimizer", lambda **_: None)

    run_module.run_gold_simulation_from_silver(
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        bronze_dir=bronze_dir,
        required_input_files=None,
    )

    teams_out = read_parquet(gold_dir / "simulation" / "teams.parquet")
    assert {row["team_id"] for row in teams_out.to_dict(orient="records")} == {"player_t1", "boss_t1"}
