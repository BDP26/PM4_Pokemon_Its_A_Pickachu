from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.common.io import write_parquet
from src.pipeline.common.simulation_config import load_runtime_battle_policy_config
from src.pipeline.gold.inputs.team_tables import load_reconstructed_teams_from_silver, validate_reconstructed_teams_for_simulation
from src.pipeline.gold.simulation import run_gold_simulation as run_module


def _write_compact_inputs(simulation_dir: Path) -> None:
    write_parquet(
        simulation_dir / "source_teams_red.parquet",
        [
            {"source_team_id": "player_t1", "game_version": "red", "team_role": "player", "origin": "generated", "is_player_candidate": True},
            {"source_team_id": "boss_t1", "game_version": "red", "team_role": "boss", "origin": "kaggle", "is_player_candidate": False},
        ],
    )
    write_parquet(
        simulation_dir / "source_team_members_red.parquet",
        [
            {"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "slot": 1, "pokemon_species": "pikachu", "level": 12},
            {"team_member_id": "m2", "source_team_id": "boss_t1", "game_version": "red", "slot": 1, "pokemon_species": "geodude", "level": 12, "fixed_moves": ["tackle"]},
        ],
    )
    write_parquet(
        simulation_dir / "member_moveset_combos_red.parquet",
        [
            {"moveset_combo_id": "c1", "team_id": "player_t1", "pokemon_instance_id": "m1", "slot_index": 1, "combo_rank": 1, "move_1": "thunderbolt"},
            {"moveset_combo_id": "c2", "team_id": "boss_t1", "pokemon_instance_id": "m2", "slot_index": 1, "combo_rank": 1, "move_1": "tackle"},
        ],
    )
    write_parquet(
        simulation_dir / "member_move_options_red.parquet",
        [{"team_member_id": "m1", "source_team_id": "player_t1", "game_version": "red", "move_name": "thunderbolt", "option_rank": 1}],
    )


def test_runtime_policy_profile_non_strict_values_fallback_to_strict(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PM4_SIM_POLICY_PROFILE", "coverage")
    monkeypatch.delenv("PM4_SIM_FAIL_ON_DEGRADED_DATA", raising=False)
    monkeypatch.delenv("PM4_SIM_BATTLE_TRIALS", raising=False)

    cfg = load_runtime_battle_policy_config()
    assert cfg.profile == "strict"
    assert cfg.fail_on_degraded_data is True
    assert int(cfg.n_battle_trials) == 15


def test_runtime_policy_profile_strict_is_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PM4_SIM_POLICY_PROFILE", raising=False)
    monkeypatch.delenv("PM4_SIM_FAIL_ON_DEGRADED_DATA", raising=False)
    monkeypatch.delenv("PM4_SIM_BATTLE_TRIALS", raising=False)

    cfg = load_runtime_battle_policy_config()
    assert cfg.profile == "strict"
    assert cfg.fail_on_degraded_data is True
    assert int(cfg.n_battle_trials) == 15


def test_input_integrity_gate_detects_invalid_team_rows(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    simulation_dir = silver_dir / "simulation"
    simulation_dir.mkdir(parents=True, exist_ok=True)
    _write_compact_inputs(simulation_dir)

    teams = load_reconstructed_teams_from_silver(silver_dir=silver_dir)
    teams[0] = dict(teams[0])
    teams[0]["team_id"] = ""
    teams[0]["moves"] = [["thunderbolt", "null"]]

    issues = validate_reconstructed_teams_for_simulation(teams)
    assert any("missing team_id" in issue for issue in issues)


def test_input_integrity_gate_requires_player_coverage_for_each_boss_context(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    simulation_dir = silver_dir / "simulation"
    simulation_dir.mkdir(parents=True, exist_ok=True)
    _write_compact_inputs(simulation_dir)

    teams = load_reconstructed_teams_from_silver(silver_dir=silver_dir)
    teams = [row for row in teams if not bool(row.get("is_player_candidate", False))]
    teams[0]["boss_name"] = "brock"
    teams[0]["gym"] = "brock"
    teams[0]["game_version"] = "red"
    issues = validate_reconstructed_teams_for_simulation(teams)
    assert any("missing player candidate teams for boss context" in issue for issue in issues)


def test_gold_simulation_fails_fast_on_integrity_issues(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    bronze_dir = tmp_path / "bronze"
    simulation_dir = silver_dir / "simulation"
    simulation_dir.mkdir(parents=True, exist_ok=True)
    _write_compact_inputs(simulation_dir)

    def _broken_loader(**kwargs):
        rows = load_reconstructed_teams_from_silver(**kwargs)
        rows[0] = dict(rows[0])
        rows[0]["game_version"] = ""
        return rows

    monkeypatch.setattr(run_module, "load_reconstructed_teams_from_silver", _broken_loader)
    with pytest.raises(ValueError, match="input integrity gate failed"):
        run_module.run_gold_simulation_from_silver(
            silver_dir=silver_dir,
            gold_dir=gold_dir,
            bronze_dir=bronze_dir,
            required_input_files=None,
        )
