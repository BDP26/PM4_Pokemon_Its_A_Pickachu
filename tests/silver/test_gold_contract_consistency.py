from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_json, write_json, write_parquet
from src.pipeline.gold.orchestration.build_gold import _REQUIRED_MANIFEST_DATASET_FILES
from src.pipeline.gold.simulation import run_gold_simulation as gold_sim_module
from src.pipeline.silver.reporting.silver_manifest import create_silver_manifest


def test_gold_required_manifest_keys_match_declared_strict_contract(tmp_path: Path) -> None:
    create_silver_manifest(tmp_path)
    manifest = read_json(tmp_path / "manifest.json")
    declared = set(manifest["contracts"]["gold_strict"]["required_dataset_keys"]) - {"boss_records"}
    assert declared == set(_REQUIRED_MANIFEST_DATASET_FILES)


def test_manifest_dataset_entries_use_compact_sharded_contract(tmp_path: Path) -> None:
    simulation = tmp_path / "simulation"
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir(parents=True, exist_ok=True)
    simulation.mkdir(parents=True, exist_ok=True)

    write_parquet(simulation / "source_teams_red.parquet", [{"source_team_id": "t1", "game_version": "red"}])
    write_parquet(simulation / "source_team_members_red.parquet", [{"team_member_id": "m1", "source_team_id": "t1", "game_version": "red", "slot": 1, "pokemon_species": "pikachu", "level": 10}])
    write_parquet(simulation / "member_move_options_red.parquet", [{"team_member_id": "m1", "source_team_id": "t1", "game_version": "red", "move_name": "tackle", "option_rank": 1}])
    write_json(snapshots / "red_boss_snapshots.jsonl", [])

    create_silver_manifest(tmp_path)
    manifest = read_json(tmp_path / "manifest.json")
    datasets = manifest["datasets"]

    assert datasets["simulation_inputs_teams"]["glob"] == "source_teams_*.parquet"
    assert datasets["source_team_members"]["glob"] == "source_team_members_*.parquet"
    assert datasets["member_move_options"]["glob"] == "member_move_options_*.parquet"


def test_gold_simulation_defaults_to_sharded_discovery(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def _fake_loader(**kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(gold_sim_module, "load_reconstructed_teams_from_silver", _fake_loader)
    gold_sim_module.run_gold_simulation_from_silver(
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        bronze_dir=tmp_path / "bronze",
        required_input_files=None,
    )

    assert captured["teams_path"] is None
    assert captured["team_members_path"] is None
    assert captured["member_move_options_path"] is None
