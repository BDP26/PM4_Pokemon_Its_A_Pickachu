from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.pipeline.common.io import write_json
from src.pipeline.gold.orchestration.build_gold import (
    GoldContractError,
    _load_and_validate_gold_contract,
    _normalize_game_key_to_game_version,
)


class GoldContractValidationTests(unittest.TestCase):
    def test_missing_manifest_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            silver_dir = Path(tmpdir)
            with self.assertRaisesRegex(GoldContractError, "missing_manifest"):
                _load_and_validate_gold_contract(silver_dir)

    def test_missing_snapshot_list_fails_fast(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            silver_dir = Path(tmpdir)
            write_json(silver_dir / "manifest.json", {"datasets": {"boss_records": {"files": []}}})
            with self.assertRaisesRegex(GoldContractError, "missing_snapshot_files"):
                _load_and_validate_gold_contract(silver_dir)

    def test_valid_manifest_contract_resolves_required_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            silver_dir = Path(tmpdir)
            (silver_dir / "snapshots").mkdir(parents=True, exist_ok=True)
            (silver_dir / "simulation").mkdir(parents=True, exist_ok=True)
            (silver_dir / "references").mkdir(parents=True, exist_ok=True)

            (silver_dir / "snapshots" / "red_boss_snapshots.jsonl").write_text("{}\n", encoding="utf-8")
            (silver_dir / "simulation" / "teams.parquet").write_text("", encoding="utf-8")
            (silver_dir / "simulation" / "team_members.parquet").write_text("", encoding="utf-8")
            (silver_dir / "simulation" / "team_member_moves.parquet").write_text("", encoding="utf-8")
            (silver_dir / "references" / "pokemon_reference.json").write_text("{}", encoding="utf-8")
            (silver_dir / "references" / "snapshot_available_pokemon.parquet").write_text("", encoding="utf-8")
            (silver_dir / "references" / "encounters.parquet").write_text("", encoding="utf-8")

            write_json(
                silver_dir / "manifest.json",
                {
                    "datasets": {
                        "boss_records": {"files": ["snapshots/red_boss_snapshots.jsonl"]},
                        "simulation_inputs_teams": {"file": "simulation/teams.parquet"},
                        "team_members": {"file": "simulation/team_members.parquet"},
                        "team_member_moves": {"file": "simulation/team_member_moves.parquet"},
                        "pokemon_reference": {"file": "references/pokemon_reference.json"},
                        "snapshot_available_pokemon": {"file": "references/snapshot_available_pokemon.parquet"},
                        "encounters": {"file": "references/encounters.parquet"},
                    }
                },
            )

            contract = _load_and_validate_gold_contract(silver_dir)
            self.assertEqual(len(contract["snapshot_files"]), 1)
            self.assertIn("team_members", contract["required_files"])

    def test_valid_manifest_contract_accepts_partitioned_directory_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            silver_dir = Path(tmpdir)
            (silver_dir / "snapshots").mkdir(parents=True, exist_ok=True)
            (silver_dir / "simulation" / "teams.parquet" / "game_version=red").mkdir(parents=True, exist_ok=True)
            (silver_dir / "simulation" / "team_members.parquet" / "game_version=red").mkdir(parents=True, exist_ok=True)
            (silver_dir / "simulation" / "team_member_moves.parquet" / "game_version=red").mkdir(parents=True, exist_ok=True)
            (silver_dir / "references" / "snapshot_available_pokemon.parquet" / "game_version=red").mkdir(
                parents=True,
                exist_ok=True,
            )
            (silver_dir / "references" / "encounters.parquet" / "game=red").mkdir(parents=True, exist_ok=True)
            (silver_dir / "references").mkdir(parents=True, exist_ok=True)

            (silver_dir / "snapshots" / "red_boss_snapshots.jsonl").write_text("{}\n", encoding="utf-8")
            (silver_dir / "references" / "pokemon_reference.json").write_text("{}", encoding="utf-8")

            write_json(
                silver_dir / "manifest.json",
                {
                    "datasets": {
                        "boss_records": {"files": ["snapshots/red_boss_snapshots.jsonl"]},
                        "simulation_inputs_teams": {"file": "simulation/teams.parquet"},
                        "team_members": {"file": "simulation/team_members.parquet"},
                        "team_member_moves": {"file": "simulation/team_member_moves.parquet"},
                        "pokemon_reference": {"file": "references/pokemon_reference.json"},
                        "snapshot_available_pokemon": {"file": "references/snapshot_available_pokemon.parquet"},
                        "encounters": {"file": "references/encounters.parquet"},
                    }
                },
            )

            contract = _load_and_validate_gold_contract(silver_dir)
            self.assertIn("simulation_inputs_teams", contract["required_files"])

    def test_normalize_legacy_game_column(self) -> None:
        source = pd.DataFrame([{"game": " Red ", "part": 1}])
        normalized = _normalize_game_key_to_game_version(source, source_name="test")
        self.assertIn("game_version", normalized.columns)
        self.assertEqual(normalized.loc[0, "game_version"], "red")

    def test_normalize_requires_game_version_or_game(self) -> None:
        source = pd.DataFrame([{"part": 1}])
        with self.assertRaisesRegex(GoldContractError, "missing_game_version_column"):
            _normalize_game_key_to_game_version(source, source_name="test")


if __name__ == "__main__":
    unittest.main()

