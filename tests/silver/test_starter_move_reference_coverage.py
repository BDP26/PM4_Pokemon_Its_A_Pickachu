from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.config import game_config
from src.pipeline.silver.inputs.builders import bootstrap_moves
from src.pipeline.silver.orchestration import build_silver


def test_starter_species_universe_includes_starters_and_evolutions(monkeypatch) -> None:
    def _fake_rules(species: str) -> dict[str, dict[str, object]]:
        return {
            species: {"base_species": species},
            f"{species}-mid": {"base_species": species},
            f"{species}-final": {"base_species": species},
        }

    monkeypatch.setattr(bootstrap_moves, "get_species_evolution_rules", _fake_rules)

    universe = build_silver._collect_starter_chain_species_by_game([{"game_key": "red"}])
    assert "red" in universe
    assert "bulbasaur" in universe["red"]
    assert "bulbasaur-mid" in universe["red"]
    assert "bulbasaur-final" in universe["red"]


def test_validate_starter_chain_move_coverage_requires_learnable_rows(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Starter-chain move reference validation failed"):
        build_silver._validate_starter_chain_move_coverage(
            learnable_moves_df=pd.DataFrame(
                [
                    {
                        "game_version": "red",
                        "pokemon_species": "bulbasaur",
                        "move_name": "tackle",
                        "learned_level": 1,
                    }
                ]
            ),
            starter_chain_species_by_game={"red": {"bulbasaur", "ivysaur"}},
            diagnostics_dir=tmp_path,
        )


def test_validate_starter_chain_move_coverage_passes_with_complete_rows(tmp_path: Path) -> None:
    build_silver._validate_starter_chain_move_coverage(
        learnable_moves_df=pd.DataFrame(
            [
                {"game_version": "red", "pokemon_species": "bulbasaur", "move_name": "tackle", "learned_level": 1},
                {"game_version": "red", "pokemon_species": "ivysaur", "move_name": "razor-leaf", "learned_level": 20},
            ]
        ),
        starter_chain_species_by_game={"red": {"bulbasaur", "ivysaur"}},
        diagnostics_dir=tmp_path,
    )


def test_validate_universal_starter_family_move_coverage_writes_gaps_without_raising(tmp_path: Path) -> None:
    missing_rows = build_silver._validate_universal_starter_family_move_coverage(
        learnable_moves_df=pd.DataFrame(
            [
                {
                    "game_version": "red",
                    "pokemon_species": "bulbasaur",
                    "move_name": "tackle",
                    "learned_level": 1,
                }
            ]
        ),
        universal_starter_species_by_game={"red": {"bulbasaur", "ivysaur"}},
        diagnostics_dir=tmp_path,
    )

    assert any(
        row["game_version"] == "red" and row["species_name"] == "ivysaur"
        for row in missing_rows
    )
    assert (tmp_path / "starter_family_move_gaps.csv").exists()


def test_all_starter_family_bootstrap_entries_include_cross_version_families() -> None:
    entries = build_silver._all_starter_family_bootstrap_entries(
        [
            {"game_key": "red"},
            {"game_key": "silver"},
        ]
    )
    entry_keys = {(species, game_version) for species, _, game_version, _ in entries}

    assert ("bulbasaur", "silver") in entry_keys
    assert ("ivysaur", "silver") in entry_keys
    assert ("blastoise", "silver") in entry_keys


def test_resolve_starter_species_for_level_does_not_call_pokeapi(monkeypatch) -> None:
    # bulbasaur is in the static family lookup so _starter_family_rules returns early
    # without ever importing or calling get_species_evolution_rules.
    game_config._starter_family_rules.cache_clear()

    assert game_config.resolve_starter_species_for_level("bulbasaur", 5) == "bulbasaur"
    assert game_config.resolve_starter_species_for_level("bulbasaur", 16) == "ivysaur"


def test_level_cap_filtering_remains_in_reference_context() -> None:
    context = MoveReferenceContext(
        move_profiles={
            "tackle": {"power": 40, "damage_class": "physical"},
            "hyper-beam": {"power": 150, "damage_class": "special"},
        },
        learnable_by_game_species={
            ("red", "bulbasaur"): {"tackle": 1, "hyper-beam": 50},
        },
    )
    assert "tackle" in context.learnable_moves("bulbasaur", 10, "red")
    assert "hyper-beam" not in context.learnable_moves("bulbasaur", 10, "red")


def test_diagnose_player_missing_damaging_moves_writes_summary(tmp_path: Path) -> None:
    class _Context:
        def damaging_moves(self, species: str, level: int, game_version: str) -> list[str]:
            del species, level, game_version
            return []

        def learnable_levels(self, species: str, game_version: str) -> dict[str, int]:
            del game_version
            if species == "blastoise":
                return {}
            return {"splash": 1}

    df = build_silver._diagnose_player_missing_damaging_moves(
        progression_source_teams=[
            {
                "game_version": "silver",
                "source_team_id": "player-source:silver:a",
                "boss_id": "boss:silver:falkner",
                "pokemon": ["blastoise", "magikarp"],
                "levels": [44, 5],
            },
            {
                "game_version": "silver",
                "source_team_id": "player-source:silver:b",
                "boss_id": "boss:silver:bugsy",
                "pokemon": ["blastoise"],
                "levels": [44],
            },
        ],
        reference_context=_Context(),
        diagnostics_dir=tmp_path,
    )

    assert not df.empty
    summary_path = tmp_path / "player_no_damaging_move_gaps_summary.csv"
    assert summary_path.exists()
    summary_df = pd.read_csv(summary_path)
    blastoise_row = summary_df[
        (summary_df["game_version"] == "silver")
        & (summary_df["pokemon_species"] == "blastoise")
        & (summary_df["level"] == 44)
        & (summary_df["reason"] == "missing_learnable_reference")
    ]
    assert not blastoise_row.empty
    assert int(blastoise_row.iloc[0]["affected_team_count"]) == 2

