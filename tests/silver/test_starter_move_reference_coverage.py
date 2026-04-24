from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.config import game_config
from src.pipeline.silver.orchestration import build_silver


def test_starter_species_universe_includes_starters_and_evolutions(monkeypatch) -> None:
    def _fake_rules(species: str) -> dict[str, dict[str, object]]:
        return {
            species: {"base_species": species},
            f"{species}-mid": {"base_species": species},
            f"{species}-final": {"base_species": species},
        }

    monkeypatch.setattr(build_silver, "get_species_evolution_rules", _fake_rules)

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


def test_resolve_starter_species_for_level_does_not_call_pokeapi(monkeypatch) -> None:
    def _fail_call(_: str) -> dict[str, dict[str, object]]:
        raise AssertionError("unexpected pokeapi call")

    monkeypatch.setattr(game_config, "get_species_evolution_rules", _fail_call)
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
