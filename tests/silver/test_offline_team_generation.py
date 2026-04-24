from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from src.pipeline.silver.inputs.builders.player_teams import _rank_candidate_pool, build_player_teams_from_progression_context
from src.pipeline.silver.inputs.sources.boss_teams import extract_boss_teams_from_kaggle_source


def test_player_team_generation_requires_reference_context() -> None:
    with pytest.raises(ValueError, match="reference_context is required"):
        build_player_teams_from_progression_context([])


def test_boss_team_generation_requires_reference_context(tmp_path) -> None:
    with pytest.raises(ValueError, match="reference_context is required"):
        extract_boss_teams_from_kaggle_source(tmp_path, allowed_versions={"red"})


def test_team_generation_modules_do_not_import_connector_runtime() -> None:
    player_module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")
    boss_module = importlib.import_module("src.pipeline.silver.inputs.sources.boss_teams")

    forbidden_import_fragments = ("requests", "pokebase", "pokeapi", "connector")
    for module in (player_module, boss_module):
        source = Path(module.__file__).read_text(encoding="utf-8")
        for fragment in forbidden_import_fragments:
            assert f"import {fragment}" not in source


def test_candidate_pool_ranking_prefers_level_realism() -> None:
    ranked, diagnostics = _rank_candidate_pool(
        [
            ("species_far", 90, 60, 50),
            ("species_close", 60, 20, 50),
        ],
        boss_level=20,
        pool_size=2,
    )
    assert ranked[0][0] == "species_close"
    assert diagnostics["output"] == 2
