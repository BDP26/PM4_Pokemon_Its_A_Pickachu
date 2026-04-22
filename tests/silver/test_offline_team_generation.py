from __future__ import annotations

import importlib

import pytest

from src.pipeline.silver.inputs.builders.player_teams import build_player_teams_from_progression_context
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

    assert "_build_member_detail" not in vars(player_module)
    assert "_build_member_moves" not in vars(player_module)
    assert "_build_member_detail" not in vars(boss_module)
    assert "_build_member_moves" not in vars(boss_module)
