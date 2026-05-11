from __future__ import annotations

import importlib


def test_team_structure_limits_are_env_driven(monkeypatch) -> None:
    monkeypatch.setenv("PM4_TEAM_MEMBER_LIMIT", "6")
    monkeypatch.setenv("PM4_MEMBER_LEVEL", "25")
    monkeypatch.setenv("PM4_TEAM_TYPE_WEIGHT_CAP", "2.0")

    module = importlib.import_module("src.pipeline.silver.config.team_config")
    module = importlib.reload(module)

    assert module.DEFAULT_TEAM_MEMBER_LIMIT == 6
    assert module.DEFAULT_MEMBER_LEVEL == 25
    assert module.DEFAULT_TEAM_TYPE_WEIGHT_CAP == 2.0


def test_member_moveset_cap_is_env_driven(monkeypatch) -> None:
    monkeypatch.setenv("PM4_MEMBER_MOVESET_COMBO_LIMIT", "9")

    module = importlib.import_module("src.pipeline.silver.config.team_config")
    module = importlib.reload(module)

    assert module.DEFAULT_MEMBER_MOVESET_COMBO_LIMIT == 9
