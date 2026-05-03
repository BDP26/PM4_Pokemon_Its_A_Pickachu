from __future__ import annotations

import importlib


def test_team_structure_limits_are_env_driven(monkeypatch) -> None:
    monkeypatch.setenv("PM4_TEAM_MEMBER_LIMIT", "6")
    monkeypatch.setenv("PM4_MEMBER_LEVEL", "25")

    module = importlib.import_module("src.pipeline.silver.config.team_config")
    module = importlib.reload(module)

    assert module.DEFAULT_TEAM_MEMBER_LIMIT == 6
    assert module.DEFAULT_MEMBER_LEVEL == 25


def test_zero_variant_caps_are_supported(monkeypatch) -> None:
    monkeypatch.setenv("PM4_MEMBER_MOVESET_COMBO_LIMIT", "9")
    monkeypatch.setenv("PM4_TEAM_VARIANT_LIMIT", "0")
    monkeypatch.setenv("PM4_MOVESET_VARIANT_LIMIT_PER_TEAM", "0")

    module = importlib.import_module("src.pipeline.silver.config.team_config")
    module = importlib.reload(module)

    assert module.DEFAULT_MEMBER_MOVESET_COMBO_LIMIT == 9
    assert module.DEFAULT_TEAM_VARIANT_LIMIT == 0
    assert module.DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM == 0
