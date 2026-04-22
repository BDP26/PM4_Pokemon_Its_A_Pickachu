from __future__ import annotations

import importlib


def test_effective_limit_unbounded_when_large_variants_enabled(monkeypatch) -> None:
    module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")

    monkeypatch.setattr(module, "ALLOW_LARGE_TEAM_VARIANTS", True)
    monkeypatch.setattr(module, "DEFAULT_TEAM_VARIANT_LIMIT", 0)
    monkeypatch.setattr(module, "DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM", 60)

    assert module._effective_team_variant_limit(variant_space_size=5000) is None


def test_effective_limit_conservative_mode_uses_smallest_active_cap(monkeypatch) -> None:
    module = importlib.import_module("src.pipeline.silver.inputs.builders.player_teams")

    monkeypatch.setattr(module, "ALLOW_LARGE_TEAM_VARIANTS", False)
    monkeypatch.setattr(module, "DEFAULT_TEAM_VARIANT_LIMIT", 120)
    monkeypatch.setattr(module, "DEFAULT_MOVESET_VARIANT_LIMIT_PER_TEAM", 60)

    assert module._effective_team_variant_limit(variant_space_size=5000) == 60
