from __future__ import annotations

from src.pipeline.silver.move_power import resolve_effective_power


def test_resolve_effective_power_examples() -> None:
    assert resolve_effective_power("focus-blast", 120, "special") == (120.0, "direct_power")
    assert resolve_effective_power("electric-terrain", None, "status") == (0.0, "status_no_damage")
    assert resolve_effective_power("kings-shield", None, "status") == (0.0, "status_no_damage")
    assert resolve_effective_power("seismic-toss", None, "physical", level=42) == (42.0, "fixed_damage_level")
    assert resolve_effective_power("night-shade", None, "special", level=35) == (35.0, "fixed_damage_level")
    assert resolve_effective_power("dragon-rage", None, "special") == (40.0, "fixed_damage_40")
    assert resolve_effective_power("sonic-boom", None, "special") == (20.0, "fixed_damage_20")
    assert resolve_effective_power("grass-knot", None, "special") == (60.0, "variable_power_proxy")
