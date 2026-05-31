from __future__ import annotations

from src.pipeline.gold.simulation.team_battle_simulations import _stable_pair_seed, _stable_sequence_seed


def test_stable_pair_seed_is_deterministic() -> None:
    seed_a = _stable_pair_seed("player:1", "boss:2", 42)
    seed_b = _stable_pair_seed("player:1", "boss:2", 42)
    assert seed_a == seed_b


def test_stable_pair_seed_changes_when_inputs_change() -> None:
    baseline = _stable_pair_seed("player:1", "boss:2", 42)
    assert baseline != _stable_pair_seed("player:1", "boss:3", 42)
    assert baseline != _stable_pair_seed("player:1", "boss:2", 99)


def test_stable_sequence_seed_is_deterministic() -> None:
    seed_a = _stable_sequence_seed("player:1", "gold:elite_four_champion", 42)
    seed_b = _stable_sequence_seed("player:1", "gold:elite_four_champion", 42)
    assert seed_a == seed_b
