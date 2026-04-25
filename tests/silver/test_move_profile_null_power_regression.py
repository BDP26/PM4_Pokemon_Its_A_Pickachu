from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.silver.inputs.reference_context import load_move_reference_tables


def test_null_power_moves_are_valid_profiles_in_reference_cache(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True)

    problem_moves = [
        {"move_name": "attack-order", "type": "bug", "damage_class": "physical", "power": 90},
        {"move_name": "boomburst", "type": "normal", "damage_class": "special", "power": 140},
        {"move_name": "dazzling-gleam", "type": "fairy", "damage_class": "special", "power": 80},
        {"move_name": "defend-order", "type": "bug", "damage_class": "status", "power": None},
        {"move_name": "electric-terrain", "type": "electric", "damage_class": "status", "power": None},
        {"move_name": "fly", "type": "flying", "damage_class": "physical", "power": 90},
        {"move_name": "focus-blast", "type": "fighting", "damage_class": "special", "power": 120},
        {"move_name": "heal-order", "type": "bug", "damage_class": "status", "power": None},
        {"move_name": "infestation", "type": "bug", "damage_class": "special", "power": 20},
        {"move_name": "kings-shield", "type": "steel", "damage_class": "status", "power": None},
    ]
    write_parquet(references_dir / "move_reference.parquet", problem_moves)
    write_parquet(
        references_dir / "learnable_moves.parquet",
        [
            {
                "game_version": "x",
                "pokemon_species": "aegislash",
                "move_name": "kings-shield",
                "learned_level": 1,
                "learn_method": "level-up",
            }
        ],
    )

    move_profiles, _ = load_move_reference_tables(silver_dir=silver_dir)
    assert set(move_profiles) == {row["move_name"] for row in problem_moves}

    for move_name in sorted(move_profiles):
        profile = move_profiles[move_name]
        assert profile["type"] is not None
        assert profile["damage_class"] is not None
        assert profile["effective_power"] is not None
        assert profile["power_handling"] is not None

    status_examples = [move_profiles["electric-terrain"], move_profiles["kings-shield"], move_profiles["defend-order"], move_profiles["heal-order"]]
    assert {example["power_handling"] for example in status_examples} == {"status_no_damage"}
    assert all(float(example["effective_power"]) == 0.0 for example in status_examples)

    written = read_parquet(references_dir / "move_reference.parquet")
    assert "move_name" in written.columns
