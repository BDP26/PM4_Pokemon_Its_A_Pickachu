from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.silver.inputs.connectors import pokeapi_moves


def test_persist_move_reference_includes_complete_kings_shield_metadata(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "kings-shield",
                "power": None,
                "raw_power": None,
                "damage_class": "status",
                "type": "steel",
                "accuracy": None,
                "pp": 10,
                "effective_power": 0.0,
                "power_handling": "status_no_damage",
                "is_status_move": True,
                "is_damage_move": False,
                "is_null_power": True,
            }
        ],
    )
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
        partition_cols=["game_version", "pokemon_species"],
    )

    pokeapi_moves._clear_loaded_caches()
    pokeapi_moves.persist_move_reference_cache(
        [("aegislash", 50, "x", ["King's Shield"])],
        silver_dir=silver_dir,
    )

    move_reference_df = read_parquet(references_dir / "move_reference.parquet")
    kings_shield = move_reference_df.loc[move_reference_df["move_name"] == "kings-shield"].iloc[0].to_dict()

    assert kings_shield["type"] == "steel"
    assert kings_shield["damage_class"] == "status"
    assert float(kings_shield["effective_power"]) == 0.0
    assert bool(kings_shield["is_status_move"]) is True
    assert bool(kings_shield["is_damage_move"]) is False

    assert move_reference_df["type"].isna().sum() == 0
    assert move_reference_df["damage_class"].isna().sum() == 0


def test_move_reference_validation_rejects_incomplete_rows() -> None:
    invalid_rows = [
        {
            "move_name": "kings-shield",
            "type": None,
            "damage_class": None,
            "effective_power": 0.0,
            "power_handling": "status_no_damage",
            "is_status_move": True,
            "is_damage_move": False,
        }
    ]

    try:
        pokeapi_moves._validate_move_reference_rows(invalid_rows)
    except ValueError as exc:
        message = str(exc)
        assert "Invalid move_reference rows: total=1" in message
        assert "kings-shield" in message
    else:
        raise AssertionError("Expected ValueError for invalid move reference rows")
