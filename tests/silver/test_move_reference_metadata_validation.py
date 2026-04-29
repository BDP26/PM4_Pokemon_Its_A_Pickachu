from __future__ import annotations

from pathlib import Path

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.silver.inputs.connectors import pokeapi_moves


def test_api_move_profile_uses_offline_seed_for_gen6_kaggle_moves(monkeypatch) -> None:
    def _fail_live_lookup(_: str) -> None:
        raise AssertionError("live pokebase lookup should not run for offline-seeded moves")

    monkeypatch.setattr(pokeapi_moves.pb, "move", _fail_live_lookup)

    profile = pokeapi_moves._api_move_profile("Dazzling Gleam")

    assert profile["move_name"] == "dazzling-gleam"
    assert profile["type"] == "fairy"
    assert profile["damage_class"] == "special"
    assert float(profile["effective_power"]) == 80.0
    assert bool(profile["is_damage_move"]) is True


def test_api_move_profile_uses_readonly_pokebase_cache_before_live_lookup(monkeypatch) -> None:
    monkeypatch.setattr(
        pokeapi_moves,
        "_cached_pokebase_payload",
        lambda endpoint, resource_name_or_id=None: {
            "power": 40,
            "accuracy": 100,
            "pp": 30,
            "damage_class": {"name": "special"},
            "type": {"name": "poison"},
        }
        if endpoint == "move" and resource_name_or_id == "acid"
        else None,
    )

    def _fail_live_lookup(_: str) -> None:
        raise AssertionError("live pokebase lookup should not run when cache payload exists")

    monkeypatch.setattr(pokeapi_moves.pb, "move", _fail_live_lookup)

    profile = pokeapi_moves._api_move_profile("acid")

    assert profile["move_name"] == "acid"
    assert profile["type"] == "poison"
    assert profile["damage_class"] == "special"
    assert float(profile["effective_power"]) == 40.0


def test_api_learnable_move_levels_uses_readonly_pokebase_cache(monkeypatch) -> None:
    def _cached_payload(endpoint: str, resource_name_or_id=None):
        if endpoint == "pokemon" and resource_name_or_id == "bulbasaur":
            return {
                "moves": [
                    {
                        "move": {"name": "tackle"},
                        "version_group_details": [
                            {
                                "version_group": {"name": "red-blue"},
                                "move_learn_method": {"name": "level-up"},
                                "level_learned_at": 1,
                            }
                        ],
                    },
                    {
                        "move": {"name": "growl"},
                        "version_group_details": [
                            {
                                "version_group": {"name": "red-blue"},
                                "move_learn_method": {"name": "machine"},
                                "level_learned_at": 1,
                            }
                        ],
                    },
                ]
            }
        return None

    monkeypatch.setattr(pokeapi_moves, "_cached_pokebase_payload", _cached_payload)
    monkeypatch.setattr(pokeapi_moves.pb, "pokemon", lambda _: (_ for _ in ()).throw(AssertionError("live lookup should not run")))

    move_levels = pokeapi_moves._api_learnable_move_levels_for_species("bulbasaur", "red")

    assert move_levels == {"tackle": 1}


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


def test_persist_move_reference_preserves_existing_non_target_moves(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "frost-breath",
                "power": 60,
                "raw_power": 60,
                "damage_class": "special",
                "type": "ice",
                "accuracy": 90,
                "pp": 10,
                "effective_power": 60.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            },
            {
                "move_name": "tackle",
                "power": 40,
                "raw_power": 40,
                "damage_class": "physical",
                "type": "normal",
                "accuracy": 100,
                "pp": 35,
                "effective_power": 40.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            },
        ],
    )
    write_parquet(
        references_dir / "learnable_moves.parquet",
        [
            {
                "game_version": "black",
                "pokemon_species": "vanillish",
                "move_name": "frost-breath",
                "learned_level": 1,
                "learn_method": "level-up",
            },
            {
                "game_version": "black",
                "pokemon_species": "lillipup",
                "move_name": "tackle",
                "learned_level": 1,
                "learn_method": "level-up",
            },
        ],
        partition_cols=["game_version", "pokemon_species"],
    )

    pokeapi_moves._clear_loaded_caches()
    pokeapi_moves.persist_move_reference_cache(
        [("lillipup", 10, "black", ["tackle"])],
        silver_dir=silver_dir,
    )

    move_reference_df = read_parquet(references_dir / "move_reference.parquet")
    assert set(move_reference_df["move_name"].tolist()) == {"frost-breath", "tackle"}


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


def test_bootstrap_move_reference_preserves_existing_non_target_rows(tmp_path: Path, monkeypatch) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "thunder-shock",
                "power": 40,
                "raw_power": 40,
                "damage_class": "special",
                "type": "electric",
                "accuracy": 100,
                "pp": 30,
                "effective_power": 40.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            }
        ],
    )
    write_parquet(
        references_dir / "learnable_moves.parquet",
        [
            {
                "game_version": "red",
                "pokemon_species": "pikachu",
                "move_name": "thunder-shock",
                "learned_level": 1,
                "learn_method": "level-up",
            }
        ],
        partition_cols=["game_version", "pokemon_species"],
    )

    monkeypatch.setattr(
        pokeapi_moves,
        "_api_learnable_move_levels_for_species",
        lambda species, game_version: {"vine-whip": 1} if (species, game_version) == ("bulbasaur", "red") else {},
    )
    monkeypatch.setattr(
        pokeapi_moves,
        "_api_move_profile",
        lambda move_name: {
            "move_name": move_name,
            "power": 45,
            "raw_power": 45,
            "damage_class": "physical",
            "type": "grass",
            "accuracy": 100,
            "pp": 25,
            "effective_power": 45.0,
            "power_handling": "direct_power",
            "is_status_move": False,
            "is_damage_move": True,
            "is_null_power": False,
        },
    )

    pokeapi_moves._clear_loaded_caches()
    pokeapi_moves.bootstrap_move_reference_cache(
        [("bulbasaur", 5, "red", [])],
        silver_dir=silver_dir,
    )

    learnable_df = read_parquet(references_dir / "learnable_moves.parquet")
    move_reference_df = read_parquet(references_dir / "move_reference.parquet")

    assert set(learnable_df["pokemon_species"].tolist()) == {"pikachu", "bulbasaur"}
    assert set(move_reference_df["move_name"].tolist()) == {"thunder-shock", "vine-whip"}


def test_bootstrap_move_reference_writes_offline_seed_profiles_without_live_api(
    tmp_path: Path,
    monkeypatch,
) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "surf",
                "power": 90,
                "raw_power": 90,
                "damage_class": "special",
                "type": "water",
                "accuracy": 100,
                "pp": 15,
                "effective_power": 90.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            }
        ],
    )
    monkeypatch.setattr(pokeapi_moves, "_api_learnable_move_levels_for_species", lambda species, game_version: {})

    def _fail_live_lookup(_: str) -> None:
        raise AssertionError("live pokebase lookup should not run for offline-seeded moves")

    monkeypatch.setattr(pokeapi_moves.pb, "move", _fail_live_lookup)

    pokeapi_moves._clear_loaded_caches()
    pokeapi_moves.bootstrap_move_reference_cache(
        [("noivern", 65, "x", ["boomburst"])],
        silver_dir=silver_dir,
    )

    move_reference_df = read_parquet(references_dir / "move_reference.parquet")
    boomburst = move_reference_df.loc[move_reference_df["move_name"] == "boomburst"].iloc[0].to_dict()

    assert boomburst["type"] == "normal"
    assert boomburst["damage_class"] == "special"
    assert float(boomburst["effective_power"]) == 140.0
