from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.common.io import write_json, write_parquet
from src.pipeline.silver.simulation import type_matchups
from src.pipeline.silver.simulation.type_matchups import BattleSimulationConfig, build_team_battle_simulations


def _write_reference_parquets(silver_dir: Path) -> None:
    refs = silver_dir / "references"
    refs.mkdir(parents=True, exist_ok=True)
    write_parquet(
        refs / "pokemon_data.parquet",
        [
            {
                "name": "braixen",
                "pokemon_species": "braixen",
                "pokeapi_id": 654,
                "source_url": "https://example/pokemon/braixen",
                "type_1": "fire",
                "type_2": None,
                "base_hp": 59,
                "base_attack": 59,
                "base_defense": 58,
                "base_special_attack": 90,
                "base_special_defense": 70,
                "base_speed": 73,
                "height": 10,
                "weight": 145,
                "base_experience": 143,
                "is_default": True,
                "requested_pokemon_name": "Braixen",
                "normalized_requested_name": "braixen",
                "normalized_species": "braixen",
                "resolved_pokemon_name": "braixen",
                "resolved_pokeapi_id": 654,
                "resolution_method": "exact",
                "resolution_warning": None,
            },
            {
                "name": "barboach",
                "pokemon_species": "barboach",
                "pokeapi_id": 339,
                "source_url": "https://example/pokemon/barboach",
                "type_1": "water",
                "type_2": "ground",
                "base_hp": 50,
                "base_attack": 48,
                "base_defense": 43,
                "base_special_attack": 46,
                "base_special_defense": 41,
                "base_speed": 60,
                "height": 4,
                "weight": 19,
                "base_experience": 58,
                "is_default": True,
                "requested_pokemon_name": "BARBOACH",
                "normalized_requested_name": "barboach",
                "normalized_species": "barboach",
                "resolved_pokemon_name": "barboach",
                "resolved_pokeapi_id": 339,
                "resolution_method": "exact",
                "resolution_warning": None,
            },
        ],
    )
    write_parquet(
        refs / "move_reference.parquet",
        [
            {
                "move_name": "rock-tomb",
                "power": 60,
                "raw_power": 60,
                "damage_class": "physical",
                "type": "rock",
                "accuracy": 95,
                "pp": 15,
                "effective_power": 60.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            },
            {
                "move_name": "ember",
                "power": 40,
                "raw_power": 40,
                "damage_class": "special",
                "type": "fire",
                "accuracy": 100,
                "pp": 25,
                "effective_power": 40.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            },
            {
                "move_name": "aerial-ace",
                "power": 60,
                "raw_power": 60,
                "damage_class": "physical",
                "type": "flying",
                "accuracy": None,
                "pp": 20,
                "effective_power": 60.0,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            },
            {
                "move_name": "growl",
                "power": None,
                "raw_power": None,
                "damage_class": None,
                "type": "normal",
                "accuracy": 100,
                "pp": 40,
                "effective_power": None,
                "power_handling": "status_no_damage",
                "is_status_move": True,
                "is_damage_move": False,
                "is_null_power": True,
            },
        ],
    )


def test_loads_braixen_and_barboach_from_pokemon_reference(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    _write_reference_parquets(silver_dir)

    profiles = type_matchups.load_pokemon_profiles_from_silver(silver_dir)

    assert profiles["braixen"]["name"] == "braixen"
    assert profiles["barboach"]["name"] == "barboach"


def test_pokemon_aliases_resolve_from_all_reference_name_columns(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    refs = silver_dir / "references"
    refs.mkdir(parents=True)
    write_parquet(
        refs / "pokemon_data.parquet",
        [
            {
                "name": "mr-mime",
                "pokemon_species": "mr-mime",
                "pokeapi_id": 122,
                "source_url": "x",
                "type_1": "psychic",
                "type_2": "fairy",
                "base_hp": 40,
                "base_attack": 45,
                "base_defense": 65,
                "base_special_attack": 100,
                "base_special_defense": 120,
                "base_speed": 90,
                "requested_pokemon_name": "Mr Mime",
                "normalized_requested_name": "mr_mime",
                "normalized_species": "mr mime",
                "resolved_pokemon_name": "mr-mime",
            }
        ],
    )
    write_parquet(refs / "move_reference.parquet", [{"move_name": "tackle", "type": "normal", "damage_class": "physical", "effective_power": 40, "raw_power": 40, "power": 40, "accuracy": 100, "pp": 35, "power_handling": "direct_power", "is_status_move": False, "is_damage_move": True, "is_null_power": False}])

    profiles = type_matchups.load_pokemon_profiles_from_silver(silver_dir)

    assert profiles["mr-mime"]["name"] == "mr-mime"
    assert profiles["mr-mime"]["species"] == "mr-mime"


def test_loads_rock_tomb_and_ember_from_move_reference(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    _write_reference_parquets(silver_dir)

    moves = type_matchups.load_move_profiles_from_silver(silver_dir)

    assert moves["rock-tomb"]["type"] == "Rock"
    assert moves["ember"]["effective_power"] == pytest.approx(40.0)


def test_status_null_power_defaults_effective_power_to_zero(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    _write_reference_parquets(silver_dir)

    moves = type_matchups.load_move_profiles_from_silver(silver_dir)

    assert moves["growl"]["effective_power"] == pytest.approx(0.0)
    assert moves["growl"]["damage_class"] == "status"


def test_null_accuracy_is_allowed(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    _write_reference_parquets(silver_dir)

    moves = type_matchups.load_move_profiles_from_silver(silver_dir)

    assert moves["aerial-ace"]["accuracy"] is None


def test_missing_pokemon_raises_by_default(tmp_path: Path) -> None:
    type_matchups._install_reference_profiles(pokemon_profiles={}, move_profiles={"ember": {"name": "ember", "type": "Fire", "damage_class": "special"}})
    teams = [{"team_id": "t1", "pokemon": ["missingmon"], "moves": [["ember"]], "levels": [10]}]

    with pytest.raises(ValueError, match=r"Missing Pokemon profiles in pokemon_data\.parquet: total=1"):
        type_matchups._validate_profile_coverage(teams)


def test_missing_move_raises_by_default(tmp_path: Path) -> None:
    type_matchups._install_reference_profiles(
        pokemon_profiles={"braixen": {"name": "braixen", "types": ["Fire"], "stats": {"hp": 59}, "moves": []}},
        move_profiles={},
    )
    teams = [{"team_id": "t1", "pokemon": ["braixen"], "moves": [["missing-move"]], "levels": [10]}]

    with pytest.raises(ValueError, match=r"Missing move profiles in move_reference\.parquet: total=1"):
        type_matchups._validate_profile_coverage(teams)


def test_fallback_only_when_pm4_allow_simulation_fallbacks_is_set(monkeypatch) -> None:
    type_matchups._install_reference_profiles(pokemon_profiles={}, move_profiles={})
    warnings = type_matchups.WarningCollector()

    monkeypatch.delenv("PM4_ALLOW_SIMULATION_FALLBACKS", raising=False)
    with pytest.raises(ValueError, match="Missing Pokemon profile"):
        type_matchups._get_pokemon_profile("unknown", warnings)

    monkeypatch.setenv("PM4_ALLOW_SIMULATION_FALLBACKS", "1")
    profile, degraded = type_matchups._get_pokemon_profile("unknown", warnings)
    assert degraded is True
    assert profile["name"] == "unknown"


def test_spark_and_local_engines_use_same_profile_dictionaries(tmp_path: Path, monkeypatch) -> None:
    silver_dir = tmp_path / "silver"
    bronze_dir = tmp_path / "bronze"
    _write_reference_parquets(silver_dir)
    write_json(bronze_dir / "type_chart.json", {"Fire": {"Water": 0.5}, "Rock": {"Fire": 2.0}, "Normal": {"Normal": 1.0}, "Water": {"Fire": 2.0}, "Ground": {"Fire": 2.0}})

    teams = [
        {"team_id": "A", "is_player_candidate": True, "game_version": "red", "gym": "brock", "avg_level": 12, "pokemon": ["braixen"], "levels": [12], "moves": [["ember"]]},
        {"team_id": "B", "boss_name": "Brock", "game_version": "red", "gym": "brock", "avg_level": 12, "pokemon": ["barboach"], "levels": [12], "moves": [["rock-tomb"]]},
    ]

    seen: dict[str, dict[str, dict]] = {}

    def _fake_local(teams_data, type_chart, config):
        seen["local"] = {"pokemon": dict(type_matchups._LOCAL_POKEMON_PROFILES), "moves": dict(type_matchups._LOCAL_MOVE_PROFILES)}
        return []

    def _fake_spark(teams_data, type_chart, config):
        seen["spark"] = {"pokemon": dict(type_matchups._LOCAL_POKEMON_PROFILES), "moves": dict(type_matchups._LOCAL_MOVE_PROFILES)}
        return []

    monkeypatch.setattr(type_matchups, "_enrich_teams_with_boss_context", lambda teams_data, _silver_dir: teams_data)
    monkeypatch.setattr(type_matchups, "_run_local_simulations", _fake_local)
    monkeypatch.setattr(type_matchups, "_run_spark_simulations", _fake_spark)

    build_team_battle_simulations(teams_data=teams, silver_dir=silver_dir, bronze_dir=bronze_dir, force_spark=False)
    build_team_battle_simulations(teams_data=teams, silver_dir=silver_dir, bronze_dir=bronze_dir, force_spark=True)

    assert set(seen["local"]["pokemon"].keys()) == set(seen["spark"]["pokemon"].keys())
    assert set(seen["local"]["moves"].keys()) == set(seen["spark"]["moves"].keys())


def test_no_local_cache_wording_or_external_profile_fetch_in_normal_mode() -> None:
    source = Path(type_matchups.__file__).read_text()
    assert "local cache" not in source
    assert "_fetch_pokemon_profile_from_api" not in source
    assert "_fetch_move_profile_from_api" not in source


def test_build_simulation_raises_for_missing_profiles_before_engine(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    bronze_dir = tmp_path / "bronze"
    _write_reference_parquets(silver_dir)
    write_json(bronze_dir / "type_chart.json", {"Normal": {"Normal": 1.0}})

    teams = [
        {"team_id": "A", "is_player_candidate": True, "game_version": "red", "gym": "brock", "avg_level": 10, "pokemon": ["missingmon"], "levels": [10], "moves": [["ember"]]},
        {"team_id": "B", "boss_name": "Brock", "game_version": "red", "gym": "brock", "avg_level": 10, "pokemon": ["barboach"], "levels": [10], "moves": [["rock-tomb"]]},
    ]

    with pytest.raises(ValueError, match=r"Missing Pokemon profiles in pokemon_data\.parquet: total=1"):
        build_team_battle_simulations(teams_data=teams, silver_dir=silver_dir, bronze_dir=bronze_dir, runtime_config=BattleSimulationConfig(n_battle_trials=1))
