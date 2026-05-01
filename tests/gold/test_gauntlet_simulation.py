from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.common.io import read_parquet, write_json, write_parquet
from src.pipeline.gold.simulation.team_battle_simulations import (
    BattleSimulationConfig,
    _install_reference_profiles,
    _stable_sequence_seed,
    build_team_battle_simulations,
    load_move_profiles_from_silver,
    load_pokemon_profiles_from_silver,
    simulate_gauntlet,
    simulate_team_battle,
)


def _write_reference_profiles(silver_dir: Path, *, include_bosses: bool = True) -> None:
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    write_parquet(
        references_dir / "pokemon_data.parquet",
        [
            {
                "name": "hero",
                "pokemon_species": "hero",
                "type_1": "normal",
                "type_2": None,
                "base_hp": 1,
                "base_attack": 120,
                "base_defense": 50,
                "base_special_attack": 50,
                "base_special_defense": 50,
                "base_speed": 40,
            },
            {
                "name": "chipper",
                "pokemon_species": "chipper",
                "type_1": "normal",
                "type_2": None,
                "base_hp": 1,
                "base_attack": 30,
                "base_defense": 50,
                "base_special_attack": 30,
                "base_special_defense": 30,
                "base_speed": 50,
            },
            {
                "name": "closer",
                "pokemon_species": "closer",
                "type_1": "normal",
                "type_2": None,
                "base_hp": 1,
                "base_attack": 90,
                "base_defense": 50,
                "base_special_attack": 50,
                "base_special_defense": 50,
                "base_speed": 50,
            },
        ],
    )
    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "tackle",
                "type": "normal",
                "damage_class": "physical",
                "power": 40,
                "raw_power": 40,
                "effective_power": 40,
                "accuracy": 100,
                "pp": 35,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            }
        ],
    )
    if include_bosses:
        write_parquet(
            references_dir / "bosses.parquet",
            [
                {
                    "boss_id": "gold:brock",
                    "game_version": "gold",
                    "boss_name_canonical": "Brock",
                    "boss_order": 1,
                    "boss_role": "gym",
                },
                {
                    "boss_id": "gold:will",
                    "game_version": "gold",
                    "boss_name_canonical": "Will",
                    "boss_order": 2,
                    "boss_role": "elite_four",
                },
                {
                    "boss_id": "gold:lance",
                    "game_version": "gold",
                    "boss_name_canonical": "Lance",
                    "boss_order": 3,
                    "boss_role": "champion",
                },
            ],
        )


def _battle_config() -> BattleSimulationConfig:
    return BattleSimulationConfig(
        n_battle_trials=1,
        damage_randomness_min=1.0,
        damage_randomness_max=1.0,
        crit_chance=0.0,
        rng_seed=7,
    )


def test_gauntlet_preserves_player_hp_between_bosses(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    _write_reference_profiles(silver_dir, include_bosses=False)
    _install_reference_profiles(
        load_pokemon_profiles_from_silver(silver_dir),
        load_move_profiles_from_silver(silver_dir),
    )

    player_team = {
        "team_id": "PLAYER",
        "game_version": "gold",
        "team_role": "player",
        "origin": "generated",
        "is_player_candidate": True,
        "gym": "lance",
        "pokemon": ["hero"],
        "levels": [50],
        "moves": [["tackle"]],
        "avg_level": 50,
    }
    boss_one = {
        "team_id": "WILL",
        "game_version": "gold",
        "team_role": "boss",
        "origin": "kaggle",
        "is_player_candidate": False,
        "boss_name": "Will",
        "gym": "will",
        "pokemon": ["chipper"],
        "levels": [50],
        "moves": [["tackle"]],
        "avg_level": 50,
    }
    boss_two = {
        "team_id": "LANCE",
        "game_version": "gold",
        "team_role": "boss",
        "origin": "kaggle",
        "is_player_candidate": False,
        "boss_name": "Lance",
        "gym": "lance",
        "pokemon": ["closer"],
        "levels": [50],
        "moves": [["tackle"]],
        "avg_level": 50,
    }

    single_battle = simulate_team_battle(
        attacker_team=player_team,
        defender_team=boss_two,
        type_chart={"Normal": {"Normal": 1.0}},
        attacker_game_version="gold",
        defender_game_version="gold",
        n_trials=1,
        rng_seed=11,
        config=_battle_config(),
    )
    assert bool(single_battle["attacker_win"])

    gauntlet = simulate_gauntlet(
        player_team=player_team,
        boss_teams=[boss_one, boss_two],
        type_chart={"Normal": {"Normal": 1.0}},
        attacker_game_version="gold",
        n_trials=1,
        rng_seed=_stable_sequence_seed("PLAYER", "gold:elite_four_champion", 7),
        config=_battle_config(),
        boss_sequence_id="gold:elite_four_champion",
    )

    rows = gauntlet["battle_rows"]
    assert len(rows) == 2
    assert bool(rows[0]["attacker_win"])
    assert not bool(rows[1]["attacker_win"])
    assert rows[0]["remaining_team_state"] != rows[1]["remaining_team_state"]
    assert rows[1]["boss_sequence_id"] == "gold:elite_four_champion"
    assert rows[1]["sequence_position"] == 2


def test_simulate_team_battle_supports_double_battle_mode(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    _write_reference_profiles(silver_dir, include_bosses=False)
    _install_reference_profiles(
        load_pokemon_profiles_from_silver(silver_dir),
        load_move_profiles_from_silver(silver_dir),
    )

    attacker_team = {
        "team_id": "DOUBLE_PLAYER",
        "game_version": "ruby",
        "team_role": "player",
        "origin": "generated",
        "is_player_candidate": True,
        "gym": "tate-and-liza",
        "pokemon": ["hero", "closer"],
        "levels": [50, 50],
        "moves": [["tackle"], ["tackle"]],
        "avg_level": 50,
    }
    defender_team = {
        "team_id": "DOUBLE_BOSS",
        "game_version": "ruby",
        "team_role": "boss",
        "origin": "kaggle",
        "is_player_candidate": False,
        "boss_name": "Tate and Liza",
        "gym": "tate-and-liza",
        "battle_type": "double",
        "pokemon": ["chipper", "chipper"],
        "levels": [50, 50],
        "moves": [["tackle"], ["tackle"]],
        "avg_level": 50,
    }

    result = simulate_team_battle(
        attacker_team=attacker_team,
        defender_team=defender_team,
        type_chart={"Normal": {"Normal": 1.0}},
        attacker_game_version="ruby",
        defender_game_version="ruby",
        n_trials=1,
        rng_seed=13,
        config=_battle_config(),
    )

    summaries = result["representative_duel_summaries"]
    assert summaries
    assert any("events" in entry for entry in summaries)
    assert float(result["battle_turns"]) >= 1.0


def test_missing_boss_team_fails_fast_for_gauntlet_validation(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    _write_reference_profiles(silver_dir)
    write_json(bronze_dir / "type_chart.json", {"Normal": {"Normal": 1.0}})

    teams = [
        {
            "team_id": "GAUNTLET_PLAYER",
            "is_player_candidate": True,
            "game_version": "gold",
            "team_role": "player",
            "origin": "generated",
            "gym": "lance",
            "avg_level": 50,
            "pokemon": ["hero"],
            "levels": [50],
            "moves": [["tackle"]],
        },
        {
            "team_id": "BROCK_TEAM",
            "boss_name": "Brock",
            "game_version": "gold",
            "team_role": "boss",
            "origin": "kaggle",
            "is_player_candidate": False,
            "gym": "brock",
            "avg_level": 50,
            "pokemon": ["chipper"],
            "levels": [50],
            "moves": [["tackle"]],
        },
        {
            "team_id": "LANCE_TEAM",
            "boss_name": "Lance",
            "game_version": "gold",
            "team_role": "boss",
            "origin": "kaggle",
            "is_player_candidate": False,
            "gym": "lance",
            "avg_level": 50,
            "pokemon": ["closer"],
            "levels": [50],
            "moves": [["tackle"]],
        },
    ]

    with pytest.raises(ValueError, match="Missing boss teams"):
        build_team_battle_simulations(
            teams_data=teams,
            silver_dir=silver_dir,
            output_dir=silver_dir,
            bronze_dir=bronze_dir,
            runtime_config=_battle_config(),
            force_spark=False,
        )


def test_gauntlet_selects_blue_team_variant_by_player_starter(tmp_path: Path) -> None:
    bronze_dir = tmp_path / "bronze"
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)
    write_json(bronze_dir / "type_chart.json", {"Normal": {"Normal": 1.0}})
    write_parquet(
        references_dir / "pokemon_data.parquet",
        [
            {"name": "hero", "pokemon_species": "hero", "type_1": "normal", "type_2": None, "base_hp": 1, "base_attack": 120, "base_defense": 50, "base_special_attack": 50, "base_special_defense": 50, "base_speed": 40},
            {"name": "chipper", "pokemon_species": "chipper", "type_1": "normal", "type_2": None, "base_hp": 1, "base_attack": 30, "base_defense": 50, "base_special_attack": 30, "base_special_defense": 30, "base_speed": 50},
            {"name": "charizard", "pokemon_species": "charizard", "type_1": "fire", "type_2": "flying", "base_hp": 1, "base_attack": 90, "base_defense": 50, "base_special_attack": 90, "base_special_defense": 50, "base_speed": 80},
            {"name": "blastoise", "pokemon_species": "blastoise", "type_1": "water", "type_2": None, "base_hp": 1, "base_attack": 85, "base_defense": 50, "base_special_attack": 85, "base_special_defense": 50, "base_speed": 78},
            {"name": "venusaur", "pokemon_species": "venusaur", "type_1": "grass", "type_2": "poison", "base_hp": 1, "base_attack": 82, "base_defense": 50, "base_special_attack": 82, "base_special_defense": 50, "base_speed": 80},
        ],
    )
    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "tackle",
                "type": "normal",
                "damage_class": "physical",
                "power": 40,
                "raw_power": 40,
                "effective_power": 40,
                "accuracy": 100,
                "pp": 35,
                "power_handling": "direct_power",
                "is_status_move": False,
                "is_damage_move": True,
                "is_null_power": False,
            }
        ],
    )
    write_parquet(
        references_dir / "bosses.parquet",
        [
            {
                "boss_id": "red:lorelei",
                "game_version": "red",
                "boss_name_canonical": "Lorelei",
                "boss_order": 1,
                "boss_role": "elite_four",
                "is_simulatable": True,
            },
            {
                "boss_id": "red:blue",
                "game_version": "red",
                "boss_name_canonical": "Blue",
                "boss_order": 2,
                "boss_role": "champion",
                "starter_dependency_type": "team_variant",
                "has_team_variants": True,
                "is_simulatable": True,
            },
        ],
    )

    teams = [
        {
            "team_id": "PLAYER_BULBASAUR",
            "is_player_candidate": True,
            "game_version": "red",
            "team_role": "player",
            "origin": "generated",
            "boss_id": "red:blue",
            "boss_name": "Blue",
            "starter_base": "bulbasaur",
            "gym": "blue",
            "avg_level": 50,
            "pokemon": ["hero"],
            "levels": [50],
            "moves": [["tackle"]],
        },
        {
            "team_id": "LORELEI_TEAM",
            "boss_id": "red:lorelei",
            "boss_name": "Lorelei",
            "game_version": "red",
            "team_role": "boss",
            "origin": "kaggle",
            "is_player_candidate": False,
            "gym": "lorelei",
            "avg_level": 45,
            "pokemon": ["chipper"],
            "levels": [45],
            "moves": [["tackle"]],
        },
        {
            "team_id": "BLUE_CHARIZARD_TEAM",
            "boss_id": "red:blue",
            "boss_name": "Blue",
            "game_version": "red",
            "team_role": "boss",
            "origin": "kaggle",
            "is_player_candidate": False,
            "gym": "blue",
            "team_variant": "charizard_variant",
            "starter_type": "grass",
            "variant_dimension": "starter_type",
            "avg_level": 50,
            "pokemon": ["charizard"],
            "levels": [50],
            "moves": [["tackle"]],
        },
        {
            "team_id": "BLUE_BLASTOISE_TEAM",
            "boss_id": "red:blue",
            "boss_name": "Blue",
            "game_version": "red",
            "team_role": "boss",
            "origin": "kaggle",
            "is_player_candidate": False,
            "gym": "blue",
            "team_variant": "blastoise_variant",
            "starter_type": "fire",
            "variant_dimension": "starter_type",
            "avg_level": 50,
            "pokemon": ["blastoise"],
            "levels": [50],
            "moves": [["tackle"]],
        },
        {
            "team_id": "BLUE_VENUSAUR_TEAM",
            "boss_id": "red:blue",
            "boss_name": "Blue",
            "game_version": "red",
            "team_role": "boss",
            "origin": "kaggle",
            "is_player_candidate": False,
            "gym": "blue",
            "team_variant": "venusaur_variant",
            "starter_type": "water",
            "variant_dimension": "starter_type",
            "avg_level": 50,
            "pokemon": ["venusaur"],
            "levels": [50],
            "moves": [["tackle"]],
        },
    ]

    build_team_battle_simulations(
        teams_data=teams,
        silver_dir=silver_dir,
        output_dir=silver_dir,
        bronze_dir=bronze_dir,
        runtime_config=_battle_config(),
        force_spark=False,
    )

    out = read_parquet(silver_dir / "simulation" / "team_battle_simulations.parquet")
    gauntlet_rows = out[out["team_id_attacker"] == "PLAYER_BULBASAUR"].sort_values("sequence_position")
    assert gauntlet_rows["team_id_defender"].tolist().count("BLUE_CHARIZARD_TEAM") >= 1
