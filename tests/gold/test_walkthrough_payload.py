from __future__ import annotations

import math
from pathlib import Path

from src.pipeline.common.io import read_json, write_json, write_jsonl, write_parquet
from src.pipeline.gold.reporting.build_walkthrough_web import (
    _build_encounter_summary,
    _load_boss_metadata,
    _load_move_reference,
    _select_diverse_team_payloads,
    build_walkthrough_best_teams_payload,
)


def test_load_boss_metadata_treats_nan_flags_as_false(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        references_dir / "bosses.parquet",
        [
            {
                "game_version": "platinum",
                "boss_name": "Gardenia",
                "progression_depth": math.nan,
                "is_branching": math.nan,
                "has_team_variants": math.nan,
                "is_optional": math.nan,
                "is_postgame": math.nan,
                "is_simulatable": math.nan,
            }
        ],
    )
    write_json(
        silver_dir / "manifest.json",
        {
            "datasets": {
                "bosses": {
                    "file": "references/bosses.parquet",
                }
            }
        },
    )

    metadata = _load_boss_metadata(silver_dir, {"datasets": {"bosses": {"file": "references/bosses.parquet"}}})
    row = metadata[("platinum", "gardenia")]

    assert row["progression_depth"] is None
    assert row["is_branching"] is False
    assert row["has_team_variants"] is False
    assert row["is_optional"] is False
    assert row["is_postgame"] is False
    assert row["is_simulatable"] is False


def test_load_move_reference_treats_nan_flags_as_false(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    references_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {
                "move_name": "thunderbolt",
                "type": "electric",
                "damage_class": "special",
                "is_status_move": math.nan,
                "is_damage_move": math.nan,
            }
        ],
    )

    reference = _load_move_reference(
        silver_dir,
        {"datasets": {"move_reference": {"file": "references/move_reference.parquet"}}},
    )

    row = reference["thunderbolt"]
    assert row["is_status_move"] is False
    assert row["is_damage_move"] is False


def test_build_encounter_summary_aggregates_species_and_locations() -> None:
    summary = _build_encounter_summary(
        {
            "route-2": [
                {
                    "species": "pidgey",
                    "encounter_chance_max": 45,
                    "capture_rate": 255,
                    "level_min": 2,
                    "level_max": 4,
                    "encounter_methods": ["walk"],
                },
                {
                    "species": "rattata",
                    "encounter_chance_max": 35,
                    "capture_rate": 255,
                    "level_min": 2,
                    "level_max": 4,
                    "encounter_methods": ["walk"],
                },
            ],
            "viridian-forest": [
                {
                    "species": "pidgey",
                    "encounter_chance_max": 25,
                    "capture_rate": 255,
                    "level_min": 3,
                    "level_max": 5,
                    "encounter_methods": ["walk"],
                }
            ],
        }
    )

    assert summary["species_count"] == 2
    assert summary["location_count"] == 2
    assert summary["methods"] == ["walk"]
    assert summary["species"][0]["species"] == "pidgey"
    assert summary["species"][0]["location_count"] == 2
    assert summary["locations"][0]["location"] == "route-2"


def test_build_walkthrough_payload_uses_current_silver_reference_tables(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    references_dir = silver_dir / "references"
    simulation_dir = silver_dir / "simulation"
    references_dir.mkdir(parents=True, exist_ok=True)
    simulation_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    write_json(
        silver_dir / "manifest.json",
        {
            "datasets": {
                "bosses": {"file": "references/bosses.parquet"},
                "boss_teams": {"file": "references/boss_teams.parquet"},
                "pokemon_reference": {"file": "references/pokemon_reference.parquet"},
                "move_reference": {"file": "references/move_reference.parquet"},
                "encounters": {"file": "references/encounters.jsonl"},
            }
        },
    )
    write_parquet(
        references_dir / "bosses.parquet",
        [
            {
                "boss_id": "boss:red:brock:abc123",
                "game_version": "red",
                "boss_name": "Brock",
                "boss_name_canonical": "Brock",
                "boss_role": "gym",
                "battle_type": "single",
                "location_name": "Pewter Gym",
                "progression_order": 1,
                "boss_order": 1,
                "gym_index": 1,
                "is_simulatable": True,
                "is_branching": False,
                "has_team_variants": False,
                "is_optional": False,
                "is_postgame": False,
            }
        ],
    )
    write_parquet(
        references_dir / "boss_teams.parquet",
        [
            {
                "boss_team_id": "boss-team-brock",
                "boss_id": "boss:red:brock:abc123",
                "boss_name": "Brock",
                "boss_role": "gym",
                "battle_type": "single",
                "progression_order": 1,
                "pokemon_slot": 1,
                "pokemon_species": "geodude",
                "level": 12,
                "move_1": "tackle",
                "game_version": "red",
            }
        ],
    )
    write_parquet(
        references_dir / "pokemon_reference.parquet",
        [
            {"pokemon_species": "bulbasaur", "name": "bulbasaur", "url": "pokebase://pokemon/1"},
            {"pokemon_species": "geodude", "name": "geodude", "url": "pokebase://pokemon/74"},
            {"pokemon_species": "pidgey", "name": "pidgey", "url": "pokebase://pokemon/16"},
        ],
    )
    write_parquet(
        references_dir / "move_reference.parquet",
        [
            {"move_name": "tackle", "type": "normal", "damage_class": "physical", "is_status_move": False, "is_damage_move": True},
        ],
    )
    write_jsonl(
        references_dir / "encounters.jsonl",
        [
            {
                "boss_id": "red:brock",
                "game": "red",
                "location": "route-2",
                "pokemon": "pidgey",
                "level_min": 3,
                "level_max": 5,
                "encounter_chance_max": 45,
                "capture_rate": 255,
                "methods": ["walk"],
            }
        ],
    )
    write_parquet(
        simulation_dir / "source_teams_red.parquet",
        [
            {
                "source_team_id": "player-team-1",
                "game_version": "red",
                "team_role": "player",
                "origin": "player",
                "starter_base": "bulbasaur",
                "starter_evolved_species": "bulbasaur",
                "avg_level": 10,
            }
        ],
    )
    write_parquet(
        simulation_dir / "source_team_members_red.parquet",
        [
            {
                "team_member_id": "member-1",
                "source_team_id": "player-team-1",
                "game_version": "red",
                "slot": 1,
                "pokemon_species": "bulbasaur",
                "level": 10,
            }
        ],
    )
    write_parquet(
        simulation_dir / "member_moveset_combos_red.parquet",
        [
            {
                "moveset_combo_id": "combo-1",
                "team_id": "player-team-1",
                "pokemon_instance_id": "member-1",
                "slot_index": 1,
                "combo_rank": 1,
                "move_1": "tackle",
            }
        ],
    )
    write_parquet(
        gold_dir / "best_team_by_boss_version.parquet",
        [
            {
                "boss_team_id": "boss-team-brock",
                "boss_name": "brock",
                "game_version": "red",
                "player_team_id": "player-team-1",
                "mc_win_rate": 0.8,
                "wins": 8,
                "losses": 2,
                "n_trials": 10,
                "player_avg_level": 10,
                "boss_avg_level": 12,
            }
        ],
    )
    write_parquet(
        gold_dir / "team_rankings_by_boss_version.parquet",
        [
            {
                "boss_team_id": "boss-team-brock",
                "boss_name": "brock",
                "game_version": "red",
                "player_team_id": "player-team-1",
                "mc_win_rate": 0.8,
                "wins": 8,
                "losses": 2,
                "n_trials": 10,
                "rank_in_boss_version": 1,
                "player_avg_level": 10,
                "boss_avg_level": 12,
            }
        ],
    )

    output_path = build_walkthrough_best_teams_payload(silver_dir=silver_dir, gold_dir=gold_dir)

    assert output_path == gold_dir / "walkthrough_best_teams.json"
    payload = read_json(output_path)
    assert payload["versions"] == ["red"]
    assert len(payload["walkthroughs"]["red"]) == 1
    row = payload["walkthroughs"]["red"][0]
    assert row["boss_name"] == "Brock"
    assert row["heading"] == "Pewter Gym"
    assert row["location_count"] == 1
    assert row["reachable_location_pokemon"]["route-2"] == ["pidgey"]
    assert row["recommended_team"]["team_id"] == "player-team-1"


def test_walkthrough_sequence_payload_uses_sequence_wins_and_trials(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    references_dir = silver_dir / "references"
    simulation_dir = silver_dir / "simulation"
    references_dir.mkdir(parents=True, exist_ok=True)
    simulation_dir.mkdir(parents=True, exist_ok=True)
    gold_dir.mkdir(parents=True, exist_ok=True)

    write_json(
        silver_dir / "manifest.json",
        {
            "datasets": {
                "bosses": {"file": "references/bosses.parquet"},
                "boss_teams": {"file": "references/boss_teams.parquet"},
                "pokemon_reference": {"file": "references/pokemon_reference.parquet"},
                "move_reference": {"file": "references/move_reference.parquet"},
                "encounters": {"file": "references/encounters.jsonl"},
            }
        },
    )
    write_parquet(
        references_dir / "bosses.parquet",
        [
            {
                "boss_id": "boss:red:brock:abc123",
                "game_version": "red",
                "boss_name": "Brock",
                "boss_name_canonical": "Brock",
                "boss_role": "gym",
                "battle_type": "single",
                "location_name": "Pewter Gym",
                "progression_order": 1,
                "boss_order": 1,
                "gym_index": 1,
                "is_simulatable": True,
            }
        ],
    )
    write_parquet(
        references_dir / "boss_teams.parquet",
        [
            {
                "boss_team_id": "boss-team-brock",
                "boss_id": "boss:red:brock:abc123",
                "boss_name": "Brock",
                "boss_role": "gym",
                "battle_type": "single",
                "progression_order": 1,
                "pokemon_slot": 1,
                "pokemon_species": "geodude",
                "level": 12,
                "move_1": "tackle",
                "game_version": "red",
            }
        ],
    )
    write_parquet(references_dir / "pokemon_reference.parquet", [{"pokemon_species": "bulbasaur", "name": "bulbasaur", "url": "pokebase://pokemon/1"}])
    write_parquet(references_dir / "move_reference.parquet", [{"move_name": "tackle", "type": "normal", "damage_class": "physical", "is_status_move": False, "is_damage_move": True}])
    write_jsonl(references_dir / "encounters.jsonl", [])
    write_parquet(
        simulation_dir / "source_teams_red.parquet",
        [{"source_team_id": "player-team-1", "game_version": "red", "team_role": "player", "origin": "player", "starter_base": "bulbasaur", "starter_evolved_species": "bulbasaur", "avg_level": 10}],
    )
    write_parquet(
        simulation_dir / "source_team_members_red.parquet",
        [{"team_member_id": "member-1", "source_team_id": "player-team-1", "game_version": "red", "slot": 1, "pokemon_species": "bulbasaur", "level": 10}],
    )
    write_parquet(
        simulation_dir / "member_moveset_combos_red.parquet",
        [{"moveset_combo_id": "combo-1", "team_id": "player-team-1", "pokemon_instance_id": "member-1", "slot_index": 1, "combo_rank": 1, "move_1": "tackle"}],
    )
    write_parquet(
        gold_dir / "best_team_by_boss_version.parquet",
        [{"boss_team_id": "boss-team-brock", "boss_name": "brock", "game_version": "red", "player_team_id": "player-team-1", "mc_win_rate": 0.8, "wins": 8, "losses": 2, "n_trials": 10, "player_avg_level": 10, "boss_avg_level": 12}],
    )
    write_parquet(
        gold_dir / "team_rankings_by_boss_version.parquet",
        [{"boss_team_id": "boss-team-brock", "boss_name": "brock", "game_version": "red", "player_team_id": "player-team-1", "mc_win_rate": 0.8, "wins": 8, "losses": 2, "n_trials": 10, "rank_in_boss_version": 1, "player_avg_level": 10, "boss_avg_level": 12}],
    )
    write_parquet(
        gold_dir / "team_rankings_e4_champion_sequence_by_version_starter.parquet",
        [
            {
                "effective_game_version": "red",
                "starter_base": "bulbasaur",
                "player_team_id": "player-team-1",
                "sequence_win_rate": 0.6,
                "sequence_score": 0.6,
                "bosses_covered": 5,
                "degraded_ratio": 0.0,
                "rank_in_sequence": 1,
                "sequence_wins": 6,
                "sequence_n_trials": 10,
            }
        ],
    )

    payload = read_json(build_walkthrough_best_teams_payload(silver_dir=silver_dir, gold_dir=gold_dir))
    seq_team = payload["elite_four_champion_sequence_by_version"]["red"]["by_starter"]["bulbasaur"][0]
    assert seq_team["wins"] == 6
    assert seq_team["n_trials"] == 10


def test_select_diverse_team_payloads_reduces_clone_dominance() -> None:
    clone_a = {
        "team_id": "a",
        "mc_win_rate": 0.82,
        "wins": 410,
        "rank_in_boss_version": 1,
        "pokemon": [{"name": "pidgey"}, {"name": "rattata"}, {"name": "caterpie"}],
    }
    clone_b = {
        "team_id": "b",
        "mc_win_rate": 0.81,
        "wins": 405,
        "rank_in_boss_version": 2,
        "pokemon": [{"name": "pidgey"}, {"name": "rattata"}, {"name": "weedle"}],
    }
    diverse = {
        "team_id": "c",
        "mc_win_rate": 0.79,
        "wins": 395,
        "rank_in_boss_version": 3,
        "pokemon": [{"name": "sandshrew"}, {"name": "oddish"}, {"name": "mankey"}],
    }

    selected = _select_diverse_team_payloads(
        [clone_a, clone_b, diverse],
        limit=2,
        progression_depth=0.1,
        realism_lookup={},
    )
    selected_ids = {row["team_id"] for row in selected}
    assert "a" in selected_ids
    assert "c" in selected_ids
