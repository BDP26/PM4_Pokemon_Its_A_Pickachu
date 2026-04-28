from __future__ import annotations

from src.pipeline.silver.transforms.normalized_tables import build_bosses_table
from src.pipeline.silver.enrichment.schema_normalizer import normalize_boss_records


def test_normalize_boss_records_preserves_encounter_metadata() -> None:
    snapshots, pokemon_reference, encounters = normalize_boss_records(
        [
            {
                "boss_id": "red:misty",
                "game": "red",
                "version": "red",
                "boss_name": "Misty",
                "boss_order": 2,
                "part": "part2",
                "heading": "Misty",
                "reachable_locations": ["route-24"],
                "reachable_pokemon_count": 1,
                "reachable_location_encounters": {
                    "route-24": [
                        {
                            "species": "psyduck",
                            "pokemon_url": "https://pokeapi.co/api/v2/pokemon/54/",
                            "level_min": 18,
                            "level_max": 20,
                            "encounter_chance_min": 25,
                            "encounter_chance_max": 65,
                            "capture_rate": 190,
                            "encounter_methods": ["walk"],
                        }
                    ]
                },
            }
        ]
    )

    assert snapshots
    assert pokemon_reference["psyduck"]["url"]
    assert encounters == [
        {
            "boss_id": "red:misty",
            "game": "red",
            "location": "route-24",
            "pokemon": "psyduck",
            "level_min": 18,
            "level_max": 20,
            "encounter_chance_min": 25,
            "encounter_chance_max": 65,
            "capture_rate": 190,
            "methods": ["walk"],
        }
    ]


def test_build_bosses_table_creates_conditional_striaton_rows() -> None:
    rows = build_bosses_table(
        {
            "black": {
                "boss_mapping": [
                    {"boss_order": 1, "boss_id": "black:chili", "boss_name_canonical": "Chili", "dataset_boss_candidates": ["Chili"]},
                    {"boss_order": 2, "boss_id": "black:cilan", "boss_name_canonical": "Cilan", "dataset_boss_candidates": ["Cilan"]},
                    {"boss_order": 3, "boss_id": "black:cress", "boss_name_canonical": "Cress", "dataset_boss_candidates": ["Cress"]},
                    {"boss_order": 4, "boss_id": "black:lenora", "boss_name_canonical": "Lenora", "dataset_boss_candidates": ["Lenora"]},
                ]
            },
            "white": {
                "boss_mapping": [
                    {"boss_order": 1, "boss_id": "white:chili", "boss_name_canonical": "Chili", "dataset_boss_candidates": ["Chili"]},
                    {"boss_order": 2, "boss_id": "white:cilan", "boss_name_canonical": "Cilan", "dataset_boss_candidates": ["Cilan"]},
                    {"boss_order": 3, "boss_id": "white:cress", "boss_name_canonical": "Cress", "dataset_boss_candidates": ["Cress"]},
                    {"boss_order": 4, "boss_id": "white:lenora", "boss_name_canonical": "Lenora", "dataset_boss_candidates": ["Lenora"]},
                ]
            },
        }
    )

    striaton = [row for row in rows if row["game_version"] == "black-white"]
    assert [(row["boss_name_canonical"], row["starter_condition"], row["gym_index"]) for row in striaton] == [
        ("Chili", "grass", 1),
        ("Cilan", "water", 1),
        ("Cress", "fire", 1),
    ]
    assert all(row["is_simulatable"] is True for row in striaton)


def test_build_bosses_table_preserves_all_kaggle_aliases_for_variant_champions() -> None:
    rows = build_bosses_table(
        {
            "blue": {
                "boss_mapping": [
                    {
                        "boss_order": 13,
                        "boss_id": "blue:blue",
                        "boss_name_canonical": "Blue",
                        "dataset_boss_candidates": [
                            "Champion Blue Squirtle",
                            "Champion Blue Bulbasaur",
                            "Champion Blue Charmander",
                        ],
                    }
                ]
            }
        }
    )

    assert len(rows) == 1
    row = rows[0]
    assert row["boss_id"] == "blue:blue"
    assert row["boss_name_kaggle"] == "Champion Blue Squirtle"
    assert row["boss_name_aliases"] == [
        "Blue",
        "Champion Blue Squirtle",
        "Champion Blue Bulbasaur",
        "Champion Blue Charmander",
    ]
    assert row["is_simulatable"] is True


def test_build_bosses_table_marks_alder_simulatable() -> None:
    rows = build_bosses_table(
        {
            "black": {
                "boss_mapping": [
                    {"boss_order": 15, "boss_id": "black:alder", "boss_name_canonical": "Alder", "dataset_boss_candidates": ["Champion Alder"]},
                ]
            }
        }
    )

    by_id = {row["boss_id"]: row for row in rows}
    assert by_id["black:alder"]["is_simulatable"] is True
