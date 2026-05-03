from __future__ import annotations

import json
import math

import pandas as pd
from pathlib import Path
import pytest

from src.pipeline.silver.inputs.builders.player_teams import build_progression_source_teams_from_encounters
from src.pipeline.silver.transforms.progression_depth import (
    build_boss_level_table_with_mapping,
    build_progression_depth_context,
    build_progression_depth_table,
)


def test_build_progression_depth_table_accumulates_species_and_formula() -> None:
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red:brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
            {"boss_id": "red:misty", "game_version": "red", "boss_name_canonical": "Misty", "boss_order": 2},
            {"boss_id": "red:lt-surge", "game_version": "red", "boss_name_canonical": "Lt Surge", "boss_order": 3},
        ]
    )
    encounters_df = pd.DataFrame(
        [
            {"game": "red", "boss_id": "red:brock", "pokemon": "geodude"},
            {"game": "red", "boss_id": "red:brock", "pokemon": "sandshrew"},
            {"game": "red", "boss_id": "red:misty", "pokemon": "geodude"},
            {"game": "red", "boss_id": "red:misty", "pokemon": "sandshrew"},
            {"game": "red", "boss_id": "red:misty", "pokemon": "psyduck"},
            {"game": "red", "boss_id": "red:lt-surge", "pokemon": "geodude"},
            {"game": "red", "boss_id": "red:lt-surge", "pokemon": "sandshrew"},
            {"game": "red", "boss_id": "red:lt-surge", "pokemon": "psyduck"},
            {"game": "red", "boss_id": "red:lt-surge", "pokemon": "voltorb"},
            {"game": "red", "boss_id": "red:lt-surge", "pokemon": "pikachu"},
        ]
    )

    out = build_progression_depth_table(bosses_df=bosses_df, encounters_df=encounters_df)

    assert out["boss_id"].tolist() == ["red:brock", "red:misty", "red:lt-surge"]
    assert out["boss_index"].tolist() == [1, 2, 3]
    assert out["available_species_count"].tolist() == [2, 3, 5]
    assert out["max_species_count"].tolist() == [5, 5, 5]

    expected = [
        0.6 * (1 / 3) + 0.4 * (2 / 5),
        0.6 * (2 / 3) + 0.4 * (3 / 5),
        0.6 * (3 / 3) + 0.4 * (5 / 5),
    ]
    assert all(math.isclose(actual, want, rel_tol=1e-9) for actual, want in zip(out["progression_depth"], expected, strict=True))
    assert out["progression_depth"].tolist() == sorted(out["progression_depth"].tolist())


def test_build_progression_depth_table_rejects_boss_order_gaps() -> None:
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red:brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
            {"boss_id": "red:misty", "game_version": "red", "boss_name_canonical": "Misty", "boss_order": 3},
        ]
    )
    encounters_df = pd.DataFrame(
        [
            {"game": "red", "boss_id": "red:brock", "pokemon": "geodude"},
            {"game": "red", "boss_id": "red:misty", "pokemon": "geodude"},
            {"game": "red", "boss_id": "red:misty", "pokemon": "staryu"},
        ]
    )

    with pytest.raises(ValueError, match="gym_index values must be contiguous"):
        build_progression_depth_table(bosses_df=bosses_df, encounters_df=encounters_df)


def test_build_progression_depth_table_uses_cumulative_species_union() -> None:
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red:brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
            {"boss_id": "red:misty", "game_version": "red", "boss_name_canonical": "Misty", "boss_order": 2},
        ]
    )
    encounters_df = pd.DataFrame(
        [
            {"game": "red", "boss_id": "red:brock", "pokemon": "geodude"},
            {"game": "red", "boss_id": "red:brock", "pokemon": "sandshrew"},
            {"game": "red", "boss_id": "red:misty", "pokemon": "psyduck"},
        ]
    )

    out = build_progression_depth_table(bosses_df=bosses_df, encounters_df=encounters_df)

    assert out["available_species_count"].tolist() == [2, 3]
    assert out["max_species_count"].tolist() == [3, 3]
    assert out["progression_depth"].tolist() == sorted(out["progression_depth"].tolist())


def test_build_progression_depth_table_maps_legacy_encounter_boss_ids_to_canonical_ids() -> None:
    bosses_df = pd.DataFrame(
        [
            {
                "boss_id": "boss:black:chili:abc123",
                "game_version": "black",
                "boss_name_canonical": "Chili",
                "boss_order": 1,
                "gym_index": 1,
            },
            {
                "boss_id": "boss:black:lenora:def456",
                "game_version": "black",
                "boss_name_canonical": "Lenora",
                "boss_order": 2,
                "gym_index": 2,
            },
        ]
    )
    encounters_df = pd.DataFrame(
        [
            {"game": "black", "boss_id": "black:chili", "pokemon": "patrat"},
            {"game": "black", "boss_id": "black:chili", "pokemon": "lillipup"},
            {"game": "black", "boss_id": "black:lenora", "pokemon": "watchog"},
        ]
    )

    out = build_progression_depth_table(bosses_df=bosses_df, encounters_df=encounters_df)

    assert out["boss_id"].tolist() == ["boss:black:chili:abc123", "boss:black:lenora:def456"]
    assert out["available_species_count"].tolist() == [2, 3]


def test_build_boss_level_table_with_mapping_uses_bronze_fallback_for_missing_bosses(tmp_path: Path) -> None:
    bronze_dir = tmp_path
    bulbapedia_dir = bronze_dir / "bulbapedia"
    bulbapedia_dir.mkdir(parents=True)
    payload = {
        "game_key": "black",
        "bosses": ["Cilan", "Alder"],
        "parts": [
            {
                "part": 1,
                "html": """
                <div class="partycontainer">
                  <div class="partyname"><b><span>Alder</span></b></div>
                  <span class="PKMNlevel"><span class="PKMNsmall">Lv.</span>52</span>
                  <span class="PKMNlevel"><span class="PKMNsmall">Lv.</span>52</span>
                  <span class="PKMNlevel"><span class="PKMNsmall">Lv.</span>52</span>
                  <span class="PKMNlevel"><span class="PKMNsmall">Lv.</span>52</span>
                  <span class="PKMNlevel"><span class="PKMNsmall">Lv.</span>52</span>
                  <span class="PKMNlevel"><span class="PKMNsmall">Lv.</span>54</span>
                </div>
                """,
            }
        ],
    }
    (bulbapedia_dir / "black.json").write_text(json.dumps(payload), encoding="utf-8")

    boss_team_members_df = pd.DataFrame(
        [
            {"game_version": "black", "boss_name": "chili", "level": 14},
            {"game_version": "black", "boss_name": "chili", "level": 12},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"game_version": "black", "boss_name_canonical": "Cilan", "boss_name_kaggle": "Chili"},
            {"game_version": "black", "boss_name_canonical": "Alder", "boss_name_kaggle": "Champion Alder"},
        ]
    )

    out = build_boss_level_table_with_mapping(
        boss_team_members_df=boss_team_members_df,
        bosses_df=bosses_df,
        bronze_dir=bronze_dir,
    )

    assert sorted(
        out[["game_version", "boss_name"]].to_dict(orient="records"),
        key=lambda row: row["boss_name"],
    ) == [
        {"game_version": "black", "boss_name": "alder"},
        {"game_version": "black", "boss_name": "cilan"},
    ]
    alder = out[out["boss_name"] == "alder"].iloc[0]
    assert int(alder["boss_ace_level"]) == 54
    assert int(alder["boss_avg_level"]) == 52


def test_progression_source_teams_use_precomputed_progression_depth_for_level_caps() -> None:
    encounters_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "location": "pewter-gym", "pokemon": "geodude", "level_min": 10, "level_max": 15, "encounter_chance_max": 100, "capture_rate": 120, "game": "red"},
            {"boss_id": "red-misty", "location": "route-24", "pokemon": "psyduck", "level_min": 18, "level_max": 25, "encounter_chance_max": 100, "capture_rate": 190, "game": "red"},
            {"boss_id": "red-misty", "location": "route-24", "pokemon": "geodude", "level_min": 10, "level_max": 15, "encounter_chance_max": 10, "capture_rate": 120, "game": "red"},
        ]
    )
    bosses_df = pd.DataFrame(
        [
            {"boss_id": "red-brock", "game_version": "red", "boss_name_canonical": "Brock", "boss_order": 1},
            {"boss_id": "red-misty", "game_version": "red", "boss_name_canonical": "Misty", "boss_order": 2},
        ]
    )
    progression_depth_df = build_progression_depth_table(bosses_df=bosses_df, encounters_df=encounters_df)
    boss_level_df = pd.DataFrame(
        [
            {"game_version": "red", "boss_name": "brock", "boss_ace_level": 12, "boss_avg_level": 10},
            {"game_version": "red", "boss_name": "misty", "boss_ace_level": 20, "boss_avg_level": 18},
        ]
    )
    progression_context = build_progression_depth_context(
        progression_depth_df=progression_depth_df,
        boss_level_df=boss_level_df,
    )

    source_teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters_df,
        bosses_df=bosses_df,
        progression_depth_context=progression_context,
        catch_pool_size=1,
    )

    brock = next(team for team in source_teams if team["boss_id"] == "red-brock")
    misty = next(team for team in source_teams if team["boss_id"] == "red-misty")

    assert brock["avg_level"] == 8
    assert brock["levels"] == [8]
    assert math.isclose(float(brock["progression_depth"]), 0.6 * (1 / 2) + 0.4 * (1 / 2), rel_tol=1e-9)
    assert brock["level_cap_offset"] == 4

    assert misty["avg_level"] == 19
    assert misty["levels"] == [19]
    assert math.isclose(float(misty["progression_depth"]), 1.0, rel_tol=1e-9)
    assert misty["level_cap_offset"] == 1


def test_progression_depth_context_resolves_unique_variant_boss_names() -> None:
    progression_depth_df = pd.DataFrame(
        [
            {
                "game_version": "red",
                "boss_id": "red-blue",
                "boss_name": "blue",
                "boss_index": 13,
                "max_boss_index": 13,
                "available_species_count": 151,
                "max_species_count": 151,
                "progression_depth": 1.0,
            }
        ]
    )
    boss_level_df = pd.DataFrame(
        [
            {"game_version": "red", "boss_name": "blue", "boss_ace_level": 65, "boss_avg_level": 61},
        ]
    )

    context = build_progression_depth_context(
        progression_depth_df=progression_depth_df,
        boss_level_df=boss_level_df,
    )

    resolved = context.require(
        game_version="red",
        boss_id="",
        boss_name="champion blue bulbasaur",
    )

    assert resolved.boss_id == "red-blue"
    assert resolved.boss_name == "blue"
