from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from src.pipeline.silver.inputs.builders.player_teams import build_player_team_compact_tables
from src.pipeline.silver.inputs.reference_context import MoveReferenceContext
from src.pipeline.silver.transforms.keys import make_team_id


def _reference_context() -> MoveReferenceContext:
    return MoveReferenceContext(
        move_profiles={
            "tackle": {"power": 40, "damage_class": "physical"},
            "quick-attack": {"power": 40, "damage_class": "physical"},
            "thunder-shock": {"power": 40, "damage_class": "special"},
            "growl": {"power": 0, "damage_class": "status"},
            "vine-whip": {"power": 45, "damage_class": "physical"},
            "ember": {"power": 40, "damage_class": "special"},
            "water-gun": {"power": 40, "damage_class": "special"},
        },
        learnable_by_game_species={
            ("red", "pikachu"): {"tackle": 1, "quick-attack": 5, "thunder-shock": 1, "growl": 1},
            ("red", "pidgey"): {"tackle": 1, "quick-attack": 9},
            ("black-white", "snivy"): {"tackle": 1, "vine-whip": 7},
            ("black-white", "tepig"): {"tackle": 1, "ember": 7},
            ("black-white", "oshawott"): {"tackle": 1, "water-gun": 7},
            ("black-white", "patrat"): {"tackle": 1},
        },
    )


def test_compact_team_tables_are_linear() -> None:
    compact = build_player_team_compact_tables(
        progression_source_teams=[
            {
                "team_id": "progression:red:1",
                "game_version": "red",
                "boss_name": "brock",
                "avg_level": 10,
                "pokemon": ["pidgey"],
                "levels": [10],
                "progression_pool_id": "pool1",
            }
        ],
        reference_context=_reference_context(),
    )

    assert compact["source_teams"]
    assert compact["source_team_members"]
    assert len(compact["source_team_members"]) <= len(compact["source_teams"]) * 6
    assert len(compact["member_moveset_combos"]) <= len(compact["source_team_members"]) * 10
    assert len(compact["member_move_options"]) <= len(compact["source_team_members"]) * 8


def test_per_member_moveset_combos_preserved_without_team_cartesian_expansion(monkeypatch) -> None:
    monkeypatch.setattr("src.pipeline.silver.inputs.builders.player_teams.DEFAULT_MEMBER_MOVE_OPTION_LIMIT", 8)
    monkeypatch.setattr("src.pipeline.silver.inputs.builders.player_teams.DEFAULT_MEMBER_MOVESET_COMBO_LIMIT", 10)
    move_profiles = {f"move-{idx}": {"power": 50, "damage_class": "physical"} for idx in range(1, 9)}
    learnable = {f"move-{idx}": 1 for idx in range(1, 9)}
    context = MoveReferenceContext(
        move_profiles=move_profiles,
        learnable_by_game_species={("red", f"mon-{slot}"): learnable for slot in range(1, 7)},
    )
    compact = build_player_team_compact_tables(
        progression_source_teams=[
            {
                "team_id": "progression:red:1",
                "game_version": "red",
                "boss_name": "brock",
                "avg_level": 20,
                "pokemon": [f"mon-{idx}" for idx in range(2, 7)],
                "levels": [20] * 5,
                "progression_pool_id": "pool1",
            }
        ],
        reference_context=context,
    )
    team_count = len(compact["source_teams"])
    assert team_count >= 1
    assert len(compact["source_team_members"]) == team_count * 6
    assert len(compact["member_moveset_combos"]) <= team_count * 60


def test_pokemon_moveset_options_are_reused_across_members() -> None:
    compact = build_player_team_compact_tables(
        progression_source_teams=[
            {
                "team_id": "progression:red:1",
                "game_version": "red",
                "boss_name": "brock",
                "avg_level": 10,
                "pokemon": ["pidgey", "pidgey"],
                "levels": [10, 10],
                "progression_pool_id": "pool1",
            }
        ],
        reference_context=_reference_context(),
    )

    contexts = [
        row for row in compact["pokemon_moveset_options"] if row.get("pokemon_species") == "pidgey" and "move_name" not in row
    ]
    assert len(contexts) == 1


def test_deterministic_team_id_is_stable_across_subprocesses() -> None:
    code = "from src.pipeline.silver.transforms.keys import make_team_id; import json; print(json.dumps({'id': make_team_id('player-source','red','brock',source_team_id='s1',variant='starter:bulbasaur')}))"
    out_a = subprocess.check_output([sys.executable, "-c", code], text=True).strip()
    out_b = subprocess.check_output([sys.executable, "-c", code], text=True).strip()
    assert json.loads(out_a)["id"] == json.loads(out_b)["id"]


def test_no_python_hash_for_deterministic_pipeline_ids() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    result = subprocess.run(
        [
            "bash",
            "-lc",
            # Exclude Spark's F.hash (deterministic) and pyspark hash functions;
            # only flag bare Python hash() calls which are non-deterministic across processes.
            "rg -n 'hash\\(' src/pipeline/silver src/pipeline/gold --glob '*.py' | grep -v 'F\\.hash\\|pyspark.*hash\\|hashlib\\|#.*hash'",
        ],
        cwd=repo_root,
        text=True,
        capture_output=True,
    )
    assert result.returncode == 1, result.stdout


def test_make_team_id_is_stable() -> None:
    assert make_team_id("player", "red", "brock") == make_team_id("player", "red", "brock")


def test_player_team_builder_does_not_cartesian_expand_member_movesets() -> None:
    source = Path("src/pipeline/silver/inputs/builders/player_teams.py").read_text(encoding="utf-8")
    assert "product(*per_member_variants)" not in source


def test_starter_condition_limits_striaton_player_team_generation() -> None:
    compact = build_player_team_compact_tables(
        progression_source_teams=[
            {
                "team_id": "progression:black-white:1",
                "game_version": "black-white",
                "boss_id": "black-white:chili",
                "boss_name": "chili",
                "starter_condition": "snivy",
                "gym_index": 1,
                "avg_level": 10,
                "pokemon": ["patrat"],
                "levels": [10],
                "progression_pool_id": "pool-bw-1",
            }
        ],
        reference_context=_reference_context(),
    )

    assert len(compact["source_teams"]) == 1
    team = compact["source_teams"][0]
    assert team["starter_base"] == "snivy"
    assert team["boss_id"] == "black-white:chili"
    assert team["starter_condition"] == "snivy"


def test_canonical_starter_type_limits_striaton_player_team_generation() -> None:
    compact = build_player_team_compact_tables(
        progression_source_teams=[
            {
                "team_id": "progression:black-white:1",
                "game_version": "black-white",
                "boss_id": "black-white:chili",
                "boss_name": "chili",
                "starter_condition": "grass",
                "gym_index": 1,
                "avg_level": 10,
                "pokemon": ["patrat"],
                "levels": [10],
                "progression_pool_id": "pool-bw-1",
            }
        ],
        reference_context=_reference_context(),
    )

    assert len(compact["source_teams"]) == 1
    team = compact["source_teams"][0]
    assert team["starter_base"] == "snivy"
    assert team["starter_type"] == "grass"
    assert team["boss_id"] == "black-white:chili"


def test_nan_starter_condition_falls_back_to_game_starters() -> None:
    compact = build_player_team_compact_tables(
        progression_source_teams=[
            {
                "team_id": "progression:blue:1",
                "game_version": "blue",
                "boss_id": "blue:brock",
                "boss_name": "brock",
                "starter_condition": float("nan"),
                "gym_index": 1,
                "avg_level": 9,
                "pokemon": ["pidgey"],
                "levels": [9],
                "progression_pool_id": "pool-blue-1",
            }
        ],
        reference_context=_reference_context(),
    )

    starters = {row["starter_base"] for row in compact["source_teams"]}
    member_species = {row["pokemon_species"] for row in compact["source_team_members"] if row["slot"] == 1}

    assert "nan" not in starters
    assert "nan" not in member_species
    assert starters == {"bulbasaur", "charmander", "squirtle"}
