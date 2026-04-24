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
        },
        learnable_by_game_species={
            ("red", "pikachu"): {"tackle": 1, "quick-attack": 5, "thunder-shock": 1, "growl": 1},
            ("red", "pidgey"): {"tackle": 1, "quick-attack": 9},
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
            "rg -n 'hash\\(' src/pipeline/silver src/pipeline/gold --glob '*.py'",
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
