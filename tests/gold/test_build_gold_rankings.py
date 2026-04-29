from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.pipeline.common.io import read_parquet, write_parquet
from src.pipeline.gold.orchestration import build_gold


def test_apply_level_plausibility_filter_uses_silver_level_cap_not_boss_average() -> None:
    rows = pd.DataFrame(
        [
            {
                "player_team_id": "brock-valid",
                "player_avg_level": 8,
                "boss_avg_level": 10,
                "boss_ace_level": 12,
                "level_cap_offset": 4,
            },
            {
                "player_team_id": "giovanni-valid",
                "player_avg_level": 64,
                "boss_avg_level": 61,
                "boss_ace_level": 65,
                "level_cap_offset": 1,
            },
            {
                "player_team_id": "over-cap",
                "player_avg_level": 21,
                "boss_avg_level": 18,
                "boss_ace_level": 20,
                "level_cap_offset": 1,
            },
        ]
    )

    filtered = build_gold._apply_level_plausibility_filter(rows)

    assert set(filtered["player_team_id"]) == {"brock-valid", "giovanni-valid"}


def test_build_starter_rankings_writes_diagnostics_and_excludes_yellow_coverage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    gold_dir = tmp_path / "gold"
    simulation_dir = gold_dir / "simulation"
    simulation_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        simulation_dir / "teams.parquet",
        [
            {
                "team_id": "player-brock-bulbasaur",
                "game_version": "red",
                "boss_id": "red:brock",
                "starter_base": "bulbasaur",
                "starter_evolved_species": "bulbasaur",
                "avg_level": 8,
                "progression_depth": 0.5,
                "boss_ace_level": 12,
                "level_cap_offset": 4,
                "boss_name": None,
                "levels": [8],
                "pokemon": ["bulbasaur"],
                "source_team_id": "player-brock-bulbasaur",
            },
            {
                "team_id": "boss-brock",
                "game_version": "red",
                "boss_id": "red:brock",
                "boss_name": "brock",
                "avg_level": 10,
                "levels": [12],
                "pokemon": ["geodude"],
                "source_team_id": "boss-brock",
            },
            {
                "team_id": "player-diantha-froakie",
                "game_version": "y",
                "boss_id": "y:diantha",
                "starter_base": "froakie",
                "starter_evolved_species": "greninja",
                "avg_level": 64,
                "progression_depth": 1.0,
                "boss_ace_level": 65,
                "level_cap_offset": 1,
                "boss_name": None,
                "levels": [64],
                "pokemon": ["greninja"],
                "source_team_id": "player-diantha-froakie",
            },
            {
                "team_id": "boss-diantha",
                "game_version": "y",
                "boss_id": "y:diantha",
                "boss_name": "diantha",
                "avg_level": 61,
                "levels": [65],
                "pokemon": ["gardevoir"],
                "source_team_id": "boss-diantha",
            },
        ],
    )
    write_parquet(
        simulation_dir / "monte_carlo_results.parquet",
        [
            {
                "scenario_id": "brock-1",
                "player_team_id": "player-brock-bulbasaur",
                "boss_team_id": "boss-brock",
                "boss_name": "brock",
                "game_version": "red",
                "mc_win_rate": 0.61,
                "wins": 9,
                "losses": 6,
                "n_trials": 15,
                "degraded_data": False,
                "simulation_mode": "gym",
            },
            {
                "scenario_id": "diantha-1",
                "player_team_id": "player-diantha-froakie",
                "boss_team_id": "boss-diantha",
                "boss_name": "diantha",
                "game_version": "y",
                "mc_win_rate": 0.54,
                "wins": 8,
                "losses": 7,
                "n_trials": 15,
                "degraded_data": False,
                "simulation_mode": "gym",
            },
        ],
    )

    monkeypatch.setattr(
        build_gold,
        "get_games_config",
        lambda: [
            {
                "game_key": "red",
                "bosses": ["Brock"],
                "starter_choices": ["bulbasaur"],
            },
            {
                "game_key": "y",
                "bosses": ["Diantha"],
                "starter_choices": ["froakie"],
            },
            {
                "game_key": "yellow",
                "bosses": ["Brock"],
                "starter_choices": ["pikachu"],
            },
        ],
    )

    outputs = build_gold._build_starter_rankings_from_monte_carlo(
        gold_dir=gold_dir,
        gold_simulation_dir=simulation_dir,
    )

    assert "team_rankings_by_boss_version_starter.parquet" in outputs
    assert "best_team_by_boss_version_starter.parquet" in outputs
    assert "debug/ranking_plausibility_filter_diagnostics.parquet" in outputs

    rankings = read_parquet(gold_dir / "team_rankings_by_boss_version_starter.parquet")
    groups = {
        (row["effective_game_version"], row["effective_boss_name"], row["starter_base"])
        for row in rankings.to_dict(orient="records")
    }
    assert ("red", "brock", "bulbasaur") in groups
    assert ("y", "diantha", "froakie") in groups

    diagnostics = read_parquet(gold_dir / "debug" / "ranking_plausibility_filter_diagnostics.parquet")
    assert set(diagnostics["game_version"]) == {"red", "y"}
    brock_diag = diagnostics[
        (diagnostics["game_version"] == "red")
        & (diagnostics["boss_name"] == "brock")
        & (diagnostics["starter_base"] == "bulbasaur")
    ].iloc[0]
    diantha_diag = diagnostics[
        (diagnostics["game_version"] == "y")
        & (diagnostics["boss_name"] == "diantha")
        & (diagnostics["starter_base"] == "froakie")
    ].iloc[0]

    assert int(brock_diag["rows_before_plausibility_filter"]) == 1
    assert int(brock_diag["rows_after_plausibility_filter"]) == 1
    assert brock_diag["status"] == "ok"
    assert int(diantha_diag["rows_before_plausibility_filter"]) == 1
    assert int(diantha_diag["rows_after_plausibility_filter"]) == 1
    assert diantha_diag["status"] == "ok"


def test_gold_simulation_artifact_consistency_rejects_stale_team_snapshot(tmp_path: Path) -> None:
    simulation_dir = tmp_path / "gold" / "simulation"
    simulation_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(
        simulation_dir / "teams.parquet",
        [
            {"team_id": "player-current", "boss_name": None, "game_version": "blue", "pokemon": ["venusaur"], "avg_level": 62},
            {"team_id": "boss-current", "boss_name": "blue", "game_version": "blue", "pokemon": ["blastoise"], "avg_level": 61},
        ],
    )
    write_parquet(
        simulation_dir / "team_battle_simulations.parquet",
        [
            {
                "team_id_attacker": "player-stale",
                "team_id_defender": "boss-current",
                "attacker_win": False,
                "battle_turns": 3,
                "simulation_score": -1.0,
                "simulation_mode": "gauntlet",
            }
        ],
    )
    write_parquet(
        simulation_dir / "battle_seeds.parquet",
        [
            {
                "scenario_id": "player-stale_vs_boss-current__mode=gauntlet|sequence=blue:elite_four_champion|position=5",
                "player_team_id": "player-stale",
                "boss_team_id": "boss-current",
                "predicted_player_win_chance": 0.0,
            }
        ],
    )
    write_parquet(
        simulation_dir / "monte_carlo_results.parquet",
        [
            {
                "scenario_id": "player-stale_vs_boss-current__mode=gauntlet|sequence=blue:elite_four_champion|position=5",
                "player_team_id": "player-stale",
                "boss_team_id": "boss-current",
                "predicted_player_win_chance": 0.0,
                "n_trials": 1,
                "wins": 0,
                "losses": 1,
                "mc_win_rate": 0.0,
            }
        ],
    )

    try:
        build_gold._assert_gold_simulation_artifact_consistency(simulation_dir)
    except build_gold.GoldContractError as exc:
        message = str(exc)
        assert "inconsistent_gold_simulation_snapshot" in message
        assert "player-stale" in message
    else:
        raise AssertionError("Expected GoldContractError for stale Gold simulation snapshot")


def test_validate_non_yellow_starter_boss_coverage_tolerates_no_valid_team_generated() -> None:
    boss_rank = pd.DataFrame(
        [
            {
                "effective_game_version": "gold",
                "effective_boss_name": "falkner",
                "starter_base": "chikorita",
            }
        ]
    )
    diagnostics = pd.DataFrame(
        [
            {
                "game_version": "gold",
                "boss_name": "falkner",
                "starter_base": "chikorita",
                "rows_before_plausibility_filter": 1,
                "rows_after_plausibility_filter": 1,
                "rows_removed": 0,
                "status": "ok",
                "removal_reason": "",
            },
            {
                "game_version": "gold",
                "boss_name": "bugsy",
                "starter_base": "chikorita",
                "rows_before_plausibility_filter": 0,
                "rows_after_plausibility_filter": 0,
                "rows_removed": 0,
                "status": "no_valid_team_generated",
                "removal_reason": "no_valid_team_generated",
            },
        ]
    )

    original_get_games_config = build_gold.get_games_config
    build_gold.get_games_config = lambda: [
        {
            "game_key": "gold",
            "bosses": ["Falkner", "Bugsy"],
            "starter_choices": ["chikorita"],
        }
    ]
    try:
        build_gold._validate_non_yellow_starter_boss_coverage(
            boss_rank=boss_rank,
            diagnostics_df=diagnostics,
        )
    finally:
        build_gold.get_games_config = original_get_games_config


def test_validate_non_yellow_starter_boss_coverage_tolerates_gauntlet_only_missing_group() -> None:
    boss_rank = pd.DataFrame(
        [
            {
                "effective_game_version": "red",
                "effective_boss_name": "brock",
                "starter_base": "bulbasaur",
            }
        ]
    )
    diagnostics = pd.DataFrame(
        [
            {
                "game_version": "red",
                "boss_name": "brock",
                "starter_base": "bulbasaur",
                "rows_before_plausibility_filter": 120,
                "rows_after_plausibility_filter": 120,
                "rows_single_boss_mode": 120,
                "rows_gauntlet_mode": 0,
                "rows_removed": 0,
                "status": "ok",
                "removal_reason": "",
            },
            {
                "game_version": "red",
                "boss_name": "lorelei",
                "starter_base": "bulbasaur",
                "rows_before_plausibility_filter": 480,
                "rows_after_plausibility_filter": 480,
                "rows_single_boss_mode": 0,
                "rows_gauntlet_mode": 480,
                "rows_removed": 0,
                "status": "ok",
                "removal_reason": "",
            },
        ]
    )

    original_get_games_config = build_gold.get_games_config
    build_gold.get_games_config = lambda: [
        {
            "game_key": "red",
            "bosses": ["Brock", "Lorelei"],
            "starter_choices": ["bulbasaur"],
        }
    ]
    try:
        build_gold._validate_non_yellow_starter_boss_coverage(
            boss_rank=boss_rank,
            diagnostics_df=diagnostics,
        )
    finally:
        build_gold.get_games_config = original_get_games_config


def test_load_and_validate_gold_contract_ignores_legacy_snapshot_entries(tmp_path: Path) -> None:
    silver_dir = tmp_path / "silver"
    references_dir = silver_dir / "references"
    simulation_dir = silver_dir / "simulation"
    references_dir.mkdir(parents=True, exist_ok=True)
    simulation_dir.mkdir(parents=True, exist_ok=True)

    write_parquet(references_dir / "pokemon_data.parquet", [{"pokemon_species": "pikachu"}])
    write_parquet(simulation_dir / "move_data.parquet", [{"move_name": "tackle"}])
    write_parquet(simulation_dir / "source_teams_red.parquet", [{"team_id": "t1", "game_version": "red"}])
    write_parquet(simulation_dir / "source_team_members_red.parquet", [{"team_member_id": "m1", "source_team_id": "t1", "game_version": "red", "slot": 1, "pokemon_species": "pikachu", "level": 10}])
    write_parquet(simulation_dir / "member_moveset_combos_red.parquet", [{"moveset_combo_id": "c1", "team_id": "t1", "pokemon_instance_id": "m1", "slot_index": 1, "move_1": "tackle"}])

    (silver_dir / "manifest.json").write_text(
        """{
          "datasets": {
            "boss_records": {"files": ["snapshots/red_boss_snapshots.jsonl"]},
            "pokemon_data": {"file": "references/pokemon_data.parquet"},
            "move_data": {"file": "simulation/move_data.parquet"},
            "simulation_inputs_teams": {"files": ["simulation/source_teams_red.parquet"]},
            "source_team_members": {"files": ["simulation/source_team_members_red.parquet"]},
            "member_moveset_combos": {"files": ["simulation/member_moveset_combos_red.parquet"]}
          }
        }""",
        encoding="utf-8",
    )

    contract = build_gold._load_and_validate_gold_contract(silver_dir)

    assert "pokemon_data" in contract["required_files"]
