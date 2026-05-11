from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from src.pipeline.silver.inputs.builders import player_teams as player_teams_module
from src.pipeline.silver.inputs.builders.player_teams import build_progression_source_teams_from_encounters
from src.pipeline.silver.inputs.reference_context import load_reference_context
from src.pipeline.silver.orchestration.build_silver import _evolution_rules_map_from_rows
from src.pipeline.silver.transforms.progression_depth import ProgressionDepthContext, ProgressionDepthEntry


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _silver_dir() -> Path:
    return _repo_root() / "data" / "silver"


def test_diamond_volkner_core_collapse_stage_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    silver_dir = _silver_dir()
    references_dir = silver_dir / "references"
    if not references_dir.exists():
        pytest.skip(f"Silver references not found at {references_dir}")

    encounters = pd.read_parquet(references_dir / "encounters.parquet")
    bosses = pd.read_parquet(references_dir / "bosses.parquet")
    pokemon_data = pd.read_parquet(references_dir / "pokemon_data.parquet")
    evolution_rules_df = pd.read_parquet(references_dir / "evolution_rules.parquet")

    # Canonical Diamond Volkner id used across references.
    volkner_boss_id = "boss:diamond:volkner:fb9103302ec4"
    game_version = "diamond"

    encounters = encounters.loc[:, ~encounters.columns.duplicated()].copy()
    if "game_version" not in encounters.columns and "game" in encounters.columns:
        encounters["game_version"] = encounters["game"]
    encounters["game_version"] = encounters["game_version"].astype(str).str.lower()
    encounters["boss_id"] = encounters["boss_id"].astype(str).str.lower()
    encounters = encounters[
        (encounters["game_version"] == game_version)
        & (encounters["boss_id"] == volkner_boss_id)
    ].copy()
    assert not encounters.empty, "No Diamond Volkner encounters available for debug probe"
    # Builder canonicalizes `game` -> `game_version`; keep only one source column.
    if "game" not in encounters.columns:
        encounters["game"] = game_version
    encounters = encounters.drop(columns=["game_version"], errors="ignore")

    bosses = bosses.copy()
    bosses["game_version"] = bosses["game_version"].astype(str).str.lower()
    bosses["boss_id"] = bosses["boss_id"].astype(str).str.lower()
    bosses = bosses[(bosses["game_version"] == game_version) & (bosses["boss_id"] == volkner_boss_id)].copy()
    assert not bosses.empty, "No Diamond Volkner boss row available for debug probe"

    # Build a minimal progression context from current simulation artifacts.
    simulation_teams = pd.read_parquet(silver_dir / "simulation" / "source_teams_diamond.parquet")
    volkner_player = simulation_teams[
        (simulation_teams["team_role"].astype(str).str.lower() == "player")
        & (simulation_teams["boss_id"].astype(str).str.lower() == volkner_boss_id)
    ]
    assert not volkner_player.empty, "No Diamond Volkner player teams available to seed progression context"
    seed = volkner_player.iloc[0]
    entry = ProgressionDepthEntry(
        game_version=game_version,
        boss_id=volkner_boss_id,
        boss_name=str(seed.get("boss_name") or "volkner").strip().lower(),
        boss_index=int(seed.get("boss_index") or 8),
        max_boss_index=int(seed.get("max_boss_index") or 8),
        available_species_count=int(seed.get("available_species_count") or 0),
        max_species_count=int(seed.get("max_species_count") or 0),
        progression_depth=float(seed.get("progression_depth") or 0.0),
        boss_ace_level=int(seed.get("boss_ace_level") or 49),
        boss_avg_level=int(seed.get("boss_avg_level") or 49),
        starter_condition=None,
    )
    progression_context = ProgressionDepthContext(
        by_boss_id={(game_version, volkner_boss_id): entry},
        by_boss_name={(game_version, entry.boss_name): entry},
    )

    pokemon_types_by_species: dict[str, tuple[str | None, str | None]] = {}
    for row in pokemon_data.to_dict(orient="records"):
        species = str(row.get("pokemon_species") or "").strip().lower()
        if not species:
            continue
        t1 = str(row.get("type_1") or "").strip().lower() or None
        t2 = str(row.get("type_2") or "").strip().lower() or None
        if t1 or t2:
            pokemon_types_by_species[species] = (t1, t2)

    evolution_rules_by_game = _evolution_rules_map_from_rows(evolution_rules_df)
    reference_context = load_reference_context(silver_dir=silver_dir)

    stage_counts: dict[str, Any] = {}
    stage_samples: dict[str, Any] = {}

    original_filter = player_teams_module._filter_candidates_with_damaging_moves
    original_rank = player_teams_module._rank_candidate_pool
    original_generate = player_teams_module._generate_diverse_species_combos

    def wrapped_filter(
        candidates: list[tuple[str, int, int, int]],
        *,
        level_cap: int,
        game_version: str,
        reference_context: Any,
    ) -> tuple[list[tuple[str, int, int, int]], dict[str, int]]:
        stage_counts["candidates_before_move_filter"] = len(candidates)
        stage_samples["sample_before_move_filter"] = [row[0] for row in candidates[:20]]
        filtered, diag = original_filter(
            candidates,
            level_cap=level_cap,
            game_version=game_version,
            reference_context=reference_context,
        )
        stage_counts["candidates_after_move_filter"] = len(filtered)
        stage_samples["sample_after_move_filter"] = [row[0] for row in filtered[:20]]
        stage_counts["removed_no_damaging_moves"] = int(diag.get("removed_no_damaging_moves", 0))
        return filtered, diag

    def wrapped_rank(
        candidates: list[tuple[str, int, int, int]],
        *,
        boss_level: int,
        pool_size: int,
        progression_depth: float | None = None,
    ) -> tuple[list[tuple[str, int, int, int]], dict[str, int]]:
        stage_counts["candidates_before_rank"] = len(candidates)
        ranked, diag = original_rank(
            candidates,
            boss_level=boss_level,
            pool_size=pool_size,
            progression_depth=progression_depth,
        )
        stage_counts["candidate_pool_size_after_rank"] = len(ranked)
        stage_counts["rank_pruned"] = int(diag.get("pruned", 0))
        stage_samples["sample_ranked_pool_top20"] = [row[0] for row in ranked[:20]]
        return ranked, diag

    def wrapped_generate(
        candidates: list[tuple[str, int, int, int]],
        team_fill_size: int,
        combo_limit: int,
        *,
        progression_depth: float | None,
        pokemon_types_by_species: dict[str, tuple[str | None, str | None]],
        game_type_target_distribution: dict[str, float],
        boss_type_profile: dict[str, float],
    ) -> list[list[str]]:
        stage_counts["team_fill_size"] = team_fill_size
        stage_counts["combo_limit"] = combo_limit
        stage_counts["candidates_seen_by_combo_stage"] = len(candidates)
        combos = original_generate(
            candidates,
            team_fill_size,
            combo_limit,
            progression_depth=progression_depth,
            pokemon_types_by_species=pokemon_types_by_species,
            game_type_target_distribution=game_type_target_distribution,
            boss_type_profile=boss_type_profile,
        )
        stage_counts["combo_count_generated"] = len(combos)
        stage_samples["sample_generated_combos_top10"] = [combo[:6] for combo in combos[:10]]
        return combos

    monkeypatch.setattr(player_teams_module, "_filter_candidates_with_damaging_moves", wrapped_filter)
    monkeypatch.setattr(player_teams_module, "_rank_candidate_pool", wrapped_rank)
    monkeypatch.setattr(player_teams_module, "_generate_diverse_species_combos", wrapped_generate)

    teams = build_progression_source_teams_from_encounters(
        encounters_df=encounters,
        bosses_df=bosses,
        progression_depth_context=progression_context,
        catch_pool_size=20,
        evolution_rules_by_game=evolution_rules_by_game,
        reference_context=reference_context,
        pokemon_types_by_species=pokemon_types_by_species,
        boss_type_profile_by_key={(game_version, volkner_boss_id): {}},
    )

    # Core collapse check from the generated progression teams (pre-starter expansion).
    cores = {tuple(str(species).strip().lower() for species in row.get("pokemon", [])) for row in teams}

    debug_message = (
        "Volkner collapse probe: "
        f"before_move={stage_counts.get('candidates_before_move_filter')} "
        f"after_move={stage_counts.get('candidates_after_move_filter')} "
        f"after_rank={stage_counts.get('candidate_pool_size_after_rank')} "
        f"team_fill={stage_counts.get('team_fill_size')} "
        f"combo_count={stage_counts.get('combo_count_generated')} "
        f"progression_team_count={len(teams)} "
        f"unique_cores={len(cores)}"
    )
    print(debug_message)
    print(
        "Volkner collapse details: "
        f"removed_no_damaging={stage_counts.get('removed_no_damaging_moves')} "
        f"rank_pruned={stage_counts.get('rank_pruned')}"
    )
    print(f"sample_before_move_filter={stage_samples.get('sample_before_move_filter', [])}")
    print(f"sample_after_move_filter={stage_samples.get('sample_after_move_filter', [])}")
    print(f"sample_ranked_pool_top20={stage_samples.get('sample_ranked_pool_top20', [])}")
    print(f"sample_generated_combos_top10={stage_samples.get('sample_generated_combos_top10', [])}")
    print(f"final_progression_cores={sorted(cores)}")

    # This test is meant to expose where collapse occurs: we still have a pool, but one combo/core survives.
    assert stage_counts.get("candidate_pool_size_after_rank", 0) >= stage_counts.get("team_fill_size", 0), debug_message
    assert stage_counts.get("combo_count_generated", 0) <= 1, debug_message
    assert len(cores) <= 1, debug_message
