from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.silver.inputs.builders import player_teams as player_teams_module
from src.pipeline.silver.inputs.builders.player_teams import build_progression_source_teams_from_encounters
from src.pipeline.silver.inputs.reference_context import load_reference_context
from src.pipeline.silver.orchestration.build_silver import _evolution_rules_map_from_rows
from src.pipeline.silver.transforms.progression_depth import ProgressionDepthContext, ProgressionDepthEntry


def main() -> None:
    silver_dir = Path("data/silver")
    references_dir = silver_dir / "references"
    simulation_dir = silver_dir / "simulation"

    game_version = "diamond"
    volkner_boss_id = "boss:diamond:volkner:fb9103302ec4"

    encounters = pd.read_parquet(references_dir / "encounters.parquet")
    bosses = pd.read_parquet(references_dir / "bosses.parquet")
    pokemon_data = pd.read_parquet(references_dir / "pokemon_data.parquet")
    evolution_rules_df = pd.read_parquet(references_dir / "evolution_rules.parquet")
    sim_teams = pd.read_parquet(simulation_dir / "source_teams_diamond.parquet")

    encounters = encounters.loc[:, ~encounters.columns.duplicated()].copy()
    if "game_version" not in encounters.columns and "game" in encounters.columns:
        encounters["game_version"] = encounters["game"]
    encounters["game_version"] = encounters["game_version"].astype(str).str.lower()
    encounters["boss_id"] = encounters["boss_id"].astype(str).str.lower()
    encounters = encounters[
        (encounters["game_version"] == game_version)
        & (encounters["boss_id"] == volkner_boss_id)
    ].copy()
    encounters = encounters.drop(columns=["game_version"], errors="ignore")
    if "game" not in encounters.columns:
        encounters["game"] = game_version

    bosses = bosses.copy()
    bosses["game_version"] = bosses["game_version"].astype(str).str.lower()
    bosses["boss_id"] = bosses["boss_id"].astype(str).str.lower()
    bosses = bosses[
        (bosses["game_version"] == game_version)
        & (bosses["boss_id"] == volkner_boss_id)
    ].copy()

    sim_volkner = sim_teams[
        (sim_teams["team_role"].astype(str).str.lower() == "player")
        & (sim_teams["boss_id"].astype(str).str.lower() == volkner_boss_id)
    ]
    seed = sim_volkner.iloc[0]
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
    original_filter = player_teams_module._filter_candidates_with_damaging_moves
    original_rank = player_teams_module._rank_candidate_pool
    original_generate = player_teams_module._generate_diverse_species_combos

    def wrapped_filter(candidates, *, level_cap, game_version, reference_context):
        stage_counts["before_move"] = len(candidates)
        filtered, diag = original_filter(
            candidates,
            level_cap=level_cap,
            game_version=game_version,
            reference_context=reference_context,
        )
        stage_counts["after_move"] = len(filtered)
        stage_counts["removed_no_damaging"] = int(diag.get("removed_no_damaging_moves", 0))
        return filtered, diag

    def wrapped_rank(candidates, *, boss_level, pool_size, progression_depth=None):
        stage_counts["before_rank"] = len(candidates)
        ranked, diag = original_rank(
            candidates,
            boss_level=boss_level,
            pool_size=pool_size,
            progression_depth=progression_depth,
        )
        stage_counts["after_rank"] = len(ranked)
        stage_counts["rank_pruned"] = int(diag.get("pruned", 0))
        return ranked, diag

    def wrapped_generate(candidates, team_fill_size, combo_limit, *, progression_depth, pokemon_types_by_species, game_type_target_distribution, boss_type_profile):
        stage_counts["team_fill"] = int(team_fill_size)
        combos = original_generate(
            candidates,
            team_fill_size,
            combo_limit,
            progression_depth=progression_depth,
            pokemon_types_by_species=pokemon_types_by_species,
            game_type_target_distribution=game_type_target_distribution,
            boss_type_profile=boss_type_profile,
        )
        stage_counts["combo_count"] = len(combos)
        return combos

    player_teams_module._filter_candidates_with_damaging_moves = wrapped_filter
    player_teams_module._rank_candidate_pool = wrapped_rank
    player_teams_module._generate_diverse_species_combos = wrapped_generate
    try:
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
    finally:
        player_teams_module._filter_candidates_with_damaging_moves = original_filter
        player_teams_module._rank_candidate_pool = original_rank
        player_teams_module._generate_diverse_species_combos = original_generate

    cores = [tuple(str(species).strip().lower() for species in row.get("pokemon", [])) for row in teams]
    unique_cores = sorted(set(cores))
    print(
        "Volkner smoke:",
        f"before_move={stage_counts.get('before_move', 0)}",
        f"after_move={stage_counts.get('after_move', 0)}",
        f"after_rank={stage_counts.get('after_rank', 0)}",
        f"team_fill={stage_counts.get('team_fill', 0)}",
        f"combo_count={stage_counts.get('combo_count', 0)}",
        f"progression_team_count={len(teams)}",
        f"unique_cores={len(unique_cores)}",
    )
    for idx, core in enumerate(unique_cores, start=1):
        print(f"core_{idx}={core}")


if __name__ == "__main__":
    main()
