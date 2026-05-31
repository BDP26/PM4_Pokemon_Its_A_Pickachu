from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import pandas as pd

from src.pipeline.silver.inputs.builders import player_teams as player_teams_module
from src.pipeline.silver.inputs.builders.player_teams import build_progression_source_teams_from_encounters
from src.pipeline.silver.inputs.reference_context import load_reference_context
from src.pipeline.silver.orchestration.build_silver import _evolution_rules_map_from_rows
from src.pipeline.silver.transforms.progression_depth import ProgressionDepthContext, ProgressionDepthEntry


def _resolve_target_boss_id(teams_df: pd.DataFrame, *, game_version: str, boss_query: str) -> str:
    q = str(boss_query or "").strip().lower()
    if not q:
        raise ValueError("boss query is required")
    rows = teams_df[teams_df["game_version"].astype(str).str.lower().eq(game_version)].copy()
    if rows.empty:
        raise ValueError(f"no source_teams rows for game={game_version}")
    boss_id_match = rows["boss_id"].astype(str).str.lower().eq(q)
    if boss_id_match.any():
        return str(rows.loc[boss_id_match, "boss_id"].iloc[0]).strip().lower()
    boss_name_match = rows["boss_name"].astype(str).str.lower().eq(q)
    if boss_name_match.any():
        return str(rows.loc[boss_name_match, "boss_id"].iloc[0]).strip().lower()
    contains_match = rows["boss_id"].astype(str).str.lower().str.contains(q, na=False)
    if contains_match.any():
        return str(rows.loc[contains_match, "boss_id"].iloc[0]).strip().lower()
    raise ValueError(f"could not resolve boss from query={boss_query!r} in game={game_version}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Smoke-run progression team generation for a single game+boss.")
    parser.add_argument("--silver-dir", default="data/silver")
    parser.add_argument("--game", required=True, help="game key, e.g. diamond")
    parser.add_argument("--boss", required=True, help="boss name or boss_id query, e.g. volkner")
    parser.add_argument("--catch-pool-size", type=int, default=20)
    args = parser.parse_args()

    silver_dir = Path(args.silver_dir)
    references_dir = silver_dir / "references"
    simulation_dir = silver_dir / "simulation"
    game_version = str(args.game).strip().lower()

    encounters = pd.read_parquet(references_dir / "encounters.parquet")
    bosses = pd.read_parquet(references_dir / "bosses.parquet")
    pokemon_data = pd.read_parquet(references_dir / "pokemon_data.parquet")
    evolution_rules_df = pd.read_parquet(references_dir / "evolution_rules.parquet")
    shard_path = simulation_dir / f"source_teams_{game_version}.parquet"
    if not shard_path.exists():
        available = sorted(path.name for path in simulation_dir.glob("source_teams_*.parquet"))
        raise FileNotFoundError(
            f"missing shard {shard_path}. available source_teams shards={available}"
        )
    sim_teams = pd.read_parquet(shard_path)

    sim_teams = sim_teams.copy()
    sim_teams["game_version"] = sim_teams["game_version"].astype(str).str.lower()
    sim_teams["boss_id"] = sim_teams["boss_id"].astype(str).str.lower()
    sim_teams["boss_name"] = sim_teams["boss_name"].astype(str).str.lower()

    target_boss_id = _resolve_target_boss_id(sim_teams, game_version=game_version, boss_query=args.boss)

    encounters = encounters.loc[:, ~encounters.columns.duplicated()].copy()
    if "game_version" not in encounters.columns and "game" in encounters.columns:
        encounters["game_version"] = encounters["game"]
    encounters["game_version"] = encounters["game_version"].astype(str).str.lower()
    encounters["boss_id"] = encounters["boss_id"].astype(str).str.lower()
    encounters = encounters[
        (encounters["game_version"] == game_version)
        & (encounters["boss_id"] == target_boss_id)
    ].copy()
    encounters = encounters.drop(columns=["game_version"], errors="ignore")
    if "game" not in encounters.columns:
        encounters["game"] = game_version

    bosses = bosses.copy()
    bosses["game_version"] = bosses["game_version"].astype(str).str.lower()
    bosses["boss_id"] = bosses["boss_id"].astype(str).str.lower()
    bosses = bosses[
        (bosses["game_version"] == game_version)
        & (bosses["boss_id"] == target_boss_id)
    ].copy()

    sim_target = sim_teams[
        (sim_teams["team_role"].astype(str).str.lower() == "player")
        & (sim_teams["boss_id"].astype(str).str.lower() == target_boss_id)
    ]
    if sim_target.empty:
        raise ValueError(f"no player source teams found for game={game_version} boss_id={target_boss_id}")
    seed = sim_target.iloc[0]
    entry = ProgressionDepthEntry(
        game_version=game_version,
        boss_id=target_boss_id,
        boss_name=str(seed.get("boss_name") or "").strip().lower(),
        boss_index=int(seed.get("boss_index") or 0),
        max_boss_index=int(seed.get("max_boss_index") or 0),
        available_species_count=int(seed.get("available_species_count") or 0),
        max_species_count=int(seed.get("max_species_count") or 0),
        progression_depth=float(seed.get("progression_depth") or 0.0),
        boss_ace_level=int(seed.get("boss_ace_level") or 1),
        boss_avg_level=int(seed.get("boss_avg_level") or 1),
        starter_condition=None,
    )
    progression_context = ProgressionDepthContext(
        by_boss_id={(game_version, target_boss_id): entry},
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
            catch_pool_size=max(1, int(args.catch_pool_size)),
            evolution_rules_by_game=evolution_rules_by_game,
            reference_context=reference_context,
            pokemon_types_by_species=pokemon_types_by_species,
            boss_type_profile_by_key={(game_version, target_boss_id): {}},
        )
    finally:
        player_teams_module._filter_candidates_with_damaging_moves = original_filter
        player_teams_module._rank_candidate_pool = original_rank
        player_teams_module._generate_diverse_species_combos = original_generate

    cores = [tuple(str(species).strip().lower() for species in row.get("pokemon", [])) for row in teams]
    unique_cores = sorted(set(cores))
    print(
        "TeamGen smoke:",
        f"game={game_version}",
        f"boss_id={target_boss_id}",
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
