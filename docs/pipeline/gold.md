# Gold Layer

## Purpose

Gold is the serving layer. It consumes Silver contracts, runs battle simulations, and publishes ranking/analytics outputs.

## Code entrypoint

- Runner: `build_gold_from_silver`
- File: `src/pipeline/gold/orchestration/build_gold.py`
- CLI: `PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers gold`

## Required inputs

Gold reads strictly from `data/silver/manifest.json` and does not do loose file discovery. If a required dataset key is missing or its file is stale, Gold fails immediately with `GoldContractError`.

Required manifest dataset keys:
- `pokemon_data`
- `simulation_inputs_teams`
- `source_team_members`
- `member_moveset_combos`

## Main steps

1. Load Silver contract datasets from manifest entries.
2. Build progression and location aggregate outputs.
3. Run team-vs-boss battle simulations (`data/gold/simulation`).
4. Run Monte Carlo aggregation and compute recommendation rankings.
5. Build recommendation tables by boss/version/starter, walkthrough payload, and Gold manifest.

## Outputs

- `data/gold/simulation/team_battle_simulations.parquet`
- `data/gold/simulation/monte_carlo_results.parquet`
- `data/gold/team_recommendations.parquet`
- `data/gold/best_team_by_boss.parquet`
- `data/gold/team_rankings_by_boss_version.parquet`
- `data/gold/team_rankings_by_boss_version_starter.parquet`
- `data/gold/best_team_by_boss_version_starter.parquet`
- `data/gold/team_rankings_e4_champion_sequence_by_version_starter.parquet`
- `data/gold/best_team_by_e4_champion_sequence_version_starter.parquet`
- `data/gold/walkthrough_best_teams.json`
- `data/gold/manifest.json`

## Spark and local fallback

Battle simulation can run on Spark or local Python:
- `PIPELINE_USE_PYSPARK=1` (default): use Spark if available.
- `PIPELINE_USE_PYSPARK=0`: skip Spark and run local engine.

If Spark startup is interrupted (e.g. `KeyboardInterrupt` during `SparkSession.getOrCreate()`), terminate stale Spark Java processes before retrying.

## Operational notes

- Gold fails fast on manifest contract violations (`GoldContractError`).
- If simulation output quality looks wrong, validate Silver contract first, then rerun `silver -> gold`.

