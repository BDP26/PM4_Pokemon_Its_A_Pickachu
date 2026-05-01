# Gold Layer

## Purpose

Gold is the serving layer. It consumes Silver contracts, runs battle simulations, and publishes ranking/analytics outputs.

## Code entrypoint

- Runner: `build_gold_from_silver`
- File: `src/pipeline/gold/orchestration/build_gold.py`
- CLI: `PYTHONPATH=src python -m src.pipeline.run_pipeline layers gold`

## Required inputs

Gold reads strictly from `data/silver/manifest.json` and does not do loose file discovery.

Typical required datasets include:
- `boss_records`
- `simulation_inputs_teams`
- `source_team_members`
- `member_move_options`
- reference tables used by simulation and reporting

## Main steps

1. Load Silver contract datasets from manifest entries.
2. Build progression and location aggregate outputs.
3. Run team-vs-boss battle simulations (`data/gold/simulation`).
4. Run Monte Carlo aggregation and compute recommendation rankings.
5. Build walkthrough payload and write Gold manifest.

## Outputs

- `data/gold/game_progression_summary.csv`
- `data/gold/location_popularity.parquet`
- `data/gold/simulation/team_battle_simulations.parquet`
- `data/gold/simulation/monte_carlo_results.parquet`
- `data/gold/team_recommendations.parquet`
- `data/gold/best_team_by_boss.parquet`
- `data/gold/team_rankings_by_boss_version.parquet`
- `data/gold/walkthrough_best_teams.json`
- `data/gold/manifest.json`

## Spark and local fallback

Battle simulation can run on Spark or local Python:
- `PIPELINE_USE_PYSPARK=1` (default): use Spark if available.
- `PIPELINE_USE_PYSPARK=0`: skip Spark and run local engine.

## Operational notes

- Gold fails fast on manifest contract violations (`GoldContractError`).
- If simulation output quality looks wrong, validate Silver contract first, then rerun `silver -> gold`.

