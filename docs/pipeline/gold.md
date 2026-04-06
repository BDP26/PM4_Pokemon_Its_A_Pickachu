# Gold Layer

The Gold layer aggregates Silver snapshots into analytics-ready outputs.

## Purpose

- Produce compact, analysis-focused datasets.
- Expose progression and location popularity metrics across games.
- Provide a final layer with clear provenance metadata.
- Turn Silver simulation outputs into ranked team recommendations.

## Entrypoint

- Function: `build_gold_from_silver`
- File: `src/pipeline/gold/orchestration/build_gold.py`
- Orchestration: `src/pipeline/run_pipeline.py` (`all` or `layers gold`)

## Required Inputs

- `data/silver/snapshots/*_boss_snapshots.jsonl` (primary)
- Legacy fallback: `data/silver/*_boss_snapshots.jsonl`
- `data/silver/simulation/teams.parquet` (simulation input copied into Gold)

If no Silver snapshot files are present, the layer raises `FileNotFoundError`.

## Core Processing

1. Load all per-game Silver snapshot JSONL files.
2. Concatenate into one dataframe.
3. Build game progression summary:
   - Number of boss steps per game
   - Final reachable location count
   - Maximum reachable location count
4. Build cross-game location popularity by exploding reachable location arrays.
5. Run the Gold simulation stage into `data/gold/simulation/`.
6. Aggregate `teams.parquet` + `monte_carlo_results.parquet` into team rankings and best-team outputs.
7. Build the walkthrough payload (`walkthrough_best_teams.json`) for the static web view.
8. Write Gold manifest with source file list and output inventory.

## Outputs

- `data/gold/game_progression_summary.csv`
- `data/gold/location_popularity.parquet`
- `data/gold/manifest.json`
- `data/gold/simulation/teams.parquet`
- `data/gold/simulation/team_battle_simulations.parquet`
- `data/gold/simulation/battle_seeds.parquet`
- `data/gold/simulation/monte_carlo_results.parquet`

Optional outputs (if Silver simulation artifacts are present):

- `data/gold/team_recommendations.parquet`
- `data/gold/best_team_by_boss.parquet`
- `data/gold/team_rankings_by_boss_version.parquet`
- `data/gold/best_team_by_boss_version.parquet`
- `data/gold/best_team_by_boss_version.csv`
- `data/gold/walkthrough_best_teams.json`


## Notes and Constraints

- Gold intentionally keeps only derived, analysis-oriented fields.
- Metrics depend on Silver snapshot completeness and mapping quality.
- Output schemas are stable for BI/notebook downstream use.
- **Simulation Note:** Silver produces the simulation inputs (`teams.parquet`, `teams.jsonl`). Gold runs the battle simulation stage into `data/gold/simulation/`, then consumes the resulting Monte-Carlo results to rank teams and to prepare the walkthrough payload. The underlying damage model is reused, not redefined.


