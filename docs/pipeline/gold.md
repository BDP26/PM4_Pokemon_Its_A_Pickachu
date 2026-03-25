# Gold Layer

The Gold layer aggregates Silver snapshots into analytics-ready outputs.

## Purpose

- Produce compact, analysis-focused datasets.
- Expose progression and location popularity metrics across games.
- Provide a final layer with clear provenance metadata.

## Entrypoint

- Function: `build_gold_from_silver`
- File: `src/pipeline/gold/build_gold.py`
- Orchestration: `src/pipeline/run_pipeline.py` (`all` or `layers gold`)

## Required Inputs

- `data/silver/snapshots/*_boss_snapshots.jsonl` (primary)
- Legacy fallback: `data/silver/*_boss_snapshots.jsonl`

If no Silver snapshot files are present, the layer raises `FileNotFoundError`.

## Core Processing

1. Load all per-game Silver snapshot JSONL files.
2. Concatenate into one dataframe.
3. Build game progression summary:
   - Number of boss steps per game
   - Final reachable location count
   - Maximum reachable location count
4. Build cross-game location popularity by exploding reachable location arrays.
5. Write Gold manifest with source file list and output inventory.

## Outputs

- `data/gold/game_progression_summary.csv`
- `data/gold/location_popularity.jsonl`
- `data/gold/manifest.json`

## Notes and Constraints

- Gold intentionally keeps only derived, analysis-oriented fields.
- Metrics depend on Silver snapshot completeness and mapping quality.
- Output schemas are stable for BI/notebook downstream use.


