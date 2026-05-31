# Silver Layer

## Purpose

Silver is the core transformation layer. It converts Bronze raw data into normalized tables, diagnostics, and simulation-ready contracts.

## Code entrypoint

- Runner: `build_silver_from_bronze`
- File: `src/pipeline/silver/orchestration/build_silver.py`
- CLI: `PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers silver`

## Required inputs

- `data/bronze/pokeapi/location_index.json`
- `data/bronze/bulbapedia/*.json`
- Required: `data/bronze/kagglehub/gym_leaders_elite_four.csv`

## Main steps

1. Parse walkthrough pages into boss progression snapshots.
2. Harmonize boss naming and IDs for cross-source joins.
3. Build location mappings and reachable species context.
4. Build canonical reference tables (`bosses`, `encounters`, `pokemon_data`, `move_reference`, `learnable_moves`).
5. Build simulation input tables (`source_teams`, `source_team_members`, `member_move_options`, compact move-set combos).
6. Emit diagnostics and relational checks.
7. Write `data/silver/manifest.json`.

When invoked via `src.pipeline.run_pipeline layers silver`, a contract gate runs immediately after build:

- `python -m src.pipeline.silver.validation.validate_silver_contract --fail-on-error`

## Why Silver manifest matters

`data/silver/manifest.json` is the strict contract boundary for Gold. Gold expects dataset entries in this manifest and fails fast if required keys or files are missing/stale.

## Important outputs

**Snapshots**
- `data/silver/snapshots/*_boss_snapshots.jsonl`

**Reference tables** (game-agnostic)
- `data/silver/references/bosses.parquet`
- `data/silver/references/encounters.parquet`
- `data/silver/references/pokemon_data.parquet`
- `data/silver/references/move_reference.parquet`
- `data/silver/references/learnable_moves.parquet`

**Simulation inputs** (one file per game version, `<game>` = e.g. `red`, `black`)
- `data/silver/simulation/source_teams_<game>.parquet`
- `data/silver/simulation/source_team_members_<game>.parquet`
- `data/silver/simulation/member_moveset_combos_<game>.parquet`
- `data/silver/simulation/member_move_options_<game>.parquet`
- `data/silver/simulation/pokemon_moveset_options_<game>.parquet`
- `data/silver/simulation/pokemon_combat_pool_<game>.parquet`
- `data/silver/simulation/simulation_sampling_plan_<game>.parquet`

**Diagnostics and contract**
- `data/silver/diagnostics/*`
- `data/silver/manifest.json`

## Operational notes

- Silver is intentionally strict because Gold and simulation depend on schema stability.
- If this layer changes IDs or schema, update contract tests and manifest logic together.
