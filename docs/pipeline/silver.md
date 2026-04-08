# Silver Layer

The Silver layer transforms raw Bronze payloads into cleaned, mapped, and enriched datasets.

## Purpose

- Parse walkthrough progression into structured boss snapshots.
- Map walkthrough locations to PokeAPI slugs and reachable Pokemon.
- Harmonize bosses with Kaggle-compatible naming for team joins.
- Produce normalized Silver artifacts and references for analytics and simulation.

## Entrypoint

- Function: `build_silver_from_bronze`
- File: `src/pipeline/silver/orchestration/build_silver.py`
- Orchestration: `src/pipeline/run_pipeline.py` (`all` or `layers silver`)

## Required Inputs

- `data/bronze/pokeapi/location_index.json`
- `data/bronze/bulbapedia/*.json`
- Optional enrichment input: `data/bronze/kagglehub/gym_leaders_elite_four.csv`

If required Bronze files are missing, the layer raises `FileNotFoundError`.

## Core Processing

1. Validate Bronze input presence and load game configs.
2. Parse each game walkthrough into boss progression records (`extract_game_data`).
3. Enrich boss records using Kaggle harmonization (`enrich_boss_records`).
4. Resolve location area and Pokemon availability maps from PokeAPI.
5. Enrich records with location Pokemon/encounter details.
6. Write per-game normalized snapshot files.
7. Build reference artifacts:
   - Pokemon reference index
   - Encounter methods reference
   - Boss mapping by version
   - Unmapped location diagnostics (detailed, summary, compact)
8. Extract simulation inputs (`teams.parquet`, `teams.jsonl`) from available boss/team data.
   - Also materialize combinatorial 4-move sets per pokemon per team.
9. Generate Silver manifest.

## Outputs

Primary outputs in `data/silver/`:

- `snapshots/{game_key}_boss_snapshots.jsonl`
- `mappings/location_to_area_map.json`
- `mappings/location_to_pokemon_map.json`
- `mappings/boss_mapping_by_version.json`
- `references/pokemon_reference.json`
- `references/encounter_methods_reference.json`
- `references/encounters.jsonl`
- `diagnostics/unmapped_locations_detailed.json`
- `diagnostics/unmapped_locations_summary.json`
- `diagnostics/unmapped_locations.json`
- `manifest.json`

Optional simulation inputs (generated when boss team data is available):

- `simulation/teams.parquet` (primary)
- `simulation/teams.jsonl` (line-delimited view)
- `simulation/member_movesets.parquet` (all combinatorial 4-move sets per pokemon per team)

Gold consumes the Silver simulation inputs and writes the battle matrix, seeds, and Monte-Carlo outputs into `data/gold/simulation/`.

Validate Gold simulation artifacts after a full run:

- `PYTHONPATH=src python -m src.pipeline.run_pipeline validate-simulation`

## Notes and Constraints

- Location mapping quality is measurable via unmapped diagnostics.
- Kaggle enrichment improves joinability but does not overwrite canonical boss identity keys.
- Silver keeps a balance between normalized references and per-game snapshots.



