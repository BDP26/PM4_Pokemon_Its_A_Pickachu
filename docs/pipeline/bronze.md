# Bronze Layer

The Bronze layer ingests raw external data with minimal transformation.

## Purpose

- Preserve upstream payloads from Bulbapedia and PokeAPI.
- Store a reproducible Kaggle snapshot for boss/team enrichment.
- Snapshot the effective pipeline config and manual override folder.
- Provide stable raw inputs for Silver without business-level filtering.

## Entrypoint

- Function: `fetch_bronze_sources`
- File: `src/pipeline/bronze/orchestration/fetch_sources.py`
- Orchestration: `src/pipeline/run_pipeline.py` (`all` or `layers bronze`)

## Input Sources

- Bulbapedia MediaWiki API (`BULBA_API`) for walkthrough pages and parts.
- PokeAPI (`POKEAPI`) for location index.
- Kaggle dataset `maxiboo/pokemon-gen-1-9-gym-leaders-elite-four`.

## Processing Steps

1. Ensure medallion directories exist.
2. Download and store the PokeAPI location index as JSON.
3. For each configured game (`get_games_config`):
   - Resolve an existing walkthrough root title.
   - Discover walkthrough part pages.
   - Fetch each part HTML payload.
   - Write one raw game JSON file.
4. Download Kaggle dataset files, copy raw files, and export a normalized CSV.
5. Snapshot the effective Bronze config and create a config manifest.
6. Write a Kaggle manifest with provenance metadata.

## Outputs

- `data/bronze/pokeapi/location_index.json`
- `data/bronze/bulbapedia/{game_key}.json`
- `data/bronze/kagglehub/raw/*`
- `data/bronze/kagglehub/gym_leaders_elite_four.csv`
- `data/bronze/kagglehub/manifest.json`
- `data/bronze/config/games_config.json`
- `data/bronze/config/manifest.json`
- `data/bronze/config/overrides/*` (optional, manual overrides)

## Notes and Constraints

- Bronze data is intentionally raw and can be large.
- Walkthrough page existence checks are cached in-memory per run.
- Missing game walkthrough roots are skipped instead of failing the full run.

