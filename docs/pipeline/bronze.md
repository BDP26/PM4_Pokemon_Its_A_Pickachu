# Bronze Layer

## Purpose

Bronze collects raw source data and stores it reproducibly. It does not apply business logic beyond minimal normalization needed for storage.

## Code entrypoint

- Runner: `fetch_bronze_sources`
- File: `src/pipeline/bronze/orchestration/fetch_sources.py`
- CLI: `PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers bronze`

## Inputs

- Bulbapedia MediaWiki API pages (walkthrough HTML payloads)
- PokeAPI location index
- Kaggle dataset: `maxiboo/pokemon-gen-1-9-gym-leaders-elite-four`

## Main steps

1. Create medallion directories if missing.
2. Fetch PokeAPI location index.
3. For each configured game, fetch walkthrough pages and persist raw payloads.
4. Download and persist Kaggle team data.
5. Snapshot effective config and write manifests.

## Outputs

- `data/bronze/bulbapedia/*.json`
- `data/bronze/pokeapi/location_index.json`
- `data/bronze/pokeapi/location_pokemon_snapshot.json`
- `data/bronze/kagglehub/gym_leaders_elite_four.csv`
- `data/bronze/kagglehub/manifest.json`
- `data/bronze/config/games_config.json`
- `data/bronze/config/manifest.json`

## Caching and state tracking

Bronze tracks source state in `data/bronze/source_state.json`. Each source entry stores:
- A SHA-256 signature of the fetched payload
- A fingerprint of the fetching code itself

On subsequent runs, Bronze skips re-fetching a source if both the payload signature and code fingerprint are unchanged. Modifying the fetching logic (e.g. changing a parser) invalidates the fingerprint and triggers a full re-fetch for that source.

## Operational notes

- Bronze intentionally keeps noisy/raw data for traceability.
- Bronze fails fast when `location_pokemon_snapshot.json` cannot be built with non-empty `location_pokemon_map`.
- Missing walkthrough roots are skipped, not fatal to full Bronze execution.
- Kaggle export (`data/bronze/kagglehub/gym_leaders_elite_four.csv`) is mandatory for Silver and must be present after Bronze.
- Gold does not read Bronze directly; data must flow through Silver contracts and `data/silver/manifest.json`.
