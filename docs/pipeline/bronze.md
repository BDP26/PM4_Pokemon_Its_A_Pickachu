# Bronze Layer

## Purpose

Bronze collects raw source data and stores it reproducibly. It does not apply business logic beyond minimal normalization needed for storage.

## Code entrypoint

- Runner: `fetch_bronze_sources`
- File: `src/pipeline/bronze/orchestration/fetch_sources.py`
- CLI: `PYTHONPATH=src python -m src.pipeline.run_pipeline layers bronze`

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
- `data/bronze/kagglehub/gym_leaders_elite_four.csv`
- `data/bronze/kagglehub/manifest.json`
- `data/bronze/config/games_config.json`
- `data/bronze/config/manifest.json`

## Operational notes

- Bronze intentionally keeps noisy/raw data for traceability.
- Missing walkthrough roots are skipped, not fatal to full Bronze execution.
- Kaggle export (`data/bronze/kagglehub/gym_leaders_elite_four.csv`) is mandatory for Silver and must be present after Bronze.
- Gold does not read Bronze directly; data must flow through Silver contracts.
