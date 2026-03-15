# PM4 – Pokémon It's A Pikachu
> Gotta catch 'em all (or just the MVPs)

## Medallion Architecture

The pipeline follows a **Bronze → Silver → Gold** medallion structure:

| Layer | Folder | Content |
|-------|--------|---------|
| **Bronze** | `data/bronze/` | Raw API responses – Bulbapedia HTML walkthrough pages + PokéAPI location index. Large files; excluded from git. |
| **Silver** | `data/silver/` | Cleaned & validated data: per-game boss-progression snapshots (`*_data.jsonl`), mapped location areas (`location_to_area_map.json`), unmapped location audit log. |
| **Gold** | `data/gold/` | Analytics-ready datasets: game progression summary (CSV) and cross-game location popularity ranking (JSONL). |

## Project Structure

```
PM4_Pokemon_Its_A_Pickachu/
├── data/
│   ├── bronze/                  # raw API dumps (git-ignored)
│   │   ├── bulbapedia/          # {game_key}.json per game
│   │   └── pokeapi/             # location_index.json
│   ├── silver/                  # cleaned per-game JSONL + helper JSONs
│   └── gold/                    # aggregated analytics outputs
│
├── src/
│   └── pipeline/
│       ├── settings.py          # paths & constants
│       ├── run_pipeline.py      # CLI entrypoint
│       ├── common/
│       │   ├── http.py          # shared retry session
│       │   └── io.py            # JSON / JSONL helpers
│       ├── bronze/
│       │   └── fetch_sources.py # ingest Bulbapedia + PokéAPI → bronze
│       ├── silver/
│       │   ├── game_config.py   # game metadata (versions, bosses, route prefixes)
│       │   ├── location_mapper.py # LocationMapper class
│       │   ├── build_silver.py  # bronze → silver transformation
│       │   └── bootstrap_legacy.py  # one-time migration from pokemon_big_data_outputs/
│       └── gold/
│           └── build_gold.py    # silver → gold aggregations
│
├── pokemon_big_data_outputs/    # legacy outputs (kept for reference)
├── Scripts/
│   └── loading_location.ipynb  # original exploration notebook (reference only)
└── requirements.txt
```

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# 1. One-time migration: copy existing results into data/silver
PYTHONPATH=src python -m pipeline.run_pipeline --layer bootstrap-silver

# 2. Full run from scratch (slow – hits APIs)
PYTHONPATH=src python -m pipeline.run_pipeline --layer all

# Or run individual layers
PYTHONPATH=src python -m pipeline.run_pipeline --layer bronze
PYTHONPATH=src python -m pipeline.run_pipeline --layer silver
PYTHONPATH=src python -m pipeline.run_pipeline --layer gold
```

## Silver Schema (`*_data.jsonl`)

Each line is one boss-fight snapshot:

```json
{
  "game": "red",
  "boss_name": "Cerulean Gym[edit source]",
  "part": 5,
  "reachable_locations": ["kanto-route-1", "viridian-city", "..."],
  "location_count": 14
}
```

## Gold Outputs

| File | Description |
|------|-------------|
| `game_progression_summary.csv` | Per-game: boss steps, final & max reachable location counts |
| `location_popularity.jsonl` | Per location slug: how many games include it, total mentions |
| `manifest.json` | Provenance metadata for the gold build |
