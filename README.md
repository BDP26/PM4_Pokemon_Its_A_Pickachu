# PM4 – Pokémon It's A Pikachu
> Gotta catch 'em all (or just the MVPs)

## Medallion Architecture

The pipeline follows a **Bronze → Silver → Gold** medallion structure:

| Layer | Folder | Content |
|-------|--------|---------|
| **Bronze** | `data/bronze/` | Raw API responses (Bulbapedia + PokéAPI) plus optional KaggleHub snapshot for gym leaders / elite four. Large files; excluded from git. |
| **Silver** | `data/silver/` | Cleaned & validated data: per-game boss-progression snapshots (`*_data.jsonl`), harmonized boss-bridge for Kaggle joins (`boss_mapping_by_version.json`), mapped location areas (`location_to_area_map.json`), available Pokemon per location (`location_to_pokemon_map.json`), unmapped location audit log. |
| **Gold** | `data/gold/` | Analytics-ready datasets: game progression summary (CSV) and cross-game location popularity ranking (JSONL). |

## Project Structure

```
PM4_Pokemon_Its_A_Pickachu/
├── data/
│   ├── bronze/                  # raw API dumps (git-ignored)
│   │   ├── bulbapedia/          # {game_key}.json per game
│   │   ├── pokeapi/             # location_index.json
│   │   └── kagglehub/           # optional Kaggle raw files + manifest + CSV export
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
│       └── gold/
│           └── build_gold.py    # silver → gold aggregations
│
├── Scripts/
│   └── loading_location.ipynb  # original exploration notebook (reference only)
└── requirements.txt
```

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Full run from scratch (slow – hits APIs)
PYTHONPATH=src python -m pipeline.run_pipeline --layer all

# Or run individual layers
PYTHONPATH=src python -m pipeline.run_pipeline --layer bronze
PYTHONPATH=src python -m pipeline.run_pipeline --layer bronze --with-kaggle
PYTHONPATH=src python -m pipeline.run_pipeline --layer silver
PYTHONPATH=src python -m pipeline.run_pipeline --layer gold
```

Kaggle-Quelle in Bronze ist fest konfiguriert auf `maxiboo/pokemon-gen-1-9-gym-leaders-elite-four`.
Mit `--with-kaggle` werden die Dateien unter `data/bronze/kagglehub/` gespeichert.

## Silver Schema (`*_data.jsonl`)

Each line is one boss-fight snapshot:

```json
{
  "game": "red",
  "version": "red",
  "version_name": "Red",
  "boss_id": "red:misty",
  "boss_order": 2,
  "boss_name": "Misty",
  "boss_name_canonical": "Misty",
  "boss_name_source": "Cerulean City",
  "dataset_game": "Red",
  "dataset_boss_candidates": ["misty"],
  "part": 5,
  "reachable_locations": ["kanto-route-1", "viridian-city", "..."],
  "location_count": 14,
  "reachable_location_pokemon": {
    "viridian-city": ["pidgey", "rattata", "..."],
    "kanto-route-1": ["oddish", "pidgey", "..."],
    "...": []
  },
  "reachable_location_encounters": {
    "viridian-city": [
      {
        "species": "pidgey",
        "pokemon_url": "https://pokeapi.co/api/v2/pokemon/16/",
        "level_min": 2,
        "level_max": 5,
        "encounter_methods": ["walk"],
        "encounter_method_urls": ["https://pokeapi.co/api/v2/encounter-method/1/"]
      }
    ]
  },
  "reachable_pokemon_count": 37
}
```

`reachable_location_pokemon` is version-aware where possible (e.g. `red` vs `blue`); if PokeAPI only has grouped encounter versions, a grouped/all fallback is used.
`reachable_location_encounters` provides the team-building details per location (API link, level range, and catch/encounter method).

### Boss Mapping Bridge (`data/silver/boss_mapping_by_version.json`)

Diese Datei ist die stabile Harmonisierung zwischen Walkthrough-Bossen und Kaggle-Teams:

- enthält pro Version (`version`) die komplette Boss-Reihenfolge (`boss_order`)
- behält kanonische Inhalte unverändert (`boss_id`, `boss_name_canonical`, `boss_slug`)
- ergänzt join-fähige Kaggle-Felder (`dataset_game`, `dataset_boss_candidates`)
- ist für spätere Team-Joins gedacht, ohne die eigentlichen Game-Bossdaten zu verändern

Minimaler Join-Ansatz:

1. Match `dataset_game` auf Kaggle-Spalte `Game`
2. Match `dataset_boss_candidates` auf Kaggle-Spalte `Gym leader`
3. Verwende `boss_id` als stabilen Schlüssel im weiteren Pipeline-Flow

## Gold Outputs

| File | Description |
|------|-------------|
| `game_progression_summary.csv` | Per-game: boss steps, final & max reachable location counts |
| `location_popularity.jsonl` | Per location slug: how many games include it, total mentions |
| `manifest.json` | Provenance metadata for the gold build |
