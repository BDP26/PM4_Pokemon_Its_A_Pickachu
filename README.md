# PM4 – Pokémon It's A Pikachu
> Gotta catch 'em all (or just the MVPs)

## Medallion Architecture

The pipeline follows a **Bronze → Silver → Gold** medallion structure:

| Layer | Folder | Content |
|-------|--------|---------|
| **Bronze** | `data/bronze/` | Raw API responses (Bulbapedia + PokéAPI) plus optional KaggleHub snapshot for gym leaders / elite four. Large files; excluded from git. |
| **Silver** | `data/silver/` | Cleaned & validated data in structured folders: snapshots (`snapshots/*.jsonl`), mappings (`mappings/*.json`), references (`references/*`), diagnostics (`diagnostics/*`), optional simulation (`simulation/*.jsonl`), plus `manifest.json`. |
| **Gold** | `data/gold/` | Analytics-ready datasets: game progression summary (CSV) and cross-game location popularity ranking (JSONL). |

## Project Structure

```
PM4_Pokemon_Its_A_Pickachu/
├── data/
│   ├── bronze/                  # raw API dumps (git-ignored)
│   │   ├── bulbapedia/          # {game_key}.json per game
│   │   ├── pokeapi/             # location_index.json
│   │   └── kagglehub/           # optional Kaggle raw files + manifest + CSV export
│   ├── silver/
│   │   ├── snapshots/           # per-game boss snapshots
│   │   ├── mappings/            # location and boss mapping artifacts
│   │   ├── references/          # normalized lookup/reference files
│   │   ├── diagnostics/         # unmapped-location audit outputs
│   │   └── simulation/          # optional battle simulation artifacts
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

## Layer Documentation

- `docs/pipeline/bronze.md` - raw ingestion layer details and source contracts
- `docs/pipeline/silver.md` - transformation layer, enrichment steps, and silver artifacts
- `docs/pipeline/gold.md` - analytics layer outputs and aggregation logic

## Quickstart

```bash
# Install dependencies
pip install -r requirements.txt

# Full run from scratch (slow – hits APIs)
PYTHONPATH=src python -m src.pipeline.run_pipeline all

# Or run individual layers
PYTHONPATH=src python -m src.pipeline.run_pipeline layers bronze
PYTHONPATH=src python -m src.pipeline.run_pipeline layers silver
PYTHONPATH=src python -m src.pipeline.run_pipeline layers gold
PYTHONPATH=src python -m src.pipeline.run_pipeline layers bronze silver gold
```

Kaggle-Quelle in Bronze ist fest konfiguriert auf `maxiboo/pokemon-gen-1-9-gym-leaders-elite-four`.
Der Kaggle-Download wird im aktuellen Code standardmaessig in Bronze ausgefuehrt und unter `data/bronze/kagglehub/` gespeichert.

## Simulation Smoke Check

After running the Silver layer, validate simulation outputs with:

```bash
PYTHONPATH=src python -m src.pipeline.run_pipeline validate-simulation
```

The check validates file presence, required fields, score ranges, and cross-file references for:

- `data/silver/simulation/teams.jsonl`
- `data/silver/simulation/type_matchups.jsonl`
- `data/silver/simulation/battle_seeds.jsonl`

Die Matchup-Bewertung ist schadensbasiert: Fuer jedes Pokemon werden nur bis zum Kampf natuerlich erlernte Moves (Level-up), ohne TM/HM, verwendet. Die erwartete Schadensformel folgt der vereinfachten Pokemon-Gleichung mit Level, Angriff/Spezial-Angriff, Verteidigung/Spezial-Verteidigung, Basisstaerke, STAB, Typen-Effekt und Zufalls-/Krit-Modifikator.

## Silver Schema (`*_boss_snapshots.jsonl`)

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

### Boss Mapping Bridge (`data/silver/mappings/boss_mapping_by_version.json`)

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
| `team_recommendations.jsonl` | (Optional) Ranked player teams by Monte-Carlo win rate across scenarios |
| `best_team_by_boss.jsonl` | (Optional) Best-performing team suggestion per boss matchup |
| `team_rankings_by_boss_version.jsonl` | (Optional) Ranked teams per boss with same-version constraint |
| `best_team_by_boss_version.jsonl` | (Optional) Best team per boss and game version |
| `best_team_by_boss_version.csv` | (Optional) CSV export of best team per boss and game version |
| `walkthrough_best_teams.json` | (Optional) Web payload for walkthrough team recommendations |
| `manifest.json` | Provenance metadata for the gold build |

## Walkthrough Web Overview

A small static page is available at:

- `docs/walkthrough_teams.html`

It renders the best team per boss for a selected version and lets you also pick a starter for the whole walkthrough. The starter choice stays fixed within that version.

It uses:

- `data/gold/walkthrough_best_teams.json`

Tip: Serve the repository root with a local web server so `fetch(...)` can load JSON files.

