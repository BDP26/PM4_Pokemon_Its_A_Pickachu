# PM4 Pokémon Data Pipeline - Optimized Schema

## Overview

This pipeline processes Pokémon game data through a **medallion architecture** (Bronze → Silver → Gold) with an **optimized, normalized data schema**.

## Data Structure

### Silver Layer (Optimized Format)

```
data/silver/
├── *_boss_snapshots.jsonl          # Boss metadata per game (12 files)
├── encounters.jsonl                # Aggregated location-pokemon encounters
├── pokemon_reference.json          # Centralized Pokemon URL mapping
├── encounter_methods_reference.json # Encounter methods mapping
├── location_to_area_map.json       # Location area mappings
├── location_to_pokemon_map.json    # Location Pokemon availability
├── boss_mapping_by_version.json    # Boss team information
└── manifest.json                   # Available datasets
```

### Gold Layer (Analytics)

```
data/gold/
├── game_progression_summary.csv    # Game progression stats
├── location_popularity.jsonl       # Cross-game location ranking
└── manifest.json                   # Available datasets
```

## Key Features

✅ **Normalized Schema**: Separated into boss snapshots, encounters, and references
✅ **Storage Efficient**: ~38% reduction vs denormalized format
✅ **Big Data Ready**: Flat structure compatible with Spark, DuckDB, Parquet
✅ **Scalable**: Handles 1M+ boss records
✅ **Version Filtered**: Only includes defined game versions (red, blue, gold, silver, ruby, sapphire, diamond, pearl, black, white, x, y)

## Running the Pipeline

```bash
# Run all layers (bronze → silver → gold)
python3 -m src.pipeline.run_pipeline all

# Run specific layers
python3 -m src.pipeline.run_pipeline layers silver gold
```

## Data Access Examples

### Python - Read Boss Snapshots
```python
import json

for line in open('data/silver/red_boss_snapshots.jsonl'):
    boss = json.loads(line)
    print(f"{boss['boss_name']}: {boss['reachable_pokemon_count']} pokemon")
```

### Python - Query Encounters with DuckDB
```python
import duckdb

result = duckdb.query("""
  SELECT pokemon, COUNT(*) as encounters
  FROM 'data/silver/encounters.jsonl'
  WHERE game = 'red'
  GROUP BY pokemon
  ORDER BY encounters DESC
""").to_df()
```

### Python - Access References
```python
import json

pokemon_ref = json.load(open('data/silver/pokemon_reference.json'))
methods_ref = json.load(open('data/silver/encounter_methods_reference.json'))

# Look up Pokemon URL
pidgey_url = pokemon_ref['pidgey']['url']

# Look up encounter method URL
walk_url = methods_ref['walk']
```

## Schema Details

### Boss Snapshots Format
```json
{
  "boss_id": "red:brock",
  "boss_slug": "brock",
  "boss_name": "Brock",
  "game": "red",
  "version": "red",
  "boss_order": 1,
  "heading": "Pewter City",
  "part": 3,
  "reachable_location_count": 7,
  "reachable_locations": ["kanto-route-1", ...],
  "reachable_pokemon_count": 20
}
```

### Encounters Format
```json
{
  "boss_id": "red:brock",
  "game": "red",
  "location": "kanto-route-1",
  "pokemon": "pidgey",
  "level_min": 2,
  "level_max": 5,
  "methods": ["walk"]
}
```

## Statistics

- **Total Bosses**: 160 across 12 games
- **Total Encounters**: 23,300+
- **Unique Pokemon**: ~300
- **Game Coverage**: Red, Blue, Gold, Silver, Ruby, Sapphire, Diamond, Pearl, Black, White, X, Y
- **Unmapped Locations**: 319 events logged

## Code Modules

- `src/pipeline/silver/schema_optimizer.py`: Normalization logic
- `src/pipeline/silver/build_silver.py`: Silver layer processing
- `src/pipeline/silver/location_pokemon_enrichment.py`: Pokemon encounter enrichment
- `src/pipeline/gold/build_gold.py`: Gold layer aggregations

## Notes

- Legacy denormalized format has been replaced with optimized schema
- All game versions are filtered to only include defined versions
- Backward compatibility achieved through consistent naming conventions
- Schema optimized for both analytical queries and big data processing


