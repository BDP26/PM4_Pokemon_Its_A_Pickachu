# PM4 - Pokemon It's A Pikachu
> Gotta catch 'em all (or just the MVPs)

## Medallion-Architektur

Die Pipeline folgt einer **Bronze -> Silver -> Gold** Struktur:

| Layer | Ordner | Inhalt |
|-------|--------|--------|
| **Bronze** | `data/bronze/` | Rohdaten aus Bulbapedia, PokeAPI und Kaggle sowie Config-Snapshots (`config/`). |
| **Silver** | `data/silver/` | Normalisierte Snapshots, Mappings, Referenzen, Diagnostik und Simulations-Inputs. |
| **Gold** | `data/gold/` | Aggregierte Analyse-Datasets, Simulationsergebnisse und Walkthrough-Web-Payload. |

## Projektstruktur (neues Setup)

```text
PM4_Pokemon_Its_A_Pickachu/
|- data/
|  |- bronze/
|  |  |- bulbapedia/
|  |  |- pokeapi/
|  |  |- kagglehub/
|  |  |- config/
|  |  \- type_chart.json
|  |- silver/
|  |  |- snapshots/
|  |  |- mappings/
|  |  |- references/
|  |  |- diagnostics/
|  |  |- simulation/
|  |  \- manifest.json
|  \- gold/
|     |- simulation/
|     \- manifest.json
|- docs/
|  |- pipeline/
|  |  |- bronze.md
|  |  |- silver.md
|  |  \- gold.md
|  \- walkthrough_teams.html
|- notebooks/
|  \- loading_location.ipynb
|- src/pipeline/
|  |- run_pipeline.py
|  |- settings.py
|  |- common/
|  |- bronze/
|  |  |- inputs/
|  |  |- enrichment/
|  |  \- orchestration/
|  |- silver/
|  |  |- inputs/
|  |  |- enrichment/
|  |  |- simulation/
|  |  |- reporting/
|  |  \- orchestration/
|  \- gold/
|     |- simulation/
|     |- reporting/
|     \- orchestration/
\- requirements.txt
```

## Layer-Dokumentation

- `docs/pipeline/bronze.md` - Ingestion, Quellvertraege, Bronze-Artefakte
- `docs/pipeline/silver.md` - Normalisierung, Enrichment, Silver-Artefakte
- `docs/pipeline/gold.md` - Aggregationen, Simulation, Gold-Artefakte

## Quickstart

```bash
pip install -r requirements.txt

# Voller Lauf (bronze -> silver -> gold)
PYTHONPATH=src python -m src.pipeline.run_pipeline all

# Einzelne Layer in gewuenschter Reihenfolge
PYTHONPATH=src python -m src.pipeline.run_pipeline layers bronze
PYTHONPATH=src python -m src.pipeline.run_pipeline layers silver
PYTHONPATH=src python -m src.pipeline.run_pipeline layers gold
PYTHONPATH=src python -m src.pipeline.run_pipeline layers bronze silver gold

# Simulation Smoke Checks fuer Gold-Ausgaben
PYTHONPATH=src python -m src.pipeline.run_pipeline validate-simulation
```

## Silver -> Gold Contract (strict)

Gold liest Inputs ausschliesslich aus `data/silver/manifest.json` und nutzt keine Dateisystem-Discovery mehr.

Erforderliche Dataset-Keys fuer Gold:

- `boss_records` (`files[]`)
- `simulation_inputs_teams` (`file`)
- `team_members` (`file`)
- `team_member_moves` (`file`)
- `pokemon_reference` (`file`)
- `snapshot_available_pokemon` (`file`)
- `encounters` (`file`)

Bei fehlenden/ungueltigen Eintraegen bricht Gold sofort mit `GoldContractError` ab (z. B. `[gold.contract] missing_dataset_file ...`).

### Canonical Simulation Schema Contract

Die Monte-Carlo-Ausgabe nutzt jetzt durchgehend den kanonischen Schluesselraum und ist mit Gold/Validierung synchron:

- `scenario_id`
- `player_team_id`
- `boss_team_id`
- `mc_win_rate`

`team_id_attacker`/`team_id_defender` bleiben auf dem Team-Battle-Artefakt, werden aber fuer Monte-Carlo intern auf die kanonischen Felder gemappt.

Typischer Repair-Flow:

```bash
PYTHONPATH=src python -m src.pipeline.run_pipeline layers silver
PYTHONPATH=src python -m src.pipeline.run_pipeline layers gold
```

Hinweise:
- Kaggle-Quelle in Bronze: `maxiboo/pokemon-gen-1-9-gym-leaders-elite-four`
- Download-Ziel: `data/bronze/kagglehub/`

## Wichtige Silver-Artefakte

- `data/silver/snapshots/*_boss_snapshots.jsonl`
- `data/silver/mappings/location_to_area_map.json`
- `data/silver/mappings/location_to_pokemon_map.json`
- `data/silver/mappings/boss_mapping_by_version.json`
- `data/silver/references/encounters.jsonl`
- `data/silver/references/pokemon_reference.json`
- `data/silver/references/encounter_methods_reference.json`
- `data/silver/simulation/teams.parquet`
- `data/silver/simulation/teams.jsonl`
- `data/silver/manifest.json`

## Silver Orchestration Stages

`build_silver_from_bronze` wurde in explizite Stages aufgeteilt (z. B. Parse-Stage in `src/pipeline/silver/orchestration/stages.py`), damit einzelne Schritte isoliert testbar und evolvierbar sind.

## Team-Generierung: Scored constrained search

Die Kandidatenbildung fuer Spielerteams nutzt jetzt einen scored constrained pool statt rein frueher Trunkierung:

- Scoring-Signale: Encounter-Chance, Capture-Rate, Level-Realismus zum Boss-Level
- Family-Dedupe vor Pool-Limit
- Diagnostik im Log: `pruned_candidates`, `family_pruned_candidates`, `pruned_combos`

## Gold-Outputs

| Datei | Beschreibung |
|------|--------------|
| `data/gold/game_progression_summary.csv` | Boss-Progression und erreichbare Orte pro Spiel |
| `data/gold/location_popularity.parquet` | Standort-Popularitaet ueber alle Spiele |
| `data/gold/team_recommendations.parquet` | Team-Ranking auf Basis Monte-Carlo Win-Rate |
| `data/gold/team_rankings_by_boss_version.parquet` | Team-Ranking je Boss innerhalb gleicher Version |
| `data/gold/best_team_by_boss.parquet` | Bestes Team je Boss-Matchup |
| `data/gold/best_team_by_boss_version.parquet` | Bestes Team je Boss und Version |
| `data/gold/best_team_by_boss_version.csv` | CSV-Export des besten Teams je Boss/Version |
| `data/gold/walkthrough_best_teams.json` | Web-Payload fuer die Walkthrough-Seite |
| `data/gold/manifest.json` | Gold-Provenienz und Output-Liste |

Simulationsergebnisse liegen in:
- `data/gold/simulation/teams.parquet`
- `data/gold/simulation/team_battle_simulations.parquet`
- `data/gold/simulation/battle_seeds.parquet`
- `data/gold/simulation/monte_carlo_results.parquet`

## Walkthrough-Webseite

Statische Seite:
- `docs/walkthrough_teams.html`

Benutzte Daten:
- `data/gold/walkthrough_best_teams.json`

Start lokal im Projekt-Root:

```bash
python3 -m http.server 8000 --bind 127.0.0.1
```

Dann im Browser oeffnen:
- `http://127.0.0.1:8000/docs/walkthrough_teams.html`
