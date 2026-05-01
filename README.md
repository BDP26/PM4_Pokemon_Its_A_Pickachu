# PM4 - Pokemon It's A Pikachu

Pokemon data pipeline with a medallion architecture (`bronze -> silver -> gold`) plus battle simulation and team ranking.

## What this project does

The pipeline combines three external sources:
- Bulbapedia walkthrough pages (progression and locations)
- PokeAPI metadata (location and species context)
- Kaggle gym/elite/champion team data

It turns them into:
- Clean reference tables (`silver`)
- Simulation inputs (`silver/simulation`)
- Ranked team outputs and walkthrough payloads (`gold`)

## Architecture at a glance

| Layer | Main output path | Goal |
|---|---|---|
| `bronze` | `data/bronze/` | Ingest and persist raw source payloads with minimal transformation. |
| `silver` | `data/silver/` | Normalize, map, and enrich data into validated contracts and simulation inputs. |
| `gold` | `data/gold/` | Run simulations, aggregate metrics, and produce analytics-ready datasets. |

Detailed layer docs:
- [docs/pipeline/bronze.md](/Users/priyanthvijayasures/Documents/000_Schule/Bachelor Data Science/6. Semester/PM4/PM4_Pokemon_Its_A_Pickachu/docs/pipeline/bronze.md)
- [docs/pipeline/silver.md](/Users/priyanthvijayasures/Documents/000_Schule/Bachelor Data Science/6. Semester/PM4/PM4_Pokemon_Its_A_Pickachu/docs/pipeline/silver.md)
- [docs/pipeline/gold.md](/Users/priyanthvijayasures/Documents/000_Schule/Bachelor Data Science/6. Semester/PM4/PM4_Pokemon_Its_A_Pickachu/docs/pipeline/gold.md)

## Setup

1. Create and activate a Python 3.11+ virtual environment.
2. Install dependencies:

```bash
pip install -r requirements.txt
```

## Run the pipeline

From repo root:

```bash
# all layers in order
PYTHONPATH=src python -m src.pipeline.run_pipeline all

# selected layers
PYTHONPATH=src python -m src.pipeline.run_pipeline layers bronze
PYTHONPATH=src python -m src.pipeline.run_pipeline layers silver
PYTHONPATH=src python -m src.pipeline.run_pipeline layers gold

# simulation smoke checks for gold outputs
PYTHONPATH=src python -m src.pipeline.run_pipeline validate-simulation
```

## Spark usage in this codebase

The team battle simulation has two engines:
- Local Python engine
- PySpark engine

Engine selection:
- `PIPELINE_USE_PYSPARK=1` (default): try Spark
- `PIPELINE_USE_PYSPARK=0`: force local engine

Example:

```bash
PIPELINE_USE_PYSPARK=0 PYTHONPATH=src python -m src.pipeline.run_pipeline layers gold
```

## Running Spark locally

Spark is used through `pyspark~=3.5.3` and requires Java.

### macOS

1. Install Java (LTS recommended, e.g. Temurin 17).
2. Set `JAVA_HOME`:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

3. Verify:

```bash
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

### Windows

1. Install Java (LTS recommended, e.g. Temurin 17).
2. Set environment variables (System or User):
- `JAVA_HOME=C:\Program Files\Eclipse Adoptium\jdk-17...`
- Add `%JAVA_HOME%\bin` to `Path`

3. Open a new terminal and verify:

```powershell
java -version
python -c "import pyspark; print(pyspark.__version__)"
```

### Spark troubleshooting

- If Spark startup fails, set `PIPELINE_USE_PYSPARK=0` to run with the local engine.
- If port `4040` is already used, stop the existing Spark app or rerun after it exits.
- Always restart your terminal after changing `JAVA_HOME`/`Path`.

## Key contracts

- Gold reads inputs strictly from `data/silver/manifest.json`.
- Missing required datasets cause fail-fast `GoldContractError`.
- Canonical Monte Carlo columns are:
  - `scenario_id`
  - `player_team_id`
  - `boss_team_id`
  - `mc_win_rate`

## Useful outputs

- `data/silver/manifest.json`: Silver output contract
- `data/gold/simulation/monte_carlo_results.parquet`: simulation win-rates
- `data/gold/team_recommendations.parquet`: ranked team recommendations
- `data/gold/walkthrough_best_teams.json`: payload for walkthrough view

## Walkthrough page

Serve locally:

```bash
python -m http.server 8000 --bind 127.0.0.1
```

Open:
- `http://127.0.0.1:8000/docs/walkthrough_teams.html`
