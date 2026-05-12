# PM4 - Pokemon It's A Pikachu

Pokemon data pipeline with a medallion architecture (`bronze -> silver -> gold`) plus battle simulation and team ranking.

## Authors

- Hamidi Egzon
- Priyanth Vijayasures

## What this project does

The pipeline combines three external sources:
- Bulbapedia walkthrough pages (progression and locations)
- PokeAPI metadata (location and species context)
- Kaggle gym/elite/champion team data

It turns them into:
- Canonical reference tables (`silver/references`)
- Simulation inputs (`silver/simulation`)
- Ranked recommendation outputs and walkthrough payloads (`gold`)

## Architecture at a glance

| Layer | Main output path | Goal |
|---|---|---|
| `bronze` | `data/bronze/` | Ingest and persist raw source payloads with minimal transformation. |
| `silver` | `data/silver/` | Normalize, map, and enrich data into validated contracts and simulation inputs. |
| `gold` | `data/gold/` | Run simulations, aggregate metrics, and produce analytics-ready datasets. |

Detailed layer docs:
- [docs/pipeline/bronze.md](docs/pipeline/bronze.md)
- [docs/pipeline/silver.md](docs/pipeline/silver.md)
- [docs/pipeline/gold.md](docs/pipeline/gold.md)
- [docs/pipeline/elite_four_gauntlet.md](docs/pipeline/elite_four_gauntlet.md)

## Setup

1. Create and activate a Python 3.11+ virtual environment named `.venv`.

### macOS / Linux

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Windows (PowerShell)

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Verify that commands use `.venv` Python:

```bash
which python
python -V
```

## Environment setup

Set runtime environment variables once per terminal session before running the pipeline.

### macOS / Linux (bash, zsh)

```bash
export PYTHONPATH="$PWD"
export PIPELINE_USE_PYSPARK=1
```

Optional (if Spark should be disabled):

```bash
export PIPELINE_USE_PYSPARK=0
```

If Java is installed but not detected for Spark:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export PATH="$JAVA_HOME/bin:$PATH"
```

### Windows (PowerShell)

```powershell
$env:PYTHONPATH = "$PWD"
$env:PIPELINE_USE_PYSPARK = "1"
```

Optional (if Spark should be disabled):

```powershell
$env:PIPELINE_USE_PYSPARK = "0"
```

## Run the pipeline

From repo root:

```bash
# all layers in order
PYTHONPATH="$PWD" .venv/bin/python -m src.pipeline.run_pipeline all

# one or more selected layers
PYTHONPATH="$PWD" .venv/bin/python -m src.pipeline.run_pipeline layers bronze
PYTHONPATH="$PWD" .venv/bin/python -m src.pipeline.run_pipeline layers silver
PYTHONPATH="$PWD" .venv/bin/python -m src.pipeline.run_pipeline layers gold

# optional cleanup flag for silver
PYTHONPATH="$PWD" .venv/bin/python -m src.pipeline.run_pipeline layers silver --hard-cleanup
```

## Running tests

From repo root with the virtual environment active:

```bash
PYTHONPATH="$PWD" pytest
```

Run a specific layer's tests:

```bash
PYTHONPATH="$PWD" pytest tests/silver/
PYTHONPATH="$PWD" pytest tests/gold/
```

## Silver contract gate

When running `layers silver`, the runner executes:
1. `build_silver_from_bronze`
2. `src.pipeline.silver.validation.validate_silver_contract --fail-on-error`

This means Silver only succeeds if the persisted physical contract validates.

## Spark usage in this codebase

Gold team battle simulation supports two engines:
- Local Python engine
- PySpark engine

Engine selection:
- `PIPELINE_USE_PYSPARK=1` (default): attempt Spark
- `PIPELINE_USE_PYSPARK=0`: force local engine

Example:

```bash
PIPELINE_USE_PYSPARK=0 PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers gold
```

Spark UI is configured for localhost (`127.0.0.1`) and usually binds on port `4040`.

## Running Spark locally

Spark is used through `pyspark~=3.5.x` and requires Java.

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

- If Spark startup fails, use `PIPELINE_USE_PYSPARK=0` and run Gold on local engine.
- If port `4040` is in use, check `4041`, `4042`, ... or stop the previous Spark app.
- After interruption, kill stale Spark Java processes before retry.
- Restart your terminal after changing `JAVA_HOME` / `PATH`.

## Key contracts

- Gold reads inputs strictly from `data/silver/manifest.json`.
- Missing required datasets fail fast with `GoldContractError`.
- Core Monte Carlo columns in Gold simulation output:
  - `scenario_id`
  - `player_team_id`
  - `boss_team_id`
  - `mc_win_rate`

## Useful outputs

- `data/silver/manifest.json`: Silver output contract boundary
- `data/gold/simulation/monte_carlo_results.parquet`: simulation win-rates
- `data/gold/team_recommendations.parquet`: ranked team recommendations
- `data/gold/walkthrough_best_teams.json`: payload for walkthrough view
- `data/gold/manifest.json`: Gold dataset inventory

## Walkthrough page

Serve locally:

```bash
python -m http.server 8000 --bind 127.0.0.1
```

Open:
- `http://127.0.0.1:8000/docs/walkthrough_teams.html`
