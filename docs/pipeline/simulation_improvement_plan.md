# Simulation Improvement Plan

Use this file as the execution tracker for simulation quality and coverage work.

## Status Legend

- `[ ]` not started
- `[~]` in progress
- `[x]` done
- `blocked` waiting on decision/dependency

## Goals

1. Ensure simulation artifacts are coherent and reproducible.
2. Distinguish real simulated outcomes from policy placeholders.
3. Provide mode-specific coverage metrics (`gym/boss`, `double`, `gauntlet`).
4. Improve low/zero win-coverage with controlled policy tuning.

## Step Tracker

### 1) Add run identity + consistency checks
- Status: `[x]`
- Why: mixed-run artifacts create false validation and false zero-win signals.
- Files:
  - `src/pipeline/gold/simulation/run_gold_simulation.py`
  - `src/pipeline/silver/simulation/validate_simulation.py`
- Dependencies:
  - Stable run metadata format (run_id, created_at, input_fingerprint)
- Acceptance:
  - Validator fails on mixed-run artifacts and passes on coherent artifacts.

### 2) Make gold simulation writes atomic
- Status: `[x]`
- Why: interrupted runs must not leave partial artifact sets.
- Files:
  - `src/pipeline/gold/simulation/run_gold_simulation.py`
- Dependencies:
  - Temporary output directory + final swap strategy
- Acceptance:
  - Interrupted run cannot leave half-old/half-new simulation outputs.

### 3) Add mode-aware coverage diagnostics
- Status: `[x]`
- Why: aggregated coverage hides root causes across different simulation modes.
- Files:
  - `src/pipeline/gold/orchestration/build_gold.py`
  - `data/gold/debug/*` (new diagnostics parquet)
- Dependencies:
  - Reliable `simulation_mode` field and warning taxonomy
- Acceptance:
  - Coverage is reported separately for `gym/boss`, `double`, `gauntlet`.

### 4) Classify zero outcomes by cause
- Status: `[x]`
- Why: policy-filtered and placeholder rows should not be treated as true duel losses.
- Files:
  - `src/pipeline/gold/simulation/team_battle_simulations.py`
  - `src/pipeline/gold/simulation/monte_carlo_optimizer.py`
- Dependencies:
  - Shared enum for cause tags
- Acceptance:
  - Each zero-win row has a normalized cause (`simulated_loss`, `level_filter`, `version_filter`, `gauntlet_placeholder`).

### 5) Tighten validator mode semantics
- Status: `[x]`
- Why: intentional placeholder rows must not fail validation.
- Files:
  - `src/pipeline/silver/simulation/validate_simulation.py`
- Dependencies:
  - Warning/cause tags from simulation output
- Acceptance:
  - `validate_simulation_gold` passes for valid filtered placeholders.

### 6) Add pre-simulation input integrity gate
- Status: `[x]`
- Why: simulation realism is meaningless if inputs are malformed.
- Files:
  - `src/pipeline/gold/inputs/team_tables.py`
  - `src/pipeline/gold/simulation/run_gold_simulation.py`
- Dependencies:
  - Strict checks for required columns, team IDs, invalid move placeholders
- Acceptance:
  - Gold run fails early with actionable error on bad inputs.

### 7) Add adaptive Monte Carlo for borderline scenarios
- Status: `[x]`
- Why: low trial counts can misclassify near-threshold win rates as zero.
- Files:
  - `src/pipeline/gold/simulation/monte_carlo_optimizer.py`
- Dependencies:
  - Threshold policy for reruns (e.g., 0%–2% bucket)
- Acceptance:
  - Borderline scenarios can be re-estimated with higher confidence.

### 8) Expose policy profiles (`strict`, `coverage`)
- Status: `[x]`
- Why: tuning should be coherent, not ad hoc env-variable combinations.
- Files:
  - `src/pipeline/common/simulation_config.py`
  - `src/pipeline/silver/config/team_config.py`
- Dependencies:
  - Agreed defaults for both profiles
- Acceptance:
  - Single profile switch controls related policy knobs consistently.

### 9) Add deterministic scenario replay utility
- Status: `[ ]`
- Why: zero-win investigations need reproducible battle traces.
- Files:
  - `scripts/replay_sim_scenario.py` (new)
  - `src/pipeline/gold/simulation/team_battle_simulations.py` (as needed)
- Dependencies:
  - Stable scenario IDs and seed persistence
- Acceptance:
  - Any scenario can be replayed exactly from artifact IDs.

### 10) Add regression tests for new contracts
- Status: `[x]`
- Why: prevents drift back to mixed artifacts and ambiguous metrics.
- Files:
  - `tests/gold/test_simulation_integration.py`
  - `tests/silver/test_simulation_contracts.py`
- Dependencies:
  - Steps 1–4 implemented
- Acceptance:
  - CI catches contract and diagnostics regressions.

## Dependency Map (High-Level)

1. Step 1 -> Step 2 -> Step 3
2. Step 3 -> Step 4 -> Step 10
3. Step 6 should be done before major tuning (Steps 7–8)
4. Step 9 can start after Step 1 (run identity + deterministic seeds)

## Current Snapshot

- `validate_simulation_gold`: passing after placeholder-aware validator adjustment.
- Remaining work focus:
  - artifact consistency guarantees
  - mode-separated coverage reporting
  - root-cause-aware zero-win policy tuning
