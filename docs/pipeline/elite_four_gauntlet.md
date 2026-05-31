# Elite Four + Champion Gauntlet Simulation

## Overview

The system computes the probability that a player team completes the full Elite Four + Champion sequence in a single run.

## How it works

### Sequence

For each game version (e.g. `red`, `blue`, `black`) there is a fixed battle order:

1. **Elite Four Trainer 1**
2. **Elite Four Trainer 2**
3. **Elite Four Trainer 3**
4. **Elite Four Trainer 4**
5. **Champion** (may vary by starter choice)

### Win probability

The gauntlet win probability is the **product** of the individual per-battle win rates:

```
P(gauntlet) = P(vs E4_1) × P(vs E4_2) × P(vs E4_3) × P(vs E4_4) × P(vs Champion)
```

Example:
```
P(vs E4_1) = 0.8
P(vs E4_2) = 0.7
P(vs E4_3) = 0.6
P(vs E4_4) = 0.5
P(vs Champion) = 0.4

P(gauntlet) = 0.8 × 0.7 × 0.6 × 0.5 × 0.4 = 0.0672 → 6.72% completion chance
```

### Starter dependency

Champions can have different teams depending on the player's starter (e.g. Champion Blue in `blue` version). The system resolves the correct Champion opponent using the starter of the player team being evaluated.

### Missing data

If no simulation data exists for a specific matchup, a neutral 50% win rate is assumed as a conservative fallback.

## Outputs

Gauntlet results are persisted through the standard Gold rankings (there is no separate `elite_four_gauntlet_results.parquet`).

Relevant Gold outputs:

- `data/gold/team_rankings_e4_champion_sequence_by_version_starter.parquet`
- `data/gold/best_team_by_e4_champion_sequence_version_starter.parquet`
- `data/gold/simulation/monte_carlo_results.parquet`

Key columns in the sequence ranking tables:

| Column | Description |
|--------|-------------|
| `effective_game_version` | Game version (e.g. `red`) |
| `starter_base` | Starter Pokémon (e.g. `bulbasaur`) |
| `player_team_id` | Player team identifier |
| `sequence_completion_prob` | Combined probability of clearing the full sequence |
| `sequence_expected_wins` | Expected number of wins across the sequence |
| `strict_clear_rate` | Binary clear-rate indicator |
| `rank_in_sequence` | Rank within `(version, starter)` group |

## Usage

Gauntlet simulation runs as part of the normal Gold layer build:

```bash
PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers gold
```

To run without Spark:

```bash
PIPELINE_USE_PYSPARK=0 PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers gold
```

## Data source and layer boundary

- Gauntlet evaluation is based on Silver/Gold simulation artifacts, not filesystem discovery.
- Gold enforces a strict manifest contract via `data/silver/manifest.json`.

## Team tier interpretation

| Tier | Gauntlet win chance | Description |
|------|---------------------|-------------|
| **Strong** | > 30% | Consistently clears the full sequence |
| **Average** | 10–30% | Solid chance distribution across bosses |
| **Weak** | < 10% | Specialised against individual opponents |
| **Non-viable** | 0% | No valid route through the sequence |

