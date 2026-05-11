# Elite Four + Champion Gauntlet Simulation

## Übersicht

Das System berechnet die Wahrscheinlichkeit, dass ein Player-Team die vollständige Elite Four + Champion Sequenz besteht.

## Funktionsweise

### Sequenz
Für jedes Spiel (z.B. "red", "blue", "black") gibt es eine feste Reihenfolge:
1. **Elite Four Trainer 1**
2. **Elite Four Trainer 2**
3. **Elite Four Trainer 3**
4. **Elite Four Trainer 4**
5. **Champion** (optional: kann je nach Starter unterschiedlich sein)

### Win-Berechnung

Die Gewinnwahrscheinlichkeit für die komplette Sequenz ist das **Produkt** aller einzelnen Kampf-Wahrscheinlichkeiten:

```
P(gauntlet) = P(vs E4_1) × P(vs E4_2) × P(vs E4_3) × P(vs E4_4) × P(vs Champion)
```

Beispiel:
```
P(vs E4_1) = 0.8
P(vs E4_2) = 0.7
P(vs E4_3) = 0.6
P(vs E4_4) = 0.5
P(vs Champion) = 0.4

P(gauntlet) = 0.8 × 0.7 × 0.6 × 0.5 × 0.4 = 0.0672 = 6.72% Gewinnchance
```

### Starter-Abhängigkeit

Für Champions kann es unterschiedliche Teams je nach Starter geben (z.B. in "blue" Version gibt es Champion Blue mit verschiedenen Starters). 
Das System kalkuliert dies ein und nutzt den Starter des Player-Teams zur Bestimmung des Champion-Gegners.

### Fehlende Daten

Falls keine Simulationsdaten für eine bestimmte Matchup vorhanden sind, wird eine neutrale 50% Wahrscheinlichkeit angenommen (conservativ).

## Output-Format

Die Gauntlet-Auswertung wird aktuell ueber Gold-Rankings persistiert (kein separates
`elite_four_gauntlet_results.parquet` als primaerer Output).

Wichtige Gold-Outputs:

- `data/gold/team_rankings_e4_champion_sequence_by_version_starter.parquet`
- `data/gold/best_team_by_e4_champion_sequence_version_starter.parquet`
- `data/gold/simulation/monte_carlo_results.parquet`

Typische Felder in den Sequence-Rankings:

| Spalte | Beschreibung |
|--------|------------|
| `effective_game_version` | Spiel-Version (z.B. `red`) |
| `starter_base` | Starter-Pokemon (z.B. `bulbasaur`) |
| `player_team_id` | Player-Team ID |
| `sequence_completion_prob` | Geschlossene Wahrscheinlichkeit fuer komplette Sequenz |
| `sequence_expected_wins` | Erwartete Siege ueber die Sequenz |
| `strict_clear_rate` | Strikter Clear-Rate-Indikator |
| `rank_in_sequence` | Rang innerhalb `(version, starter)` |

## Verwendung

Die Berechnung erfolgt im Gold-Layer waehrend des normalen Pipeline-Runs:

```bash
PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers gold
```

Optional lokal ohne Spark:

```bash
PIPELINE_USE_PYSPARK=0 PYTHONPATH="$PWD" python -m src.pipeline.run_pipeline layers gold
```

## Datenquelle und Layer-Grenze

- Die Gauntlet-Bewertung basiert auf Silver/Gold-Simulationsartefakten, nicht auf Dateisystem-Discovery.
- Fuer Gold gilt ein strikter Manifest-Contract ueber `data/silver/manifest.json`.

## Strategische Auswertung

Basierend auf den Gauntlet-Ergebnissen können Teams nach ihrer Tauglichkeit bewertet werden:

- **Stark** (> 30% Gauntlet-Gewinnchance): Teams, die die vollständige Sequenz konsistent bestehen
- **Mittel** (10-30% Gauntlet-Gewinnchance): Teams mit guter Chancenverteilung
- **Schwach** (< 10% Gauntlet-Gewinnchance): Spezialisierte Teams gegen einzelne Gegner
- **Nicht-spielbar** (0% Gauntlet-Gewinnchance): Keine valide Route durch die Sequenz

