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

`elite_four_gauntlet_results.parquet` enthält:

| Spalte | Beschreibung |
|--------|------------|
| `gauntlet_id` | Eindeutige ID (player_team_id + gauntlet + game + starter) |
| `player_team_id` | Player-Team ID |
| `game_version` | Spiel-Version (z.B. "red") |
| `starter_base` | Starter-Pokémon (z.B. "bulbasaur") |
| `elite_four_count` | Anzahl Elite Four Trainer (normalerweise 4) |
| `elite_four_team_ids` | Komma-getrennte IDs der E4 Teams |
| `champion_team_id` | ID des Champion-Teams |
| `elite_four_win_prob_1` bis `elite_four_win_prob_4` | Einzelne Gewinnchancen pro E4 Trainer |
| `champion_win_prob` | Gewinnchance gegen Champion |
| `cumulative_gauntlet_win_probability` | **Finale Gewinnchance für komplette Sequenz** |
| `is_viable` | True wenn mindestens eine theoretische Gewinnchance > 0 |

## Verwendung

```python
from src.pipeline.silver.simulation.elite_four_gauntlet import build_elite_four_gauntlet_results

# Berechne Gauntlet-Wahrscheinlichkeiten
rows_written = build_elite_four_gauntlet_results()
print(f"Calculated gauntlet scenarios for {rows_written} teams")
```

## Strategische Auswertung

Basierend auf den Gauntlet-Ergebnissen können Teams nach ihrer Tauglichkeit bewertet werden:

- **Stark** (> 30% Gauntlet-Gewinnchance): Teams, die die vollständige Sequenz konsistent bestehen
- **Mittel** (10-30% Gauntlet-Gewinnchance): Teams mit guter Chancenverteilung
- **Schwach** (< 10% Gauntlet-Gewinnchance): Spezialisierte Teams gegen einzelne Gegner
- **Nicht-spielbar** (0% Gauntlet-Gewinnchance): Keine valide Route durch die Sequenz

