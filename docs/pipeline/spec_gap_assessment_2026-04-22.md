# Code-Bewertung gegen Soll-Architektur (Silver/Gold)

Datum: 2026-04-22

## Kurzfazit

- **Stark umgesetzt:** per-game Team-Sharding in Silver, normalisierte Teamtabellen (Meta/Members/Moves), mehrstufige Teamkandidaten-Hierarchie, rundenbasierte Battle-Logik inkl. Accuracy/Crit/Randomness.
- **Hauptlücken:** inkonsistente Dateiverträge zwischen Silver↔Manifest↔Gold, Battle-Seed-Builder erwartet alte unsharded Datei, Simulation nutzt weiterhin API-Fallbacks zur Laufzeit statt rein vorbereiteten Pools.

## Bewertungsraster (0–5)

| Bereich | Score | Begründung |
|---|---:|---|
| Parquet-first | 4 | Kernoutputs liegen als Parquet vor; zusätzlich existieren noch JSON/JSONL-Pfade für einige Artefakte. |
| Per-game statt global | 4 | Team- und Move-Pools werden pro Spiel geshardet geschrieben; einige globale Referenzen bleiben absichtlich zentral. |
| Normalisierte Teamdaten | 5 | Team-Meta, Team-Members und Team-Member-Moves werden sauber erzeugt. |
| „Einmal vorbereiten, dann wiederverwenden" (Moves/Combat) | 3 | Gute Vorbereitungslogik vorhanden, aber Kampfsimulation hat noch API-Lookups/Fallbackpfade. |
| Echte Kampflogik | 5 | Turn-order, Accuracy, Damage-Rolls, Crits, STAB, Typen, HP/KOs und Multi-Trials sind implementiert. |
| Silver→Gold Vertragskonsistenz | 2 | Mehrere Pfad-/Sharding-Inkonsistenzen zwischen Producer (Silver), Manifest und Consumer (Gold/Battle Seeds). |

## Erfüllte Kernvorgaben

1. **Per-game Team-Shards in Silver:**
   - Silver schreibt `teams_<game>.parquet`, `team_members_<game>.parquet`, `team_member_moves_<game>.parquet`, `learnable_moves_<game>.parquet`, `pokemon_combat_pool_<game>.parquet`.
2. **Normalisierte Teamstruktur:**
   - Team-Metadaten werden aus validierten Teams extrahiert.
   - Members/Moves werden in separate Tabellen transformiert.
3. **Hierarchische Team-Erzeugung:**
   - Progression → Source Teams → Starter-Varianten → Moveset-Varianten → Player-Candidates ist erkennbar umgesetzt.
4. **Rundenbasierte Simulation:**
   - Move-Auswahl, Speed-Reihenfolge, Hit-Checks, Damage-Formel, Crit/Randomness und Team-vs-Team-Abarbeitung vorhanden.
   - Mehrfachtrials liefern empirische Win-Chance.

## Kritische Abweichungen (priorisiert)

### P0 — Vertragsbruch Silver↔Gold wegen unsharded Pfaden

- Silver erzeugt ausschließlich per-game Team-Shards.
- Manifest und Teile der Gold/Simulation-Pfade erwarten jedoch `teams.parquet`, `team_members.parquet`, `team_member_moves.parquet`.
- Risiko: Gold-Contract-Checks und Folgejobs können trotz korrekter Silver-Shards fehlschlagen.

### P0 — Battle Seeds erwartet `teams.parquet`

- `build_battle_seeds` liest fest `simulation/teams.parquet`.
- In Silver werden per-game Shards geschrieben, nicht die globale Datei.
- Folge: Seed-Erzeugung kann „no teams found, skipping" liefern, obwohl Daten da sind.

### P1 — Simulation hat weiterhin Runtime-API-Fallbacks

- `type_matchups.py` lädt Pokemon/Move-Profile zur Laufzeit via `pokebase` und nutzt Fallback-Defaults bei Fehlern.
- Das widerspricht der Sollregel „keine versteckten API-Fallbacks mitten in der Simulation".
- Positiv: Warnings/`degraded_data` sind vorhanden; negativ: Reproduzierbarkeit hängt weiterhin potenziell an externen API-Calls.

### P1 — Gold-Simulation defaultet auf alte Non-sharded Eingaben

- `run_gold_simulation_from_silver` setzt ohne `required_input_files` auf `teams.parquet`/`team_members.parquet`/`team_member_moves.parquet`.
- Der eigentliche Loader kann Shards lesen, wird hier aber auf alte Pfade festgelegt.

## Empfohlene nächste Schritte

1. **Einheitlichen Dateivertrag festziehen (P0):**
   - Manifest auf shard-aware Datasets umstellen (z. B. `files: [...]` oder `glob` pro Teamtabelle).
   - Gold-Contract-Resolver und `run_gold_simulation_from_silver` auf denselben Vertrag ausrichten.
2. **Battle Seeds shard-aware machen (P0):**
   - Teams aus `teams_*.parquet` laden (analog zu `gold/inputs/team_tables.py`) statt fixe Einzeldatei.
3. **Simulation von API entkoppeln (P1):**
   - Combat-Profile ausschließlich aus vorbereiteten Silver-Referenztabellen ziehen.
   - Runtime-Fallback nur noch strikt deterministisch (`struggle`) und ohne Netzabhängigkeit.
4. **Optional (P2):**
   - Simulationsergebnisse zusätzlich per-game sharden, falls Datenmenge stark wächst.

## Gesamturteil

Die Implementierung liegt **nahe an der Zielarchitektur** und ist bei Team-Modellierung + Kampflogik bereits sehr stark. Der größte Blocker ist derzeit **nicht** die Fachlogik, sondern die **Schnittstellenkonsistenz der Artefakte** (Sharding/Manifest/Consumer) und die letzte Meile zur vollständig vorberechneten, API-freien Simulation.
