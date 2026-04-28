# Silver Data Contract

Audit basis: persisted artifacts under `data/silver` as observed on 2026-04-26.

This document separates:
- physical Silver: what is actually persisted on disk right now
- conceptual Silver: the business model those artifacts implement
- out-of-scope Gold concepts: concepts that must not be claimed as Silver

## Legend
- Persisted table: parquet file or parquet dataset directory physically present under `data/silver`
- Derived table: business concept derived during the pipeline but not necessarily persisted as its own table
- Sharded table: same schema repeated across multiple `simulation/*_<game_version>.parquet` files
- Partitioned table: one parquet dataset directory with `column=value/part-000.parquet` fragments
- Composite key: more than one column required for uniqueness
- Inferred FK: validated by join behavior, but not enforced by the storage engine
- Gold-boundary output: must stay out of Silver

## Contract-wide physical notes
- All parquet columns are physically nullable in the Arrow schema. This is a writer artifact of the pandas/pyarrow path, not a statement of business-requiredness.
- Requiredness in this contract is therefore documented as observed non-null behavior plus validation rules, not as Arrow `nullable=false`.
- JSON and JSONL artifacts are persisted, but the ERDs only model tabular/parquet contract surfaces.

## Executive findings
- `simulation/source_team_members_<game>.parquet` has a major contract gap: `team_role` and `origin` are null on 223,200 player rows. Boss rows are populated.
- `references/locations.parquet` contains 25 rows with `game_version='unknown'`, so `locations -> games` is not fully valid.
- `references/encounters.parquet` contains 36 rows for `black:cilan` / `white:cilan` that do not match `references/progression_depth.parquet`, because progression uses `black-white:*` Striaton boss ids.
- `references/learnable_moves.parquet` contains 98 rows for 6 species missing from `references/pokemon_data.parquet`: `bellossom`, `crobat`, `espeon`, `kabutops`, `magcargo`, `omastar`.
- `simulation/pokemon_moveset_options_<game>.parquet` is a mixed representation: 2,431 rows are context headers with null `move_name`, `option_rank`, and `option_score`; the remaining rows are actual move options.
- All core simulation-input joins otherwise validate on the current persisted artifacts:
  - `source_teams -> source_team_members`
  - `source_team_members -> member_move_options`
  - `member_move_options -> pokemon_moveset_options`
  - `member_moveset_combos -> source_team_members`
  - `simulation_sampling_plan -> source_teams`
  - `move_data -> source_team_members`

## Physical inventory

### Reference layer artifacts

| Artifact | Structure | Rows | Physical path / partitioning |
| --- | --- | ---: | --- |
| `references/games.parquet` | partitioned table | 13 | partitioned by `region`; 6 fragments |
| `references/bosses.parquet` | partitioned table | 161 | partitioned by `game_version`, `boss_role`; 37 fragments |
| `references/boss_team_members.parquet` | partitioned table | 2,276 | partitioned by `game_version`, `boss_role`; 13 fragments |
| `references/locations.parquet` | partitioned table | 627 | partitioned by `game_version`, `mapping_status`; 25 fragments |
| `references/encounters.parquet` | partitioned table | 41,375 | partitioned by `game`; 13 fragments |
| `references/progression_depth.parquet` | partitioned table | 161 | partitioned by `game_version`; 13 fragments |
| `references/pokemon_reference.parquet` | parquet file | 587 | single file |
| `references/pokemon_data.parquet` | parquet file | 587 | single file |
| `references/move_reference.parquet` | parquet file | 565 | single file |
| `references/learnable_moves.parquet` | partitioned table | 25,137 | partitioned by `game_version`, `pokemon_species`; 2,174 fragments |
| `references/encounter_methods_reference.json` | json | 0 top-level keys | single file |
| `references/encounters.jsonl` | jsonl | 41,321 | single file |

Reference partition counts:

```text
games.parquet by region:
hoenn=2, johto=2, kalos=2, kanto=2, sinnoh=2, unova=3

bosses.parquet by game_version:
black-white=3, black=14, blue=13, diamond=13, gold=13, pearl=13,
red=13, ruby=13, sapphire=13, silver=13, white=14, x=13, y=13

boss_team_members.parquet by game_version:
black-white=24, black=168, blue=204, diamond=205, gold=186, pearl=205,
red=204, ruby=200, sapphire=200, silver=186, white=168, x=163, y=163

encounters.parquet by game:
black-white=54, black=2380, blue=2492, diamond=4275, gold=5323, pearl=4273,
red=2486, ruby=2785, sapphire=2790, silver=5369, white=2380, x=3384, y=3384

locations.parquet by game_version:
black=30, blue=43, diamond=45, gold=83, pearl=45, red=43, ruby=55,
sapphire=55, silver=83, unknown=25, white=30, x=45, y=45

progression_depth.parquet by game_version:
black-white=3, black=14, blue=13, diamond=13, gold=13, pearl=13,
red=13, ruby=13, sapphire=13, silver=13, white=14, x=13, y=13
```

### Simulation layer artifacts

The following are sharded tables. Exact persisted files exist for each of:
`black-white`, `black`, `blue`, `diamond`, `gold`, `pearl`, `red`, `ruby`, `sapphire`, `silver`, `white`, `x`, `y`.

| Artifact family | File pattern | Shards | Total rows |
| --- | --- | ---: | ---: |
| `simulation/source_teams_<game>.parquet` | 13 exact files | 13 | 37,357 |
| `simulation/source_team_members_<game>.parquet` | 13 exact files | 13 | 223,784 |
| `simulation/member_move_options_<game>.parquet` | 13 exact files | 13 | 1,036,047 |
| `simulation/member_moveset_combos_<game>.parquet` | 13 exact files | 13 | 1,501,886 |
| `simulation/pokemon_combat_pool_<game>.parquet` | 13 exact files | 13 | 2,420 |
| `simulation/pokemon_moveset_options_<game>.parquet` | 13 exact files | 13 | 10,293 |
| `simulation/simulation_sampling_plan_<game>.parquet` | 13 exact files | 13 | 37,200 |
| `simulation/move_data.parquet` | single file | 1 | 584 |

Simulation shard row counts:

```text
source_teams:
black-white=243, black=2892, blue=3133, diamond=3133, gold=3133, pearl=3133,
red=3133, ruby=3133, sapphire=3133, silver=3133, white=2892, x=3133, y=3133

source_team_members:
black-white=1446, black=17322, blue=18773, diamond=18772, gold=18768, pearl=18772,
red=18773, ruby=18770, sapphire=18770, silver=18768, white=17322, x=18764, y=18764

member_move_options:
black-white=4604, black=105899, blue=44233, diamond=109362, gold=53129, pearl=109245,
red=43660, ruby=71109, sapphire=71124, silver=52574, white=105731, x=132672, y=132705

member_moveset_combos:
black-white=7866, black=170856, blue=46124, diamond=176020, gold=41692, pearl=174532,
red=45350, ruby=100992, sapphire=100992, silver=41692, white=169680, x=212670, y=213420

pokemon_combat_pool:
black-white=20, black=179, blue=181, diamond=222, gold=220, pearl=223,
red=183, ruby=203, sapphire=203, silver=220, white=179, x=194, y=193

pokemon_moveset_options:
black-white=61, black=945, blue=500, diamond=1092, gold=715, pearl=1086,
red=510, ruby=743, sapphire=743, silver=716, white=943, x=1124, y=1115

simulation_sampling_plan:
black-white=240, black=2880, blue=3120, diamond=3120, gold=3120, pearl=3120,
red=3120, ruby=3120, sapphire=3120, silver=3120, white=2880, x=3120, y=3120
```

### Persisted JSON / JSONL auxiliaries

These are persisted Silver artifacts but not ERD entities:

- `_state/location_enrichment_diagnostics.json`
- `_state/silver_state.json`
- `diagnostics/performance_summary.json`
- `diagnostics/relational_validation.json`
- `diagnostics/unmapped_locations.json`
- `diagnostics/unmapped_locations_detailed.json`
- `diagnostics/unmapped_locations_summary.json`
- `manifest.json`
- `mappings/boss_mapping_by_version.json`
- `mappings/location_to_area_map.json`
- `mappings/location_to_pokemon_map.json`
- `snapshots/{black,blue,diamond,gold,pearl,red,ruby,sapphire,silver,white,x,y}_boss_snapshots.jsonl`

## Keys

### Validated primary / candidate keys

| Artifact | Validated key | Key type | Status |
| --- | --- | --- | --- |
| `games.parquet` | `game_version` | natural | validated |
| `bosses.parquet` | `boss_id` | deterministic surrogate | validated |
| `progression_depth.parquet` | `(game_version, boss_id)` | composite | validated |
| `boss_team_members.parquet` | `(game_version, boss_id, slot, move_slot)` | composite | validated |
| `locations.parquet` | `(game_version, location_id)` | composite; `location_id` also globally unique observed | validated |
| `encounters.parquet` | `(game, boss_id, location, pokemon, level_min, level_max, encounter_chance_min, encounter_chance_max)` | wide natural composite | validated |
| `pokemon_reference.parquet` | `pokemon_species` | natural business key | validated |
| `pokemon_data.parquet` | `pokemon_species` | natural business key | validated |
| `move_reference.parquet` | `move_name` | natural business key | validated |
| `learnable_moves.parquet` | `(game_version, pokemon_species, move_name)` | composite | validated |
| `source_teams_<game>.parquet` | `source_team_id` | deterministic surrogate | validated |
| `source_team_members_<game>.parquet` | `team_member_id`; alternate `(source_team_id, slot)` | surrogate + natural composite | validated |
| `member_move_options_<game>.parquet` | `(team_member_id, move_name)`; alternate `(team_member_id, option_rank)` | composite | validated |
| `member_moveset_combos_<game>.parquet` | `moveset_combo_id`; alternate `(pokemon_instance_id, combo_rank)` | surrogate + natural composite | validated |
| `pokemon_combat_pool_<game>.parquet` | `(game_version, pokemon_species, level)` | composite | validated |
| `pokemon_moveset_options_<game>.parquet` | `(moveset_context_id, move_name)` | composite | validated |
| `simulation_sampling_plan_<game>.parquet` | `(source_team_id, sampling_seed)` | composite | validated |
| `move_data.parquet` | `pokemon_instance_id` | surrogate | validated |

### Explicit non-keys / uncertain keys

- `pokemon_data.name` is not unique: current duplicate is `jellicent` / `jellicent-male`.
- `pokemon_data.pokeapi_id` is not unique for the same reason.
- `pokemon_combat_pool.(game_version, pokemon_species)` is not unique because multiple levels are allowed per species.
- `pokemon_moveset_options.(game_version, pokemon_species, level, move_name)` is not stable because header rows have null `move_name`, and multiple contexts can exist for the same species/level.
- `boss_team_members.boss_id` is unique only within that artifact family. It does not physically join to `bosses.boss_id`.
- `source_teams` does not expose a stable natural key for player-generated teams beyond the persisted deterministic surrogate.

## Relationships

### Validated physical or inferred joins

| From | To | Join | Cardinality | Type | Status |
| --- | --- | --- | --- | --- | --- |
| `progression_depth` | `bosses` | `(game_version, boss_id)` | one-to-one observed | physical | validated |
| `boss source_teams` | `bosses` | `(game_version, boss_id)` | one-to-one observed | inferred | validated |
| `player source_teams` | `progression_depth` | `(game_version, boss_id)` | many-to-one | inferred | validated |
| `source_team_members` | `source_teams` | `source_team_id` | many-to-one | physical | validated |
| `member_move_options` | `source_team_members` | `team_member_id` | many-to-one | physical | validated |
| `member_move_options` | `source_teams` | `source_team_id` | many-to-one | physical | validated |
| `member_move_options` | `pokemon_moveset_options` | `(moveset_context_id, move_name)` | many-to-one | inferred reusable-context join | validated |
| `member_moveset_combos` | `source_team_members` | `pokemon_instance_id -> team_member_id` | many-to-one | inferred | validated |
| `member_moveset_combos` | `source_teams` | `team_id -> source_team_id` | many-to-one | inferred | validated |
| `pokemon_moveset_options` | `pokemon_combat_pool` | `(game_version, pokemon_species, level)` | many-to-one | inferred | validated |
| `simulation_sampling_plan` | `source_teams` | `source_team_id` | one-to-one for player teams | inferred | validated |
| `move_data` | `source_team_members` | `pokemon_instance_id -> team_member_id` | one-to-zero/one from member side; exactly one from move_data side | inferred | validated |
| `move_data` | `source_teams` | `team_id -> source_team_id` | many-to-one | inferred | validated |
| `move_data` | `pokemon_data` | `species -> pokemon_species` | many-to-one | inferred | validated |
| `move_data` | `games` | `game_version` | many-to-one | inferred | validated |
| `learnable_moves` | `move_reference` | `move_name` | many-to-one | physical | validated |

### Known relationship gaps

| From | To | Join | Gap |
| --- | --- | --- | --- |
| `locations` | `games` | `game_version` | 25 `unknown` rows do not resolve |
| `encounters` | `progression_depth` | `(game, boss_id)` | 36 Striaton rows use `black:cilan` / `white:cilan`, while progression uses `black-white:*` |
| `learnable_moves` | `pokemon_data` | `pokemon_species` | 98 rows for 6 species do not resolve |
| `boss_team_members` | `bosses` | attempted `(game_version, boss_id)` or `(game_version, boss_name)` | current physical ids are from a different key system; no validated join |

### Optional versus required

- `source_teams -> source_team_members` is required for all current rows.
- `source_team_members -> member_move_options` is required for player members, optional for boss/kaggle members with fixed moves only.
- `source_team_members -> member_moveset_combos` is required for player members, optional for boss/kaggle members.
- `source_teams -> simulation_sampling_plan` is required for player teams, optional for boss teams.
- `source_team_members -> move_data` is optional overall and currently present only for boss/kaggle members.

## Model scopes

### Silver reference layer

Persisted physical artifacts:
- `references/games.parquet`
- `references/pokemon_reference.parquet`
- `references/pokemon_data.parquet`
- `references/move_reference.parquet`
- `references/encounter_methods_reference.json`

Business meaning:
- canonical game versions
- canonical species and combat stats
- canonical move metadata

### Silver progression / availability layer

Persisted physical artifacts:
- `references/bosses.parquet`
- `references/locations.parquet`
- `references/encounters.parquet`
- `references/progression_depth.parquet`
- `references/learnable_moves.parquet`
- `references/boss_team_members.parquet`
- `references/encounters.jsonl`
- `snapshots/*_boss_snapshots.jsonl`

Business meaning:
- progression checkpoints
- location coverage and encounter availability before each checkpoint
- learnset availability by game and species
- boss roster rows

### Silver team generation layer

Persisted physical artifacts:
- `simulation/source_teams_<game>.parquet`
- `simulation/source_team_members_<game>.parquet`
- `simulation/pokemon_combat_pool_<game>.parquet`
- `simulation/simulation_sampling_plan_<game>.parquet`

Business meaning:
- generated player candidate teams
- canonicalized boss source teams
- per-boss player combat pools
- deterministic sampling instructions for Gold expansion

### Silver move option / moveset layer

Persisted physical artifacts:
- `simulation/member_move_options_<game>.parquet`
- `simulation/pokemon_moveset_options_<game>.parquet`
- `simulation/member_moveset_combos_<game>.parquet`
- `simulation/move_data.parquet`

Business meaning:
- reusable moveset contexts
- per-member legal move options
- bounded per-member moveset combinations
- boss-member move detail cache

### Silver simulation-input contract

Gold-facing persisted inputs:
- required in practice: `source_teams_<game>.parquet`, `source_team_members_<game>.parquet`, `member_moveset_combos_<game>.parquet`
- optional helper inputs: `member_move_options_<game>.parquet`, `simulation_sampling_plan_<game>.parquet`, `move_data.parquet`
- reference dependencies: `games`, `bosses`, `pokemon_data`, `move_reference`

Current contract note:
- `manifest.json` still treats these as the strict Gold-facing inputs, and the files are physically present.

### Derived-but-not-persisted intermediates

Do not place these in the physical ERD:
- full team-cartesian expansions across all member movesets
- a separate normalized `moveset_contexts` table; the concept is embedded in `pokemon_moveset_options`
- a normalized `encounter_methods_bridge`; encounter methods are currently denormalized
- a normalized `boss_move_profiles` bridge from `move_data.move_details`
- a separate `team_member_fixed_moves` bridge

### Gold-only concepts that must not appear in Silver

Do not claim any of these as Silver tables:
- `BOSS_AVAILABLE_POOLS` as a physical Silver table
- `SIMULATION_RESULTS`
- Monte Carlo results, battle seeds, team battle simulations
- sampled combo metrics such as `sampled_combo_count`
- Gold ranking outputs such as best team, recommendation, or win-rate aggregates

Important boundary note:
- `boss_ace_level` and `boss_avg_level` are currently persisted on `source_teams_<game>.parquet`, not on `bosses.parquet`.

## Table-by-table glossary

### `references/games.parquet`
- Purpose: canonical supported game versions and region grouping.
- Persisted path: `data/silver/references/games.parquet`
- Grain: one row per `game_version`.
- Primary key / natural key: `game_version`.
- Foreign keys: none outbound.
- Columns: `game_version: large_string`, `version_group: large_string`, `generation: int64`, `is_supported: bool`, partition `region: string`.
- Nullable fields: none observed.
- Derived fields: `version_group`, `generation`, `region` come from configured game metadata.
- Downstream consumers: progression joins, simulation team artifacts, Gold battle simulation setup.
- Known risks or contract gaps: none observed.

### `references/bosses.parquet`
- Purpose: canonical boss dimension for progression and simulation.
- Persisted path: `data/silver/references/bosses.parquet`
- Grain: one row per boss per `game_version`.
- Primary key / natural key: `boss_id` (deterministic surrogate); `(game_version, boss_name_canonical)` is unique observed.
- Foreign keys: `game_version -> games.game_version`.
- Columns: `boss_id`, `boss_name_canonical`, `boss_name_kaggle`, `boss_name_aliases:list<string>`, `boss_order`, `boss_index`, `gym_index`, `starter_condition`, partitions `game_version`, `boss_role`.
- Nullable fields: `starter_condition` is null on 158 rows.
- Derived fields: `boss_order`, `boss_index`, `boss_role`, aliases.
- Downstream consumers: `progression_depth`, boss `source_teams`, Gold boss resolution.
- Known risks or contract gaps: no direct physical join from `boss_team_members`.

### `references/boss_team_members.parquet`
- Purpose: normalized boss member move rows used as a reference fact.
- Persisted path: `data/silver/references/boss_team_members.parquet`
- Grain: one row per boss member per move slot.
- Primary key / natural key: `(game_version, boss_id, slot, move_slot)`.
- Foreign keys: no validated FK to `bosses`; `game_version` aligns to `games`.
- Columns: `boss_id`, `boss_name`, `starter_condition`, `gym_index`, `slot`, `pokemon_species`, `level`, `move_name`, `move_slot`, `source`, partitions `game_version`, `boss_role`.
- Nullable fields: `starter_condition` null on 2,252 rows; `gym_index` null on all 2,276 rows.
- Derived fields: fully exploded move slots.
- Downstream consumers: boss roster documentation, validation, progression-depth helper logic.
- Known risks or contract gaps: its `boss_id` key system does not match `references/bosses.parquet`.

### `references/locations.parquet`
- Purpose: canonical location lookup with mapping status.
- Persisted path: `data/silver/references/locations.parquet`
- Grain: one row per location id.
- Primary key / natural key: `(game_version, location_id)`; `location_id` also globally unique observed.
- Foreign keys: intended `game_version -> games.game_version`.
- Columns: `location_id`, `walkthrough_location_name`, `normalized_location_name`, `pokeapi_area_slug:list<string>`, partitions `game_version`, `mapping_status`.
- Nullable fields: `pokeapi_area_slug` null on 25 `unknown` rows.
- Derived fields: normalized naming, mapping status, mapped PokeAPI area slugs.
- Downstream consumers: encounter normalization, location mapping docs.
- Known risks or contract gaps: 25 rows use `game_version='unknown'`.

### `references/encounters.parquet`
- Purpose: pre-boss encounter availability fact.
- Persisted path: `data/silver/references/encounters.parquet`
- Grain: one row per `(game, boss_id, location, pokemon, level range, encounter chance range)`.
- Primary key / natural key: wide composite on those fields.
- Foreign keys: intended `game -> games.game_version`; intended `(game, boss_id) -> progression_depth`.
- Columns: `boss_id`, `location`, `pokemon`, `level_min`, `level_max`, `encounter_chance_min`, `encounter_chance_max`, `capture_rate`, `methods:list<null>`, partition `game`.
- Nullable fields: `capture_rate` null on 352 rows.
- Derived fields: normalized location/species rows and encounter range fields.
- Downstream consumers: player team generation, progression calculations, catch availability analysis.
- Known risks or contract gaps: 36 Striaton rows do not match `progression_depth`; `methods` is physically unusable as typed.

### `references/progression_depth.parquet`
- Purpose: progression difficulty / availability profile per boss step.
- Persisted path: `data/silver/references/progression_depth.parquet`
- Grain: one row per `(game_version, boss_id)`.
- Primary key / natural key: `(game_version, boss_id)`.
- Foreign keys: `game_version -> games`, `boss_id -> bosses`.
- Columns: `boss_id`, `boss_name`, `boss_index`, `max_boss_index`, `available_species_count`, `max_species_count`, `progression_depth`, `starter_condition`, partition `game_version`.
- Nullable fields: none observed.
- Derived fields: `progression_depth`, counts, max values.
- Downstream consumers: player `source_teams`, Gold progression balancing.
- Known risks or contract gaps: none in-key; joins to `encounters` are broken only for the Striaton special case.

### `references/pokemon_reference.parquet`
- Purpose: simple canonical species-to-name/url lookup.
- Persisted path: `data/silver/references/pokemon_reference.parquet`
- Grain: one row per `pokemon_species`.
- Primary key / natural key: `pokemon_species`.
- Foreign keys: none outbound.
- Columns: `pokemon_species`, `name`, `url`.
- Nullable fields: none observed.
- Derived fields: none.
- Downstream consumers: reporting, sprite/name resolution.
- Known risks or contract gaps: none observed.

### `references/pokemon_data.parquet`
- Purpose: battle-ready species reference with stats and resolution metadata.
- Persisted path: `data/silver/references/pokemon_data.parquet`
- Grain: one row per `pokemon_species`.
- Primary key / natural key: `pokemon_species`.
- Foreign keys: none outbound.
- Columns: `name`, `pokemon_species`, `pokeapi_id`, `source_url`, `type_1`, `type_2`, `base_hp`, `base_attack`, `base_defense`, `base_special_attack`, `base_special_defense`, `base_speed`, `height`, `weight`, `base_experience`, `is_default`, `requested_pokemon_name`, `normalized_requested_name`, `normalized_species`, `resolved_pokemon_name`, `resolved_pokeapi_id`, `is_default_variety`, `is_legendary`, `is_mythical`, `resolution_method`, `resolution_warning`.
- Nullable fields: `type_2` null on 304 rows; `resolution_warning` null on all rows.
- Derived fields: all resolution metadata columns.
- Downstream consumers: Gold battle simulation, move/profile validation, team artifact species checks.
- Known risks or contract gaps: `name` and `pokeapi_id` are not unique.

### `references/move_reference.parquet`
- Purpose: canonical move metadata.
- Persisted path: `data/silver/references/move_reference.parquet`
- Grain: one row per `move_name`.
- Primary key / natural key: `move_name`.
- Foreign keys: none outbound.
- Columns: `move_name`, `power`, `raw_power`, `damage_class`, `type`, `accuracy`, `pp`, `effective_power`, `power_handling`, `is_status_move`, `is_damage_move`, `is_null_power`.
- Nullable fields: `power` and `raw_power` null on 248 rows; `accuracy` null on 161 rows.
- Derived fields: `effective_power`, boolean flags, `power_handling`.
- Downstream consumers: learnable move validation, move option generation, Gold battle simulation.
- Known risks or contract gaps: nullable `power` / `accuracy` are expected for status or special-handling moves.

### `references/learnable_moves.parquet`
- Purpose: authoritative species/game learnset fact.
- Persisted path: `data/silver/references/learnable_moves.parquet`
- Grain: one row per `(game_version, pokemon_species, move_name)`.
- Primary key / natural key: `(game_version, pokemon_species, move_name)`.
- Foreign keys: intended to `games`, `pokemon_data`, `move_reference`.
- Columns: `move_name`, `learned_level`, `learn_method`, partitions `game_version`, `pokemon_species`.
- Nullable fields: none observed.
- Derived fields: none.
- Downstream consumers: move option generation, move_data profiles, validation.
- Known risks or contract gaps: 98 rows for 6 species are missing from `pokemon_data`.

### `simulation/source_teams_<game>.parquet`
- Purpose: compact Silver team contract for boss teams and generated player teams.
- Persisted path: `data/silver/simulation/source_teams_<game>.parquet`
- Grain: one row per source team.
- Primary key / natural key: `source_team_id`.
- Foreign keys: `game_version -> games`; boss rows `boss_id -> bosses`; player rows `(game_version, boss_id) -> progression_depth`.
- Columns: `source_team_id`, `game_version`, `team_role`, `origin`, `boss_id`, `boss_name`, `gym_index`, `starter_condition`, `starter_base`, `starter_evolved_species`, `progression_source_team_id`, `progression_pool_id`, `avg_level`, `member_count`, `is_player_candidate`, `boss_index`, `max_boss_index`, `available_species_count`, `max_species_count`, `progression_depth`, `boss_ace_level`, `boss_avg_level`, `level_cap_offset`.
- Nullable fields: 157 boss rows have null `gym_index`, `starter_base`, `starter_evolved_species`, `progression_source_team_id`, `progression_pool_id`, `boss_ace_level`, `boss_avg_level`, `level_cap_offset`; `starter_condition` is null on 37,114 rows.
- Derived fields: all progression metrics plus boss level summaries.
- Downstream consumers: Gold compact-team loader and simulation pairing.
- Known risks or contract gaps: none on key integrity; `boss_ace_level` / `boss_avg_level` live here, not in `bosses`.

### `simulation/source_team_members_<game>.parquet`
- Purpose: compact team member contract, one row per team slot.
- Persisted path: `data/silver/simulation/source_team_members_<game>.parquet`
- Grain: one row per team slot.
- Primary key / natural key: `team_member_id`; alternate `(source_team_id, slot)`.
- Foreign keys: `source_team_id -> source_teams`; `pokemon_species -> pokemon_data`.
- Columns: `team_member_id`, `source_team_id`, `game_version`, `team_role`, `origin`, `boss_id`, `boss_name`, `gym_index`, `starter_condition`, `slot`, `pokemon_species`, `level`, `fixed_moves:list<string>`, `progression_pool_id`, `is_starter`.
- Nullable fields: `team_role` and `origin` null on 223,200 player rows; `gym_index` and `progression_pool_id` null on 584 boss rows; `starter_condition` null on 222,338 rows; `fixed_moves` null on 223,200 player rows.
- Derived fields: deterministic member ids and starter flag.
- Downstream consumers: move option generation, moveset combos, Gold compact expansion.
- Known risks or contract gaps: `team_role` and `origin` should be treated as contract failures for player rows.

### `simulation/member_move_options_<game>.parquet`
- Purpose: per-member legal move options.
- Persisted path: `data/silver/simulation/member_move_options_<game>.parquet`
- Grain: one row per `(team_member_id, move_name)`.
- Primary key / natural key: `(team_member_id, move_name)`; alternate `(team_member_id, option_rank)`.
- Foreign keys: `team_member_id -> source_team_members`; `source_team_id -> source_teams`; `(moveset_context_id, move_name) -> pokemon_moveset_options`.
- Columns: `team_member_id`, `source_team_id`, `game_version`, `slot`, `pokemon_species`, `level`, `move_name`, `option_rank`, `option_score`, `moveset_context_id`.
- Nullable fields: none observed.
- Derived fields: ranked move option metadata.
- Downstream consumers: Gold compact expansion fallback path, diagnostics.
- Known risks or contract gaps: reused contexts mean `moveset_context_id + move_name` is not unique in this table by design.

### `simulation/member_moveset_combos_<game>.parquet`
- Purpose: bounded per-member moveset combinations.
- Persisted path: `data/silver/simulation/member_moveset_combos_<game>.parquet`
- Grain: one row per member moveset combo.
- Primary key / natural key: `moveset_combo_id`; alternate `(pokemon_instance_id, combo_rank)`.
- Foreign keys: `team_id -> source_teams.source_team_id`; `pokemon_instance_id -> source_team_members.team_member_id`; `move_1..move_4 -> move_reference.move_name`.
- Columns: `moveset_combo_id`, `team_id`, `pokemon_instance_id`, `slot_index`, `game_version`, `pokemon_name`, `level`, `moves:list<string>`, `move_count`, `combo_rank`, `combo_score`, `source`, `move_1`, `move_2`, `move_3`, `move_4`.
- Nullable fields: `move_1` null on 11,436 rows; `move_2` null on 41,797 rows; `move_3` null on 65,712 rows; `move_4` null on 91,973 rows.
- Derived fields: scalar move slots and deterministic combo ids.
- Downstream consumers: Gold compact expansion and bounded sampling.
- Known risks or contract gaps: none observed; duplicate moves within a combo are currently absent.

### `simulation/pokemon_combat_pool_<game>.parquet`
- Purpose: progression-legal species/level pool for generated player teams.
- Persisted path: `data/silver/simulation/pokemon_combat_pool_<game>.parquet`
- Grain: one row per `(game_version, pokemon_species, level)`.
- Primary key / natural key: `(game_version, pokemon_species, level)`.
- Foreign keys: `game_version -> games`; `pokemon_species -> pokemon_data`.
- Columns: `game_version`, `pokemon_species`, `level`.
- Nullable fields: none observed.
- Derived fields: level-capped pool rows.
- Downstream consumers: moveset-context generation and player team synthesis.
- Known risks or contract gaps: `(game_version, pokemon_species)` is intentionally non-unique.

### `simulation/pokemon_moveset_options_<game>.parquet`
- Purpose: reusable moveset context table.
- Persisted path: `data/silver/simulation/pokemon_moveset_options_<game>.parquet`
- Grain: mixed. One header row per `moveset_context_id`, plus zero-to-many option rows per context.
- Primary key / natural key: `(moveset_context_id, move_name)` is unique observed.
- Foreign keys: `(game_version, pokemon_species, level) -> pokemon_combat_pool`.
- Columns: `moveset_context_id`, `game_version`, `pokemon_species`, `level`, `move_policy`, `candidate_move_count`, `move_name`, `option_rank`, `option_score`.
- Nullable fields: `candidate_move_count` null on 7,862 rows; `move_name`, `option_rank`, `option_score` null on 2,431 header rows.
- Derived fields: reusable context ids, move policy, ranks, scores.
- Downstream consumers: `member_move_options`.
- Known risks or contract gaps: the mixed header/detail representation is compact but not fully normalized.

### `simulation/simulation_sampling_plan_<game>.parquet`
- Purpose: deterministic sampling metadata for Gold expansion.
- Persisted path: `data/silver/simulation/simulation_sampling_plan_<game>.parquet`
- Grain: one row per player `source_team_id`.
- Primary key / natural key: `(source_team_id, sampling_seed)`.
- Foreign keys: `source_team_id -> source_teams`.
- Columns: `source_team_id`, `sampling_seed`, `move_policy`, `max_moves_per_member`, `estimated_combo_space`.
- Nullable fields: none observed.
- Derived fields: deterministic seed and estimated combo space.
- Downstream consumers: Gold simulation expansion and sampling.
- Known risks or contract gaps: boss teams intentionally have no rows here.

### `simulation/move_data.parquet`
- Purpose: boss-member move profile cache.
- Persisted path: `data/silver/simulation/move_data.parquet`
- Grain: one row per `pokemon_instance_id`.
- Primary key / natural key: `pokemon_instance_id`.
- Foreign keys: `pokemon_instance_id -> source_team_members.team_member_id`; `team_id -> source_teams.source_team_id`; `species -> pokemon_data.pokemon_species`; `game_version -> games.game_version`.
- Columns: `pokemon_instance_id`, `team_id`, `species`, `level`, `game_version`, `provided_moves:list<string>`, `learnable_moves:list<string>`, `move_details:struct<per-move metadata>`, `slot_index`.
- Nullable fields: none observed at top level.
- Derived fields: all array and struct payloads.
- Downstream consumers: move-profile diagnostics and potential boss move enrichment.
- Known risks or contract gaps: currently a boss-only subset; there is no corresponding move_data row for generated player members.

### Non-tabular persisted artifacts

Short glossary:

- `manifest.json`: persisted Silver manifest used by Gold discovery; current file inventory matches the declared shard paths.
- `mappings/location_to_area_map.json`: lookup from normalized location slug to mapped areas.
- `mappings/location_to_pokemon_map.json`: lookup from normalized location slug to available species payloads.
- `mappings/boss_mapping_by_version.json`: boss-name mapping metadata by version.
- `snapshots/*_boss_snapshots.jsonl`: boss snapshot records with reachable locations and counts; useful for audit, not used as a physical ERD table.
- `references/encounters.jsonl`: JSONL view of encounter rows; not the authoritative physical contract because `encounters.parquet` is the tabular source.
- `diagnostics/*.json`, `_state/*.json`: operational state and validation outputs, not business contract tables.

## Bridge-table notes

### Current multi-valued fields

- `bosses.boss_name_aliases:list<string>`
  - Current physical representation: inline list.
  - Fully normalized equivalent: `boss_aliases(boss_id, alias_name)`.
  - Worth normalizing: low. Keep inline unless alias analytics become first-class.

- `locations.pokeapi_area_slug:list<string>`
  - Current physical representation: inline list.
  - Fully normalized equivalent: `location_area_bridge(location_id, area_slug)`.
  - Worth normalizing: medium if area-level joins become contract-critical.

- `encounters.methods:list<null>`
  - Current physical representation: unusable typed list with no concrete element values.
  - Fully normalized equivalent: `encounter_methods_bridge(encounter_natural_key..., method_name)`.
  - Worth normalizing: high if method analytics matter; current physical type is weak.

- `source_team_members.fixed_moves:list<string>`
  - Current physical representation: inline fixed move array, present only on boss rows.
  - Fully normalized equivalent: `team_member_fixed_moves(team_member_id, move_slot, move_name)`.
  - Worth normalizing: medium. Current inline list is compact, but the bridge would align better with `boss_team_members`.

- `member_moveset_combos.moves:list<string>` plus `move_1..move_4`
  - Current physical representation: both array and scalar slot columns.
  - Fully normalized equivalent: `member_moveset_combo_moves(moveset_combo_id, move_slot, move_name)`.
  - Worth normalizing: low-medium. The current scalar columns are Gold-friendly.

- `move_data.provided_moves:list<string>`
- `move_data.learnable_moves:list<string>`
- `move_data.move_details:struct<move_slug -> detail>`
  - Current physical representation: one wide cache row per boss member.
  - Fully normalized equivalent: `member_move_detail(pokemon_instance_id, move_name, source_type, accuracy, damage_class, effective_power, ...)`.
  - Worth normalizing: medium only if `move_data` becomes a general-purpose Silver contract surface. Right now it behaves more like a boss-profile cache.

## Gold boundary

The following belong to Gold or later and must stay out of the physical Silver ERD:
- simulation battle matrices
- Monte Carlo outputs
- recommendation tables
- ranking tables
- walkthrough payloads
- sampled-combo metrics
- any `SIMULATION_RESULTS`-style entity
