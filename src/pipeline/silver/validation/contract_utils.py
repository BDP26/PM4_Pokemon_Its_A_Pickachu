from __future__ import annotations

from pathlib import Path

import pandas as pd

GAME_SHARDS = [
    "black-white",
    "black",
    "blue",
    "diamond",
    "gold",
    "pearl",
    "red",
    "ruby",
    "sapphire",
    "silver",
    "white",
    "x",
    "y",
]


def load_simulation_shards(simulation_dir: Path, prefix: str) -> pd.DataFrame:
    import pyarrow as pa
    import pyarrow.parquet as pq

    tables: list[pa.Table] = []
    for game_version in GAME_SHARDS:
        path = simulation_dir / f"{prefix}_{game_version}.parquet"
        if path.exists():
            tables.append(pq.read_table(path))
    if not tables:
        return pd.DataFrame()

    # Build a unified schema: for each field, prefer a non-double/non-null type
    # (double is the pandas default for all-NA columns written to parquet).
    field_types: dict[str, pa.DataType] = {}
    for table in tables:
        for field in table.schema:
            current = field_types.get(field.name)
            # Prefer any type over null; prefer non-double over double when
            # another shard already gave us a richer type.
            if current is None:
                field_types[field.name] = field.type
            elif current == pa.float64() and field.type != pa.float64():
                field_types[field.name] = field.type

    unified_schema = pa.schema(
        [pa.field(name, dtype) for name, dtype in field_types.items()]
    )

    # Cast every table to the unified schema, filling missing columns with null.
    aligned: list[pa.Table] = []
    for table in tables:
        columns: dict[str, pa.ChunkedArray] = {}
        for field in unified_schema:
            if table.schema.get_field_index(field.name) >= 0:
                col = table.column(field.name)
                if col.type != field.type:
                    col = col.cast(field.type, safe=False)
                columns[field.name] = col
            else:
                columns[field.name] = pa.array(
                    [None] * len(table), type=field.type
                )
        aligned.append(pa.table(columns, schema=unified_schema))

    return pa.concat_tables(aligned).to_pandas()

