from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Iterable

_PANDAS = None


def _pd():
    global _PANDAS
    if _PANDAS is None:
        import pandas as _pandas

        _PANDAS = _pandas
    return _PANDAS


def write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        json.dump(payload, file_handle, indent=2, allow_nan=False)


def read_json(path: Path) -> object:
    with path.open("r", encoding="utf-8") as file_handle:
        return json.load(file_handle)


def write_jsonl(path: Path, records: Iterable[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file_handle:
        for record in records:
            file_handle.write(json.dumps(record, ensure_ascii=False, allow_nan=False))
            file_handle.write("\n")


def _to_dataframe(records: Iterable[dict] | pd.DataFrame) -> pd.DataFrame:
    pd = _pd()
    return records if isinstance(records, pd.DataFrame) else pd.DataFrame(list(records))


def _normalize_parquet_dataframe(dataframe: pd.DataFrame) -> pd.DataFrame:
    pd = _pd()
    normalized = dataframe.copy()
    for column in normalized.columns:
        series = normalized[column]
        if not pd.api.types.is_object_dtype(series.dtype):
            continue
        non_null = series.dropna()
        if non_null.empty or non_null.map(lambda value: isinstance(value, str)).all():
            normalized[column] = series.astype("string")
    return normalized


def _remove_path_if_exists(path: Path) -> None:
    if not path.exists():
        return
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink()


def _existing_partitions(dataframe: pd.DataFrame, partition_cols: list[str] | tuple[str, ...] | None) -> list[str]:
    if not partition_cols:
        return []
    return [column for column in partition_cols if column in dataframe.columns]


def _write_non_partitioned_parquet(path: Path, dataframe: pd.DataFrame, cleanup_any_path: bool = False) -> None:
    if cleanup_any_path:
        _remove_path_if_exists(path)
    elif path.exists() and path.is_dir():
        shutil.rmtree(path)
    dataframe.to_parquet(path, index=False)


def _write_partitioned_parquet(path: Path, dataframe: pd.DataFrame, partitions: list[str]) -> None:
    _remove_path_if_exists(path)
    path.mkdir(parents=True, exist_ok=True)
    grouped = dataframe.groupby(partitions, dropna=False, sort=True)
    for partition_values, frame in grouped:
        values = partition_values if isinstance(partition_values, tuple) else (partition_values,)
        partition_dir = path
        for column, value in zip(partitions, values, strict=False):
            partition_dir = partition_dir / f"{column}={value}"
        partition_dir.mkdir(parents=True, exist_ok=True)
        frame.drop(columns=partitions, errors="ignore").to_parquet(
            partition_dir / "part-000.parquet",
            index=False,
        )


def write_parquet(
    path: Path,
    records: Iterable[dict] | pd.DataFrame,
    partition_cols: list[str] | tuple[str, ...] | None = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    dataframe = _normalize_parquet_dataframe(_to_dataframe(records))
    try:
        partitions = _existing_partitions(dataframe, partition_cols)
        if partition_cols and not partitions:
            _write_non_partitioned_parquet(path, dataframe, cleanup_any_path=True)
            return

        if partitions:
            _write_partitioned_parquet(path, dataframe, partitions)
            return

        _write_non_partitioned_parquet(path, dataframe)
    except ImportError as exc:
        raise ImportError(
            "Parquet write requires pyarrow or fastparquet. Install dependency: pyarrow"
        ) from exc


def read_jsonl(path: Path) -> pd.DataFrame:
    pd = _pd()
    return pd.read_json(path, lines=True)


def read_parquet(path: Path) -> pd.DataFrame:
    pd = _pd()
    try:
        return pd.read_parquet(path)
    except ImportError as exc:
        raise ImportError(
            "Parquet read requires pyarrow or fastparquet. Install dependency: pyarrow"
        ) from exc


def read_many_parquet(paths: list[Path]) -> pd.DataFrame:
    import pyarrow as pa
    import pyarrow.parquet as pq

    existing_paths = [path for path in paths if path.exists()]
    if not existing_paths:
        _pd()  # keep lazy-import side-effect consistent
        return _pd().DataFrame()

    tables: list[pa.Table] = [pq.read_table(path) for path in existing_paths]

    # Unify schema: if a column is float64 (all-NA default) in some shards
    # but a richer type in others, promote to the richer type.
    field_types: dict[str, pa.DataType] = {}
    for table in tables:
        for field in table.schema:
            current = field_types.get(field.name)
            if current is None or (current == pa.float64() and field.type != pa.float64()):
                field_types[field.name] = field.type

    unified_schema = pa.schema([pa.field(n, t) for n, t in field_types.items()])

    aligned: list[pa.Table] = []
    for table in tables:
        cols: dict[str, pa.ChunkedArray] = {}
        for field in unified_schema:
            if table.schema.get_field_index(field.name) >= 0:
                col = table.column(field.name)
                if col.type != field.type:
                    col = col.cast(field.type, safe=False)
                cols[field.name] = col
            else:
                cols[field.name] = pa.array([None] * len(table), type=field.type)
        aligned.append(pa.table(cols, schema=unified_schema))

    return pa.concat_tables(aligned).to_pandas()
