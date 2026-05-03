from __future__ import annotations

import pandas as pd

from src.pipeline.silver.transforms.keys import normalize_key_part


def test_normalize_key_part_treats_pandas_na_as_empty_string() -> None:
    assert normalize_key_part(pd.NA) == ""
