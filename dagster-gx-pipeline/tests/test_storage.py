from __future__ import annotations

from typing import Any, cast

import duckdb
import pandas as pd
import pytest
from conftest import PARTITION
from dagster_gx_pipeline.settings import Settings
from dagster_gx_pipeline.storage import (
    latest_partition,
    partition_row_count,
    read_partition,
    replace_partition,
    table_row_count,
)


def _frame(partition: str, values: list[int]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "partition_date": [partition] * len(values),
            "value": values,
        }
    )


def test_replace_partition_is_idempotent_and_preserves_other_partitions(
    runtime_settings: Settings,
) -> None:
    del runtime_settings
    replace_partition("raw_market_data", PARTITION, _frame(PARTITION, [1, 2]))
    replace_partition("raw_market_data", "2024-05-02", _frame("2024-05-02", [9]))
    replace_partition("raw_market_data", PARTITION, _frame(PARTITION, [3, 4, 5]))

    observed = read_partition("raw_market_data", PARTITION)
    assert observed["value"].tolist() == [3, 4, 5]
    assert table_row_count("raw_market_data") == 4
    assert partition_row_count("raw_market_data", PARTITION) == 3
    assert latest_partition("raw_market_data") == "2024-05-02"


def test_failed_partition_insert_rolls_back_delete(runtime_settings: Settings) -> None:
    del runtime_settings
    original = _frame(PARTITION, [1, 2])
    replace_partition("raw_market_data", PARTITION, original)
    incompatible = original.assign(unexpected="not in the persisted schema")

    with pytest.raises(duckdb.Error):
        replace_partition("raw_market_data", PARTITION, incompatible)

    restored = read_partition("raw_market_data", PARTITION)
    assert restored["value"].tolist() == [1, 2]
    assert list(restored.columns) == ["partition_date", "value"]


def test_storage_rejects_unapproved_identifiers(runtime_settings: Settings) -> None:
    del runtime_settings
    with pytest.raises(ValueError, match="Unsupported table"):
        replace_partition(
            cast(Any, "raw_market_data; DROP TABLE x"), PARTITION, _frame(PARTITION, [1])
        )

    replace_partition("raw_market_data", PARTITION, _frame(PARTITION, [1]))
    with pytest.raises(ValueError, match="Unsupported ordering"):
        read_partition("raw_market_data", PARTITION, order_by="value")
