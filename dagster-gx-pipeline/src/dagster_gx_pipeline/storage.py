from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Literal, cast

import duckdb
import pandas as pd

from dagster_gx_pipeline.settings import get_settings

TableName = Literal["raw_market_data", "cleaned_market_data", "daily_market_summary"]
TABLE_NAMES: frozenset[str] = frozenset(
    {"raw_market_data", "cleaned_market_data", "daily_market_summary"}
)


def _validated_table_name(table: str) -> TableName:
    if table not in TABLE_NAMES:
        raise ValueError(f"Unsupported table name: {table}")
    return cast(TableName, table)


@contextmanager
def duckdb_connection(path: Path | None = None) -> Iterator[duckdb.DuckDBPyConnection]:
    database_path = path or get_settings().duckdb_path
    database_path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(database_path))
    try:
        yield connection
    finally:
        connection.close()


def replace_partition(table: TableName, partition_key: str, frame: pd.DataFrame) -> None:
    """Atomically replace one partition using an explicit column projection."""

    table = _validated_table_name(table)
    columns = list(frame.columns)
    quoted_columns = ", ".join(f'"{column}"' for column in columns)
    with duckdb_connection() as connection:
        connection.register("incoming_partition", frame)
        connection.execute("BEGIN TRANSACTION")
        try:
            connection.execute(
                f'CREATE TABLE IF NOT EXISTS "{table}" AS SELECT * FROM incoming_partition LIMIT 0'
            )
            connection.execute(f'DELETE FROM "{table}" WHERE partition_date = ?', [partition_key])
            connection.execute(
                f'INSERT INTO "{table}" ({quoted_columns}) '
                f"SELECT {quoted_columns} FROM incoming_partition"
            )
            connection.execute("COMMIT")
        except Exception:
            connection.execute("ROLLBACK")
            raise
        finally:
            connection.unregister("incoming_partition")


def read_partition(
    table: TableName,
    partition_key: str,
    *,
    order_by: str = "partition_date",
) -> pd.DataFrame:
    table = _validated_table_name(table)
    if order_by not in {"partition_date", "timestamp"}:
        raise ValueError(f"Unsupported ordering column: {order_by}")
    with duckdb_connection() as connection:
        return connection.execute(
            f'SELECT * FROM "{table}" WHERE partition_date = ? ORDER BY "{order_by}"',
            [partition_key],
        ).df()


def latest_partition(table: TableName) -> str | None:
    table = _validated_table_name(table)
    with duckdb_connection() as connection:
        row = connection.execute(f'SELECT MAX(partition_date) FROM "{table}"').fetchone()
    return str(row[0]) if row and row[0] is not None else None


def table_row_count(table: TableName) -> int:
    table = _validated_table_name(table)
    with duckdb_connection() as connection:
        row = connection.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()
    return int(row[0]) if row else 0


def partition_row_count(table: TableName, partition_key: str) -> int:
    table = _validated_table_name(table)
    with duckdb_connection() as connection:
        row = connection.execute(
            f'SELECT COUNT(*) FROM "{table}" WHERE partition_date = ?',
            [partition_key],
        ).fetchone()
    return int(row[0]) if row else 0
