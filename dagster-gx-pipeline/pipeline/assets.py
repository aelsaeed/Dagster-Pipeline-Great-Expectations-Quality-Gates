from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd
from dagster import (
    AssetCheckResult,
    AssetCheckSpec,
    AssetIn,
    DailyPartitionsDefinition,
    Field,
    FreshnessPolicy,
    MetadataValue,
    asset,
)

from pipeline.api import fetch_market_data
from pipeline.io import duckdb_conn

PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2024-01-01")


def _partition_dt(partition_key: str) -> datetime:
    return datetime.strptime(partition_key, "%Y-%m-%d").replace(tzinfo=timezone.utc)


@asset(
    partitions_def=PARTITIONS_DEF,
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1440),
    check_specs=[
        AssetCheckSpec(name="raw_row_count_positive", asset_key="raw_asset"),
    ],
    config_schema={"deterministic": Field(bool, default_value=False)},
)
def raw_asset(context) -> pd.DataFrame:
    deterministic = context.op_config.get("deterministic", False)
    partition_dt = _partition_dt(context.partition_key)
    payload = fetch_market_data(partition_dt, deterministic=deterministic)

    timestamp_ms, price = payload["prices"][0]
    _, market_cap = payload["market_caps"][0]
    _, total_volume = payload["total_volumes"][0]

    raw_df = pd.DataFrame(
        [
            {
                "partition_date": context.partition_key,
                "timestamp_ms": int(timestamp_ms),
                "price_usd": float(price),
                "market_cap_usd": float(market_cap),
                "volume_usd": float(total_volume),
                "fetched_at": payload["fetched_at"],
            }
        ]
    )

    with duckdb_conn() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS raw_prices AS SELECT * FROM raw_df LIMIT 0"
        )
        conn.execute("DELETE FROM raw_prices WHERE partition_date = ?", [context.partition_key])
        conn.register("raw_df", raw_df)
        conn.execute("INSERT INTO raw_prices SELECT * FROM raw_df")

    context.add_output_metadata({"records": len(raw_df)})
    return raw_df


@asset(
    partitions_def=PARTITIONS_DEF,
    ins={"raw_asset": AssetIn()},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1440),
    check_specs=[
        AssetCheckSpec(name="cleaned_row_count_positive", asset_key="cleaned_asset"),
        AssetCheckSpec(name="cleaned_null_threshold", asset_key="cleaned_asset"),
    ],
)
def cleaned_asset(context, raw_asset: pd.DataFrame) -> pd.DataFrame:
    cleaned = raw_asset.copy()
    cleaned["timestamp"] = pd.to_datetime(cleaned["timestamp_ms"], unit="ms", utc=True)
    cleaned = cleaned[[
        "partition_date",
        "timestamp",
        "price_usd",
        "market_cap_usd",
        "volume_usd",
        "fetched_at",
    ]]

    with duckdb_conn() as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS cleaned_prices AS SELECT * FROM cleaned LIMIT 0"
        )
        conn.execute("DELETE FROM cleaned_prices WHERE partition_date = ?", [context.partition_key])
        conn.register("cleaned", cleaned)
        conn.execute("INSERT INTO cleaned_prices SELECT * FROM cleaned")

    context.add_output_metadata({"records": len(cleaned)})
    return cleaned


@asset(
    partitions_def=PARTITIONS_DEF,
    ins={"cleaned_asset": AssetIn()},
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=1440),
    check_specs=[
        AssetCheckSpec(name="agg_row_count_positive", asset_key="agg_asset"),
    ],
)
def agg_asset(context, cleaned_asset: pd.DataFrame) -> pd.DataFrame:
    agg = (
        cleaned_asset
        .groupby("partition_date", as_index=False)
        .agg(
            avg_price_usd=("price_usd", "mean"),
            max_price_usd=("price_usd", "max"),
            min_price_usd=("price_usd", "min"),
            total_volume_usd=("volume_usd", "sum"),
        )
    )

    with duckdb_conn() as conn:
        conn.execute("CREATE TABLE IF NOT EXISTS daily_agg AS SELECT * FROM agg LIMIT 0")
        conn.execute("DELETE FROM daily_agg WHERE partition_date = ?", [context.partition_key])
        conn.register("agg", agg)
        conn.execute("INSERT INTO daily_agg SELECT * FROM agg")

    context.add_output_metadata({"records": len(agg)})
    return agg


@raw_asset.check
def raw_row_count_positive(raw_asset: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed=len(raw_asset) > 0,
        metadata={"row_count": len(raw_asset)},
    )


@cleaned_asset.check
def cleaned_row_count_positive(cleaned_asset: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed=len(cleaned_asset) > 0,
        metadata={"row_count": len(cleaned_asset)},
    )


@cleaned_asset.check
def cleaned_null_threshold(cleaned_asset: pd.DataFrame) -> AssetCheckResult:
    nulls = cleaned_asset.isna().sum().to_dict()
    null_ratio = cleaned_asset.isna().mean().to_dict()
    passed = all(value < 0.01 for value in null_ratio.values())
    return AssetCheckResult(
        passed=passed,
        metadata={
            "null_counts": MetadataValue.json(nulls),
            "null_ratio": MetadataValue.json(null_ratio),
        },
    )


@agg_asset.check
def agg_row_count_positive(agg_asset: pd.DataFrame) -> AssetCheckResult:
    return AssetCheckResult(
        passed=len(agg_asset) > 0,
        metadata={"row_count": len(agg_asset)},
    )
