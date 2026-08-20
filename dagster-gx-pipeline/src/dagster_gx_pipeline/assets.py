import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import dagster as dg
import pandas as pd

from dagster_gx_pipeline.api import MarketDataError, fetch_market_data
from dagster_gx_pipeline.settings import get_settings
from dagster_gx_pipeline.storage import replace_partition

PARTITIONS = dg.DailyPartitionsDefinition(start_date="2024-01-01", timezone="UTC")
FRESHNESS_POLICY = dg.FreshnessPolicy.cron(
    deadline_cron="0 3 * * *",
    lower_bound_delta=timedelta(hours=2),
    timezone="UTC",
)
OWNER = "aelsaeed@dhapdigital.com"


class IngestionConfig(dg.Config):
    deterministic: bool = False
    fixture_path: str | None = None


def partition_datetime(partition_key: str) -> datetime:
    return datetime.strptime(partition_key, "%Y-%m-%d").replace(tzinfo=UTC)


def payload_to_frame(payload: dict[str, object], partition_key: str) -> pd.DataFrame:
    prices = cast(list[list[float]], payload["prices"])
    market_caps = cast(list[list[float]], payload["market_caps"])
    total_volumes = cast(list[list[float]], payload["total_volumes"])
    series = {
        "price_usd": pd.DataFrame(prices, columns=["timestamp_ms", "price_usd"]),
        "market_cap_usd": pd.DataFrame(market_caps, columns=["timestamp_ms", "market_cap_usd"]),
        "volume_usd": pd.DataFrame(total_volumes, columns=["timestamp_ms", "volume_usd"]),
    }
    frame = series["price_usd"]
    for name in ("market_cap_usd", "volume_usd"):
        frame = frame.merge(series[name], on="timestamp_ms", how="outer")

    frame.insert(0, "partition_date", partition_key)
    frame["timestamp_ms"] = frame["timestamp_ms"].astype("int64")
    frame["fetched_at"] = str(payload["fetched_at"])
    frame = frame.sort_values("timestamp_ms", kind="stable").reset_index(drop=True)

    observed_dates = set(
        pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True).dt.strftime("%Y-%m-%d")
    )
    if observed_dates != {partition_key}:
        raise MarketDataError(
            f"Source timestamps {sorted(observed_dates)} do not match partition {partition_key}"
        )
    return frame


def _metadata(frame: pd.DataFrame, partition_key: str) -> dict[str, object]:
    preview = json.loads(frame.head(3).to_json(orient="records", date_format="iso"))
    return {
        "partition": partition_key,
        "row_count": len(frame),
        "columns": list(frame.columns),
        "preview": dg.MetadataValue.json(preview),
    }


@dg.asset(
    partitions_def=PARTITIONS,
    group_name="market_quality",
    kinds={"api", "python", "duckdb"},
    owners=[OWNER],
    freshness_policy=FRESHNESS_POLICY,
    retry_policy=dg.RetryPolicy(
        max_retries=2,
        delay=1,
        backoff=dg.Backoff.EXPONENTIAL,
        jitter=dg.Jitter.PLUS_MINUS,
    ),
    description="Partition-aligned Bitcoin market observations from CoinGecko or a fixture.",
)
def raw_market_data(
    context: dg.AssetExecutionContext,
    config: IngestionConfig,
) -> pd.DataFrame:
    settings = get_settings()
    fixture_path = (
        Path(config.fixture_path)
        if config.fixture_path
        else (settings.fixture_dir / "valid_market_data.json")
    )
    payload = fetch_market_data(
        partition_datetime(context.partition_key),
        deterministic=config.deterministic,
        fixture_path=fixture_path,
    )
    frame = payload_to_frame(payload, context.partition_key)
    replace_partition("raw_market_data", context.partition_key, frame)
    context.add_output_metadata(_metadata(frame, context.partition_key))
    context.log.info("Persisted %d raw observations", len(frame))
    return frame


@dg.asset(
    partitions_def=PARTITIONS,
    group_name="market_quality",
    kinds={"pandas", "python", "duckdb"},
    owners=[OWNER],
    freshness_policy=FRESHNESS_POLICY,
    description="Typed and timestamp-normalized observations ready for contract validation.",
)
def cleaned_market_data(
    context: dg.AssetExecutionContext,
    raw_market_data: pd.DataFrame,
) -> pd.DataFrame:
    cleaned = raw_market_data.copy()
    cleaned["timestamp"] = pd.to_datetime(cleaned.pop("timestamp_ms"), unit="ms", utc=True)
    cleaned = cleaned[
        [
            "partition_date",
            "timestamp",
            "price_usd",
            "market_cap_usd",
            "volume_usd",
            "fetched_at",
        ]
    ]
    replace_partition("cleaned_market_data", context.partition_key, cleaned)
    context.add_output_metadata(_metadata(cleaned, context.partition_key))
    return cleaned


@dg.asset(
    partitions_def=PARTITIONS,
    group_name="market_quality",
    kinds={"pandas", "python", "duckdb"},
    owners=[OWNER],
    freshness_policy=FRESHNESS_POLICY,
    description=(
        "Daily price range and observed volume, produced only after the cleaned gate passes."
    ),
)
def daily_market_summary(
    context: dg.AssetExecutionContext,
    cleaned_market_data: pd.DataFrame,
) -> pd.DataFrame:
    summary = cleaned_market_data.groupby("partition_date", as_index=False).agg(
        observation_count=("timestamp", "count"),
        avg_price_usd=("price_usd", "mean"),
        max_price_usd=("price_usd", "max"),
        min_price_usd=("price_usd", "min"),
        max_observed_volume_usd=("volume_usd", "max"),
    )
    replace_partition("daily_market_summary", context.partition_key, summary)
    context.add_output_metadata(_metadata(summary, context.partition_key))
    return summary
