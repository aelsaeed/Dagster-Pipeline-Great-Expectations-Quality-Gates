from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import pandas as pd
import pytest
from dagster_gx_pipeline.assets import payload_to_frame
from dagster_gx_pipeline.settings import Settings, get_settings

PARTITION = "2024-05-01"


@pytest.fixture
def runtime_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Settings:
    runtime_dir = tmp_path / "runtime"
    monkeypatch.setenv("PIPELINE_RUNTIME_DIR", str(runtime_dir))
    monkeypatch.setenv("DAGSTER_HOME", str(runtime_dir / "dagster"))
    for variable in (
        "PIPELINE_DATA_DIR",
        "PIPELINE_REPORTS_DIR",
        "PIPELINE_DUCKDB_PATH",
        "PIPELINE_GX_PROJECT_DIR",
        "COINGECKO_API_KEY",
    ):
        monkeypatch.delenv(variable, raising=False)

    settings = get_settings()
    settings.ensure_runtime_dirs()
    (settings.dagster_home / "dagster.yaml").write_text(
        "telemetry:\n  enabled: false\n", encoding="utf-8"
    )
    return settings


@pytest.fixture
def valid_payload(runtime_settings: Settings) -> dict[str, Any]:
    return cast(
        dict[str, Any],
        json.loads(
            (runtime_settings.fixture_dir / "valid_market_data.json").read_text(encoding="utf-8")
        ),
    )


@pytest.fixture
def cleaned_frame(valid_payload: dict[str, Any]) -> pd.DataFrame:
    raw = payload_to_frame(valid_payload, PARTITION)
    cleaned = raw.copy()
    cleaned["timestamp"] = pd.to_datetime(cleaned.pop("timestamp_ms"), unit="ms", utc=True)
    return cleaned[
        [
            "partition_date",
            "timestamp",
            "price_usd",
            "market_cap_usd",
            "volume_usd",
            "fetched_at",
        ]
    ]


@pytest.fixture
def aggregate_frame(cleaned_frame: pd.DataFrame) -> pd.DataFrame:
    return cleaned_frame.groupby("partition_date", as_index=False).agg(
        observation_count=("timestamp", "count"),
        avg_price_usd=("price_usd", "mean"),
        max_price_usd=("price_usd", "max"),
        min_price_usd=("price_usd", "min"),
        max_observed_volume_usd=("volume_usd", "max"),
    )
