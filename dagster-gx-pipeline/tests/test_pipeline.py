from __future__ import annotations

import pytest
from conftest import PARTITION
from dagster_gx_pipeline.runner import execute_partition, main
from dagster_gx_pipeline.settings import Settings


def test_execute_partition_runs_three_assets_and_two_blocking_checks(
    runtime_settings: Settings,
) -> None:
    del runtime_settings
    exit_code, summary = execute_partition(PARTITION)

    assert exit_code == 0
    assert summary["success"] is True
    assert summary["assets_materialized"] == [
        "cleaned_market_data",
        "daily_market_summary",
        "raw_market_data",
    ]
    assert summary["row_counts"] == {
        "raw_market_data": 4,
        "cleaned_market_data": 4,
        "daily_market_summary": 1,
    }
    assert summary["asset_checks"] == [
        {
            "asset": "cleaned_market_data",
            "check": "cleaned_data_contract",
            "passed": True,
        },
        {
            "asset": "daily_market_summary",
            "check": "aggregate_data_contract",
            "passed": True,
        },
    ]


def test_invalid_fixture_fails_and_blocks_daily_summary(runtime_settings: Settings) -> None:
    fixture = runtime_settings.fixture_dir / "invalid_negative_price.json"

    exit_code, summary = execute_partition(PARTITION, fixture=fixture)

    assert exit_code == 1
    assert summary["success"] is False
    assert summary["assets_materialized"] == ["cleaned_market_data", "raw_market_data"]
    assert summary["row_counts"] == {
        "raw_market_data": 4,
        "cleaned_market_data": 4,
        "daily_market_summary": 0,
    }
    assert summary["asset_checks"] == [
        {
            "asset": "cleaned_market_data",
            "check": "cleaned_data_contract",
            "passed": False,
        }
    ]


def test_runner_main_returns_pipeline_status(
    runtime_settings: Settings,
    capsys: pytest.CaptureFixture[str],
) -> None:
    del runtime_settings, capsys
    assert main(["--partition", PARTITION]) == 0
