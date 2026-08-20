from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

import dagster as dg
import pandas as pd
from conftest import PARTITION
from dagster_gx_pipeline.definitions import daily_schedule, defs
from dagster_gx_pipeline.report import build_report
from dagster_gx_pipeline.report import main as report_main
from dagster_gx_pipeline.settings import Settings
from dagster_gx_pipeline.storage import replace_partition
from dagster_gx_pipeline.validate import main as validate_main
from dagster_gx_pipeline.validate import validate_partition


def test_validate_partition_checks_every_cleaned_row_and_returns_nonzero(
    runtime_settings: Settings,
    cleaned_frame: pd.DataFrame,
    aggregate_frame: pd.DataFrame,
) -> None:
    invalid_cleaned = cleaned_frame.copy()
    invalid_cleaned.loc[invalid_cleaned.index[-1], "price_usd"] = -1.0
    replace_partition("cleaned_market_data", PARTITION, invalid_cleaned)
    replace_partition("daily_market_summary", PARTITION, aggregate_frame)

    result = validate_partition(PARTITION)

    assert result["partition"] == PARTITION
    assert result["errors"] == []
    assert result["overall_success"] is False
    assert result["results"]["cleaned"]["success"] is False
    assert result["results"]["aggregate"]["success"] is True
    assert validate_main(["--partition", PARTITION]) == 1
    written = json.loads(
        (runtime_settings.reports_dir / "last_validation.json").read_text(encoding="utf-8")
    )
    assert written["overall_success"] is False


def test_validate_missing_partition_is_a_failure(runtime_settings: Settings) -> None:
    result = validate_partition()

    assert result["overall_success"] is False
    assert result["partition"] is None
    assert result["errors"] == ["No cleaned market-data partition is available"]
    assert validate_main([]) == 1
    assert (runtime_settings.reports_dir / "last_validation.json").exists()


def test_report_renders_evidence_and_main_uses_both_statuses(
    runtime_settings: Settings,
) -> None:
    materialization: dict[str, Any] = {
        "run_id": "run-123",
        "partition": PARTITION,
        "success": True,
        "assets_materialized": [
            "raw_market_data",
            "cleaned_market_data",
            "daily_market_summary",
        ],
        "asset_checks": [
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
        ],
        "row_counts": {
            "raw_market_data": 4,
            "cleaned_market_data": 4,
            "daily_market_summary": 1,
        },
    }
    validation: dict[str, Any] = {
        "partition": PARTITION,
        "overall_success": True,
        "errors": [],
        "results": {
            "cleaned": {
                "success": True,
                "successful_expectations": 14,
                "evaluated_expectations": 14,
            },
            "aggregate": {
                "success": True,
                "successful_expectations": 17,
                "evaluated_expectations": 17,
            },
        },
    }

    report = build_report(materialization, validation)

    assert "**Status:** PASS" in report
    assert "`cleaned_market_data/cleaned_data_contract`" in report
    assert "14/14 expectations passed" in report
    assert "17/17 expectations passed" in report
    (runtime_settings.reports_dir / "last_materialization.json").write_text(
        json.dumps(materialization), encoding="utf-8"
    )
    (runtime_settings.reports_dir / "last_validation.json").write_text(
        json.dumps(validation), encoding="utf-8"
    )
    assert report_main() == 0
    assert "**Status:** PASS" in (runtime_settings.reports_dir / "latest.md").read_text(
        encoding="utf-8"
    )

    materialization["asset_checks"] = materialization["asset_checks"][:1]
    (runtime_settings.reports_dir / "last_materialization.json").write_text(
        json.dumps(materialization), encoding="utf-8"
    )
    assert report_main() == 1
    materialization["asset_checks"] = [
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
    (runtime_settings.reports_dir / "last_materialization.json").write_text(
        json.dumps(materialization), encoding="utf-8"
    )

    validation["partition"] = "2024-05-02"
    (runtime_settings.reports_dir / "last_validation.json").write_text(
        json.dumps(validation), encoding="utf-8"
    )
    assert report_main() == 1
    validation["partition"] = PARTITION

    validation["overall_success"] = False
    (runtime_settings.reports_dir / "last_validation.json").write_text(
        json.dumps(validation), encoding="utf-8"
    )
    assert report_main() == 1


def test_partitioned_schedule_emits_previous_completed_partition() -> None:
    context = dg.build_schedule_context(
        scheduled_execution_time=datetime(2024, 5, 2, 2, tzinfo=UTC),
        repository_def=defs.get_repository_def(),
    )

    result = daily_schedule.evaluate_tick(context)

    assert result.run_requests is not None
    assert len(result.run_requests) == 1
    request = result.run_requests[0]
    assert request.partition_key == PARTITION
    assert request.tags["dagster/partition"] == PARTITION
