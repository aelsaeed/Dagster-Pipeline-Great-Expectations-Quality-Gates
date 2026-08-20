from __future__ import annotations

from pathlib import Path
from typing import cast

import pandas as pd
from conftest import PARTITION
from dagster_gx_pipeline.quality import run_quality_gate
from dagster_gx_pipeline.settings import Settings


def test_cleaned_quality_gate_accepts_valid_data_and_rejects_bad_row(
    runtime_settings: Settings,
    cleaned_frame: pd.DataFrame,
) -> None:
    valid = run_quality_gate(cleaned_frame, "cleaned", expected_partition=PARTITION)
    invalid_frame = cleaned_frame.copy()
    invalid_frame.loc[invalid_frame.index[1], "price_usd"] = -1.0
    invalid = run_quality_gate(invalid_frame, "cleaned", expected_partition=PARTITION)

    wrong_partition_frame = cleaned_frame.copy()
    wrong_partition_frame["partition_date"] = "not-a-partition"
    wrong_partition = run_quality_gate(
        wrong_partition_frame,
        "cleaned",
        expected_partition=PARTITION,
    )

    missing_fetch_time_frame = cleaned_frame.copy()
    missing_fetch_time_frame.loc[missing_fetch_time_frame.index[0], "fetched_at"] = None
    missing_fetch_time = run_quality_gate(
        missing_fetch_time_frame,
        "cleaned",
        expected_partition=PARTITION,
    )

    assert valid.success
    assert valid.evaluated_expectations == 14
    assert valid.unsuccessful_expectations == 0
    assert valid.to_dict()["failed_expectations"] == []
    assert Path(valid.data_docs_url.removeprefix("file://")).exists()

    assert not invalid.success
    assert invalid.unsuccessful_expectations == 1
    assert invalid.failed_expectations == ("expect_column_values_to_be_between",)
    assert invalid.success_percent < 100.0
    assert str(runtime_settings.gx_project_dir) in invalid.data_docs_url
    assert not wrong_partition.success
    assert "expect_column_values_to_be_in_set" in wrong_partition.failed_expectations
    assert not missing_fetch_time.success
    assert "expect_column_values_to_not_be_null" in missing_fetch_time.failed_expectations


def test_aggregate_quality_gate_checks_reconciliation(
    runtime_settings: Settings,
    aggregate_frame: pd.DataFrame,
) -> None:
    del runtime_settings
    valid = run_quality_gate(aggregate_frame, "aggregate", expected_partition=PARTITION)
    invalid_frame = aggregate_frame.copy()
    invalid_frame.loc[0, "min_price_usd"] = cast(float, invalid_frame.loc[0, "avg_price_usd"]) + 1.0
    invalid = run_quality_gate(invalid_frame, "aggregate", expected_partition=PARTITION)

    negative_min_frame = aggregate_frame.copy()
    negative_min_frame.loc[0, "min_price_usd"] = -1.0
    negative_min = run_quality_gate(
        negative_min_frame,
        "aggregate",
        expected_partition=PARTITION,
    )

    missing_volume_frame = aggregate_frame.copy()
    missing_volume_frame.loc[0, "max_observed_volume_usd"] = None
    missing_volume = run_quality_gate(
        missing_volume_frame,
        "aggregate",
        expected_partition=PARTITION,
    )

    assert valid.success
    assert valid.evaluated_expectations == 17
    assert not invalid.success
    assert "expect_column_pair_values_a_to_be_greater_than_b" in invalid.failed_expectations
    assert not negative_min.success
    assert "expect_column_values_to_be_between" in negative_min.failed_expectations
    assert not missing_volume.success
    assert "expect_column_values_to_not_be_null" in missing_volume.failed_expectations
