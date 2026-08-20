from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

import great_expectations as gx
import pandas as pd
from great_expectations.data_context.data_context.abstract_data_context import AbstractDataContext
from great_expectations.data_context.types.base import ProgressBarsConfig
from great_expectations.expectations.core.expect_column_pair_values_a_to_be_greater_than_b import (
    ExpectColumnPairValuesAToBeGreaterThanB,
)
from great_expectations.expectations.core.expect_column_values_to_be_between import (
    ExpectColumnValuesToBeBetween,
)
from great_expectations.expectations.core.expect_column_values_to_be_dateutil_parseable import (
    ExpectColumnValuesToBeDateutilParseable,
)
from great_expectations.expectations.core.expect_column_values_to_be_in_set import (
    ExpectColumnValuesToBeInSet,
)
from great_expectations.expectations.core.expect_column_values_to_be_unique import (
    ExpectColumnValuesToBeUnique,
)
from great_expectations.expectations.core.expect_column_values_to_not_be_null import (
    ExpectColumnValuesToNotBeNull,
)
from great_expectations.expectations.core.expect_table_columns_to_match_ordered_list import (
    ExpectTableColumnsToMatchOrderedList,
)
from great_expectations.expectations.core.expect_table_row_count_to_be_between import (
    ExpectTableRowCountToBeBetween,
)
from great_expectations.expectations.core.expect_table_row_count_to_equal import (
    ExpectTableRowCountToEqual,
)

from dagster_gx_pipeline.settings import get_settings

ContractName = Literal["cleaned", "aggregate"]


@dataclass(frozen=True, slots=True)
class QualityGateResult:
    contract: ContractName
    success: bool
    evaluated_expectations: int
    successful_expectations: int
    unsuccessful_expectations: int
    success_percent: float
    failed_expectations: tuple[str, ...]
    data_docs_url: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["failed_expectations"] = list(self.failed_expectations)
        return payload


def _expectation_suites(expected_partition: str) -> dict[ContractName, gx.ExpectationSuite]:
    return {
        "cleaned": gx.ExpectationSuite(
            name="cleaned_market_data_suite",
            expectations=[
                ExpectTableColumnsToMatchOrderedList(
                    column_list=[
                        "partition_date",
                        "timestamp",
                        "price_usd",
                        "market_cap_usd",
                        "volume_usd",
                        "fetched_at",
                    ]
                ),
                ExpectTableRowCountToBeBetween(min_value=3, max_value=10_000),
                ExpectColumnValuesToNotBeNull(column="partition_date"),
                ExpectColumnValuesToBeInSet(
                    column="partition_date", value_set=[expected_partition]
                ),
                ExpectColumnValuesToNotBeNull(column="timestamp"),
                ExpectColumnValuesToBeUnique(column="timestamp"),
                ExpectColumnValuesToNotBeNull(column="price_usd"),
                ExpectColumnValuesToBeBetween(
                    column="price_usd", min_value=0, max_value=1_000_000, strict_min=True
                ),
                ExpectColumnValuesToNotBeNull(column="market_cap_usd"),
                ExpectColumnValuesToBeBetween(
                    column="market_cap_usd",
                    min_value=0,
                    max_value=10_000_000_000_000,
                    strict_min=True,
                ),
                ExpectColumnValuesToNotBeNull(column="volume_usd"),
                ExpectColumnValuesToBeBetween(
                    column="volume_usd",
                    min_value=0,
                    max_value=1_000_000_000_000,
                    strict_min=True,
                ),
                ExpectColumnValuesToNotBeNull(column="fetched_at"),
                ExpectColumnValuesToBeDateutilParseable(column="fetched_at"),
            ],
            meta={"purpose": "Blocking contract before daily aggregation"},
        ),
        "aggregate": gx.ExpectationSuite(
            name="daily_market_summary_suite",
            expectations=[
                ExpectTableColumnsToMatchOrderedList(
                    column_list=[
                        "partition_date",
                        "observation_count",
                        "avg_price_usd",
                        "max_price_usd",
                        "min_price_usd",
                        "max_observed_volume_usd",
                    ]
                ),
                ExpectTableRowCountToEqual(value=1),
                ExpectColumnValuesToNotBeNull(column="partition_date"),
                ExpectColumnValuesToBeInSet(
                    column="partition_date", value_set=[expected_partition]
                ),
                ExpectColumnValuesToBeUnique(column="partition_date"),
                ExpectColumnValuesToNotBeNull(column="observation_count"),
                ExpectColumnValuesToBeBetween(
                    column="observation_count", min_value=3, max_value=10_000
                ),
                ExpectColumnValuesToNotBeNull(column="avg_price_usd"),
                ExpectColumnValuesToBeBetween(
                    column="avg_price_usd", min_value=0, max_value=1_000_000, strict_min=True
                ),
                ExpectColumnValuesToNotBeNull(column="min_price_usd"),
                ExpectColumnValuesToBeBetween(
                    column="min_price_usd", min_value=0, max_value=1_000_000, strict_min=True
                ),
                ExpectColumnValuesToNotBeNull(column="max_price_usd"),
                ExpectColumnValuesToBeBetween(
                    column="max_price_usd", min_value=0, max_value=1_000_000, strict_min=True
                ),
                ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="avg_price_usd", column_B="min_price_usd", or_equal=True
                ),
                ExpectColumnPairValuesAToBeGreaterThanB(
                    column_A="max_price_usd", column_B="avg_price_usd", or_equal=True
                ),
                ExpectColumnValuesToNotBeNull(column="max_observed_volume_usd"),
                ExpectColumnValuesToBeBetween(
                    column="max_observed_volume_usd",
                    min_value=0,
                    max_value=1_000_000_000_000,
                    strict_min=True,
                ),
            ],
            meta={"purpose": "Final aggregate reconciliation contract"},
        ),
    }


def get_context() -> AbstractDataContext:
    settings = get_settings()
    settings.ensure_runtime_dirs()
    context = gx.get_context(mode="file", project_root_dir=settings.gx_project_dir)
    context.variables.progress_bars = ProgressBarsConfig(globally=False, metric_calculations=False)
    return context


def _get_or_create_batch_definition(context: AbstractDataContext, contract: ContractName) -> Any:
    try:
        datasource: Any = context.data_sources.get("pandas_runtime")
    except KeyError:
        datasource = context.data_sources.add_pandas(name="pandas_runtime")

    asset_name = f"{contract}_dataframe"
    try:
        data_asset = datasource.get_asset(asset_name)
    except LookupError:
        data_asset = datasource.add_dataframe_asset(name=asset_name)
    try:
        return data_asset.get_batch_definition("whole_dataframe")
    except KeyError:
        return data_asset.add_batch_definition_whole_dataframe(name="whole_dataframe")


def run_quality_gate(
    frame: pd.DataFrame,
    contract: ContractName,
    *,
    expected_partition: str,
    context: AbstractDataContext | None = None,
) -> QualityGateResult:
    """Run and persist one GX 1.x checkpoint, then build local Data Docs."""

    data_context = context or get_context()
    batch_definition = _get_or_create_batch_definition(data_context, contract)
    suite = data_context.suites.add_or_update(_expectation_suites(expected_partition)[contract])
    validation = data_context.validation_definitions.add_or_update(
        gx.ValidationDefinition(
            name=f"{contract}_validation",
            data=batch_definition,
            suite=suite,
        )
    )
    checkpoint = data_context.checkpoints.add_or_update(
        gx.Checkpoint(
            name=f"{contract}_checkpoint",
            validation_definitions=[validation],
            result_format="SUMMARY",
        )
    )
    checkpoint_result = checkpoint.run(batch_parameters={"dataframe": frame})
    docs = data_context.build_data_docs(site_names=["local_site"])
    described = checkpoint_result.describe_dict()
    validations = described.get("validation_results", [])
    validation_result = validations[0] if validations else {}
    statistics = validation_result.get("statistics", {})
    failed = tuple(
        expectation.get("expectation_type", "unknown_expectation")
        for expectation in validation_result.get("expectations", [])
        if not expectation.get("success", False)
    )
    return QualityGateResult(
        contract=contract,
        success=bool(checkpoint_result.success),
        evaluated_expectations=int(statistics.get("evaluated_expectations", 0)),
        successful_expectations=int(statistics.get("successful_expectations", 0)),
        unsuccessful_expectations=int(statistics.get("unsuccessful_expectations", 0)),
        success_percent=float(statistics.get("success_percent", 0.0) or 0.0),
        failed_expectations=failed,
        data_docs_url=docs.get("local_site", ""),
    )
