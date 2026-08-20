import dagster as dg
import pandas as pd

from dagster_gx_pipeline.assets import PARTITIONS, cleaned_market_data, daily_market_summary
from dagster_gx_pipeline.quality import QualityGateResult, run_quality_gate


def _dagster_result(result: QualityGateResult) -> dg.AssetCheckResult:
    failed = list(result.failed_expectations)
    return dg.AssetCheckResult(
        passed=result.success,
        severity=dg.AssetCheckSeverity.ERROR,
        description=(
            f"GX contract passed ({result.success_percent:.1f}%)."
            if result.success
            else f"GX contract failed: {', '.join(failed)}"
        ),
        metadata={
            "contract": result.contract,
            "evaluated_expectations": result.evaluated_expectations,
            "successful_expectations": result.successful_expectations,
            "unsuccessful_expectations": result.unsuccessful_expectations,
            "success_percent": result.success_percent,
            "failed_expectations": dg.MetadataValue.json(failed),
            "data_docs": dg.MetadataValue.url(result.data_docs_url),
        },
    )


@dg.asset_check(
    asset=cleaned_market_data,
    name="cleaned_data_contract",
    description="Blocking GX schema, completeness, uniqueness, and range contract.",
    blocking=True,
    compute_kind="great_expectations",
    partitions_def=PARTITIONS,
)
def cleaned_data_contract(
    context: dg.AssetCheckExecutionContext,
    cleaned_market_data: pd.DataFrame,
) -> dg.AssetCheckResult:
    result = run_quality_gate(
        cleaned_market_data,
        "cleaned",
        expected_partition=context.partition_key,
    )
    context.log.info(
        "GX cleaned contract: %s (%0.1f%%)",
        "PASS" if result.success else "FAIL",
        result.success_percent,
    )
    return _dagster_result(result)


@dg.asset_check(
    asset=daily_market_summary,
    name="aggregate_data_contract",
    description="Blocking GX row-count, range, and min/average/max reconciliation contract.",
    blocking=True,
    compute_kind="great_expectations",
    partitions_def=PARTITIONS,
)
def aggregate_data_contract(
    context: dg.AssetCheckExecutionContext,
    daily_market_summary: pd.DataFrame,
) -> dg.AssetCheckResult:
    result = run_quality_gate(
        daily_market_summary,
        "aggregate",
        expected_partition=context.partition_key,
    )
    context.log.info(
        "GX aggregate contract: %s (%0.1f%%)",
        "PASS" if result.success else "FAIL",
        result.success_percent,
    )
    return _dagster_result(result)
