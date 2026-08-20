from __future__ import annotations

import dagster as dg

from dagster_gx_pipeline.assets import (
    PARTITIONS,
    cleaned_market_data,
    daily_market_summary,
    raw_market_data,
)
from dagster_gx_pipeline.checks import aggregate_data_contract, cleaned_data_contract

ASSETS = [raw_market_data, cleaned_market_data, daily_market_summary]
ASSET_CHECKS = [cleaned_data_contract, aggregate_data_contract]

daily_job = dg.define_asset_job(
    name="daily_market_quality_job",
    selection=dg.AssetSelection.assets(*ASSETS),
    partitions_def=PARTITIONS,
    description="Ingest, validate, and aggregate one UTC market-data partition.",
)


@dg.schedule(
    job=daily_job,
    name="daily_market_quality_schedule",
    cron_schedule="0 2 * * *",
    execution_timezone="UTC",
    description="Process the latest completed daily partition at 02:00 UTC.",
)
def daily_schedule(context: dg.ScheduleEvaluationContext) -> dg.RunRequest | dg.SkipReason:
    partition_keys = PARTITIONS.get_partition_keys(
        current_time=context.scheduled_execution_time,
    )
    if not partition_keys:
        return dg.SkipReason("No completed daily partition is available")
    return dg.RunRequest(partition_key=partition_keys[-1])


defs = dg.Definitions(
    assets=ASSETS,
    asset_checks=ASSET_CHECKS,
    jobs=[daily_job],
    schedules=[daily_schedule],
)
