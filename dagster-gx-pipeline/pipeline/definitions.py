from __future__ import annotations

from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
)

from pipeline.assets import PARTITIONS_DEF, agg_asset, cleaned_asset, raw_asset

all_assets = [raw_asset, cleaned_asset, agg_asset]

daily_job = define_asset_job(
    name="daily_job",
    selection=all_assets,
    partitions_def=PARTITIONS_DEF,
)

schedule = ScheduleDefinition(
    name="daily_schedule",
    job=daily_job,
    cron_schedule="0 2 * * *",
    run_config={},
)


defs = Definitions(
    assets=all_assets,
    schedules=[schedule],
)
