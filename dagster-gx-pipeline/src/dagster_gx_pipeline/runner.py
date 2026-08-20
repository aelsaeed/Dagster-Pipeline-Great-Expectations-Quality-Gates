from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import dagster as dg
import duckdb

from dagster_gx_pipeline.definitions import defs
from dagster_gx_pipeline.settings import get_settings
from dagster_gx_pipeline.storage import TableName, partition_row_count

DEFAULT_DEMO_PARTITION = "2024-05-01"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Execute the real partitioned Dagster job")
    parser.add_argument("--partition", default=DEFAULT_DEMO_PARTITION)
    parser.add_argument("--fixture", type=Path)
    parser.add_argument(
        "--live",
        action="store_true",
        help="Use CoinGecko instead of the deterministic packaged fixture",
    )
    return parser


def _check_payload(result: dg.ExecuteInProcessResult) -> list[dict[str, Any]]:
    return [
        {
            "asset": evaluation.asset_key.to_user_string(),
            "check": evaluation.check_name,
            "passed": evaluation.passed,
        }
        for evaluation in result.get_asset_check_evaluations()
    ]


def execute_partition(
    partition: str,
    *,
    live: bool = False,
    fixture: Path | None = None,
) -> tuple[int, dict[str, Any]]:
    settings = get_settings()
    settings.ensure_runtime_dirs()
    os.environ.setdefault("DAGSTER_HOME", str(settings.dagster_home))
    run_config: dict[str, Any] = {
        "ops": {
            "raw_market_data": {
                "config": {
                    "deterministic": not live,
                    "fixture_path": str(fixture) if fixture else None,
                }
            }
        }
    }
    instance = dg.DagsterInstance.get()
    try:
        result = defs.resolve_job_def("daily_market_quality_job").execute_in_process(
            partition_key=partition,
            run_config=run_config,
            instance=instance,
            raise_on_error=False,
        )
    finally:
        instance.dispose()

    materialized_assets = sorted(
        {
            event.asset_key.to_user_string()
            for event in result.get_asset_materialization_events()
            if event.asset_key is not None
        }
    )
    row_counts: dict[str, int] = {}
    tables: tuple[TableName, ...] = (
        "raw_market_data",
        "cleaned_market_data",
        "daily_market_summary",
    )
    for table in tables:
        try:
            row_counts[table] = partition_row_count(table, partition)
        except duckdb.CatalogException:
            row_counts[table] = 0
    summary = {
        "run_id": result.run_id,
        "partition": partition,
        "success": result.success,
        "assets_materialized": materialized_assets,
        "asset_checks": _check_payload(result),
        "row_counts": row_counts,
    }
    report_path = settings.reports_dir / "last_materialization.json"
    report_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return (0 if result.success else 1), summary


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    exit_code, summary = execute_partition(
        args.partition,
        live=args.live,
        fixture=args.fixture,
    )
    print(json.dumps(summary, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
