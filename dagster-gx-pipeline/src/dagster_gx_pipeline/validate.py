from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from typing import Any

import duckdb

from dagster_gx_pipeline.quality import ContractName, run_quality_gate
from dagster_gx_pipeline.settings import get_settings
from dagster_gx_pipeline.storage import TableName, latest_partition, read_partition


def validate_partition(partition: str | None = None) -> dict[str, Any]:
    results: dict[str, Any] = {}
    errors: list[str] = []
    resolved_partition = partition
    try:
        resolved_partition = resolved_partition or latest_partition("cleaned_market_data")
    except duckdb.CatalogException:
        resolved_partition = None

    if not resolved_partition:
        errors.append("No cleaned market-data partition is available")
    else:
        validation_targets: tuple[tuple[ContractName, TableName, str], ...] = (
            ("cleaned", "cleaned_market_data", "timestamp"),
            ("aggregate", "daily_market_summary", "partition_date"),
        )
        for contract, table, order_by in validation_targets:
            try:
                frame = read_partition(table, resolved_partition, order_by=order_by)
            except duckdb.CatalogException:
                frame = None
            if frame is None or frame.empty:
                errors.append(f"{table} has no rows for partition {resolved_partition}")
                continue
            gate = run_quality_gate(
                frame,
                contract,
                expected_partition=resolved_partition,
            )
            results[contract] = gate.to_dict()

    expected_contracts = {"cleaned", "aggregate"}
    overall_success = (
        not errors
        and set(results) == expected_contracts
        and all(result["success"] for result in results.values())
    )
    return {
        "run_at": datetime.now(UTC).isoformat(),
        "partition": resolved_partition,
        "overall_success": overall_success,
        "errors": errors,
        "results": results,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Re-run GX quality gates from DuckDB")
    parser.add_argument("--partition")
    args = parser.parse_args(argv)
    report = validate_partition(args.partition)
    settings = get_settings()
    settings.ensure_runtime_dirs()
    report_path = settings.reports_dir / "last_validation.json"
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0 if report["overall_success"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
