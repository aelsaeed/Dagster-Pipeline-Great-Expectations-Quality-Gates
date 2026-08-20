from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from dagster_gx_pipeline.settings import get_settings

EXPECTED_CONTRACTS = frozenset({"cleaned", "aggregate"})
EXPECTED_ASSETS = frozenset({"raw_market_data", "cleaned_market_data", "daily_market_summary"})
EXPECTED_CHECKS = frozenset(
    {
        ("cleaned_market_data", "cleaned_data_contract"),
        ("daily_market_summary", "aggregate_data_contract"),
    }
)


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload: object = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def evidence_is_successful(
    materialization: dict[str, Any],
    validation: dict[str, Any],
) -> bool:
    materialization_partition = materialization.get("partition")
    validation_partition = validation.get("partition")
    validation_results = validation.get("results")
    if not isinstance(validation_results, dict):
        return False
    materialized_assets = materialization.get("assets_materialized")
    asset_checks = materialization.get("asset_checks")
    if not isinstance(materialized_assets, list) or not isinstance(asset_checks, list):
        return False
    passed_checks = {
        (check.get("asset"), check.get("check"))
        for check in asset_checks
        if isinstance(check, dict) and check.get("passed") is True
    }
    return (
        isinstance(materialization_partition, str)
        and materialization_partition == validation_partition
        and isinstance(materialization.get("run_id"), str)
        and bool(materialization["run_id"])
        and materialization.get("success") is True
        and len(materialized_assets) == len(EXPECTED_ASSETS)
        and set(materialized_assets) == EXPECTED_ASSETS
        and len(asset_checks) == len(EXPECTED_CHECKS)
        and passed_checks == EXPECTED_CHECKS
        and validation.get("overall_success") is True
        and not validation.get("errors")
        and set(validation_results) == EXPECTED_CONTRACTS
        and all(
            isinstance(result, dict) and result.get("success") is True
            for result in validation_results.values()
        )
    )


def build_report(materialization: dict[str, Any], validation: dict[str, Any]) -> str:
    partition = materialization.get("partition") or validation.get("partition") or "unknown"
    overall_success = evidence_is_successful(materialization, validation)
    checks = materialization.get("asset_checks", [])
    row_counts = materialization.get("row_counts", {})
    raw_validation_results = validation.get("results", {})
    validation_results = raw_validation_results if isinstance(raw_validation_results, dict) else {}

    lines = [
        "# Pipeline Run Report",
        "",
        f"- **Status:** {'PASS' if overall_success else 'FAIL'}",
        f"- **Partition:** `{partition}`",
        f"- **Dagster run:** `{materialization.get('run_id', 'unknown')}`",
        f"- **Generated:** {datetime.now(UTC).isoformat()}",
        "",
        "## Materialized assets",
        "",
    ]
    lines.extend(f"- `{asset}`" for asset in materialization.get("assets_materialized", []))
    if not materialization.get("assets_materialized"):
        lines.append("- No materializations recorded")

    lines.extend(["", "## Dagster asset checks", ""])
    lines.extend(
        f"- {'✅' if check.get('passed') else '❌'} `{check.get('asset')}/{check.get('check')}`"
        for check in checks
    )
    if not checks:
        lines.append("- No asset-check evaluations recorded")

    lines.extend(["", "## Persisted rows", ""])
    lines.extend(f"- `{table}`: {count}" for table, count in sorted(row_counts.items()))

    lines.extend(["", "## Great Expectations contracts", ""])
    for name, result in validation_results.items():
        lines.append(
            f"- {'✅' if result.get('success') else '❌'} `{name}`: "
            f"{result.get('successful_expectations', 0)}/"
            f"{result.get('evaluated_expectations', 0)} expectations passed"
        )
    for error in validation.get("errors", []):
        lines.append(f"- ❌ {error}")
    if materialization.get("partition") != validation.get("partition"):
        lines.append(
            "- ❌ Evidence partition mismatch: "
            f"materialization={materialization.get('partition', 'missing')}, "
            f"validation={validation.get('partition', 'missing')}"
        )
    if set(validation_results) != EXPECTED_CONTRACTS:
        missing = sorted(EXPECTED_CONTRACTS - set(validation_results))
        unexpected = sorted(set(validation_results) - EXPECTED_CONTRACTS)
        details = []
        if missing:
            details.append(f"missing={','.join(missing)}")
        if unexpected:
            details.append(f"unexpected={','.join(unexpected)}")
        lines.append(f"- ❌ Validation contract set mismatch: {'; '.join(details)}")
    if not validation_results and not validation.get("errors"):
        lines.append("- No validation report found")

    lines.extend(
        [
            "",
            "## Evidence",
            "",
            "- Great Expectations Data Docs: `.artifacts/great_expectations/gx/"
            "uncommitted/data_docs/local_site/index.html`",
            "- Machine-readable materialization: `.artifacts/reports/last_materialization.json`",
            "- Machine-readable validation: `.artifacts/reports/last_validation.json`",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    settings = get_settings()
    settings.ensure_runtime_dirs()
    materialization = _load_json(settings.reports_dir / "last_materialization.json")
    validation = _load_json(settings.reports_dir / "last_validation.json")
    report = build_report(materialization, validation)
    report_path = settings.reports_dir / "latest.md"
    report_path.write_text(report, encoding="utf-8")
    print(report)
    return 0 if evidence_is_successful(materialization, validation) else 1


if __name__ == "__main__":
    raise SystemExit(main())
