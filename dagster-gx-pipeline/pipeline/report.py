from __future__ import annotations

import json
from datetime import datetime

from pipeline.settings import settings


def _load_validation() -> dict:
    report_path = settings.reports_dir / "last_validation.json"
    if report_path.exists():
        return json.loads(report_path.read_text())
    return {}


def _format_result(result: dict) -> str:
    success = result.get("success")
    stats = result.get("statistics", {})
    return f"Success: {success}\nTotal Expectations: {stats.get('evaluated_expectations')}\nSuccessful Expectations: {stats.get('successful_expectations')}"


def main() -> int:
    validation = _load_validation()
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = settings.reports_dir / "last_report.md"

    lines = [
        "# Pipeline Run Report",
        "",
        f"Generated: {datetime.utcnow().isoformat()} UTC",
        "",
    ]

    if not validation:
        lines.append("No validation data found. Run `make validate` first.")
    else:
        lines.extend(
            [
                f"Cleaned partition: {validation.get('cleaned_partition')}",
                f"Aggregated partition: {validation.get('agg_partition')}",
                "",
            ]
        )
        results = validation.get("results", {})
        if "cleaned" in results:
            lines.extend(["## Cleaned Asset Validation", "", _format_result(results["cleaned"]), ""])
        if "agg" in results:
            lines.extend(["## Aggregated Asset Validation", "", _format_result(results["agg"]), ""])

    report_path.write_text("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
