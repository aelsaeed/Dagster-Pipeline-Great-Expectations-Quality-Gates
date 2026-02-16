from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pipeline.io import duckdb_conn
from pipeline.settings import settings


def _load_json(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def _table_row_count(table: str) -> int:
    try:
        with duckdb_conn() as conn:
            result = conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()
            return int(result[0]) if result else 0
    except Exception:
        return 0


def _validation_summary(validation: dict[str, Any]) -> tuple[bool, list[str]]:
    results = validation.get("results", {})
    if not isinstance(results, dict) or not results:
        return False, []
    statuses = []
    all_passed = True
    for name, payload in results.items():
        success = bool(payload.get("success"))
        statuses.append(f"- {name}: {'PASS' if success else 'FAIL'}")
        all_passed = all_passed and success
    return all_passed, statuses


def main() -> int:
    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = settings.reports_dir / "latest.md"

    materialization = _load_json(settings.reports_dir / "last_materialization.json")
    validation = _load_json(settings.reports_dir / "last_validation.json")

    row_counts = {
        "raw_prices": _table_row_count("raw_prices"),
        "cleaned_prices": _table_row_count("cleaned_prices"),
        "daily_agg": _table_row_count("daily_agg"),
    }

    all_passed, validation_lines = _validation_summary(validation)
    partition = materialization.get("partition") or validation.get("cleaned_partition") or "unknown"

    lines = [
        "# Latest Pipeline Report",
        "",
        f"Generated: {datetime.now(UTC).isoformat()}",
        f"Partition: {partition}",
        "",
        "## Assets Materialized",
        "",
        "- raw_asset",
        "- cleaned_asset",
        "- agg_asset",
        "",
        "## Row Counts",
        "",
        f"- raw_prices: {row_counts['raw_prices']}",
        f"- cleaned_prices: {row_counts['cleaned_prices']}",
        f"- daily_agg: {row_counts['daily_agg']}",
        "",
        "## Validation",
        "",
        f"Overall: {'PASS' if all_passed else 'FAIL'}",
    ]

    lines.extend(validation_lines or ["- No validation results found"])
    lines.extend(
        [
            "",
            "## Data Docs",
            "",
            f"Open: {settings.data_docs_dir / 'index.html'}",
            "",
        ]
    )

    report_path.write_text("\n".join(lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
