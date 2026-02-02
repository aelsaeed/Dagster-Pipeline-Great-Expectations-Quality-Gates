from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from pipeline.gx import ensure_expectation_suites, get_context, run_checkpoint
from pipeline.io import duckdb_conn
from pipeline.settings import settings


def _load_table(table: str) -> tuple[str | None, list[dict]]:
    with duckdb_conn() as conn:
        result = conn.execute(f"SELECT * FROM {table} ORDER BY partition_date DESC LIMIT 1").df()
    if result.empty:
        return None, []
    partition_date = result.iloc[0]["partition_date"]
    return str(partition_date), result.to_dict(orient="records")


def main() -> int:
    context = get_context()
    ensure_expectation_suites(context)

    cleaned_partition, cleaned_records = _load_table("cleaned_prices")
    agg_partition, agg_records = _load_table("daily_agg")

    reports: dict[str, object] = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "cleaned_partition": cleaned_partition,
        "agg_partition": agg_partition,
        "results": {},
    }

    if cleaned_records:
        cleaned_df = pd.DataFrame(cleaned_records)
        reports["results"]["cleaned"] = run_checkpoint(
            context, "cleaned_checkpoint", "cleaned_suite", cleaned_df
        )

    if agg_records:
        agg_df = pd.DataFrame(agg_records)
        reports["results"]["agg"] = run_checkpoint(
            context, "agg_checkpoint", "agg_suite", agg_df
        )

    settings.reports_dir.mkdir(parents=True, exist_ok=True)
    report_path = settings.reports_dir / "last_validation.json"
    report_path.write_text(json.dumps(reports, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
