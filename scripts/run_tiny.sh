#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PROJECT_DIR="$REPO_ROOT/dagster-gx-pipeline"

export DAGSTER_HOME="${DAGSTER_HOME:-$REPO_ROOT/.dagster}"
export DATA_DIR="${DATA_DIR:-$PROJECT_DIR/data}"
export DUCKDB_PATH="${DUCKDB_PATH:-$DATA_DIR/pipeline.duckdb}"

mkdir -p "$DAGSTER_HOME" "$DATA_DIR"

cd "$PROJECT_DIR"

echo "[run] Materializing tiny deterministic dataset..."
python - <<'PY'
import json
from pathlib import Path
from dagster import build_asset_context
from pipeline.assets import agg_asset, cleaned_asset, raw_asset
from pipeline.settings import settings

partition = "2024-05-01"
context = build_asset_context(partition_key=partition, asset_config={"deterministic": True})
raw_df = raw_asset(context)
cleaned_df = cleaned_asset(context, raw_df)
agg_df = agg_asset(context, cleaned_df)

summary = {
    "partition": partition,
    "assets_materialized": ["raw_asset", "cleaned_asset", "agg_asset"],
    "row_counts": {
        "raw_prices": int(len(raw_df)),
        "cleaned_prices": int(len(cleaned_df)),
        "daily_agg": int(len(agg_df)),
    },
}
settings.reports_dir.mkdir(parents=True, exist_ok=True)
(settings.reports_dir / "last_materialization.json").write_text(json.dumps(summary, indent=2))
print(json.dumps(summary, indent=2))
PY
