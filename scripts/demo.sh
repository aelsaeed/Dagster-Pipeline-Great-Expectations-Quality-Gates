#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

bash scripts/run_tiny.sh
make validate
make report

echo "[demo] Complete. Open Data Docs: dagster-gx-pipeline/data_docs/index.html"
echo "[demo] Report: dagster-gx-pipeline/reports/latest.md"
