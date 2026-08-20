#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PIPELINE_RUNTIME_DIR="${PIPELINE_RUNTIME_DIR:-$REPO_ROOT/dagster-gx-pipeline/.artifacts}"
export DAGSTER_HOME="${DAGSTER_HOME:-$PIPELINE_RUNTIME_DIR/dagster}"

exec python -m dagster_gx_pipeline.runner --partition 2024-05-01
