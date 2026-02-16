#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

echo "[smoke] Ruff"
ruff check dagster-gx-pipeline/pipeline dagster-gx-pipeline/tests

echo "[smoke] Mypy (if configured)"
if rg -q "\[tool\.mypy\]" dagster-gx-pipeline/pyproject.toml; then
  mypy --config-file dagster-gx-pipeline/pyproject.toml dagster-gx-pipeline/pipeline dagster-gx-pipeline/tests
else
  echo "No mypy config detected; skipping"
fi

echo "[smoke] Pytest"
pytest dagster-gx-pipeline/tests

echo "[smoke] Optional docker compose validation"
if command -v docker >/dev/null && docker compose version >/dev/null 2>&1; then
  compose_file=""
  if [[ -f docker-compose.yml ]]; then
    compose_file="docker-compose.yml"
  elif [[ -f compose.yml ]]; then
    compose_file="compose.yml"
  fi

  if [[ -n "$compose_file" ]]; then
    docker compose -f "$compose_file" config >/dev/null
    echo "Validated $compose_file"
  else
    echo "No docker compose file found; skipping"
  fi
else
  echo "Docker compose is unavailable; skipping"
fi
