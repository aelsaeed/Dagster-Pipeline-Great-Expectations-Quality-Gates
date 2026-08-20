PROJECT_DIR := dagster-gx-pipeline
VENV_DIR := $(CURDIR)/$(PROJECT_DIR)/.venv
BOOTSTRAP_PYTHON ?= python3
PYTHON ?= $(VENV_DIR)/bin/python
RUNTIME_DIR ?= $(CURDIR)/$(PROJECT_DIR)/.artifacts
DAGSTER_HOME ?= $(RUNTIME_DIR)/dagster
DEMO_PARTITION ?= 2024-05-01
INVALID_FIXTURE := $(CURDIR)/$(PROJECT_DIR)/src/dagster_gx_pipeline/fixtures/invalid_negative_price.json

.PHONY: setup sync lock lint format format-check typecheck test definitions precommit check audit package \
	demo demo-failure dev run validate report clean

setup:
	$(BOOTSTRAP_PYTHON) -c 'import sys; version = sys.version_info[:2]; sys.exit(f"Python 3.11-3.13 is required; got {sys.version.split()[0]}") if not (3, 11) <= version < (3, 14) else None'
	$(BOOTSTRAP_PYTHON) -m venv $(VENV_DIR)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ./$(PROJECT_DIR)[dev]

sync:
	uv sync --project $(PROJECT_DIR) --extra dev --locked

lock:
	uv lock --project $(PROJECT_DIR)

lint:
	$(PYTHON) -m ruff check --config $(PROJECT_DIR)/pyproject.toml $(PROJECT_DIR)/src $(PROJECT_DIR)/tests

format:
	$(PYTHON) -m ruff format --config $(PROJECT_DIR)/pyproject.toml $(PROJECT_DIR)/src $(PROJECT_DIR)/tests

format-check:
	$(PYTHON) -m ruff format --check --config $(PROJECT_DIR)/pyproject.toml $(PROJECT_DIR)/src $(PROJECT_DIR)/tests

typecheck:
	cd $(PROJECT_DIR) && $(PYTHON) -m mypy src tests

test:
	cd $(PROJECT_DIR) && $(PYTHON) -m pytest

definitions:
	cd $(PROJECT_DIR) && $(PYTHON) -c \
		'from dagster import Definitions; from dagster_gx_pipeline.definitions import defs; Definitions.validate_loadable(defs)'

precommit:
	$(PYTHON) -m pre_commit run --all-files

check: format-check lint typecheck test definitions

audit:
	$(PYTHON) -m pip_audit

package:
	cd $(PROJECT_DIR) && $(PYTHON) -m build
	$(PYTHON) -m twine check $(PROJECT_DIR)/dist/*

run:
	PIPELINE_RUNTIME_DIR=$(RUNTIME_DIR) DAGSTER_HOME=$(DAGSTER_HOME) \
		$(PYTHON) -m dagster_gx_pipeline.runner --partition $(DEMO_PARTITION)

validate:
	PIPELINE_RUNTIME_DIR=$(RUNTIME_DIR) DAGSTER_HOME=$(DAGSTER_HOME) \
		$(PYTHON) -m dagster_gx_pipeline.validate --partition $(DEMO_PARTITION)

report:
	PIPELINE_RUNTIME_DIR=$(RUNTIME_DIR) DAGSTER_HOME=$(DAGSTER_HOME) \
		$(PYTHON) -m dagster_gx_pipeline.report

demo:
	@$(MAKE) --no-print-directory run validate report
	@echo ""
	@echo "Demo complete"
	@echo "Report: $(RUNTIME_DIR)/reports/latest.md"
	@echo "Data Docs: $(RUNTIME_DIR)/great_expectations/gx/uncommitted/data_docs/local_site/index.html"

demo-failure:
	@failure_runtime="$$(mktemp -d)"; \
	trap 'rm -rf "$$failure_runtime"' EXIT; \
	PIPELINE_RUNTIME_DIR="$$failure_runtime" DAGSTER_HOME="$$failure_runtime/dagster" \
		$(PYTHON) -m dagster_gx_pipeline.runner --partition $(DEMO_PARTITION) \
		--fixture $(INVALID_FIXTURE); \
	runner_status=$$?; \
	if [ "$$runner_status" -eq 0 ]; then \
		echo "ERROR: invalid data unexpectedly passed"; \
		exit 1; \
	fi; \
	if ! $(PYTHON) -c \
		'import json, sys; from pathlib import Path; summary = json.loads(Path(sys.argv[1]).read_text()); checks = summary.get("asset_checks", []); expected = not summary.get("success") and "daily_market_summary" not in summary.get("assets_materialized", []) and any(check.get("check") == "cleaned_data_contract" and check.get("passed") is False for check in checks); raise SystemExit(0 if expected else 1)' \
		"$$failure_runtime/reports/last_materialization.json"; then \
		echo "ERROR: the run failed without proving the expected blocking quality gate"; \
		exit 1; \
	fi; \
	echo "PASS: the cleaned GX contract rejected the negative price and blocked aggregation"

dev:
	cd $(PROJECT_DIR) && PIPELINE_RUNTIME_DIR=$(RUNTIME_DIR) DAGSTER_HOME=$(DAGSTER_HOME) \
		$(PYTHON) -c 'from dagster_gx_pipeline.settings import get_settings; get_settings().ensure_runtime_dirs()'
	cd $(PROJECT_DIR) && PIPELINE_RUNTIME_DIR=$(RUNTIME_DIR) DAGSTER_HOME=$(DAGSTER_HOME) \
		$(PYTHON) -m dagster dev -m dagster_gx_pipeline.definitions

clean:
	rm -rf .coverage .dagster .mypy_cache .pytest_cache .ruff_cache \
		$(PROJECT_DIR)/.artifacts $(PROJECT_DIR)/.coverage $(PROJECT_DIR)/.dagster \
		$(PROJECT_DIR)/.mypy_cache \
		$(PROJECT_DIR)/.pytest_cache $(PROJECT_DIR)/.ruff_cache $(PROJECT_DIR)/build \
		$(PROJECT_DIR)/data_docs $(PROJECT_DIR)/dist $(PROJECT_DIR)/gx \
		$(PROJECT_DIR)/reports $(PROJECT_DIR)/src/*.egg-info \
		$(PROJECT_DIR)/data/*.duckdb $(PROJECT_DIR)/data/*.duckdb.* \
		$(PROJECT_DIR)/expectations/validations $(PROJECT_DIR)/pipeline/__pycache__
	find $(PROJECT_DIR)/src $(PROJECT_DIR)/tests -type d -name '__pycache__' -prune -exec rm -rf {} +
