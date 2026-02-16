PYTHON ?= python
PROJECT_DIR := dagster-gx-pipeline
DAGSTER_HOME ?= $(CURDIR)/.dagster

.PHONY: setup lint typecheck test fmt demo dev run validate report clean

setup:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -e ./$(PROJECT_DIR)[dev]
	$(PYTHON) -m pip install pre-commit
	pre-commit install

lint:
	ruff check $(PROJECT_DIR)/pipeline $(PROJECT_DIR)/tests

typecheck:
	mypy --config-file $(PROJECT_DIR)/pyproject.toml $(PROJECT_DIR)/pipeline $(PROJECT_DIR)/tests

test:
	pytest $(PROJECT_DIR)/tests

fmt:
	ruff format $(PROJECT_DIR)/pipeline $(PROJECT_DIR)/tests

dev:
	DAGSTER_HOME=$(DAGSTER_HOME) $(PYTHON) -m dagster dev -f $(PROJECT_DIR)/pipeline/definitions.py

run:
	bash scripts/run_tiny.sh

validate:
	cd $(PROJECT_DIR) && DAGSTER_HOME=$(DAGSTER_HOME) $(PYTHON) -m pipeline.validate

report:
	cd $(PROJECT_DIR) && DAGSTER_HOME=$(DAGSTER_HOME) $(PYTHON) -m pipeline.report

demo: run validate report

clean:
	rm -rf .mypy_cache .pytest_cache .ruff_cache .dagster reports data_docs
	find $(PROJECT_DIR) -type d -name '__pycache__' -prune -exec rm -rf {} +
	find $(PROJECT_DIR) -type f -name '*.pyc' -delete
