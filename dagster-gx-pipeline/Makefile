DAGSTER_HOME?=$(CURDIR)/.dagster
PYTHON?=python

install:
	$(PYTHON) -m pip install -e .[dev]

lint:
	ruff check pipeline tests

format:
	ruff format pipeline tests

typecheck:
	mypy pipeline tests

test:
	pytest

dev:
	DAGSTER_HOME=$(DAGSTER_HOME) $(PYTHON) -m dagster dev -f pipeline/definitions.py

run:
	DAGSTER_HOME=$(DAGSTER_HOME) $(PYTHON) -m dagster job execute -f pipeline/definitions.py -j daily_job --partition=$(shell date +%F)

validate:
	DAGSTER_HOME=$(DAGSTER_HOME) $(PYTHON) -m pipeline.validate

report:
	DAGSTER_HOME=$(DAGSTER_HOME) $(PYTHON) -m pipeline.report
