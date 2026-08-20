# Local demo runbook

This runbook covers setup, deterministic execution, evidence inspection, failure triage, and cleanup. Run all commands from the repository root.

## Prerequisites

- Python 3.11, 3.12, or 3.13.
- Git and Make.
- A writable checkout.
- Network access for initial dependency installation; demo execution itself uses recorded fixtures.

Confirm the interpreter before setup:

```bash
python --version
```

## First-time setup

```bash
make setup
make check
```

`make check` should complete before evaluating the data demo. If setup used an unsupported interpreter, select a supported Python version, clean the partial environment if needed, and rerun `make setup`.

For example, when `python3` points to an older environment:

```bash
make setup BOOTSTRAP_PYTHON=python3.12
```

## Run the successful demo

```bash
make demo
```

Expected outcome:

- a real daily-partitioned Dagster run succeeds;
- `raw_market_data`, `cleaned_market_data`, and `daily_market_summary` materialize;
- `cleaned_data_contract` and `aggregate_data_contract` pass;
- the report, GX results, and Data Docs are written under `dagster-gx-pipeline/.artifacts/`.

## Inspect in Dagster

```bash
make dev
```

Use the local Dagster UI to inspect:

1. the asset lineage from raw through aggregate;
2. the partition key on each materialization;
3. the two named asset-check results;
4. row-count and validation metadata;
5. skipped downstream work in a failed run.

Stop the development server with `Ctrl-C`.

## Exercise the expected failure

```bash
make demo-failure
```

The Make target succeeds only when it observes the expected underlying Dagster failure. Confirm that its output reports:

- `cleaned_data_contract` failed for the intentional violation;
- the run is marked unsuccessful;
- `daily_market_summary` was not materialized for that failed partition;
- the negative-price expectation was the reason for rejection.

The target uses a temporary runtime directory and removes it afterward, preserving the successful demo's local run history and artifacts. Do not convert the underlying Dagster command to an unconditional success in CI; the wrapper must continue to assert the expected contract failure.

## Runtime artifacts

```text
dagster-gx-pipeline/.artifacts/
├── data/                           Runtime DuckDB and derived datasets
├── reports/                        Human-readable and machine-readable summaries
└── great_expectations/gx/          GX file context and validation results
    └── uncommitted/data_docs/
        └── local_site/index.html   Local Data Docs entry point
```

Artifacts are reproducible and ignored by Git. In CI, download the report and Data Docs from the workflow run's **Artifacts** section.

## Triage a contract failure

1. Confirm the failing partition and check name in Dagster.
2. Open the corresponding GX Data Docs result and identify the failed expectation.
3. Compare observed row counts, schema, timestamps, and values with the recorded input or source response.
4. Decide whether the cause is bad source data, a transformation defect, or an intentionally changed business rule.
5. Fix data or code; change a contract only when its intended invariant changed.
6. Run `make check` and the targeted demo again.
7. Verify that the successful rerun materializes the downstream asset for the same logical partition.

Never bypass a blocking check merely to produce the aggregate. If urgent recovery requires a policy exception in a real deployment, record the decision and preserve the rejected data for later diagnosis.

## Common issues

### Unsupported Python version

Symptom: dependency resolution or installation fails early.

Action: use Python 3.11–3.13, recreate the local environment, and rerun `make setup`.

### Dagster UI shows no demo run

Symptom: assets are defined, but no recent materialization or checks appear.

Action: run `make demo` before `make dev` and confirm both commands use the same checkout and runtime artifact directory.

### Data Docs are missing

Symptom: the Dagster run completed, but generated GX documentation is absent.

Action: inspect the asset-check events first. A setup or GX configuration error is different from an expected contract failure. Rerun `make demo` after resolving the reported error.

### DuckDB is locked

Symptom: a write fails because another process holds the local database.

Action: stop duplicate demo or Dagster processes, close external DuckDB clients, and rerun the partition. Do not delete the database while another process is active.

### `make demo-failure` exits non-zero

Symptom: Make reports an error instead of the expected `PASS: the cleaned GX contract rejected...` message.

Action: inspect the preceding output. The target exits non-zero when invalid data unexpectedly passes or when setup, loading, or orchestration prevents the expected contract assertion from completing.

## Reset local state

```bash
make clean
```

This removes generated runtime artifacts and local tool caches. Version-controlled fixtures, GX suites, and source files remain intact. Run `make demo` to recreate a clean set of evidence.

## Before opening a pull request

```bash
make check
make demo
make demo-failure
```

Review the successful report, the failed contract diagnostics, and `git status`. The working tree should contain only intentional source or documentation changes.
