# Pipeline package guide

This directory contains the executable Dagster application for the [Dagster + Great Expectations quality-gates project](../README.md). It turns multi-row Bitcoin market observations into partitioned raw, cleaned, and daily aggregate assets stored in DuckDB.

Great Expectations contracts are exposed as blocking Dagster asset checks. Validation is therefore visible in Dagster lineage and has an operational consequence: a failed cleaned-data contract prevents the aggregate asset from running.

## Components

| Path | Responsibility |
| --- | --- |
| `src/dagster_gx_pipeline/` | Dagster definitions, assets, GX-backed checks, storage access, fixtures, and reporting. |
| `src/dagster_gx_pipeline/quality.py` | Versioned GX contract definitions and Data Docs generation. |
| `src/dagster_gx_pipeline/fixtures/` | Packaged multi-row inputs used by deterministic passing and failing runs. |
| `tests/` | Transformation, contract, persistence, and real-materialization tests. |
| `.artifacts/` | Generated DuckDB, reports, GX results, and Data Docs; never source-controlled. |

## Asset and check flow

| Stage | Output | Validation behavior |
| --- | --- | --- |
| `raw_market_data` | Partition-scoped observations persisted to DuckDB. | Preserves source values for traceability. |
| `cleaned_market_data` | Typed, normalized observations for the requested partition. | `cleaned_data_contract` validates the full cleaned partition and blocks downstream work on failure. |
| `daily_market_summary` | Daily price and observed-volume metrics for the partition. | Runs only after the cleaned contract passes. |
| `aggregate_data_contract` | GX validation evidence for the daily summary. | Marks the run failed if the aggregate contract is violated. |

The job is daily-partitioned. The deterministic demo supplies a fixed partition and fixture so local runs and CI produce the same result.

## Run the application

From a repository clone, use the root automation:

```bash
cd ..
make setup
make demo
```

Useful commands from the repository root:

```bash
make dev           # Open Dagster's local UI
make check         # Run automated repository checks
make demo-failure  # Confirm bad data blocks the aggregate
make clean         # Remove generated local state
```

Python 3.11–3.13 is supported. `make demo` uses recorded data and requires no API credentials or network access after dependency installation.

When installed as a wheel, the package exposes equivalent console commands:

```bash
dagster-gx-demo --partition 2024-05-01
dagster-gx-validate --partition 2024-05-01
dagster-gx-report
```

The installed demo also uses packaged deterministic data by default. Pass `--live` to
`dagster-gx-demo` only when you intentionally want to query CoinGecko. Outside a repository clone,
runtime output defaults to `.dagster-gx-pipeline/` beneath the current working directory; set
`PIPELINE_RUNTIME_DIR` to override it.

## Runtime output

All generated state lives below `.artifacts/`:

```text
.artifacts/
├── data/                           DuckDB and runtime datasets
├── reports/                        Markdown and machine-readable run summaries
└── great_expectations/gx/          GX file context and generated validation state
    └── uncommitted/data_docs/
        └── local_site/index.html   Local Data Docs entry point
```

Keeping runtime state separate makes a demo easy to inspect, reset with `make clean`, and archive from CI without mixing generated output with source-controlled contracts.

## Extend an asset

1. Add or update the asset implementation under `src/dagster_gx_pipeline/`.
2. Preserve partition propagation and persist only data for the requested partition.
3. Add output metadata that makes the materialization inspectable in Dagster.
4. Add transformation and persistence assertions under `tests/`.
5. Run `make check`, `make demo`, and the relevant failure scenario.

## Extend a quality contract

1. Change the appropriate contract in `src/dagster_gx_pipeline/quality.py`.
2. Keep the Dagster check name stable unless the contract's public meaning changes.
3. Add a passing fixture assertion and a targeted violating case.
4. Assert both the GX result and Dagster's blocking behavior.
5. Review the generated report and Data Docs before committing.

See [Quality gates](../docs/quality-gates.md) for the contract boundary and [Architecture](../docs/architecture.md) for the execution sequence.

## Continuous integration

CI runs the automated checks on every supported Python version and executes the deterministic
materialization on Python 3.13. The latest Markdown report and GX Data Docs are uploaded from the
workflow so reviewers can verify data-quality behavior without reproducing the run locally.
