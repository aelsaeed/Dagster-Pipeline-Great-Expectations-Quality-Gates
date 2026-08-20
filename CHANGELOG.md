# Changelog

Notable project changes are documented here using the structure from [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Blocking Dagster asset checks backed by Great Expectations contracts: `cleaned_data_contract` and `aggregate_data_contract`.
- Deterministic multi-row fixtures for successful runs and an intentional contract-failure scenario.
- Partition-aligned CoinGecko range ingestion with payload-shape validation, bounded retries, and optional API-key support.
- `make demo-failure` to assert that failed cleaned data makes the underlying job fail and prevents downstream aggregate materialization.
- CI artifacts containing the generated Markdown run report and GX Data Docs.
- Architecture, quality-gate, and operational runbook documentation.
- A root `.gitignore` covering Python build output, tool caches, Dagster state, DuckDB files, and generated GX artifacts.
- A committed `uv.lock`, Python-version matrix, dependency audit, and grouped Dependabot updates.
- Installable console commands, packaged fixtures, and verified wheel/sdist builds.
- Passing and failure-path coverage for API, storage, contracts, scheduling, reports, and real Dagster execution.

### Changed

- Upgraded the supported stack to Dagster 1.13, Great Expectations 1.21, and Python 3.11–3.13.
- Replaced direct calls to decorated asset functions with a real partitioned Dagster job in demos and integration tests.
- Replaced single-row extraction with timestamp-aligned multi-row normalization and meaningful daily aggregation.
- Made DuckDB partition replacement transactional and explicit about inserted columns.
- Consolidated generated data, reports, and GX state under `dagster-gx-pipeline/.artifacts/`.
- Simplified the developer interface around `make setup`, `make demo`, `make demo-failure`, `make dev`, `make check`, and `make clean`.
- Moved the Python package to the collision-safe `src/dagster_gx_pipeline/` layout and isolated local setup in `.venv`.
- Reframed the README around observable failure behavior, architecture decisions, and reproducible evidence.

### Fixed

- Data-contract failures now produce an unsuccessful Dagster run and non-zero command status.
- Cleaned validation evaluates the complete target partition rather than a single row.
- The daily aggregate no longer materializes after a failed cleaned-data contract.
- Generated runtime output no longer pollutes the Git working tree.

## Initial implementation

### Added

- Daily-partitioned raw, cleaned, and aggregate assets for Bitcoin market data.
- Local DuckDB persistence and a recorded CoinGecko payload.
- Great Expectations suites, Data Docs generation, basic reporting, tests, and CI.
- Contributor guidance, issue templates, a pull request template, and an MIT license.
