# Dagster Pipeline + Great Expectations Quality Gates

![CI](https://github.com/aelsaeed/Dagster-Pipeline-Great-Expectations-Quality-Gates/actions/workflows/ci.yml/badge.svg)

A local-first data engineering demo using Dagster assets, DuckDB, and Great Expectations quality gates.

## Quickstart

```bash
make setup
make run
make validate
make report
```

## Demo (1-minute evaluable)

```bash
make demo
```

`make demo` runs deterministic tiny-mode materialization, validations, and report generation using a recorded fixture payload (no flaky API calls).

## Development commands

- `make dev` — start Dagster UI.
- `make run` — materialize assets end-to-end on tiny deterministic input.
- `make validate` — run Great Expectations checkpoints and build Data Docs.
- `make report` — generate `dagster-gx-pipeline/reports/latest.md`.
- `make lint` / `make typecheck` / `make test` — local quality gates.

## Data Docs

After `make validate`, open:

`dagster-gx-pipeline/data_docs/index.html`

## Recorded fixture mode (CI-safe)

The pipeline includes a deterministic fixture mode backed by `dagster-gx-pipeline/data/sample_api_payload.json`. CI uses this mode through `make run` + `make validate` so builds are stable and do not depend on live API availability.

## Architecture & Lineage

```text
CoinGecko API / Recorded Fixture
            |
        raw_asset
            |
      cleaned_asset
            |
        agg_asset
            |
 Great Expectations checkpoints
            |
       Data Docs + report
```

Mermaid architecture source is also available in `dagster-gx-pipeline/README.md`.

## Testing / CI

GitHub Actions (`.github/workflows/ci.yml`) runs on pull requests and pushes:

1. `make lint`
2. `make typecheck`
3. `make test`
4. `make run`
5. `make validate`
6. `make report`

## Troubleshooting

- If dependencies are missing, rerun `make setup`.
- If Dagster cannot write state, ensure `DAGSTER_HOME` is writable.
- If Data Docs are missing, rerun `make validate` and inspect `dagster-gx-pipeline/expectations/validations`.
