# Roadmap

## Milestone 1: Stabilize demo
- [ ] Guarantee `make demo` succeeds on a clean machine with deterministic sample data.
- [ ] Add smoke checks to keep core workflows (lint, typecheck, tests) consistently green.
- [ ] Improve failure messages and troubleshooting guidance for first-time contributors.

## Milestone 2: Observability + metrics
- [ ] Add run metrics (row counts, validation success rate, freshness) to generated reports.
- [ ] Expose basic pipeline health telemetry for local and CI runs.
- [ ] Track data quality trendlines across partitions.

## Milestone 3: Hardening + docs
- [ ] Expand test coverage for IO failures and schema drift.
- [ ] Add release process, versioning guidance, and environment promotion notes.
- [ ] Publish architecture decision records and operational runbooks.
