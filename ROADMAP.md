# Roadmap

The roadmap distinguishes capabilities demonstrated today from deliberate production extensions. Completed items are backed by runnable commands or CI evidence.

## Demonstrated baseline

- [x] Execute deterministic data through a real daily-partitioned Dagster job.
- [x] Normalize and aggregate multi-row market observations in DuckDB.
- [x] Expose GX suites as blocking Dagster checks named `cleaned_data_contract` and `aggregate_data_contract`.
- [x] Provide passing and deliberately failing demos with meaningful process exit codes.
- [x] Validate CoinGecko payload structure and require source timestamps to match the requested UTC partition.
- [x] Retry bounded transient API and rate-limit failures on the optional live path.
- [x] Replace DuckDB partitions in an explicit transaction using named columns.
- [x] Keep runtime databases, reports, and GX output under `.artifacts/`.
- [x] Run automated checks across Python 3.11–3.13.
- [x] Upload the generated report and Data Docs as CI evidence.
- [x] Document architecture, contract behavior, and operational recovery.

## Next: reliability and source correctness

- [ ] Evolve structural payload checks into a versioned, typed source contract.
- [ ] Publish retry, rate-limit, and source-latency metrics.
- [ ] Define coordination and retry behavior for concurrent DuckDB backfills.
- [ ] Add fault-injection coverage for interrupted writes and prolonged upstream outages.
- [ ] Add a quarantine path that preserves rejected live payloads for diagnosis.

## Next: observability and contract evolution

- [ ] Track validation success rates, row counts, and freshness across partitions.
- [ ] Publish contract version and expectation-level statistics as Dagster metadata.
- [ ] Add alert routing for failed blocking checks and stale partitions.
- [ ] Document compatibility rules for changing schemas and GX suites.
- [ ] Add an architecture decision record for the storage and contract boundaries.

## Future: deployment reference

- [ ] Add a containerized deployment profile without changing the zero-infrastructure local demo.
- [ ] Demonstrate a warehouse-backed storage adapter behind the same asset contracts.
- [ ] Add environment-specific secrets and configuration guidance.
- [ ] Publish a backfill and incident-response example using multiple partitions.

The project intentionally prioritizes depth in orchestration and data-quality failure behavior over adding unrelated platform components.
