# Quality gates

## Contract model

Great Expectations defines what valid data looks like. Dagster decides what a failed validation means for execution.

Each contract is exposed as a named Dagster asset check backed by a GX validation result:

| Check | Asset | Contract focus | Execution consequence |
| --- | --- | --- | --- |
| `cleaned_data_contract` | `cleaned_market_data` | Exact schema, 3–10,000 rows, unique timestamps, required market values, positive numeric ranges, and parseable fetch time. | Blocking: `daily_market_summary` is skipped and the run fails. |
| `aggregate_data_contract` | `daily_market_summary` | Exact schema, one summary row, partition uniqueness, observation count, positive ranges, and `min <= average <= max`. | The run fails and the partition is not reported as successful. |

The GX suites are defined in `dagster-gx-pipeline/src/dagster_gx_pipeline/quality.py`. GX's standard file context and validation state live under `dagster-gx-pipeline/.artifacts/great_expectations/gx/`; the Data Docs entry point is `uncommitted/data_docs/local_site/index.html` within that directory.

## Why both GX and Dagster checks?

GX supplies expressive expectations and detailed diagnostics. A GX result alone does not define orchestration behavior. Dagster asset checks attach that result to the asset graph, keep check history with the partition, and enforce the blocking dependency before downstream execution.

The integration follows four rules:

1. Validate the complete target partition, not an arbitrary latest row.
2. Treat a missing, empty, or unsuccessful expected validation as failure.
3. Preserve GX diagnostics in generated evidence and Dagster metadata.
4. Return a non-zero status when a blocking contract fails.

## Passing demonstration

```bash
make demo
```

The passing fixture contains multiple observations so cleaning and daily aggregation are meaningful. A successful run should show both named checks passing, materialize `daily_market_summary`, and write the report and Data Docs beneath `.artifacts/`.

## Blocking failure demonstration

```bash
make demo-failure
```

The failure is intentional. The underlying Dagster job returns non-zero after `cleaned_data_contract` rejects the violating fixture, and `daily_market_summary` must not appear as a materialization for that failed partition. The `make demo-failure` wrapper treats this expected rejection as a successful assertion and returns non-zero only if invalid data unexpectedly passes or the assertion cannot be completed.

This scenario is more than a unit test of a predicate: it goes through the real partitioned Dagster job and proves that a GX failure changes downstream orchestration.

## Evidence to inspect

After a normal demo run, use three complementary views:

- **Dagster UI:** check status, partition, metadata, skipped downstream asset, and run events.
- **GX Data Docs:** expectation-level successes and failures with observed values.
- **Markdown report:** compact partition, row-count, and overall contract summary suitable for CI review.

CI uploads the report and Data Docs from the workflow run. It should never upload a report marked successful when a required contract failed.

`make demo-failure` uses a temporary runtime directory and removes it afterward so the deliberate failure cannot overwrite the passing demo's evidence. Its console output is the durable proof for that assertion.

## Changing a contract safely

1. State the business or technical invariant the new expectation protects.
2. Update the version-controlled GX suite rather than embedding duplicate validation logic in an asset.
3. Add or update a valid multi-row fixture case.
4. Add a narrowly violating case that proves the expectation can fail.
5. Assert the GX result, Dagster check result, process status, and downstream blocking behavior.
6. Run `make check`, `make demo`, and `make demo-failure`.
7. Review the generated Data Docs for readable diagnostics.

If a rule is intentionally non-blocking, give it a separate check name and document why downstream consumption remains safe. Do not silently weaken one of the two public blocking contracts.

## Contract review checklist

- Does the suite validate every row in the requested partition?
- Is the key definition meaningful for multi-row market data?
- Are nullability and numeric ranges explicit?
- Are source timestamps consistent with the partition?
- Do aggregate invariants reconcile with cleaned inputs?
- Does a failed result carry enough context to diagnose the offending expectation?
- Is there a test demonstrating both the passing and failing behavior?

## Current boundary

These checks protect the local pipeline's transformation and aggregate layers. They do not yet provide historical quality trendlines, alert routing, or formal compatibility/versioning for contract changes. Those extensions are tracked in the [roadmap](../ROADMAP.md).
