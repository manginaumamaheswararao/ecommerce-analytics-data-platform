# Validation

## Local results

Run `python demo.py` from the repository root. The demo creates temporary Parquet output and an in-memory DuckDB warehouse. It removes the temporary data after completion.

| Check | Result |
|---|---|
| Initial batch | 3 unique order items and 70.00 completed-order revenue |
| Incremental batch | 3 changed sales rows, 4 warehouse order items and 180.00 revenue |
| Latest batch replay | 4 warehouse order items and 180.00 revenue |
| Older product version | Current stock remains 10 |
| Exact duplicate item | One warehouse row for the item |
| Payments | 4 attempts and 3 successes |
| Repeat customers | Customer 1 has 2 completed orders |
| Warehouse load sequence | Older and skipped batches fail and preserve prior totals |

The local run exercises Spark transformations and Parquet output. DuckDB validates the staging/upsert logic. It does not validate Redshift's COPY syntax, permissions or runtime behavior.

## AWS acceptance checklist

After checking account credits, plan and expected service costs:

1. Upload the two sample batches to S3 and verify five CSV files in each numbered prefix.
2. Run Glue batch 1 successfully and inspect its Parquet output.
3. Generate and execute batch 1 Redshift SQL in one session. Confirm 3 sales rows and 70.00 completed-order revenue.
4. Run Glue batch 2 and load its deltas. Confirm 4 sales rows and 180.00 revenue.
5. Execute batch 2 warehouse SQL again. Confirm counts and revenue are unchanged.
6. Execute all queries in analytics.sql. Confirm the payment success rate is 0.75.
7. Record Glue run IDs, timestamps, ledger rows and query results without credentials.
8. Delete demonstration compute resources and retained output when finished, after preserving the evidence you need.

## Design choices and limits

- Synthetic data represents a single currency. Current inventory is a product attribute, so the inventory query describes current stock.
- Sales facts retain the order ID at order-line grain. Payments remain separate to avoid multiplying revenue across payment attempts.
- Dimensions use Type 1 current attributes and source IDs. Location uses a deterministic hash, and dates use YYYYMMDD keys.
- Input batches are immutable upserts with full row images. New and updated records are supported. Order cancellations use status changes.
- Spark reads the current source state and rebuilds the model before computing changed rows. Only the warehouse writes are incremental.
- The demo coalesces delta output to one file per table. Larger deployments need distributed file sizing and targeted processing of affected keys.
- Run one batch at a time, publish only successful Glue output, and advance only after the warehouse transaction commits. Rerun failed Spark output from the same immutable inputs.
- Physical deletes, streaming ingestion, automatic orchestration and production scale benchmarks are future extensions.
