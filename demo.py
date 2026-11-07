"""Run the two sample batches, then check replay and warehouse results."""
from decimal import Decimal
from pathlib import Path
import tempfile

import duckdb
from pyspark.sql import SparkSession

from pipeline import run
from warehouse import load_sql


def totals(connection):
    return connection.execute("""
        SELECT COUNT(*), COUNT(DISTINCT order_item_id),
               SUM(CASE WHEN order_status = 'COMPLETE' THEN line_amount ELSE 0 END)
        FROM analytics.fact_sales
    """).fetchone()


def main():
    raw = Path(__file__).resolve().parent / "sample" / "raw"
    spark = (SparkSession.builder.master("local[2]").appName("ecommerce-demo")
             .config("spark.sql.shuffle.partitions", 2)
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    try:
        with tempfile.TemporaryDirectory() as curated, duckdb.connect() as db:
            for batch, expected in [(1, (3, 3, Decimal("70.00"))),
                                    (2, (4, 4, Decimal("180.00")))]:
                run(spark, str(raw), curated, batch)
                db.execute(load_sql(curated, batch))
                assert totals(db) == expected
                print(f"Batch {batch}: {expected[0]} sales lines, {expected[2]} booked revenue", flush=True)
            assert db.execute("SELECT COUNT(*) FROM read_parquet(?)", [
                f"{curated}/0002/delta/fact_sales/*.parquet"
            ]).fetchone() == (3,)
            db.execute(load_sql(curated, 2))
            assert totals(db) == (4, 4, Decimal("180.00"))
            assert db.execute("SELECT stock_quantity FROM analytics.dim_product WHERE product_id = 1").fetchone() == (10,)
            assert db.execute("SELECT COUNT(*) FROM analytics.fact_payment WHERE payment_status = 'SUCCESS'").fetchone() == (3,)
            for invalid_batch in (1, 4):
                try:
                    db.execute(load_sql(curated, invalid_batch))
                except duckdb.ConstraintException:
                    db.execute("ROLLBACK")
                else:
                    raise AssertionError(f"Accepted out-of-order batch {invalid_batch}")
                assert totals(db) == (4, 4, Decimal("180.00"))
            for query in Path(__file__).with_name("analytics.sql").read_text().split(";"):
                if query.strip():
                    print(db.execute(query).fetchall())
            print("PASS: incremental updates, stale versions, duplicates, warehouse replay and batch ordering.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
