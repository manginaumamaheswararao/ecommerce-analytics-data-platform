"""Glue Spark script and local Spark entry point."""
import argparse

from pyspark.sql import SparkSession, Window, functions as F

SOURCES = {
    "customers": ("customer_id", "customer_id bigint, customer_name string"),
    "products": ("product_id", "product_id bigint, product_name string, category string, stock_quantity int"),
    "orders": ("order_id", "order_id bigint, customer_id bigint, order_date date, city string, state string, order_status string"),
    "order_items": ("order_item_id", "order_item_id bigint, order_id bigint, product_id bigint, quantity int, unit_price decimal(12,2)"),
    "payments": ("payment_id", "payment_id bigint, order_id bigint, payment_date date, payment_status string, amount decimal(14,2)"),
}
# Column order is also the Redshift COPY contract.
TABLES = {
    "dim_customer": ("customer_id", "customer_id bigint, customer_name varchar(256)"),
    "dim_product": ("product_id", "product_id bigint, product_name varchar(256), category varchar(256), stock_quantity int"),
    "dim_date": ("date_key", "date_key int, calendar_date date"),
    "dim_location": ("location_key", "location_key varchar(64), city varchar(256), state varchar(256)"),
    "fact_sales": ("order_item_id", "order_item_id bigint, order_id bigint, customer_id bigint, product_id bigint, date_key int, location_key varchar(64), order_status varchar(256), quantity int, unit_price decimal(12,2), line_amount decimal(18,2)"),
    "fact_payment": ("payment_id", "payment_id bigint, order_id bigint, customer_id bigint, date_key int, payment_status varchar(256), amount decimal(14,2)"),
}


def require_empty(frame, label):
    if frame.take(1):
        raise ValueError(label)


def source_state(spark, raw, previous, name, key, schema):
    incoming = (spark.read
                .schema(schema + ", updated_at timestamp")
                .options(header=True, enforceSchema=False, mode="FAILFAST")
                .csv(f"{raw}/{name}.csv"))
    invalid = F.col(key) <= 0
    for field in incoming.schema.fields:
        column = F.col(field.name)
        invalid = invalid | column.isNull()
        if field.dataType.simpleString() == "string":
            invalid = invalid | (F.length(F.trim(column)) == 0) | (F.length(F.encode(column, "UTF-8")) > 256)
    require_empty(incoming.where(invalid), f"{name}: invalid or missing value")
    versions = incoming
    if previous:
        versions = versions.unionByName(spark.read.parquet(f"{previous}/state/{name}"))
    versions = versions.distinct()
    require_empty(
        versions.groupBy(key, "updated_at").count().where("count > 1"),
        f"{name}: conflicting rows at the same source timestamp",
    )
    rank = Window.partitionBy(key).orderBy(F.col("updated_at").desc())
    return versions.withColumn("_rank", F.row_number().over(rank)).where("_rank = 1").drop("_rank")


def date_key(column):
    return F.date_format(column, "yyyyMMdd").cast("int")


def warehouse_model(data):
    customers, products, orders, items, payments = (data[name] for name in SOURCES)
    require_empty(products.where("stock_quantity < 0"), "products: negative stock")
    require_empty(items.where("quantity <= 0 OR unit_price < 0"), "order_items: invalid quantity or price")
    require_empty(payments.where("amount < 0"), "payments: negative amount")
    for child, parent, key in [
        (orders, customers, "customer_id"), (items, orders, "order_id"),
        (items, products, "product_id"), (payments, orders, "order_id"),
    ]:
        require_empty(child.join(parent.select(key), key, "left_anti"), f"Missing parent for {key}")
    orders = orders.withColumn("location_key", F.sha2(F.to_json(F.struct("city", "state")), 256))
    dates = orders.select(F.col("order_date").alias("calendar_date")).union(
        payments.select("payment_date")
    ).distinct()
    sales = (items.join(orders.drop("updated_at"), "order_id")
             .withColumn("date_key", date_key("order_date"))
             .withColumn("line_amount", (F.col("unit_price") * F.col("quantity")).cast("decimal(18,2)")))
    payment_facts = (payments.join(orders.select("order_id", "customer_id"), "order_id")
                     .withColumn("date_key", date_key("payment_date")))
    return {
        "dim_customer": customers, "dim_product": products,
        "dim_date": dates.withColumn("date_key", date_key("calendar_date")),
        "dim_location": orders.select("location_key", "city", "state").distinct(),
        "fact_sales": sales, "fact_payment": payment_facts,
    }


def run(spark, raw_root, curated_root, batch):
    if batch < 1:
        raise ValueError("Batch must start at 1")
    output = f"{curated_root.rstrip('/')}/{batch:04d}"
    previous = f"{curated_root.rstrip('/')}/{batch - 1:04d}" if batch > 1 else None
    raw = f"{raw_root.rstrip('/')}/{batch:04d}"
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.ansi.enabled", "true")
    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    # Redshift scans each delta prefix as Parquet, so suppress Hadoop marker files.
    spark.sparkContext._jsc.hadoopConfiguration().set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    data = {
        name: source_state(spark, raw, previous, name, key, schema).persist()
        for name, (key, schema) in SOURCES.items()
    }
    model = warehouse_model(data)
    for name, frame in data.items():
        frame.write.mode("overwrite").parquet(f"{output}/state/{name}")
    for name, frame in model.items():
        columns = [definition.split()[0] for definition in TABLES[name][1].split(", ")]
        snapshot = frame.select(*columns).persist()
        snapshot.write.mode("overwrite").parquet(f"{output}/model/{name}")
        delta = snapshot
        if previous:
            delta = snapshot.subtract(spark.read.parquet(f"{previous}/model/{name}").select(*columns))
        # Coalesce the tiny demo to one file per table, including empty deltas.
        delta.coalesce(1).write.mode("overwrite").parquet(f"{output}/delta/{name}")
        snapshot.unpersist()
    for frame in data.values():
        frame.unpersist()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", required=True)
    parser.add_argument("--curated", required=True)
    parser.add_argument("--batch", type=int, required=True)
    args, _ = parser.parse_known_args()  # Glue adds its own job arguments.
    session = SparkSession.builder.appName("ecommerce-analytics").getOrCreate()
    try:
        run(session, args.raw, args.curated, args.batch)
    finally:
        session.stop()
