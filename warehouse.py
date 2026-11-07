"""Generate transactional Redshift SQL or execute the same load locally."""
import argparse
import re

from pipeline import TABLES


def load_sql(curated, batch, role=None):
    if batch < 1:
        raise ValueError("Batch must start at 1")
    if role and not (
        re.fullmatch(r"s3://[a-z0-9][a-z0-9.-]*(?:/[A-Za-z0-9_./-]*)?", curated)
        and re.fullmatch(r"arn:[a-z0-9-]+:iam::[0-9]{12}:role/[A-Za-z0-9_+=,.@/-]+", role)
    ):
        raise ValueError("Provide a curated S3 URI and an IAM role ARN")
    statements = [
        "BEGIN",
        "CREATE SCHEMA IF NOT EXISTS analytics",
        "CREATE TABLE IF NOT EXISTS analytics.loaded_batch (batch_id int NOT NULL)",
    ]
    if role:
        statements.append("LOCK TABLE analytics.loaded_batch")
    statements += [
        "CREATE TEMP TABLE batch_guard (batch_id int NOT NULL)",
        f"""INSERT INTO batch_guard
SELECT CASE WHEN COALESCE(MAX(batch_id), 0) IN ({batch - 1}, {batch}) THEN {batch} END
FROM analytics.loaded_batch""",
        "DROP TABLE batch_guard",
    ]
    for table, (key, schema) in TABLES.items():
        path = f"{curated.rstrip('/')}/{batch:04d}/delta/{table}/".replace("'", "''")
        stage = f"stage_{table}"
        target = f"analytics.{table}"
        statements.extend([
            f"CREATE TABLE IF NOT EXISTS {target} ({schema})",
            f"CREATE TEMP TABLE {stage} ({schema})",
            (f"COPY {stage} FROM '{path}' IAM_ROLE '{role}' FORMAT AS PARQUET"
             if role else f"INSERT INTO {stage} SELECT * FROM read_parquet('{path}*.parquet')"),
            f"DELETE FROM {target} USING {stage} WHERE {target}.{key} = {stage}.{key}",
            f"INSERT INTO {target} SELECT * FROM {stage}",
            f"DROP TABLE {stage}",
        ])
    statements += [
        f"DELETE FROM analytics.loaded_batch WHERE batch_id = {batch}",
        f"INSERT INTO analytics.loaded_batch VALUES ({batch})",
        "COMMIT",
    ]
    return ";\n".join(statements) + ";\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--curated", required=True)
    parser.add_argument("--batch", type=int, required=True)
    destination = parser.add_mutually_exclusive_group(required=True)
    destination.add_argument("--redshift-role")
    destination.add_argument("--duckdb")
    args = parser.parse_args()
    sql = load_sql(args.curated, args.batch, args.redshift_role)
    if args.duckdb:
        import duckdb

        with duckdb.connect(args.duckdb) as connection:
            connection.execute(sql)
    else:
        print(sql, end="")
