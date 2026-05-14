# /// script
# dependencies = ["pyspark==3.5.1"]
# ///

import argparse
import os
from dataclasses import dataclass

from pyspark.sql import SparkSession


DEFAULT_NAMESPACE = "default"
DEFAULT_TABLE = "hogflare_events_v3"


@dataclass
class DuplicateSet:
    label: str
    rows: list[tuple[str, int]]


@dataclass
class CatalogConfig:
    account_id: str
    bucket: str
    namespace: str
    table: str

    @property
    def warehouse(self) -> str:
        return f"{self.account_id}_{self.bucket}"

    @property
    def catalog_uri(self) -> str:
        return f"https://catalog.cloudflarestorage.com/{self.account_id}/{self.bucket}"

    @property
    def qualified_table(self) -> str:
        return f"r2.{self.namespace}.{self.table}"


def build_spark(token: str, config: CatalogConfig) -> SparkSession:
    return (
        SparkSession.builder.appName("hogflare-dedupe-imported-posthog-rows")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
            "org.apache.iceberg:iceberg-aws-bundle:1.6.1",
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.r2", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.r2.type", "rest")
        .config("spark.sql.catalog.r2.uri", config.catalog_uri)
        .config("spark.sql.catalog.r2.warehouse", config.warehouse)
        .config("spark.sql.catalog.r2.token", token)
        .config(
            "spark.sql.catalog.r2.header.X-Iceberg-Access-Delegation",
            "vended-credentials",
        )
        .config("spark.sql.catalog.r2.s3.remote-signing-enabled", "false")
        .config("spark.sql.defaultCatalog", "r2")
        .getOrCreate()
    )


def catalog_config_from_args(args: argparse.Namespace) -> CatalogConfig:
    account_id = args.account_id or os.environ.get("IMPORT_TARGET_ACCOUNT_ID") or os.environ.get(
        "CLOUDFLARE_ACCOUNT_ID"
    )
    bucket = args.bucket or os.environ.get("IMPORT_TARGET_BUCKET") or os.environ.get("R2_SQL_BUCKET")
    namespace = args.namespace or os.environ.get("IMPORT_TARGET_NAMESPACE") or DEFAULT_NAMESPACE
    table = args.table or os.environ.get("IMPORT_TARGET_TABLE") or DEFAULT_TABLE

    if not account_id:
        raise SystemExit("set IMPORT_TARGET_ACCOUNT_ID or pass --account-id")
    if not bucket:
        raise SystemExit("set IMPORT_TARGET_BUCKET or pass --bucket")
    if "." in table:
        namespace, table = table.rsplit(".", 1)

    return CatalogConfig(
        account_id=account_id,
        bucket=bucket,
        namespace=namespace,
        table=table,
    )


def token_from_env() -> str:
    token = os.environ.get("R2_DATA_CATALOG_TOKEN") or os.environ.get("WRANGLER_R2_SQL_AUTH_TOKEN")
    if not token:
        raise SystemExit("set R2_DATA_CATALOG_TOKEN to an R2 Admin Read & Write token")
    return token


def collect_duplicate_set(spark: SparkSession, label: str, query: str) -> DuplicateSet:
    rows = spark.sql(query).collect()
    return DuplicateSet(label=label, rows=[(row.file, int(row.pos)) for row in rows])


def collect_duplicates(spark: SparkSession, table: str) -> list[DuplicateSet]:
    event_duplicates = collect_duplicate_set(
        spark,
        "events",
        f"""
        select file, pos
        from (
            select
                _file as file,
                _pos as pos,
                row_number() over (
                    partition by uuid
                    order by __ingest_ts asc, timestamp asc, _file asc, _pos asc
                ) as rn
            from {table}
            where source = 'posthog'
              and extra like '%"hogflare_import":true%'
              and extra not like '%"hogflare_import_kind":"person"%'
              and extra not like '%"hogflare_import_kind":"group"%'
              and uuid is not null
        )
        where rn > 1
        """,
    )

    person_duplicates = collect_duplicate_set(
        spark,
        "persons",
        f"""
        select file, pos
        from (
            select
                _file as file,
                _pos as pos,
                row_number() over (
                    partition by distinct_id
                    order by __ingest_ts asc, timestamp asc, _file asc, _pos asc
                ) as rn
            from {table}
            where source = 'posthog'
              and event = '$identify'
              and extra like '%"hogflare_import_kind":"person"%'
              and distinct_id is not null
        )
        where rn > 1
        """,
    )

    group_duplicates = collect_duplicate_set(
        spark,
        "groups",
        f"""
        select file, pos
        from (
            select
                _file as file,
                _pos as pos,
                row_number() over (
                    partition by
                        get_json_object(extra, '$.group_type'),
                        get_json_object(extra, '$.group_key')
                    order by __ingest_ts asc, timestamp asc, _file asc, _pos asc
                ) as rn
            from {table}
            where source = 'posthog'
              and event = '$groupidentify'
              and extra like '%"hogflare_import_kind":"group"%'
              and get_json_object(extra, '$.group_type') is not null
              and get_json_object(extra, '$.group_key') is not null
        )
        where rn > 1
        """,
    )

    return [event_duplicates, person_duplicates, group_duplicates]


def print_counts(spark: SparkSession, table: str, prefix: str) -> None:
    event_counts = spark.sql(
        f"""
        select
            count(*) as rows,
            count(distinct uuid) as distinct_uuids,
            max(timestamp) as latest_timestamp
        from {table}
        where source = 'posthog'
          and extra like '%"hogflare_import":true%'
          and extra not like '%"hogflare_import_kind":"person"%'
          and extra not like '%"hogflare_import_kind":"group"%'
        """
    ).collect()[0]
    person_counts = spark.sql(
        f"""
        select
            count(*) as rows,
            count(distinct distinct_id) as distinct_ids
        from {table}
        where source = 'posthog'
          and event = '$identify'
          and extra like '%"hogflare_import_kind":"person"%'
        """
    ).collect()[0]
    group_counts = spark.sql(
        f"""
        select
            count(*) as rows,
            count(distinct concat(
                coalesce(get_json_object(extra, '$.group_type'), ''),
                ':',
                coalesce(get_json_object(extra, '$.group_key'), '')
            )) as distinct_groups
        from {table}
        where source = 'posthog'
          and event = '$groupidentify'
          and extra like '%"hogflare_import_kind":"group"%'
        """
    ).collect()[0]

    print(
        f"{prefix}_events rows={event_counts.rows} distinct_uuids={event_counts.distinct_uuids} "
        f"latest_timestamp={event_counts.latest_timestamp}"
    )
    print(f"{prefix}_persons rows={person_counts.rows} distinct_ids={person_counts.distinct_ids}")
    print(f"{prefix}_groups rows={group_counts.rows} distinct_groups={group_counts.distinct_groups}")


def delete_duplicates(spark: SparkSession, table: str, duplicates: list[DuplicateSet]) -> int:
    duplicate_rows = [row for duplicate_set in duplicates for row in duplicate_set.rows]
    if not duplicate_rows:
        return 0

    duplicate_df = spark.createDataFrame(duplicate_rows, "file string, pos long")
    duplicate_df.createOrReplaceTempView("duplicate_imported_posthog_rows")
    spark.sql(
        f"""
        delete from {table}
        where (_file, _pos) in (
            select file, pos
            from duplicate_imported_posthog_rows
        )
        """
    )
    return len(duplicate_rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--execute", action="store_true", help="delete duplicate imported rows")
    parser.add_argument("--account-id")
    parser.add_argument("--bucket")
    parser.add_argument("--namespace")
    parser.add_argument("--table")
    args = parser.parse_args()

    config = catalog_config_from_args(args)
    spark = build_spark(token_from_env(), config)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print_counts(spark, config.qualified_table, "before")
        duplicates = collect_duplicates(spark, config.qualified_table)
        for duplicate_set in duplicates:
            print(f"duplicate_{duplicate_set.label}={len(duplicate_set.rows)}")
        total = sum(len(duplicate_set.rows) for duplicate_set in duplicates)
        print(f"duplicate_total={total}")

        if not args.execute:
            print("dry_run=true")
            return

        deleted = delete_duplicates(spark, config.qualified_table, duplicates)
        print(f"deleted_rows={deleted}")
        print_counts(spark, config.qualified_table, "after")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
