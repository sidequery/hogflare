# /// script
# dependencies = ["pyspark==3.5.1", "requests"]
# ///

import argparse
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import requests
from pyspark.sql import SparkSession


DEFAULT_NAMESPACE = "default"
DEFAULT_TABLE = "hogflare_events_v3"
DEFAULT_POSTHOG_HOST = "https://us.posthog.com"
DEFAULT_OUTPUT = "/tmp/hogflare_missing_event_uuids.txt"


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


@dataclass
class PostHogConfig:
    host: str
    project_id: str
    token: str

    @property
    def query_url(self) -> str:
        return f"{self.host.rstrip('/')}/api/projects/{self.project_id}/query/"


def parse_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def posthog_config_from_args(args: argparse.Namespace) -> PostHogConfig:
    host = args.posthog_host or os.environ.get("POSTHOG_HOST") or DEFAULT_POSTHOG_HOST
    project_id = args.project_id or os.environ.get("POSTHOG_PROJECT_ID")
    token = args.personal_api_key or os.environ.get("POSTHOG_PERSONAL_API_KEY")

    if not project_id:
        raise SystemExit("set POSTHOG_PROJECT_ID or pass --project-id")
    if not token:
        raise SystemExit("set POSTHOG_PERSONAL_API_KEY or pass --personal-api-key")

    return PostHogConfig(host=host, project_id=project_id, token=token)


def catalog_token_from_env() -> str:
    token = os.environ.get("R2_DATA_CATALOG_TOKEN") or os.environ.get("WRANGLER_R2_SQL_AUTH_TOKEN")
    if not token:
        raise SystemExit("set R2_DATA_CATALOG_TOKEN or WRANGLER_R2_SQL_AUTH_TOKEN")
    return token


def hogql_datetime(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f")


def query_uuids(
    config: PostHogConfig,
    start: datetime,
    end: datetime,
    offset: int,
) -> list[str]:
    query = (
        "select toString(uuid) from events "
        f"where timestamp >= toDateTime64('{hogql_datetime(start)}', 6, 'UTC') "
        f"and timestamp < toDateTime64('{hogql_datetime(end)}', 6, 'UTC') "
        f"order by timestamp asc, toString(uuid) asc limit 10000 offset {offset}"
    )
    headers = {
        "Authorization": f"Bearer {config.token}",
        "Content-Type": "application/json",
    }
    for attempt in range(4):
        response = requests.post(
            config.query_url,
            headers=headers,
            json={
                "query": {"kind": "HogQLQuery", "query": query},
                "name": "hogflare uuid diff",
            },
            timeout=120,
        )
        if response.status_code < 500:
            response.raise_for_status()
            return [row[0] for row in response.json()["results"]]
        time.sleep(2**attempt)

    response.raise_for_status()
    return []


def fetch_interval(
    config: PostHogConfig,
    start: datetime,
    end: datetime,
    depth: int = 0,
) -> set[str]:
    try:
        out: set[str] = set()
        offset = 0
        while True:
            rows = query_uuids(config, start, end, offset)
            out.update(rows)
            if len(rows) < 10_000:
                return out
            offset += len(rows)
    except Exception:
        if depth >= 6 or (end - start).total_seconds() <= 3600:
            raise
        midpoint = start + (end - start) / 2
        return fetch_interval(config, start, midpoint, depth + 1) | fetch_interval(
            config,
            midpoint,
            end,
            depth + 1,
        )


def interval_bounds(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    bounds = []
    cursor = start
    while cursor < end:
        if cursor.month == 12:
            next_month = datetime(cursor.year + 1, 1, 1, tzinfo=timezone.utc)
        else:
            next_month = datetime(cursor.year, cursor.month + 1, 1, tzinfo=timezone.utc)
        interval_end = min(next_month, end)
        bounds.append((cursor, interval_end))
        cursor = interval_end
    return bounds


def build_spark(token: str, config: CatalogConfig) -> SparkSession:
    return (
        SparkSession.builder.appName("hogflare-missing-uuid-diff")
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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--account-id")
    parser.add_argument("--bucket")
    parser.add_argument("--namespace")
    parser.add_argument("--table")
    parser.add_argument("--posthog-host")
    parser.add_argument("--project-id")
    parser.add_argument("--personal-api-key")
    parser.add_argument("--from", dest="start", required=True)
    parser.add_argument("--to", dest="end")
    parser.add_argument("--output", default=os.environ.get("IMPORT_EVENT_UUIDS_FILE", DEFAULT_OUTPUT))
    args = parser.parse_args()

    catalog = catalog_config_from_args(args)
    posthog = posthog_config_from_args(args)
    start = parse_datetime(args.start)
    end = parse_datetime(args.end) if args.end else datetime.now(timezone.utc)

    spark = build_spark(catalog_token_from_env(), catalog)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        r2_rows = spark.sql(
            f"""
            select distinct uuid
            from {catalog.qualified_table}
            where source = 'posthog'
              and extra like '%"hogflare_import":true%'
              and extra not like '%"hogflare_import_kind":"person"%'
              and extra not like '%"hogflare_import_kind":"group"%'
              and timestamp >= timestamp '{start.strftime("%Y-%m-%d %H:%M:%S")}'
            """
        ).collect()
        r2_uuids = {row.uuid for row in r2_rows}
    finally:
        spark.stop()

    source_uuids: set[str] = set()
    intervals = []
    for interval_start, interval_end in interval_bounds(start, end):
        interval_uuids = fetch_interval(posthog, interval_start, interval_end)
        source_uuids.update(interval_uuids)
        intervals.append(
            (
                interval_start.strftime("%Y-%m"),
                len(interval_uuids),
                len(interval_uuids - r2_uuids),
            )
        )

    missing = sorted(source_uuids - r2_uuids)
    output = Path(args.output)
    output.write_text("\n".join(missing) + ("\n" if missing else ""))
    print(f"r2_uuid_count={len(r2_uuids)}")
    print(f"source_uuid_count={len(source_uuids)}")
    print(f"missing_uuid_count={len(missing)}")
    for month, source_count, missing_count in intervals:
        print(f"{month}: source={source_count} missing={missing_count}")
    print(f"missing_uuid_file={output}")


if __name__ == "__main__":
    main()
