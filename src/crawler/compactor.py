import datetime
import json
import logging
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import boto3
import duckdb

from crawler import settings
from crawler.results import CompactionResult

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logger = logging.getLogger(__name__)

RAW_PREFIX = "data/raw/suumo"
COMPACTED_PREFIX = "data/compacted/suumo"


def sql_string(value: str) -> str:
    """
    Escape a Python string for use as a DuckDB SQL string literal.
    """
    return "'" + value.replace("'", "''") + "'"


def load_crawl_manifest(s3_client, crawl_date: str) -> dict:
    key = f"{RAW_PREFIX}/crawl_date={crawl_date}/_manifest.json"
    response = s3_client.get_object(Bucket=settings.BUCKET_NAME, Key=key)
    return json.loads(response["Body"].read().decode("utf-8"))


def validate_crawl_date(crawl_date: str) -> None:
    """
    Validates if a provided dated is in ISO 8601 format.
    """
    try:
        datetime.date.fromisoformat(crawl_date)
    except ValueError as exc:
        raise ValueError(
            f"Invalid crawl_date {crawl_date!r}. Expected YYYY-MM-DD."
        ) from exc


def get_s3_client():
    """
    Creates an boto3-client from the provided the variables in `settings.py`
    """
    return boto3.client(
        "s3",
        region_name=settings.BUCKET_REGION,
        endpoint_url=settings.BUCKET_ENDPOINT,
        aws_access_key_id=settings.AWS_ACCESS_KEY,
        aws_secret_access_key=settings.AWS_SECRET_KEY,
    )


def get_duckdb_endpoint() -> tuple[str, bool]:
    """
    Convert a boto3-style endpoint URL:

        https://s3.example.com

    into what DuckDB's S3 secret expects:

        endpoint = s3.example.com
        use_ssl = true
    """
    endpoint_url = settings.BUCKET_ENDPOINT

    if "://" not in endpoint_url:
        return endpoint_url.rstrip("/"), True

    parsed = urlparse(endpoint_url)

    if not parsed.hostname:
        raise ValueError(f"Invalid BUCKET_ENDPOINT: {settings.BUCKET_ENDPOINT}")

    endpoint = parsed.hostname

    if parsed.port:
        endpoint = f"{endpoint}:{parsed.port}"

    return endpoint, parsed.scheme == "https"


def configure_duckdb_s3(con: duckdb.DuckDBPyConnection) -> None:
    """
    Configure DuckDB to access the same S3 bucket used by
    the scraper.
    """
    endpoint, use_ssl = get_duckdb_endpoint()

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    con.execute(
        f"""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE S3,
            PROVIDER CONFIG,
            KEY_ID {sql_string(settings.AWS_ACCESS_KEY)},
            SECRET {sql_string(settings.AWS_SECRET_KEY)},
            REGION {sql_string(settings.BUCKET_REGION)},
            ENDPOINT {sql_string(endpoint)},
            USE_SSL {"true" if use_ssl else "false"},
            URL_STYLE 'path',
            SCOPE {sql_string(f"s3://{settings.BUCKET_NAME}")}
        )
        """
    )


def list_input_shards(s3_client, crawl_date: str) -> list[str]:
    """
    Count JSONL shards for a crawl date.

    HTML archival files are ignored.
    """
    prefix = f"{RAW_PREFIX}/crawl_date={crawl_date}/"

    paginator = s3_client.get_paginator("list_objects_v2")

    shards = []

    for page in paginator.paginate(
        Bucket=settings.BUCKET_NAME,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".jsonl.gz"):
                shards.append(key)

    return shards


def validate_input_shards(
    manifest: dict,
    shard_count: int,
) -> None:
    expected_shards = sum(
        location["successful_pages"] for location in manifest["locations"]
    )

    if shard_count != expected_shards:
        raise RuntimeError(
            f"Expected {expected_shards} JSONL shards "
            f"according to crawl manifest, but found {shard_count}"
        )


def compact_crawl_date(crawl_date: str) -> CompactionResult:
    """
    Compacts a provided crawl_date to a single parquet file.
    """
    validate_crawl_date(crawl_date)

    s3_client = get_s3_client()

    manifest = load_crawl_manifest(
        s3_client,
        crawl_date,
    )
    input_shards = list_input_shards(
        s3_client=s3_client,
        crawl_date=crawl_date,
    )

    shard_count = len(input_shards)
    validate_input_shards(
        manifest,
        shard_count,
    )

    logger.info(
        "Found %s input shards for crawl_date=%s",
        shard_count,
        crawl_date,
    )

    input_glob = (
        f"s3://{settings.BUCKET_NAME}/"
        f"{RAW_PREFIX}/"
        f"crawl_date={crawl_date}/"
        f"prefecture=*/city=*/"
        f"page-*.jsonl.gz"
    )

    parquet_output_key = f"{COMPACTED_PREFIX}/crawl_date={crawl_date}/data.parquet"
    manifest_output_key = f"{COMPACTED_PREFIX}/crawl_date={crawl_date}/_manifest.json"

    logger.info("Input: %s", input_glob)
    logger.info("Output: s3://%s/%s", settings.BUCKET_NAME, parquet_output_key)

    con = duckdb.connect()

    try:
        configure_duckdb_s3(con)

        # Compaction has no meaningful row ordering requirement.
        # Let DuckDB optimize execution more freely.
        con.execute("SET preserve_insertion_order = false")

        source_query = f"""
            SELECT
                *
            FROM read_json(
                {sql_string(input_glob)},
                format = 'newline_delimited',
                compression = 'gzip',
                hive_partitioning = true,
                union_by_name = true,
                filename = true
            )
        """

        with tempfile.TemporaryDirectory(prefix="suumo_compactor_") as temp_dir:
            local_parquet_output = os.path.join(
                temp_dir,
                f"suumo-{crawl_date}.parquet",
            )

            local_manifest_output = os.path.join(
                temp_dir, f"{crawl_date}_manifest.json"
            )

            logger.info(
                "Compacting crawl_date=%s...",
                crawl_date,
            )

            con.execute(
                f"""
                COPY (
                    {source_query}
                )
                TO {sql_string(local_parquet_output)}
                (
                    FORMAT PARQUET,
                    COMPRESSION ZSTD,
                    ROW_GROUP_SIZE 250000
                )
                """
            )

            # This reads only the newly-created Parquet metadata/data
            # locally instead of rescanning all the remote JSONL files.
            row_count = con.execute(
                f"""
                SELECT count(*)
                FROM read_parquet({sql_string(local_parquet_output)})
                """
            ).fetchone()[0]

            file_size = os.path.getsize(local_parquet_output)

            if row_count == 0:
                raise RuntimeError(f"Compaction produced zero rows for {crawl_date}")

            logger.info(
                "Compaction complete: rows=%s size=%.2f MB",
                f"{row_count:,}",
                file_size / 1024 / 1024,
            )

            logger.info(
                "Uploading compacted Parquet to s3://%s/%s",
                settings.BUCKET_NAME,
                parquet_output_key,
            )

            s3_client.upload_file(
                local_parquet_output,
                settings.BUCKET_NAME,
                parquet_output_key,
            )
            compacted_manifest = {
                "crawl_date": crawl_date,
                "input_shards": shard_count,
                "output_records": row_count,
                "output_size_bytes": file_size,
                "source_crawl": {
                    "total_pages": manifest["total_pages"],
                    "successful_pages": manifest["successful_pages"],
                    "failed_pages": manifest["failed_pages"],
                    "completion_rate": manifest["completion_rate"],
                },
            }
            with open(local_manifest_output, "w", encoding="utf-8") as file:
                json.dump(
                    compacted_manifest,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )
            s3_client.upload_file(
                local_manifest_output,
                settings.BUCKET_NAME,
                manifest_output_key,
            )

    finally:
        con.close()

    logger.info(
        "Finished crawl_date=%s -> s3://%s/%s",
        crawl_date,
        settings.BUCKET_NAME,
        parquet_output_key,
    )

    return CompactionResult(
        crawl_date=crawl_date,
        input_shards=shard_count,
        output_records=row_count,
        output_size_bytes=file_size,
        output_key=local_parquet_output,
    )
