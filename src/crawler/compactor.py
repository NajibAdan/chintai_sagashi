import datetime
import logging
import os
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import boto3
import duckdb

from crawler import settings

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


def count_input_shards(s3_client, crawl_date: str) -> int:
    """
    Count JSONL shards for a crawl date.

    HTML archival files are ignored.
    """
    prefix = f"{RAW_PREFIX}/crawl_date={crawl_date}/"

    paginator = s3_client.get_paginator("list_objects_v2")

    count = 0

    for page in paginator.paginate(
        Bucket=settings.BUCKET_NAME,
        Prefix=prefix,
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith(".jsonl.gz"):
                count += 1

    return count


def compact_crawl_date(crawl_date: str) -> None:
    """
    Compacts a provided crawl_date to a single parquet file.
    """
    validate_crawl_date(crawl_date)

    s3_client = get_s3_client()

    shard_count = count_input_shards(
        s3_client=s3_client,
        crawl_date=crawl_date,
    )

    if shard_count == 0:
        raise RuntimeError(f"No JSONL shards found for crawl_date={crawl_date}")

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

    output_key = f"{COMPACTED_PREFIX}/crawl_date={crawl_date}/data.parquet"

    logger.info("Input: %s", input_glob)
    logger.info("Output: s3://%s/%s", settings.BUCKET_NAME, output_key)

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
            local_output = os.path.join(
                temp_dir,
                f"suumo-{crawl_date}.parquet",
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
                TO {sql_string(local_output)}
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
                FROM read_parquet({sql_string(local_output)})
                """
            ).fetchone()[0]

            file_size = os.path.getsize(local_output)

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
                output_key,
            )

            s3_client.upload_file(
                local_output,
                settings.BUCKET_NAME,
                output_key,
            )

    finally:
        con.close()

    logger.info(
        "Finished crawl_date=%s -> s3://%s/%s",
        crawl_date,
        settings.BUCKET_NAME,
        output_key,
    )
