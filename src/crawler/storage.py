import gzip
import json
import logging
import os
from pathlib import Path

import boto3
from dataclasses import asdict
from requests import Response

from crawler import settings
from crawler.results import CrawlResult

logger = logging.getLogger(__name__)


class CrawlStorage:
    """
    Uploads files to S3 and saves file to disk.
    """

    def __init__(self):
        self.s3_client = boto3.client(
            "s3",
            region_name=settings.BUCKET_REGION,
            endpoint_url=settings.BUCKET_ENDPOINT,
            aws_access_key_id=settings.AWS_ACCESS_KEY,
            aws_secret_access_key=settings.AWS_SECRET_KEY,
        )

        self.bucket = settings.BUCKET_NAME

    def upload(self, local_path: str, s3_key: str) -> None:
        """
        Uploads files to S3
        """
        self.s3_client.upload_file(
            local_path,
            self.bucket,
            s3_key,
        )

        logger.info(
            "[PID %s] Uploaded to S3: %s",
            os.getpid(),
            s3_key,
        )

    def save_html(
        self,
        response: Response,
        page: int,
        partition_dir: str,
    ) -> str:
        """
        Saves the crawled page to html.gz format.
        """
        html_directory = Path(partition_dir) / "html"
        html_directory.mkdir(
            parents=True,
            exist_ok=True,
        )

        file_path = html_directory / f"page-{page:06d}.html.gz"

        with gzip.open(
            file_path,
            "wt",
            encoding="utf-8",
        ) as file:
            file.write(response.text)

        logger.info(
            "[PID %s] Saved page %s --> %s",
            os.getpid(),
            page,
            file_path,
        )

        self.upload(
            str(file_path),
            str(file_path),
        )

        return str(file_path)

    def save_records(
        self,
        records: list[dict],
        page: int,
        partition_dir: str,
    ) -> str:
        """
        Saves the parsed information to jsonl.gz.
        First the file is saved to disk then uploaded to S3
        """
        file_path = Path(partition_dir) / f"page-{page:06d}-part-0000.jsonl.gz"

        with gzip.open(
            file_path,
            "wt",
            encoding="utf-8",
        ) as file:
            for record in records:
                file.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                    )
                    + "\n"
                )

        logger.info(
            "[PID %s] Wrote %s records --> %s",
            os.getpid(),
            len(records),
            file_path,
        )

        self.upload(
            str(file_path),
            str(file_path),
        )

        return str(file_path)

    def save_manifest(
        self,
        result: CrawlResult,
        partition_dir: str,
    ) -> None:
        """
        Saves a JSON manifest of CrawlResult
        """
        file_path = Path(partition_dir) / "_manifest.json"

        with file_path.open("w", encoding="utf-8") as file:
            json.dump(
                asdict(result),
                file,
                ensure_ascii=False,
                indent=2,
            )

        logger.info(
            "[PID %s] Saved manifest --> %s",
            os.getpid(),
            file_path,
        )

        self.upload(
            str(file_path),
            str(file_path),
        )
