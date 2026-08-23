import io
import json

import pytest

from crawler.compactor import (
    list_input_shards,
    load_crawl_manifest,
    validate_crawl_date,
    validate_input_shards,
)


def test_valid_crawl_date():
    validate_crawl_date("2026-08-23")


@pytest.mark.parametrize(
    "value",
    [
        "2026/08/23",
        "23-08-2026",
        "hello",
        "",
    ],
)
def test_invalid_crawl_date(value):
    with pytest.raises(ValueError):
        validate_crawl_date(value)


def test_validate_input_shards():
    manifest = {
        "locations": [
            {
                "prefecture": "test",
                "city": "test",
                "successful_pages": 10,
            },
            {
                "prefecture": "test",
                "city": "test",
                "successful_pages": 5,
            },
        ]
    }

    validate_input_shards(
        manifest=manifest,
        shard_count=15,
    )


def test_validate_input_shards_detects_missing_shard():
    manifest = {
        "locations": [
            {
                "prefecture": "miyagi",
                "city": "aoba",
                "successful_pages": 10,
            },
            {
                "prefecture": "miyagi",
                "city": "izumi",
                "successful_pages": 5,
            },
        ]
    }

    with pytest.raises(
        RuntimeError,
        match="Expected 15 JSONL shards",
    ):
        validate_input_shards(
            manifest=manifest,
            shard_count=14,
        )


def test_validate_input_shards_detects_extra_shard():
    manifest = {
        "locations": [
            {
                "successful_pages": 10,
            },
        ]
    }

    with pytest.raises(RuntimeError):
        validate_input_shards(
            manifest=manifest,
            shard_count=11,
        )


class FakePaginator:
    def paginate(self, **kwargs):
        return [
            {
                "Contents": [
                    {
                        "Key": (
                            "data/raw/suumo/"
                            "crawl_date=2026-08-23/"
                            "prefecture=test-pref/"
                            "city=test-city/"
                            "page-000001-part-0000.jsonl.gz"
                        )
                    },
                    {
                        "Key": (
                            "data/raw/suumo/"
                            "crawl_date=2026-08-23/"
                            "prefecture=test-pref/"
                            "city=test-city/"
                            "html/page-000001.html.gz"
                        )
                    },
                    {"Key": ("data/raw/suumo/crawl_date=2026-08-23/_manifest.json")},
                ]
            },
            {
                "Contents": [
                    {
                        "Key": (
                            "data/raw/suumo/"
                            "crawl_date=2026-08-23/"
                            "prefecture=test-pref/"
                            "city=test-city/"
                            "page-000002-part-0000.jsonl.gz"
                        )
                    }
                ]
            },
        ]


class FakeS3Client:
    def __init__(self):
        self.requested_bucket = None
        self.requested_key = None

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return FakePaginator()

    def get_object(self, Bucket, Key):
        self.requested_bucket = Bucket
        self.requested_key = Key

        manifest = {
            "crawl_date": "2026-08-23",
            "total_pages": 10,
            "successful_pages": 9,
            "failed_pages": 1,
            "locations": [],
        }

        return {"Body": io.BytesIO(json.dumps(manifest).encode())}


def test_load_crawl_manifest():
    s3 = FakeS3Client()

    manifest = load_crawl_manifest(
        s3_client=s3,
        crawl_date="2026-08-23",
    )

    assert manifest["crawl_date"] == "2026-08-23"
    assert manifest["total_pages"] == 10
    assert manifest["successful_pages"] == 9
    assert manifest["failed_pages"] == 1


def test_count_input_shards_ignores_html_and_manifests():
    s3 = FakeS3Client()

    input_shards = list_input_shards(
        s3_client=s3,
        crawl_date="2026-08-23",
    )

    assert len(input_shards) == 2
