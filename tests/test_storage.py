import gzip
import json

from utils import FakeResponse, get_listing_html

from crawler.storage import CrawlStorage


def test_save_records(tmp_path, monkeypatch):
    storage = CrawlStorage()

    uploaded = []

    def fake_upload(local_path, s3_key):
        uploaded.append((local_path, s3_key))

    monkeypatch.setattr(
        storage,
        "upload",
        fake_upload,
    )

    records = [
        {
            "property_name": "Test Mansion",
            "monthly_rent": "7.5万円",
            "url": "https://suumo.jp/test/1",
        },
        {
            "property_name": "Another Mansion",
            "monthly_rent": "8.2万円",
            "url": "https://suumo.jp/test/2",
        },
    ]

    path = storage.save_records(
        records=records,
        page=1,
        partition_dir=str(tmp_path),
    )

    with gzip.open(
        path,
        "rt",
        encoding="utf-8",
    ) as f:
        rows = [json.loads(line) for line in f]

    assert rows == records
    assert len(rows) == 2

    assert len(uploaded) == 1


def test_save_records_preserves_unicode(
    tmp_path,
    monkeypatch,
):
    storage = CrawlStorage()

    monkeypatch.setattr(
        storage,
        "upload",
        lambda *args: None,
    )

    records = [
        {
            "property_name": "仙台駅前マンション",
            "location": "宮城県仙台市青葉区",
            "monthly_rent": "7.5万円",
        }
    ]

    path = storage.save_records(
        records=records,
        page=1,
        partition_dir=str(tmp_path),
    )

    with gzip.open(
        path,
        "rt",
        encoding="utf-8",
    ) as f:
        result = json.loads(f.readline())

    assert result["property_name"] == "仙台駅前マンション"

    assert result["location"] == "宮城県仙台市青葉区"


def test_save_html(tmp_path, monkeypatch):
    storage = CrawlStorage()

    uploaded = []

    def fake_upload(local_path, s3_key):
        uploaded.append((local_path, s3_key))

    monkeypatch.setattr(
        storage,
        "upload",
        fake_upload,
    )

    response = FakeResponse(get_listing_html())

    path = storage.save_html(
        response=response,
        page=3,
        partition_dir=str(tmp_path),
    )

    with gzip.open(
        path,
        "rt",
        encoding="utf-8",
    ) as f:
        html = f.read()

    assert (
        "<title>【SUUMO】千葉市の賃貸(賃貸マンション・アパート)住宅のお部屋探し物件情報</title>"
        in html
    )

    assert path.endswith("html/page-000003.html.gz")

    assert len(uploaded) == 1
