import crawler.crawler as crawler_module
import re
import pytest


class FakeResponse:
    def __init__(self, page: int):
        html = f"""
        <html>
            <body>
                <div data-page="{page}"></div>
            </body>
        </html>
        """

        self.content = html.encode("utf-8")
        self.text = html


class FakeClient:
    def __init__(self):
        self.calls = []

    def fetch_page(self, url: str):
        self.calls.append(url)

        match = re.search(r"page=(\d+)", url)
        page = int(match.group(1))

        if page == 2:
            raise Exception("Maximum retry attempts reached")
        return FakeResponse(page)


class FakeStorage:
    def __init__(self):
        self.saved_html = []
        self.saved_records = []

    def save_html(
        self,
        response,
        page: int,
        partition_dir,
    ):
        self.saved_html.append(page)

    def save_records(
        self,
        records,
        page,
        partition_dir,
    ):
        self.saved_records.append(
            {
                "page": page,
                "records": records,
            }
        )

    def save_manifest(self, result, partition_dir):
        pass


def test_crawl_location_continues_after_failed_page(
    monkeypatch,
    tmp_path,
):
    fake_client = FakeClient()
    fake_storage = FakeStorage()

    # Don't create data/ inside the real project.
    monkeypatch.chdir(tmp_path)

    # Replace real network client.
    monkeypatch.setattr(
        crawler_module,
        "SuumoClient",
        lambda **kwargs: fake_client,
    )

    # Replace real local/S3 storage.
    monkeypatch.setattr(
        crawler_module,
        "CrawlStorage",
        lambda: fake_storage,
    )

    # We're testing crawl orchestration, not pagination.
    monkeypatch.setattr(
        crawler_module,
        "get_total_pages",
        lambda soup: 3,
    )

    # Page 2 always fails to produce records.
    def fake_parse_listing_page(soup, page):
        if page == 2:
            return []

        return [
            {
                "url": f"https://example.com/{page}",
                "page": page,
            }
        ]

    monkeypatch.setattr(
        crawler_module,
        "parse_listing_page",
        fake_parse_listing_page,
    )

    # Don't retry 20 times in a unit test.
    monkeypatch.setattr(
        crawler_module,
        "MAX_RETRY_ATTEMPTS",
        3,
    )

    # Don't actually sleep during retries.
    monkeypatch.setattr(
        crawler_module.time,
        "sleep",
        lambda seconds: None,
    )
    result = crawler_module.crawl_location(
        {
            "prefecture": "miyagi",
            "city": "sendai",
        }
    )

    assert result.total_pages == 3
    assert result.successful_pages == 2
    assert result.failed_pages == [2]
    assert result.records_written == 2
