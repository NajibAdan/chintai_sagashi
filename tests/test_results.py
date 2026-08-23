from crawler.results import CrawlResult, CrawlSummary


def test_crawl_summary():
    results = [
        CrawlResult(
            prefecture="miyagi",
            city="aoba",
            total_pages=10,
            successful_pages=10,
            failed_pages=[],
            records_written=500,
        ),
        CrawlResult(
            prefecture="miyagi",
            city="miyagino",
            total_pages=10,
            successful_pages=9,
            failed_pages=[7],
            records_written=440,
        ),
    ]

    summary = CrawlSummary(
        crawl_date="2026-08-23",
        locations=results,
    )

    assert summary.total_pages == 20
    assert summary.successful_pages == 19
    assert summary.failed_pages == 1
    assert summary.records_written == 940
    assert summary.completion_rate == 0.95


def test_crawl_summary_to_dict():
    result = CrawlResult(
        prefecture="miyagi",
        city="aoba",
        total_pages=10,
        successful_pages=9,
        failed_pages=[5],
        records_written=450,
    )

    summary = CrawlSummary(
        crawl_date="2026-08-23",
        locations=[result],
    )

    data = summary.to_dict()

    assert data["total_locations"] == 1
    assert data["total_pages"] == 10
    assert data["successful_pages"] == 9
    assert data["failed_pages"] == 1
    assert data["completion_rate"] == 0.9

    assert data["locations"][0]["failed_pages"] == [5]
