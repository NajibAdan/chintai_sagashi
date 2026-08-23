import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class CompactionResult:
    crawl_date: str
    input_shards: int
    output_records: int
    output_size_bytes: int
    output_key: str


@dataclass
class CrawlResult:
    prefecture: str
    city: str
    total_pages: int
    successful_pages: int = 0
    failed_pages: list[int] = field(default_factory=list)
    records_written: int = 0


@dataclass
class CrawlSummary:
    crawl_date: str
    locations: list[CrawlResult]

    @property
    def total_pages(self) -> int:
        return sum(result.total_pages for result in self.locations)

    @property
    def successful_pages(self) -> int:
        return sum(result.successful_pages for result in self.locations)

    @property
    def failed_pages(self) -> int:
        return sum(len(result.failed_pages) for result in self.locations)

    @property
    def records_written(self) -> int:
        return sum(result.records_written for result in self.locations)

    @property
    def completion_rate(self) -> float:
        if self.total_pages == 0:
            return 0.0

        return self.successful_pages / self.total_pages

    def to_dict(self) -> dict:
        return {
            "crawl_date": self.crawl_date,
            "total_locations": len(self.locations),
            "total_pages": self.total_pages,
            "successful_pages": self.successful_pages,
            "failed_pages": self.failed_pages,
            "records_written": self.records_written,
            "completion_rate": self.completion_rate,
            "locations": [
                {
                    "prefecture": result.prefecture,
                    "city": result.city,
                    "total_pages": result.total_pages,
                    "successful_pages": result.successful_pages,
                    "failed_pages": result.failed_pages,
                    "records_written": result.records_written,
                }
                for result in self.locations
            ],
        }


def log_crawl_summary(
    summary: CrawlSummary,
) -> None:
    logger.info("=" * 60)
    logger.info("CRAWL SUMMARY")
    logger.info("=" * 60)

    for result in summary.locations:
        logger.info(
            "%s/%s: %s/%s pages, %s records, failed=%s",
            result.prefecture,
            result.city,
            result.successful_pages,
            result.total_pages,
            result.records_written,
            result.failed_pages or "none",
        )

    logger.info("-" * 60)

    logger.info(
        "TOTAL: %s/%s pages (%.2f%%), %s records, %s failed pages",
        summary.successful_pages,
        summary.total_pages,
        summary.completion_rate * 100,
        summary.records_written,
        summary.failed_pages,
    )
