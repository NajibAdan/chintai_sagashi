import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class CrawlResult:
    """
    A dataclass that stores the crawl result for each location.
    """

    prefecture: str
    city: str
    total_pages: int
    successful_pages: int = 0
    failed_pages: list[int] = field(default_factory=list)
    records_written: int = 0

    @property
    def missed_pages(self) -> int:
        return len(self.failed_pages)

    @property
    def completion_rate(self) -> float:
        if self.total_pages == 0:
            return 0.0

        return self.successful_pages / self.total_pages

    def log_results(self) -> None:
        logger.info(
            "%s/%s: %s/%s pages (%.2f%%), %s records, failed=%s",
            self.prefecture,
            self.city,
            self.successful_pages,
            self.total_pages,
            self.completion_rate * 100,
            self.records_written,
            self.failed_pages or "none",
        )


def print_summary_results(results: list[CrawlResult]) -> None:
    """
    Prints the summary crawl results for each location
    """
    logger.info("=" * 60)
    logger.info("CRAWL SUMMARY")
    logger.info("=" * 60)

    for result in results:
        result.log_results()


def print_total_results(results: list[CrawlResult]) -> None:
    """
    Prints the total crawl results across all locations
    """
    total_pages = sum(result.total_pages for result in results)

    successful_pages = sum(result.successful_pages for result in results)

    total_records = sum(result.records_written for result in results)

    failed_pages = sum(result.missed_pages for result in results)

    completion_rate = successful_pages / total_pages if total_pages else 0

    logger.info("-" * 60)

    logger.info(
        "TOTAL: %s/%s pages (%.2f%%), %s records, %s failed pages",
        successful_pages,
        total_pages,
        completion_rate * 100,
        total_records,
        failed_pages,
    )


def print_results(results: list[CrawlResult]) -> None:
    """
    Prints the summary crawl results for each location and the total crawl results across all locations
    """
    print_summary_results(results=results)
    print_total_results(results=results)
