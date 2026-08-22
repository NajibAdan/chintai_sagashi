from bs4 import BeautifulSoup
import os
import time
import datetime
import crawler.settings as settings
import logging
from pathlib import Path
import multiprocessing
from crawler.compactor import compact_crawl_date
from crawler.parser import parse_listing_page
from crawler.client import SuumoClient
from crawler.storage import CrawlStorage
from crawler.results import CrawlResult, print_results

LOCATIONS = settings.CRAWL_LOCATIONS
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "suumo_crawler.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Configuration
REQUEST_DELAY = 1  # seconds between requests
MAX_RETRY_ATTEMPTS = 20
BASE_RETRY_DELAY = 3  # base delay for exponential backoff
RETRY_BACKOFF_MULTIPLIER = 1.2
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"

# Number of concurrent scraping processes (adjust 4-8 for typical systems, higher for more resources)
PROCESS_POOL_SIZE = min(len(LOCATIONS), 8)
CRAWL_DATE = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")


def get_total_pages(soup: BeautifulSoup) -> int:
    pagination = soup.find(
        "ol",
        class_="pagination-parts",
    )

    if pagination is None:
        return 1

    pages = pagination.find_all("li")

    if not pages:
        return 1

    return int(pages[-1].text.strip())


def crawl_location(location: dict) -> CrawlResult:
    prefecture = location["prefecture"]
    city = location["city"]
    base_url = f"https://suumo.jp/chintai/{prefecture}/sa_{city}/?page={{}}&pc=50"
    partition_dir = (
        f"data/raw/suumo/crawl_date={CRAWL_DATE}/prefecture={prefecture}/city={city}"
    )
    os.makedirs(partition_dir, exist_ok=True)

    storage = CrawlStorage()

    client = SuumoClient(user_agent=USER_AGENT, request_delay=REQUEST_DELAY)
    logger.info(
        "[PID %s] Starting crawl for %s/%s",
        os.getpid(),
        prefecture,
        city,
    )
    # Determine pagination
    first_response = client.fetch_page(base_url.format(1))

    first_soup = BeautifulSoup(
        first_response.content,
        "html.parser",
    )
    total_pages = get_total_pages(first_soup)

    if settings.ENVIRONMENT == "dev":
        total_pages = min(
            total_pages,
            settings.PAGES_TO_FETCH,
        )
    result = CrawlResult(prefecture=prefecture, city=city, total_pages=total_pages)

    logger.info(
        "[PID %s] Found %s pages for %s/%s",
        os.getpid(),
        total_pages,
        prefecture,
        city,
    )

    for page in range(1, total_pages + 1):
        url = base_url.format(page)

        logger.info(
            "[PID %s] Scraping page %s/%s",
            os.getpid(),
            page,
            total_pages,
        )

        for attempt in range(
            1,
            MAX_RETRY_ATTEMPTS + 1,
        ):
            response = client.fetch_page(url)

            storage.save_html(
                response=response,
                page=page,
                partition_dir=partition_dir,
            )
            soup = BeautifulSoup(
                response.content,
                "html.parser",
            )

            records = parse_listing_page(
                soup,
                page,
            )

            if records:
                break

            delay = BASE_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER**attempt)

            logger.warning(
                "[PID %s] 0 records for page %s/%s. Attempt %s/%s. Retrying in %.1fs.",
                os.getpid(),
                page,
                total_pages,
                attempt,
                MAX_RETRY_ATTEMPTS,
                delay,
            )

            time.sleep(delay)

        else:
            logger.error(
                "[PID %s] Maximum attempts reached for page %s. Skipping.",
                os.getpid(),
                page,
            )
            result.failed_pages.append(page)
            continue

        storage.save_records(records=records, page=page, partition_dir=partition_dir)
        result.successful_pages += 1
        result.records_written += len(records)
    logger.info(
        "[PID %s] Crawler finished for %s/%s",
        os.getpid(),
        prefecture,
        city,
    )
    return result


if __name__ == "__main__":
    logger.info(f"Crawler is running in {settings.ENVIRONMENT} environment")
    logger.info(
        f"Starting parallel SUUMO crawler for {len(LOCATIONS)} locations with {PROCESS_POOL_SIZE} workers"
    )

    with multiprocessing.Pool(processes=PROCESS_POOL_SIZE) as pool:
        results = pool.map(crawl_location, LOCATIONS)

    print_results(results)
    logger.info("All locations crawled")

    logger.info("Starting compactor")
    compact_crawl_date(CRAWL_DATE)
