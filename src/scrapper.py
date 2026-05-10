import requests
from bs4 import BeautifulSoup
import json
import os
import gzip
import hashlib
import time
from typing import Tuple
import datetime
import boto3
import constants
import logging
from pathlib import Path
import multiprocessing
from locations import LOCATIONS

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "suumo_scraper.log"

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
SOFT_BLOCK_TITLE = "【SUUMO】アクセス集中に関するお詫び"
# Number of concurrent scraping processes (adjust 4-8 for typical systems, higher for more resources)
PROCESS_POOL_SIZE = 8


def upload_to_s3(file_path: str, s3_path: str, s3_client) -> None:
    """
    Uploads a file to S3 with error handling
    """
    try:
        s3_client.upload_file(file_path, constants.BUCKET_NAME, s3_path)
        logger.info(f"[PID {os.getpid()}] Uploaded to S3: {s3_path}")
    except Exception as e:
        logger.error(f"[PID {os.getpid()}] Failed to upload {file_path} to S3: {e}")


def url_key(u: str) -> str:
    """
    Returns a sha1 hash of the url
    """
    return hashlib.sha1(u.encode("utf-8")).hexdigest()


def open_shard(partition_dir: str, page: int) -> Tuple[gzip.GzipFile, str]:
    """
    Creates a gzipped JSONL file for each page
    """
    path = os.path.join(partition_dir, f"page-{page:06d}-part-0000.jsonl.gz")
    return gzip.open(path, "wt", encoding="utf-8"), path


def fetch_page(
    session: requests.Session,
    url: str,
    page: int,
    save_file: bool = True,
    partition_dir: str = None,
    s3_client=None,
) -> BeautifulSoup:
    """
    A fetch page helper function that returns the BeautifulSoup content
    of a page
    """
    logger.info(f"[PID {os.getpid()}] Fetching {url}")
    r = session.get(url, timeout=30)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        logger.exception(e)
    if save_file and partition_dir and s3_client:
        save_page_to_file(r, page, partition_dir, s3_client)
    time.sleep(REQUEST_DELAY)  # Respectful scraping delay
    return BeautifulSoup(r.content, "html.parser")


def save_page_to_file(
    response: requests.Response, page: int, partition_dir: str, s3_client
) -> None:
    """
    Saves the webpage to gzipped file for archival (local and S3)
    """
    html_directory = os.path.join(partition_dir, "html")
    os.makedirs(html_directory, exist_ok=True)
    file_path = os.path.join(html_directory, f"page-{page:06d}.html.gz")
    with gzip.open(file_path, "wt", encoding="utf-8") as file:
        file.write(response.text)
    logger.info(f"[PID {os.getpid()}] Saved page {page} to file: {file_path}")

    # Also upload HTML to S3 for persistence
    s3_path = f"{partition_dir}/html/page-{page:06d}.html.gz"
    upload_to_s3(file_path, s3_path, s3_client)


def get_total_pages(session: requests.Session, base_url: str) -> int:
    """
    Returns the total number of pagination pages
    """
    # since we are doing a pre-fetch, no need to save the html file yet
    soup = fetch_page(session, base_url.format(1), 1, save_file=False)
    try:
        # Find the last page
        return int(
            soup.find("ol", class_="pagination-parts").find_all("li")[-1].text.strip()
        )
    except Exception:
        logger.exception(
            f"[PID {os.getpid()}] Failed to detect total pages. Trying again."
        )
        return 1


def scrape_location(location: dict) -> None:
    prefecture = location["prefecture"]
    city = location["city"]
    base_url = f"https://suumo.jp/chintai/{prefecture}/sa_{city}/?page={{}}&pc=50"
    crawl_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
    partition_dir = (
        f"data/raw/suumo/crawl_date={crawl_date}/prefecture={prefecture}/city={city}"
    )
    os.makedirs(partition_dir, exist_ok=True)

    s3_client = boto3.client(
        "s3",
        region_name=constants.BUCKET_REGION,
        endpoint_url=constants.BUCKET_ENDPOINT,
        aws_access_key_id=constants.AWS_ACCESS_KEY,
        aws_secret_access_key=constants.AWS_SECRET_KEY,
    )

    logger.info(f"[PID {os.getpid()}] Starting scrape for {prefecture}/{city}")

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    total_pages = 1
    while total_pages == 1:
        total_pages = get_total_pages(session, base_url)

    page = 1
    retry_attempt = 0
    attempts = MAX_RETRY_ATTEMPTS

    while page <= total_pages:
        logger.info(
            f"[PID {os.getpid()}] Scraping page {page}/{total_pages}. Attempt: {retry_attempt}/{attempts}"
        )
        url = base_url.format(page)
        soup = fetch_page(
            session,
            url,
            page,
            save_file=True,
            partition_dir=partition_dir,
            s3_client=s3_client,
        )
        retry_attempt += 1

        title_tag = soup.find("title")
        if title_tag and title_tag.text.strip() == SOFT_BLOCK_TITLE:
            time_to_sleep = BASE_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER**retry_attempt)
            logger.warning(
                f"[PID {os.getpid()}] Got soft-blocked for page: {page}/{total_pages}. Retrying after {time_to_sleep} seconds."
            )
            time.sleep(time_to_sleep)
            continue

        cassette_items = soup.find_all("div", class_="cassetteitem")

        out, path = open_shard(partition_dir, page)
        wrote = 0

        try:
            for item in cassette_items:
                property_name = item.find(
                    "div", class_="cassetteitem_content-title"
                ).text.strip()
                location_text = item.find(
                    "li", class_="cassetteitem_detail-col1"
                ).text.strip()
                stations_info = item.find("li", class_="cassetteitem_detail-col2")
                stations = [
                    station.text.strip()
                    for station in stations_info.find_all(
                        "div", class_="cassetteitem_detail-text"
                    )
                    if station.text.strip() != ""
                ]

                building_meta = item.find(
                    "li", class_="cassetteitem_detail-col3"
                ).find_all("div")
                building_age = building_meta[0].text.strip()
                building_type = building_meta[1].text.strip()

                rows = item.find_all("tr", class_="js-cassette_link")

                for row in rows:
                    try:
                        rent = row.find(
                            "span", class_="cassetteitem_price--rent"
                        ).text.strip()
                        admin_fee = row.find(
                            "span", class_="cassetteitem_price--administration"
                        ).text.strip()
                        deposit = row.find(
                            "span", class_="cassetteitem_price--deposit"
                        ).text.strip()
                        gratuity = row.find(
                            "span", class_="cassetteitem_price--gratuity"
                        ).text.strip()
                        madori = row.find(
                            "span", class_="cassetteitem_madori"
                        ).text.strip()
                        menseki = row.find(
                            "span", class_="cassetteitem_menseki"
                        ).text.strip()
                        apartment_url = (
                            "https://suumo.jp"
                            + row.find("a", class_="js-cassette_link_href")["href"]
                        )
                        apartment_floor = row.find_all("td")[2].text.strip()
                        rec = {
                            "schema_version": 2,
                            "source": "suumo",
                            "crawl_ts": datetime.datetime.now(datetime.UTC).isoformat(
                                timespec="seconds"
                            )
                            + "Z",
                            "listing_page": page,
                            "property_name": property_name,
                            "location": location_text,
                            "stations": stations,
                            "monthly_rent": rent,
                            "management_fee": admin_fee,
                            "deposit": deposit,
                            "gratuity": gratuity,
                            "apartment_floor": apartment_floor,
                            "madori": madori,
                            "menseki": menseki,
                            "building_age": building_age,
                            "building_type": building_type,
                            "url": apartment_url,
                            "url_key": url_key(apartment_url),
                        }
                        out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        wrote += 1
                    except (AttributeError, KeyError, IndexError) as e:
                        logger.warning(
                            f"[PID {os.getpid()}] Failed to parse apartment row: {e}"
                        )
                        continue
        except Exception as e:
            logger.error(f"[PID {os.getpid()}] Error processing page {page}: {e}")
        finally:
            out.close()

        if wrote > 0:
            logger.info(f"[PID {os.getpid()}] Wrote {wrote} records --> {path}")
            upload_to_s3(path, path, s3_client)
            page += 1
            retry_attempt = 0
        elif retry_attempt >= attempts:
            logger.error(
                f"[PID {os.getpid()}] Maximum attempts reached for page {page}. Moving to next page."
            )
            retry_attempt = 0
            page += 1
        else:
            time_to_sleep = BASE_RETRY_DELAY * (RETRY_BACKOFF_MULTIPLIER**retry_attempt)
            logger.warning(
                f"[PID {os.getpid()}] Got 0 records for page: {page}/{total_pages}. Retrying after {time_to_sleep} seconds."
            )
            time.sleep(time_to_sleep)

    logger.info(f"[PID {os.getpid()}] Scraper finished for {prefecture}/{city}")


if __name__ == "__main__":
    logger.info(
        f"Starting parallel SUUMO scraper for {len(LOCATIONS)} locations with {PROCESS_POOL_SIZE} workers"
    )

    with multiprocessing.Pool(processes=PROCESS_POOL_SIZE) as pool:
        pool.map(scrape_location, LOCATIONS)

    logger.info("All locations scraped")
