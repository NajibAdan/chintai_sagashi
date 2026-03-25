import requests
from bs4 import BeautifulSoup
import json
import os
import gzip
import hashlib
import time
from typing import Tuple, TextIO
import datetime
import boto3
import constants
import logging
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / "suumo_scraper.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


BASE_URL = "https://suumo.jp/chintai/miyagi/sa_sendai/?page={}&pc=50"

CRAWL_DATE = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d")
PARTITION_DIR = f"data/raw/suumo/crawl_date={CRAWL_DATE}/prefecture=miyagi/city=sendai"
os.makedirs(PARTITION_DIR, exist_ok=True)

s3 = boto3.client(
    "s3",
    region_name=constants.BUCKET_REGION,
    endpoint_url=constants.BUCKET_ENDPOINT,
    aws_access_key_id=constants.AWS_ACCESS_KEY,
    aws_secret_access_key=constants.AWS_SECRET_KEY,
)


def url_key(u: str) -> str:
    """
    Returns a sha1 hash of the url
    """
    return hashlib.sha1(u.encode("utf-8")).hexdigest()


def open_shard(page: int) -> Tuple[TextIO, str]:
    """
    Creates a gzipped JSONL file for each page
    """
    path = os.path.join(PARTITION_DIR, f"page-{page:06d}-part-0000.jsonl.gz")
    return gzip.open(path, "wt", encoding="utf-8"), path


def fetch_page(session: requests.Session, page: int) -> BeautifulSoup:
    """
    A fetch page helper function that returns the BeautifulSoup content
    of a page
    """
    url = BASE_URL.format(page)
    logger.info(f"Fetching {url}")
    r = session.get(url, timeout=30)
    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        logger.exception(e)
    return BeautifulSoup(r.content, "html.parser")


def get_total_pages(session: requests.Session) -> int:
    """
    Returns the total number of pagination pages
    """
    soup = fetch_page(session, 1)
    try:
        # Find the last page
        return int(
            soup.find("ol", class_="pagination-parts").find_all("li")[-1].text.strip()
        )
    except Exception:
        logger.exception("Failed to detect total pages. Trying again.")
        return 1


page = 1
total_pages = 1
retry_attempt = 0
attempts = 20

logger.info("Starting SUUMO scraper")
session = requests.Session()
soup = fetch_page(session, page)

while total_pages == 1:
    total_pages = get_total_pages(session)


while page <= total_pages:
    logger.info(
        f"Scraping page {page}/{total_pages}. Attempt: {retry_attempt}/{attempts}"
    )
    soup = fetch_page(session, page)
    retry_attempt += 1

    # Retry early if we get soft-blocked
    if soup.find("title").text.strip == "【SUUMO】アクセス集中に関するお詫び":
        time_to_sleep = 3 * ((1.2) ** retry_attempt)
        logger.warning(
            f"Got 0 records for page: {page}/{total_pages}. Retrying again after {time_to_sleep} seconds."
        )
        time.sleep(time_to_sleep)
    # Extract apartment details
    cassette_items = soup.find_all("div", class_="cassetteitem")

    out, path = open_shard(page)
    wrote = 0

    # Loop through each listing
    for item in cassette_items:
        property_name = item.find(
            "div", class_="cassetteitem_content-title"
        ).text.strip()
        location = item.find("li", class_="cassetteitem_detail-col1").text.strip()
        stations_info = item.find("li", class_="cassetteitem_detail-col2")
        stations = [
            station.text.strip()
            for station in stations_info.find_all(
                "div", class_="cassetteitem_detail-text"
            )
            if station.text.strip() != ""
        ]

        building_meta = item.find("li", class_="cassetteitem_detail-col3").find_all(
            "div"
        )
        building_age = building_meta[0].text.strip()
        building_type = building_meta[1].text.strip()

        rows = item.find_all("tr", class_="js-cassette_link")

        # A listing can have multiple apartments inside it
        # Loop through all of the apartments in that listing
        for row in rows:
            rent = row.find("span", class_="cassetteitem_price--rent").text.strip()
            admin_fee = row.find(
                "span", class_="cassetteitem_price--administration"
            ).text.strip()
            deposit = row.find(
                "span", class_="cassetteitem_price--deposit"
            ).text.strip()
            gratuity = row.find(
                "span", class_="cassetteitem_price--gratuity"
            ).text.strip()
            madori = row.find("span", class_="cassetteitem_madori").text.strip()
            menseki = row.find("span", class_="cassetteitem_menseki").text.strip()
            apartment_url = (
                "https://suumo.jp"
                + row.find("a", class_="js-cassette_link_href")["href"]
            )
            # which floor the apartment is on is contained in the 3rd <td> tag
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
                "location": location,
                "stations": stations,
                "monthly_rent": rent,
                "management_fee": admin_fee,
                "deposit": deposit,  # deposit fee
                "gratuity": gratuity,
                "apartment_floor": apartment_floor,
                "madori": madori,  # layout ie 1k, 2kdl
                "menseki": menseki,  # area of the apartment in m2
                "building_age": building_age,
                "building_type": building_type,
                "url": apartment_url,
                "url_key": url_key(apartment_url),
            }

            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1
    out.close()

    if wrote > 0:
        logger.info(f"Wrote {wrote} records --> {path}")
        # upload the file to S3
        logger.info(f"Uploading {path} to S3")
        s3.upload_file(path, constants.BUCKET_NAME, path)
        page += 1
        retry_attempt = 0
    elif retry_attempt >= attempts:
        logger.error(
            f"Maximum attempts reached for page {page}. Moving to the next page."
        )
        retry_attempt = 0
        page += 1
    # re-try the link again
    else:
        time_to_sleep = 3 * ((1.2) ** retry_attempt)
        logger.warning(
            f"Got 0 records for page: {page}/{total_pages}. Retrying again after {time_to_sleep} seconds."
        )
        time.sleep(time_to_sleep)
logger.info("Scraper finished")
