from random import randint
import requests
from bs4 import BeautifulSoup
import json
import os
import gzip
import hashlib
import time
from typing import Tuple, TextIO
from datetime import datetime

BASE_URL = "https://suumo.jp/chintai/miyagi/sa_sendai/?page={}"

CRAWL_DATE = datetime.utcnow().strftime("%Y-%m-%d")
PARTITION_DIR = f"data/bronze/suumo/crawl_date={CRAWL_DATE}/prefecture=miyagi"
os.makedirs(PARTITION_DIR, exist_ok=True)


def url_key(u: str) -> str:
    """
    Returns a sha1 hash of the url
    """
    return hashlib.sha1(u.encode("utf-8")).hexdigest()


def open_shard(page: int) -> Tuple[TextIO, str]:
    """
    Creates a gzipped JSONL file for each page
    """
    shard_dir = os.path.join(PARTITION_DIR, f"page={page:06d}")
    os.makedirs(shard_dir, exist_ok=True)
    path = os.path.join(shard_dir, "part-0000.jsonl.gz")
    return gzip.open(path, "wt", encoding="utf-8"), path


total_pages = 666

for page in range(1, total_pages + 1):
    print(f"Scraping page {page}/{total_pages}")
    response = requests.get(BASE_URL.format(page))
    soup = BeautifulSoup(response.content, "html.parser")

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
            rec = {
                "schema_version": 1,
                "source": "suumo",
                "crawl_ts": datetime.utcnow().isoformat(timespec="seconds") + "Z",
                "listing_page": page,
                "property_name": property_name,
                "location": location,
                "stations": stations,
                "monthly_rent": rent,
                "management_fee": admin_fee,
                "deposit": deposit,  # deposit fee
                "gratuity": gratuity,
                "madori": madori,  # layout ie 1k, 2kdl
                "menseki": menseki,  # area of the apartment in m2
                "url": apartment_url,
                "url_key": url_key(apartment_url),
            }

            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            wrote += 1
    out.close()
    print(f"Wrote {wrote} records --> {path}")
    time.sleep(randint(1, 4))
