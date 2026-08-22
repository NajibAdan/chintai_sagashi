import datetime
import hashlib
import logging
from os import getpid

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def url_key(u: str) -> str:
    """
    Returns a sha1 hash of the url
    """
    return hashlib.sha1(u.encode("utf-8")).hexdigest()


def parse_listing_page(
    soup: BeautifulSoup,
    page: int,
) -> list[dict]:
    """
    Loops through each listing in the HTML and extracts information from it.
    """
    records = []

    cassette_items = soup.find_all("div", class_="cassetteitem")

    for item in cassette_items:
        try:
            records.extend(parse_cassette_item(item, page))
        except (AttributeError, KeyError, IndexError) as e:
            logger.warning("[PID %s] Failed to parse apartment row: %s", getpid(), e)

    return records


def parse_cassette_item(item, page: int) -> list[dict]:
    """
    Loops through all the listings in an apartment group.
    """
    property_name = item.find("div", class_="cassetteitem_content-title").text.strip()

    location = item.find("li", class_="cassetteitem_detail-col1").text.strip()

    stations_info = item.find(
        "li",
        class_="cassetteitem_detail-col2",
    )

    stations = [
        station.text.strip()
        for station in stations_info.find_all(
            "div",
            class_="cassetteitem_detail-text",
        )
        if station.text.strip()
    ]

    building_meta = item.find("li", class_="cassetteitem_detail-col3").find_all("div")

    building_age = building_meta[0].text.strip()
    building_type = building_meta[1].text.strip()

    records = []

    for row in item.find_all("tr", class_="js-cassette_link"):
        try:
            record = parse_listing_row(
                row=row,
                page=page,
                property_name=property_name,
                location=location,
                stations=stations,
                building_age=building_age,
                building_type=building_type,
            )

            records.append(record)

        except (AttributeError, KeyError, IndexError) as e:
            logger.warning("[PID %s] Failed to parse apartment row: %s", getpid(), e)

    return records


def parse_listing_row(
    row,
    page: int,
    property_name: str,
    location: str,
    stations: list[str],
    building_age: str,
    building_type: str,
) -> dict:
    """
    Parses the listing information in the HTML
    """
    rent = row.find(
        "span",
        class_="cassetteitem_price--rent",
    ).text.strip()

    management_fee = row.find(
        "span",
        class_="cassetteitem_price--administration",
    ).text.strip()

    deposit = row.find(
        "span",
        class_="cassetteitem_price--deposit",
    ).text.strip()

    gratuity = row.find(
        "span",
        class_="cassetteitem_price--gratuity",
    ).text.strip()

    madori = row.find(
        "span",
        class_="cassetteitem_madori",
    ).text.strip()

    menseki = row.find(
        "span",
        class_="cassetteitem_menseki",
    ).text.strip()

    apartment_url = (
        "https://suumo.jp"
        + row.find(
            "a",
            class_="js-cassette_link_href",
        )["href"]
    )

    apartment_floor = row.find_all("td")[2].text.strip()

    crawl_ts = (
        datetime.datetime.now(datetime.UTC)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )

    return {
        "schema_version": 2,
        "source": "suumo",
        "crawl_ts": crawl_ts,
        "listing_page": page,
        "property_name": property_name,
        "location": location,
        "stations": stations,
        "monthly_rent": rent,
        "management_fee": management_fee,
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
