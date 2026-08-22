from utils import get_listing_page
from crawler.parser import parse_listing_page


def test_parse_listing_page():
    soup = get_listing_page()
    records = parse_listing_page(soup, page=1)

    assert len(records) > 0

    record = records[0]

    assert record["source"] == "suumo"
    assert record["listing_page"] == 1
    assert record["url"].startswith("https://suumo.jp/")
    assert record["property_name"]
    assert record["monthly_rent"]

def test_first_listing_values():
    soup = get_listing_page()
    records =  parse_listing_page(soup, page=1)
    assert len(records) > 0
    record = records[0]

    assert record["property_name"] == "サンパレス千種"
    assert record["monthly_rent"] == "4.9万円"
    assert record["madori"] == "3DK"
    assert record["menseki"] == "46.28m2"