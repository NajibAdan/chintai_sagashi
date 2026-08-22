from bs4 import BeautifulSoup
from utils import get_listing_page

from crawler.crawler import get_total_pages


def test_get_total_pages():
    soup = get_listing_page()
    assert get_total_pages(soup) == 119


def test_get_total_pages_without_pagination():
    html = "<html><body></body></html>"

    soup = BeautifulSoup(html, "html.parser")

    assert get_total_pages(soup) == 1
