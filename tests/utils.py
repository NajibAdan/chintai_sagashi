from bs4 import BeautifulSoup


def get_softblock_html() -> str:
    with open("tests/fixtures/soft-block.html", encoding="utf-8") as f:
        html = f.read()
    return html


def get_listing_html() -> str:
    with open("tests/fixtures/listing-page.html", encoding="utf-8") as f:
        html = f.read()
    return html


def get_listing_page() -> BeautifulSoup:
    return BeautifulSoup(get_listing_html(), "html.parser")
