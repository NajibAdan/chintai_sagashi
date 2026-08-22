from bs4 import BeautifulSoup
import requests
class FakeResponse:
    def __init__(self, html: str,status_code: int=200):
        self.content = html.encode()
        self.text = html
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code == 503:
            raise requests.exceptions.HTTPError()
        else:
            pass

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
