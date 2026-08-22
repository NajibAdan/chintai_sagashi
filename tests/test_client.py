from crawler.client import is_soft_blocked
from utils import get_softblock_html, get_listing_html

def test_detects_soft_block():
    html = get_softblock_html()
    assert is_soft_blocked(html.encode())

def test_normal_page_is_not_soft_block():
    html = get_listing_html()
    assert not is_soft_blocked(html.encode())