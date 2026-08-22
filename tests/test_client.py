from utils import FakeResponse, get_listing_html, get_softblock_html

from crawler.client import is_soft_blocked

import pytest
import requests

from crawler.client import SuumoClient, SoftBlockError

def test_detects_soft_block():
    html = get_softblock_html()
    assert is_soft_blocked(html.encode())


def test_normal_page_is_not_soft_block():
    html = get_listing_html()
    assert not is_soft_blocked(html.encode())


def test_fetch_page_returns_normal_response(monkeypatch):
    client = SuumoClient(
        user_agent="test-agent",
        request_delay=0,
    )

    response = FakeResponse(get_listing_html())

    calls = []

    def fake_get(url, timeout):
        calls.append(url)
        return response

    monkeypatch.setattr(client.session, "get", fake_get)

    result = client.fetch_page("https://example.com")

    assert result is response
    assert len(calls) == 1

def test_fetch_page_retries_soft_blocks(monkeypatch):
    client = SuumoClient(
        user_agent="test-agent",
        request_delay=0,
    )

    blocked = FakeResponse(get_softblock_html(),status_code=503)

    success = FakeResponse(get_listing_html())

    responses = iter([
        blocked,
        blocked,
        success,
    ])

    def fake_get(url, timeout):
        return next(responses)

    monkeypatch.setattr(
        client.session,
        "get",
        fake_get,
    )

    monkeypatch.setattr(
        "crawler.client.time.sleep",
        lambda _: None,
    )
    result = client.fetch_page(
        "https://example.com"
    )

    assert result is success

def test_fetch_page_retries_expected_number_of_times(monkeypatch):
    client = SuumoClient(
        user_agent="test-agent",
        request_delay=0,
    )

    blocked = FakeResponse(get_softblock_html(),status_code=503)
    success = FakeResponse(get_listing_html())
    
    responses = iter([
        blocked,
        blocked,
        success,
    ])

    request_count = 0

    def fake_get(url, timeout):
        nonlocal request_count
        request_count += 1
        return next(responses)

    monkeypatch.setattr(
        client.session,
        "get",
        fake_get,
    )

    monkeypatch.setattr(
        "crawler.client.time.sleep",
        lambda _: None,
    )

    client.fetch_page(
        "https://example.com"
    )

    assert request_count == 3

def test_fetch_page_raises_after_max_retries(monkeypatch):
    client = SuumoClient(
        user_agent="test-agent",
        request_delay=0,
    )

    blocked = FakeResponse(get_softblock_html(),status_code=503)

    def fake_get(url, timeout):
        return blocked

    monkeypatch.setattr(
        client.session,
        "get",
        fake_get,
    )

    monkeypatch.setattr(
        "crawler.client.time.sleep",
        lambda _: None,
    )

    with pytest.raises(SoftBlockError):
        client.fetch_page(
            "https://example.com",
            max_attempts=3,
        )