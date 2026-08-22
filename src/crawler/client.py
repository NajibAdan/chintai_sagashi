import logging
import time

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


class SoftBlockError(Exception):
    """
    Soft Block Exception for when SUUMO raises a 503-HTML page
    """


class SuumoClient:
    """
    A SUUMO client.

    Fetches pages and checks for soft-blocks when raised on SUUMO.
    """

    def __init__(
        self,
        user_agent: str,
        request_delay: float = 1,
        timeout: int = 30,
    ):
        self.request_delay = request_delay
        self.timeout = timeout

        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": user_agent,
            }
        )

    def fetch(self, url: str) -> requests.Response:
        """
        Fetches a URL and waits `request_delay` after each fetch
        """
        logger.info("Fetching %s", url)

        response = self.session.get(
            url,
            timeout=self.timeout,
        )

        response.raise_for_status()

        time.sleep(self.request_delay)

        return response

    def fetch_page(
        self,
        url: str,
        max_attempts: int = 20,
        base_retry_delay: float = 3,
        retry_multiplier: float = 1.2,
    ) -> requests.Response:
        """
        Fetches a SUUMO page. Retries again after a delay when a soft-block is raised
        """
        for attempt in range(1, max_attempts + 1):
            response = self.fetch(url)

            if not is_soft_blocked(response.content):
                return response

            delay = base_retry_delay * (retry_multiplier**attempt)

            logger.warning(
                "Soft blocked. Attempt %s/%s. Retrying in %.1f seconds.",
                attempt,
                max_attempts,
                delay,
            )

            time.sleep(delay)

        raise SoftBlockError(f"Maximum retry attempts reached: {url}")


SOFT_BLOCK_TITLES = {
    "【SUUMO】アクセス集中に関するお詫び",
    "【SUUMO】 ページを表示できません。",
}


def is_soft_blocked(html: bytes) -> bool:
    """
    Checks if the HTML title contains any of the so-far know titles that SUUMO displays whenever a soft-block happens.
    """
    soup = BeautifulSoup(html, "html.parser")

    title = soup.find("title")

    if not title:
        return False

    return title.text.strip() in SOFT_BLOCK_TITLES
