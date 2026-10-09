"""HTTP client for the Shop API: pagination and retries.

The API is paginated and sometimes answers 503. This client hides both: it
yields records one by one and retries transient failures with a growing
delay, so a blip does not fail the whole load.
"""

from __future__ import annotations

import logging
import random
import time
from typing import Callable, Iterator

import requests

log = logging.getLogger(__name__)

RETRYABLE_STATUS = {429, 500, 502, 503, 504}
MAX_DELAY_SECONDS = 30.0


class ApiError(RuntimeError):
    """The API could not be read: retries exhausted, or a non-retryable answer."""


class ShopApiClient:
    def __init__(
        self,
        base_url: str,
        *,
        session: requests.Session | None = None,
        page_size: int = 500,
        max_attempts: int = 6,
        backoff_seconds: float = 0.5,
        timeout_seconds: float = 15.0,
        sleep: Callable[[float], None] = time.sleep,
    ):
        self.base_url = base_url.rstrip("/")
        self.session = session or requests.Session()
        self.page_size = page_size
        self.max_attempts = max_attempts
        self.backoff_seconds = backoff_seconds
        self.timeout_seconds = timeout_seconds
        self.sleep = sleep
        self.retries = 0  # how many calls had to be repeated, for the run summary

    def _delay(self, attempt: int, retry_after: str | None) -> float:
        if retry_after is not None:
            try:
                return min(float(retry_after), MAX_DELAY_SECONDS)
            except ValueError:
                pass
        # Exponential backoff with jitter: 0.5s, 1s, 2s... each +/- 25%.
        delay = self.backoff_seconds * 2 ** (attempt - 1)
        return min(delay * random.uniform(0.75, 1.25), MAX_DELAY_SECONDS)

    def get(self, path: str, params: dict) -> dict:
        url = f"{self.base_url}{path}"
        for attempt in range(1, self.max_attempts + 1):
            retry_after = None
            try:
                response = self.session.get(url, params=params, timeout=self.timeout_seconds)
            except (requests.ConnectionError, requests.Timeout) as error:
                problem = f"{type(error).__name__}: {error}"
            else:
                if response.status_code == 200:
                    return response.json()
                if response.status_code not in RETRYABLE_STATUS:
                    raise ApiError(
                        f"GET {url} answered {response.status_code}: {response.text[:200]}"
                    )
                problem = f"HTTP {response.status_code}"
                retry_after = response.headers.get("Retry-After")

            if attempt == self.max_attempts:
                raise ApiError(f"GET {url} failed after {attempt} attempts, last error: {problem}")
            delay = self._delay(attempt, retry_after)
            self.retries += 1
            log.warning("GET %s: %s, retrying in %.1fs (attempt %d/%d)",
                        url, problem, delay, attempt, self.max_attempts)
            self.sleep(delay)
        raise AssertionError("unreachable")

    def iter_records(self, entity: str, updated_since: str | None = None) -> Iterator[dict]:
        """Every record of `/customers` or `/products`, optionally only recent changes."""
        page = 1
        while True:
            params: dict = {"page": page, "page_size": self.page_size}
            if updated_since:
                params["updated_since"] = updated_since
            body = self.get(f"/{entity}", params)
            yield from body["data"]
            if not body["pagination"]["has_next"]:
                return
            page += 1
