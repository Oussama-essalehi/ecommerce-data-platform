from __future__ import annotations

import pytest
import requests

from lakehouse.api_client import ApiError, ShopApiClient


class Response:
    def __init__(self, status_code: int, body: dict | None = None, headers: dict | None = None):
        self.status_code = status_code
        self._body = body or {}
        self.headers = headers or {}
        self.text = str(body)

    def json(self) -> dict:
        return self._body


class ScriptedSession:
    """Answers each call with the next scripted response (or raises it)."""

    def __init__(self, script: list):
        self.script = list(script)
        self.calls: list[dict] = []

    def get(self, url, params=None, timeout=None):
        self.calls.append({"url": url, "params": dict(params or {}), "timeout": timeout})
        answer = self.script.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer


def page(records: list[dict], has_next: bool) -> Response:
    return Response(200, {"data": records, "pagination": {"has_next": has_next}})


def make_client(script: list, **options) -> tuple[ShopApiClient, ScriptedSession, list[float]]:
    session, sleeps = ScriptedSession(script), []
    client = ShopApiClient("http://api:8000/", session=session, sleep=sleeps.append, **options)
    return client, session, sleeps


def test_follows_pagination_until_the_last_page():
    client, session, _ = make_client(
        [page([{"id": 1}, {"id": 2}], True), page([{"id": 3}], True), page([], False)],
        page_size=2,
    )
    assert list(client.iter_records("customers")) == [{"id": 1}, {"id": 2}, {"id": 3}]
    assert [call["params"] for call in session.calls] == [
        {"page": 1, "page_size": 2},
        {"page": 2, "page_size": 2},
        {"page": 3, "page_size": 2},
    ]
    assert session.calls[0]["url"] == "http://api:8000/customers"
    assert session.calls[0]["timeout"] == 15.0


def test_updated_since_is_sent_on_every_page():
    client, session, _ = make_client([page([{"id": 1}], True), page([{"id": 2}], False)])
    list(client.iter_records("products", updated_since="2026-10-01T00:00:00.000Z"))
    assert all(
        call["params"]["updated_since"] == "2026-10-01T00:00:00.000Z" for call in session.calls
    )


def test_retries_a_503_and_honours_retry_after():
    client, session, sleeps = make_client(
        [Response(503, headers={"Retry-After": "1"}), page([{"id": 1}], False)]
    )
    assert list(client.iter_records("customers")) == [{"id": 1}]
    assert sleeps == [1.0]
    assert client.retries == 1
    assert len(session.calls) == 2


def test_backs_off_exponentially_on_connection_errors():
    client, _, sleeps = make_client(
        [requests.ConnectionError("refused"), requests.Timeout("slow"),
         Response(502), page([{"id": 1}], False)],
        backoff_seconds=1.0,
    )
    assert list(client.iter_records("customers")) == [{"id": 1}]
    assert len(sleeps) == 3
    for delay, base in zip(sleeps, [1.0, 2.0, 4.0]):
        assert 0.75 * base <= delay <= 1.25 * base  # jitter of +/- 25%


def test_gives_up_after_max_attempts():
    client, session, sleeps = make_client([Response(503)] * 3, max_attempts=3)
    with pytest.raises(ApiError, match="failed after 3 attempts.*HTTP 503"):
        list(client.iter_records("customers"))
    assert len(session.calls) == 3
    assert len(sleeps) == 2


def test_client_errors_are_not_retried():
    client, session, sleeps = make_client([Response(422, {"detail": "bad page"})])
    with pytest.raises(ApiError, match="answered 422"):
        list(client.iter_records("customers"))
    assert len(session.calls) == 1 and sleeps == []


def test_a_failure_on_a_later_page_does_not_restart_from_page_one():
    client, session, _ = make_client(
        [page([{"id": 1}], True), Response(503), page([{"id": 2}], False)]
    )
    assert list(client.iter_records("customers")) == [{"id": 1}, {"id": 2}]
    assert [call["params"]["page"] for call in session.calls] == [1, 2, 2]
