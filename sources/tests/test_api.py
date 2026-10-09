from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta

import pytest
from fastapi.testclient import TestClient

from shop_sim.api import create_app
from shop_sim.settings import UTC

NOW = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


@pytest.fixture()
def client(settings, world):
    return TestClient(create_app(settings, world, clock=lambda: NOW))


def _all_pages(client, path, **params):
    records, page = [], 1
    while True:
        body = client.get(path, params={"page": page, "page_size": 500, **params}).json()
        records.extend(body["data"])
        if not body["pagination"]["has_next"]:
            return records, body["pagination"]
        page += 1


def test_health(client):
    assert client.get("/health").json() == {"status": "ok"}


def test_customers_pagination_covers_everyone_exactly_once(client, world):
    records, pagination = _all_pages(client, "/customers")
    expected = world.customers_as_of(NOW)
    assert pagination["total"] == len(records) == len(expected)
    assert {r["customer_id"] for r in records} == {c.customer_id for c in expected}
    assert pagination["total_pages"] == -(-len(expected) // 500)


def test_customers_not_signed_up_yet_are_hidden(client, world):
    records, _ = _all_pages(client, "/customers")
    assert len(records) < len(world.customers)
    assert max(r["created_at"] for r in records) <= "2026-06-01T12:00:00.000Z"


def test_updated_since_returns_only_recent_changes(client, world):
    since = NOW - timedelta(days=7)
    records, _ = _all_pages(client, "/customers", updated_since=since.isoformat())
    expected = [c for c in world.customers_as_of(NOW) if c.updated_at(NOW) >= since]
    assert 0 < len(records) == len(expected)
    assert all(r["updated_at"] >= "2026-05-25T12:00:00.000Z" for r in records)
    # A naive timestamp is read as UTC.
    naive, _ = _all_pages(client, "/customers", updated_since="2026-05-25T12:00:00")
    assert naive == records


def test_records_are_sorted_by_last_change(client):
    records, _ = _all_pages(client, "/products")
    keys = [(r["updated_at"], r["product_id"]) for r in records]
    assert keys == sorted(keys)


def test_products_show_their_current_price_and_status(client, world):
    records, _ = _all_pages(client, "/products")
    by_id = {r["product_id"]: r for r in records}
    assert set(by_id) == {p.product_id for p in world.products_as_of(NOW)}
    for product in world.products_as_of(NOW):
        record = by_id[product.product_id]
        if product.api_defect is None:
            assert record["unit_price"] == product.price_at(NOW)
            assert record["category"] == product.category
        discontinued = product.discontinued_at is not None and product.discontinued_at <= NOW
        assert record["is_active"] is (not discontinued)
        assert record["currency"] == "EUR"


def test_page_size_is_bounded(client):
    assert client.get("/products", params={"page_size": 501}).status_code == 422
    assert client.get("/products", params={"page": 0}).status_code == 422
    beyond = client.get("/products", params={"page": 99, "page_size": 100}).json()
    assert beyond["data"] == [] and beyond["pagination"]["has_next"] is False


def test_api_fails_on_purpose_to_exercise_retries(settings, world):
    flaky = dataclasses.replace(settings, api_error_rate=1.0)
    client = TestClient(create_app(flaky, world, clock=lambda: NOW))
    response = client.get("/customers")
    assert response.status_code == 503
    assert response.headers["retry-after"] == "1"
    assert client.get("/health").status_code == 200
