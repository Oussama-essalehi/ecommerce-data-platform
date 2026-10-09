"""Shop API: the simulated back-office that owns customers and products.

It behaves like a real source system an ingestion job has to cope with:
paginated, filterable by `updated_since` for incremental loads, and
occasionally unavailable (HTTP 503).
"""

from __future__ import annotations

import random
from datetime import datetime
from typing import Callable

from fastapi import Depends, FastAPI, HTTPException, Query

from .settings import UTC, Settings
from .world import World

MAX_PAGE_SIZE = 500


def _as_utc(ts: datetime | None) -> datetime | None:
    if ts is not None and ts.tzinfo is None:
        return ts.replace(tzinfo=UTC)
    return ts


def _page(records: list, page: int, page_size: int, render: Callable) -> dict:
    total = len(records)
    total_pages = max(1, -(-total // page_size))
    start = (page - 1) * page_size
    return {
        "data": [render(record) for record in records[start : start + page_size]],
        "pagination": {
            "page": page,
            "page_size": page_size,
            "total": total,
            "total_pages": total_pages,
            "has_next": page < total_pages,
        },
    }


def create_app(
    settings: Settings | None = None,
    world: World | None = None,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    settings = settings or Settings.from_env()
    world = world or World(settings)
    now = clock or (lambda: datetime.now(UTC))
    chaos = random.Random()

    app = FastAPI(
        title="Shop API",
        description="Simulated source system serving synthetic customers and products.",
        version="1.0.0",
    )

    def sometimes_unavailable() -> None:
        if chaos.random() < settings.api_error_rate:
            raise HTTPException(
                status_code=503,
                detail="Service temporarily unavailable, retry later.",
                headers={"Retry-After": "1"},
            )

    @app.get("/health")
    def health() -> dict:
        return {"status": "ok"}

    @app.get("/customers", dependencies=[Depends(sometimes_unavailable)])
    def customers(
        page: int = Query(1, ge=1),
        page_size: int = Query(100, ge=1, le=MAX_PAGE_SIZE),
        updated_since: datetime | None = Query(
            None, description="Only records created or changed at or after this instant."
        ),
    ) -> dict:
        as_of = now()
        since = _as_utc(updated_since)
        records = [
            (c.updated_at(as_of), c.customer_id, c)
            for c in world.customers_as_of(as_of)
        ]
        if since is not None:
            records = [r for r in records if r[0] >= since]
        # Oldest change first: new and changed records land on the last pages,
        # so pages already read do not shift while a client is paginating.
        records.sort(key=lambda r: (r[0], r[1]))
        return _page(records, page, page_size, lambda r: r[2].as_api(as_of))

    @app.get("/products", dependencies=[Depends(sometimes_unavailable)])
    def products(
        page: int = Query(1, ge=1),
        page_size: int = Query(100, ge=1, le=MAX_PAGE_SIZE),
        updated_since: datetime | None = Query(
            None, description="Only records created or changed at or after this instant."
        ),
    ) -> dict:
        as_of = now()
        since = _as_utc(updated_since)
        records = [
            (p.updated_at(as_of), p.product_id, p) for p in world.products_as_of(as_of)
        ]
        if since is not None:
            records = [r for r in records if r[0] >= since]
        records.sort(key=lambda r: (r[0], r[1]))
        return _page(records, page, page_size, lambda r: r[2].as_api(as_of))

    return app
