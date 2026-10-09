from __future__ import annotations

import dataclasses
from datetime import date

import pytest

from shop_sim.orders import OrderBook
from shop_sim.settings import Settings
from shop_sim.world import World

# A small world keeps the suite fast while exercising the same code paths.
SMALL = Settings(
    seed=7,
    start_date=date(2025, 10, 1),
    orders_per_day=80,
    n_customers=3_000,
    defect_rate=1.0,
    api_error_rate=0.0,
)


@pytest.fixture(scope="session")
def settings() -> Settings:
    return SMALL


@pytest.fixture(scope="session")
def clean_settings() -> Settings:
    """Same world, with every injected defect switched off."""
    return dataclasses.replace(SMALL, defect_rate=0.0)


@pytest.fixture(scope="session")
def world(settings: Settings) -> World:
    return World(settings)


@pytest.fixture(scope="session")
def clean_world(clean_settings: Settings) -> World:
    return World(clean_settings)


@pytest.fixture()
def book(world: World) -> OrderBook:
    return OrderBook(world)


@pytest.fixture()
def clean_book(clean_world: World) -> OrderBook:
    return OrderBook(clean_world)
