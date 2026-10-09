from __future__ import annotations

from datetime import datetime, timedelta

from shop_sim.settings import UTC
from shop_sim.world import World


def test_world_is_deterministic(settings, world):
    again = World(settings)
    assert again.customers == world.customers
    assert again.products == world.products


def test_a_different_seed_gives_a_different_world(settings, world):
    import dataclasses

    other = World(dataclasses.replace(settings, seed=settings.seed + 1))
    assert other.customers != world.customers


def test_customer_ids_and_emails_are_unique(world):
    assert len({c.customer_id for c in world.customers}) == len(world.customers)
    assert len({c.email for c in world.customers}) == len(world.customers)


def test_customer_ids_follow_signup_order(world):
    signups = [c.signup_at for c in world.customers]
    assert signups == sorted(signups)
    assert [c.customer_id for c in world.customers] == sorted(c.customer_id for c in world.customers)


def test_customers_as_of_only_returns_existing_customers(world):
    as_of = datetime(2026, 3, 1, tzinfo=UTC)
    existing = world.customers_as_of(as_of)
    assert 0 < len(existing) < len(world.customers)
    assert all(c.signup_at <= as_of for c in existing)


def test_a_move_changes_location_and_updated_at(world):
    mover = next(c for c in world.customers if c.moved_at is not None)
    before = mover.moved_at - timedelta(seconds=1)
    assert mover.location_at(before) == (mover.city, mover.postal_code)
    assert mover.location_at(mover.moved_at) == (mover.new_city, mover.new_postal_code)
    assert mover.updated_at(before) == mover.signup_at
    assert mover.updated_at(mover.moved_at) == mover.moved_at


def test_postal_codes_keep_their_leading_zero(world):
    codes = {c.postal_code for c in world.customers}
    assert all(len(code) == 5 and code.isdigit() for code in codes)
    assert any(code.startswith("0") for code in codes)


def test_product_catalog_shape(world):
    assert len(world.products) == 144
    assert len({p.product_id for p in world.products}) == 144
    assert len({p.name for p in world.products}) == 144
    assert all(price > 0 for p in world.products for _, price in p.price_points)


def test_price_history_is_applied_in_order(world):
    product = next(p for p in world.products if len(p.price_points) > 1)
    (first_from, first_price), (second_from, second_price) = product.price_points[:2]
    assert product.price_at(first_from) == first_price
    assert product.price_at(second_from - timedelta(seconds=1)) == first_price
    assert product.price_at(second_from) == second_price
    assert product.updated_at(second_from) == second_from


def test_api_view_carries_the_injected_defects(world, clean_world):
    as_of = datetime(2028, 1, 1, tzinfo=UTC)
    defects = {c.api_defect for c in world.customers} | {p.api_defect for p in world.products}
    assert {"email_case", "null_postal_code", "bad_email"} <= defects

    shouting = next(c for c in world.customers if c.api_defect == "email_case")
    assert shouting.as_api(as_of)["email"] != shouting.email
    assert shouting.as_api(as_of)["email"].strip().lower() == shouting.email

    assert all(c.api_defect is None for c in clean_world.customers)
    assert all(p.api_defect is None for p in clean_world.products)
