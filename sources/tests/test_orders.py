from __future__ import annotations

from datetime import date, datetime, time, timedelta

from shop_sim.orders import MAX_LIFECYCLE_DAYS, OrderBook, expected_orders, promotion
from shop_sim.settings import PARIS, UTC

DAY = date(2026, 3, 14)


def test_a_day_is_deterministic_whatever_was_generated_before(world):
    first = OrderBook(world)
    first.day(DAY - timedelta(days=1))
    first.day(DAY)

    second = OrderBook(world)
    second.day(DAY + timedelta(days=30))
    assert second.day(DAY) == first.day(DAY)


def test_nothing_exists_before_the_start_date(book, settings):
    assert book.day(settings.start_date - timedelta(days=1)) == ()
    assert len(book.day(settings.start_date)) > 0


def test_orders_belong_to_their_business_day(book):
    orders = book.day(DAY)
    assert all(o.created_at.astimezone(PARIS).date() == DAY for o in orders)
    assert [o.created_at for o in orders] == sorted(o.created_at for o in orders)


def test_order_ids_are_unique_and_prefixed_by_source(book):
    orders = book.day(DAY)
    assert len({o.order_id for o in orders}) == len(orders)
    for order in orders:
        prefix = "MKP" if order.channel == "marketplace" else "WEB"
        assert order.order_id.startswith(f"{prefix}-{DAY:%Y%m%d}-")
    assert {o.channel for o in orders} == {"marketplace", "web", "mobile"}


def test_orders_reference_customers_and_products_that_exist_at_that_time(book, world):
    customers = {c.customer_id: c for c in world.customers}
    for order in book.days(DAY, DAY + timedelta(days=6)):
        assert customers[order.customer_id].signup_at <= order.created_at
        for line in order.lines:
            product = world.products_by_id[line.product_id]
            assert product.created_at <= order.created_at
            assert product.discontinued_at is None or product.discontinued_at > order.created_at
            assert line.unit_price == product.price_at(order.created_at)


def test_amounts_add_up(book):
    for order in book.day(DAY):
        assert 1 <= len(order.lines) <= 5
        assert [line.line_number for line in order.lines] == list(range(1, len(order.lines) + 1))
        assert len({line.product_id for line in order.lines}) == len(order.lines)
        subtotal = round(sum(line.amount for line in order.lines), 2)
        assert order.total_amount == round(subtotal + order.shipping_fee, 2)
        if subtotal >= 60:
            assert order.shipping_fee == 0.0
        else:
            assert order.shipping_fee == (5.90 if order.channel == "marketplace" else 4.90)


def test_lifecycle_is_chronological_and_bounded(book):
    seen = set()
    for order in book.days(DAY, DAY + timedelta(days=20)):
        timestamps = [order.created_at] + [change.ts for change in order.lifecycle]
        assert timestamps == sorted(timestamps)
        assert timestamps[-1] - order.created_at < timedelta(days=MAX_LIFECYCLE_DAYS - 1)
        statuses = [change.status for change in order.lifecycle]
        assert statuses in (
            ["cancelled"],
            ["paid", "shipped", "delivered"],
            ["paid", "shipped", "delivered", "returned"],
        )
        seen.update(statuses)
    assert seen == {"cancelled", "paid", "shipped", "delivered", "returned"}


def test_status_at_follows_the_lifecycle(book):
    order = next(o for o in book.day(DAY) if len(o.lifecycle) >= 3)
    assert order.status_at(order.created_at) == ("created", order.created_at)
    paid = order.lifecycle[0]
    assert order.status_at(paid.ts - timedelta(milliseconds=1))[0] == "created"
    assert order.status_at(paid.ts) == ("paid", paid.ts)
    assert order.status_at(datetime(2030, 1, 1, tzinfo=UTC))[0] == order.lifecycle[-1].status


def test_touched_between_is_half_open(book):
    order = book.day(DAY)[0]
    start = datetime.combine(DAY, time.min, tzinfo=PARIS).astimezone(UTC)
    assert order.touched_between(start, start + timedelta(days=1))
    assert not order.touched_between(start - timedelta(days=1), order.created_at)


def test_demand_model_has_weekly_seasonal_and_promotional_effects(settings):
    saturday, tuesday = date(2026, 3, 14), date(2026, 3, 10)
    assert expected_orders(settings, saturday) > expected_orders(settings, tuesday)
    assert expected_orders(settings, date(2025, 12, 10)) > expected_orders(settings, date(2026, 2, 11))
    # Black Friday 2025 was 28 November. In 2026 the winter sales start on
    # Wednesday 7 January (the second Wednesday is the 14th, after the 12th)
    # and the summer sales on Wednesday 24 June.
    assert promotion(date(2025, 11, 28)) == ("black_friday", 1.8)
    assert promotion(date(2025, 11, 20)) is None
    assert promotion(date(2026, 1, 6)) is None
    assert promotion(date(2026, 1, 7))[0] == "soldes_hiver"
    assert promotion(date(2026, 2, 3))[0] == "soldes_hiver"
    assert promotion(date(2026, 2, 4)) is None
    assert promotion(date(2026, 6, 23)) is None
    assert promotion(date(2026, 6, 24))[0] == "soldes_ete"
    assert promotion(date(2026, 3, 14)) is None


def test_daily_volume_is_close_to_the_model(book, settings):
    days = [DAY + timedelta(days=i) for i in range(28)]
    actual = sum(len(book.day(d)) for d in days)
    expected = sum(expected_orders(settings, d) for d in days)
    assert abs(actual - expected) / expected < 0.10
