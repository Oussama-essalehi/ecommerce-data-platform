"""Order generation: demand model, baskets and order lifecycle.

`OrderBook.day(d)` is a pure function of (seed, d): a given business day
always yields the same orders, whatever was generated before it.
"""

from __future__ import annotations

import itertools
import math
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from .reference import CITIES
from .settings import PARIS, UTC, Settings, rng_for
from .world import World

CHANNELS = ["marketplace", "web", "mobile"]
CHANNEL_WEIGHTS = [0.30, 0.42, 0.28]
PAYMENT_METHODS = ["card", "paypal", "apple_pay", "installments", "bank_transfer"]
PAYMENT_WEIGHTS = [0.60, 0.20, 0.09, 0.07, 0.04]

# Share of orders per hour of the day (Paris time): quiet nights, lunch and evening peaks.
HOUR_WEIGHTS = [1, 0.5, 0.3, 0.2, 0.2, 0.4, 1, 2, 3.5, 4.5, 5, 5.5,
                6.5, 6, 5, 4.5, 4.5, 5, 6, 7, 8, 8.5, 6, 3]
_HOUR_CUM = list(itertools.accumulate(HOUR_WEIGHTS))
_CHANNEL_CUM = list(itertools.accumulate(CHANNEL_WEIGHTS))
_PAYMENT_CUM = list(itertools.accumulate(PAYMENT_WEIGHTS))
_CITY_CUM = list(itertools.accumulate(weight for _, _, weight in CITIES))

WEEKDAY_FACTOR = [0.95, 0.90, 0.92, 0.98, 1.05, 1.20, 1.15]  # Monday..Sunday
MONTH_FACTOR = [0.95, 0.85, 0.90, 0.95, 1.00, 1.00, 0.90, 0.80, 1.00, 1.00, 1.25, 1.50]

FREE_SHIPPING_FROM = 60.0

# An order can still change status this many days after it was created
# (worst case: bank transfer + shipping + delivery + return). Anything that
# needs "all orders touched on day D" looks back this far.
MAX_LIFECYCLE_DAYS = 24


@dataclass(frozen=True, slots=True)
class OrderLine:
    line_number: int
    product_id: str
    quantity: int
    unit_price: float
    discount_pct: int

    @property
    def amount(self) -> float:
        return round(self.quantity * self.unit_price * (1 - self.discount_pct / 100), 2)


@dataclass(frozen=True, slots=True)
class StatusChange:
    status: str
    ts: datetime


@dataclass(frozen=True, slots=True)
class Order:
    order_id: str
    customer_id: str
    channel: str
    created_at: datetime
    payment_method: str
    shipping_city: str
    shipping_postal_code: str
    shipping_fee: float
    lines: tuple[OrderLine, ...]
    # Status changes after creation, in chronological order.
    lifecycle: tuple[StatusChange, ...]

    @property
    def total_amount(self) -> float:
        return round(sum(line.amount for line in self.lines) + self.shipping_fee, 2)

    def status_at(self, ts: datetime) -> tuple[str, datetime]:
        """Status of the order at `ts`, and when it took that status."""
        status, since = "created", self.created_at
        for change in self.lifecycle:
            if change.ts > ts:
                break
            status, since = change.status, change.ts
        return status, since

    def touched_between(self, start: datetime, end: datetime) -> bool:
        """True if the order was created or changed status in [start, end)."""
        if start <= self.created_at < end:
            return True
        return any(start <= change.ts < end for change in self.lifecycle)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(weekday - first.weekday()) % 7 + 7 * (n - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    next_month = date(year + month // 12, month % 12 + 1, 1)
    last = next_month - timedelta(days=1)
    return last - timedelta(days=(last.weekday() - weekday) % 7)


def promotion(day: date) -> tuple[str, float] | None:
    """Sales events of the French retail calendar: (name, demand multiplier)."""
    black_friday = _nth_weekday(day.year, 11, 3, 4) + timedelta(days=1)  # day after 4th Thursday
    if black_friday - timedelta(days=4) <= day <= black_friday + timedelta(days=3):
        return "black_friday", 1.8
    # Legal dates of the French sales: second Wednesday of January (the first
    # one if that falls after the 12th), last Wednesday of June (the one
    # before if that falls after the 28th). Four weeks each.
    winter_sales = _nth_weekday(day.year, 1, 2, 2)
    if winter_sales.day > 12:
        winter_sales -= timedelta(days=7)
    if winter_sales <= day < winter_sales + timedelta(days=28):
        return "soldes_hiver", 1.3
    summer_sales = _last_weekday(day.year, 6, 2)
    if summer_sales.day > 28:
        summer_sales -= timedelta(days=7)
    if summer_sales <= day < summer_sales + timedelta(days=28):
        return "soldes_ete", 1.25
    return None


def expected_orders(settings: Settings, day: date) -> float:
    """Mean number of orders for a day: trend x weekday x month x promotions."""
    years = (day - settings.start_date).days / 365
    trend = 1 + 0.25 * years
    promo = promotion(day)
    return (
        settings.orders_per_day
        * trend
        * WEEKDAY_FACTOR[day.weekday()]
        * MONTH_FACTOR[day.month - 1]
        * (promo[1] if promo else 1.0)
    )


def _lifecycle(rng, created_at: datetime, payment_method: str) -> tuple[StatusChange, ...]:
    if rng.random() < 0.04:
        cancelled_at = created_at + timedelta(minutes=rng.uniform(5, 48 * 60))
        return (StatusChange("cancelled", cancelled_at),)
    if payment_method == "bank_transfer":
        paid_at = created_at + timedelta(days=rng.uniform(1, 3))
    else:
        paid_at = created_at + timedelta(seconds=rng.uniform(20, 900))
    shipped_at = paid_at + timedelta(days=rng.uniform(0.5, 3))
    delivered_at = shipped_at + timedelta(days=rng.uniform(1, 4))
    changes = [
        StatusChange("paid", paid_at),
        StatusChange("shipped", shipped_at),
        StatusChange("delivered", delivered_at),
    ]
    if rng.random() < 0.05:
        changes.append(StatusChange("returned", delivered_at + timedelta(days=rng.uniform(2, 12))))
    return tuple(changes)


class OrderBook:
    """Generates and caches the orders of each business day."""

    def __init__(self, world: World, settings: Settings | None = None, cache_days: int = 96):
        self.world = world
        self.settings = settings or world.settings
        self._cache: OrderedDict[date, tuple[Order, ...]] = OrderedDict()
        self._cache_days = cache_days

    def day(self, day: date) -> tuple[Order, ...]:
        """All orders created on a Europe/Paris business day, sorted by creation time."""
        if day in self._cache:
            self._cache.move_to_end(day)
            return self._cache[day]
        orders = self._generate(day)
        self._cache[day] = orders
        if len(self._cache) > self._cache_days:
            self._cache.popitem(last=False)
        return orders

    def days(self, start: date, end: date):
        """Orders of every day from start to end, both included."""
        current = start
        while current <= end:
            yield from self.day(current)
            current += timedelta(days=1)

    def _generate(self, day: date) -> tuple[Order, ...]:
        settings = self.settings
        if day < settings.start_date:
            return ()
        rng = rng_for(settings.seed, "orders", day.isoformat())

        # Poisson-like count with a little extra day-to-day noise.
        mean = expected_orders(settings, day) * math.exp(rng.gauss(0, 0.07))
        count = max(0, round(rng.gauss(mean, math.sqrt(mean))))

        promo = promotion(day)
        products, product_weights = self.world.sellable_products(day)
        cities = [(name, postal) for name, postal, _ in CITIES]
        midnight = datetime.combine(day, time.min, tzinfo=PARIS)

        drafts = []
        for _ in range(count):
            hour = rng.choices(range(24), cum_weights=_HOUR_CUM)[0]
            local = midnight + timedelta(hours=hour, seconds=rng.randrange(3600))
            created_at = local.astimezone(UTC) + timedelta(milliseconds=rng.randrange(1000))
            channel = rng.choices(CHANNELS, cum_weights=_CHANNEL_CUM)[0]
            payment_method = rng.choices(PAYMENT_METHODS, cum_weights=_PAYMENT_CUM)[0]
            customer = self.world.pick_customer(rng, created_at)

            basket_size = rng.choices([1, 2, 3, 4, 5], weights=[55, 25, 12, 5, 3])[0]
            lines: list[OrderLine] = []
            seen: set[str] = set()
            for _ in range(basket_size):
                product = rng.choices(products, cum_weights=product_weights)[0]
                if product.product_id in seen:
                    continue
                seen.add(product.product_id)
                if promo:
                    discount = rng.choice([10, 15, 20, 30]) if rng.random() < 0.6 else 0
                else:
                    discount = rng.choice([5, 10, 15]) if rng.random() < 0.08 else 0
                lines.append(
                    OrderLine(
                        line_number=len(lines) + 1,
                        product_id=product.product_id,
                        quantity=rng.choices([1, 2, 3], weights=[80, 15, 5])[0],
                        unit_price=product.price_at(created_at),
                        discount_pct=discount,
                    )
                )

            subtotal = sum(line.amount for line in lines)
            if subtotal >= FREE_SHIPPING_FROM:
                shipping_fee = 0.0
            else:
                shipping_fee = 5.90 if channel == "marketplace" else 4.90

            if rng.random() < 0.9:
                city, postal_code = customer.location_at(created_at)
            else:  # gift, office, pick-up point...
                city, postal_code = rng.choices(cities, cum_weights=_CITY_CUM)[0]

            drafts.append(
                dict(
                    customer_id=customer.customer_id,
                    channel=channel,
                    created_at=created_at,
                    payment_method=payment_method,
                    shipping_city=city,
                    shipping_postal_code=postal_code,
                    shipping_fee=shipping_fee,
                    lines=tuple(lines),
                    lifecycle=_lifecycle(rng, created_at, payment_method),
                )
            )

        # Order numbers are sequential per source system, in creation order.
        drafts.sort(key=lambda draft: draft["created_at"])
        counters = {"MKP": 0, "WEB": 0}
        orders = []
        for draft in drafts:
            prefix = "MKP" if draft["channel"] == "marketplace" else "WEB"
            counters[prefix] += 1
            orders.append(
                Order(order_id=f"{prefix}-{day:%Y%m%d}-{counters[prefix]:05d}", **draft)
            )
        return tuple(orders)
