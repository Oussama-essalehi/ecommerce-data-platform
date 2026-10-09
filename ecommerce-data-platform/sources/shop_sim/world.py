"""Master data of the shop: customers and products.

The World holds the *truth*. The API layer (see `as_api`) is what the outside
sees, and that view is where master-data defects are injected.
"""

from __future__ import annotations

import bisect
import itertools
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

from .reference import (
    CATALOG,
    CATEGORY_SEASONALITY,
    CITIES,
    EMAIL_DOMAINS,
    FIRST_NAMES,
    LAST_NAMES,
    PHONE_PREFIX,
    VARIANTS_PER_TYPE,
)
from .settings import PARIS, UTC, Settings, rng_for

# Customers keep signing up, and prices keep changing, for this long after
# start_date. Past that horizon the world simply stops evolving.
HORIZON_DAYS = 3 * 365


def iso(ts: datetime) -> str:
    """UTC ISO-8601 with millisecond precision, e.g. 2026-10-08T20:15:03.123Z."""
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _slug(text: str) -> str:
    decomposed = unicodedata.normalize("NFKD", text)
    return "".join(c for c in decomposed if c.isascii() and c.isalnum()).lower()


@dataclass(frozen=True, slots=True)
class Customer:
    customer_id: str
    first_name: str
    last_name: str
    email: str
    phone: str
    city: str
    postal_code: str
    signup_at: datetime
    moved_at: datetime | None
    new_city: str | None
    new_postal_code: str | None
    loyal: bool
    api_defect: str | None

    def location_at(self, ts: datetime) -> tuple[str, str]:
        if self.moved_at is not None and self.moved_at <= ts:
            return self.new_city, self.new_postal_code  # type: ignore[return-value]
        return self.city, self.postal_code

    def updated_at(self, as_of: datetime) -> datetime:
        if self.moved_at is not None and self.moved_at <= as_of:
            return self.moved_at
        return self.signup_at

    def as_api(self, as_of: datetime) -> dict:
        city, postal_code = self.location_at(as_of)
        email: str | None = self.email
        if self.api_defect == "email_case":
            email = f" {self.email.upper()} "
        elif self.api_defect == "bad_email":
            email = self.email.replace("@", "")
        elif self.api_defect == "null_postal_code":
            postal_code = None  # type: ignore[assignment]
        return {
            "customer_id": self.customer_id,
            "first_name": self.first_name,
            "last_name": self.last_name,
            "email": email,
            "phone": self.phone,
            "city": city,
            "postal_code": postal_code,
            "country": "FR",
            "created_at": iso(self.signup_at),
            "updated_at": iso(self.updated_at(as_of)),
        }


@dataclass(frozen=True, slots=True)
class Product:
    product_id: str
    name: str
    category: str
    brand: str
    created_at: datetime
    discontinued_at: datetime | None
    # (valid_from, price), sorted by valid_from. The first entry starts at created_at.
    price_points: tuple[tuple[datetime, float], ...]
    popularity: float
    api_defect: str | None

    def price_at(self, ts: datetime) -> float:
        price = self.price_points[0][1]
        for valid_from, value in self.price_points:
            if valid_from > ts:
                break
            price = value
        return price

    def updated_at(self, as_of: datetime) -> datetime:
        changes = [valid_from for valid_from, _ in self.price_points if valid_from <= as_of]
        if self.discontinued_at is not None and self.discontinued_at <= as_of:
            changes.append(self.discontinued_at)
        return max(changes, default=self.created_at)

    def as_api(self, as_of: datetime) -> dict:
        discontinued = self.discontinued_at is not None and self.discontinued_at <= as_of
        return {
            "product_id": self.product_id,
            "name": self.name,
            "category": None if self.api_defect == "null_category" else self.category,
            "brand": None if self.api_defect == "null_brand" else self.brand,
            "unit_price": 0.0 if self.api_defect == "zero_price" else self.price_at(as_of),
            "currency": "EUR",
            "is_active": not discontinued,
            "created_at": iso(self.created_at),
            "updated_at": iso(self.updated_at(as_of)),
        }


def _pick_defect(roll: float, rate: float, table: list[tuple[float, str]]) -> str | None:
    """Map a uniform roll to a defect label using cumulative probabilities."""
    threshold = 0.0
    for probability, label in table:
        threshold += probability * rate
        if roll < threshold:
            return label
    return None


def _build_customers(settings: Settings) -> list[Customer]:
    rng = rng_for(settings.seed, "customers")
    origin = datetime.combine(settings.start_date, time.min, tzinfo=UTC) - timedelta(days=365)
    span_seconds = (365 + HORIZON_DAYS) * 86_400
    offsets = sorted(rng.randrange(span_seconds) for _ in range(settings.n_customers))

    cities = [(name, postal) for name, postal, _ in CITIES]
    city_weights = list(itertools.accumulate(weight for _, _, weight in CITIES))

    customers: list[Customer] = []
    # IDs follow sign-up order, like a sequence in the source database would.
    for number, offset in enumerate(offsets, start=1):
        signup_at = origin + timedelta(seconds=offset)
        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        city, postal_code = rng.choices(cities, cum_weights=city_weights)[0]
        moved_at = new_city = new_postal_code = None
        if rng.random() < 0.08:
            moved_at = signup_at + timedelta(days=rng.uniform(30, 900))
            new_city, new_postal_code = rng.choices(cities, cum_weights=city_weights)[0]
        customers.append(
            Customer(
                customer_id=f"C{number:07d}",
                first_name=first_name,
                last_name=last_name,
                email=f"{_slug(first_name)}.{_slug(last_name)}{number}@{rng.choice(EMAIL_DOMAINS)}",
                phone=f"{PHONE_PREFIX}{rng.randrange(10_000):04d}",
                city=city,
                postal_code=postal_code,
                signup_at=signup_at,
                moved_at=moved_at,
                new_city=new_city,
                new_postal_code=new_postal_code,
                loyal=rng.random() < 0.15,
                api_defect=_pick_defect(
                    rng.random(),
                    settings.defect_rate,
                    [(0.010, "email_case"), (0.005, "null_postal_code"), (0.002, "bad_email")],
                ),
            )
        )
    return customers


def _build_products(settings: Settings) -> list[Product]:
    rng = rng_for(settings.seed, "products")
    start = datetime.combine(settings.start_date, time.min, tzinfo=UTC)
    horizon = start + timedelta(days=HORIZON_DAYS)

    drafts = []
    for category, spec in CATALOG.items():
        brands = spec["brands"]
        for type_index, (product_type, low, high) in enumerate(spec["types"]):
            for variant in range(VARIANTS_PER_TYPE):
                brand = brands[(type_index + variant) % len(brands)]
                drafts.append((category, brand, product_type, low, high))

    # A long-tailed popularity: a few best-sellers, many slow movers.
    ranks = list(range(1, len(drafts) + 1))
    rng.shuffle(ranks)

    products: list[Product] = []
    for number, ((category, brand, product_type, low, high), rank) in enumerate(
        zip(drafts, ranks), start=1
    ):
        if rng.random() < 0.15:  # launched during the simulated period
            created_at = start + timedelta(days=rng.uniform(30, 600))
        else:
            created_at = start - timedelta(days=rng.uniform(200, 900))
        discontinued_at = None
        if rng.random() < 0.06:
            discontinued_at = max(created_at, start) + timedelta(days=rng.uniform(60, 700))

        price = round(rng.uniform(low, high)) - 0.10
        price_points = [(created_at, round(price, 2))]
        changes = rng.choices([0, 1, 2, 3], weights=[0.4, 0.3, 0.2, 0.1])[0]
        first_possible = max(created_at, start)
        change_dates = sorted(
            first_possible + (horizon - first_possible) * rng.random() for _ in range(changes)
        )
        for change_date in change_dates:
            price = max(1.90, round(price * rng.uniform(0.9, 1.12)) - 0.10)
            price_points.append((change_date, round(price, 2)))

        products.append(
            Product(
                product_id=f"P{number:04d}",
                name=f"{product_type} {brand}",
                category=category,
                brand=brand,
                created_at=created_at,
                discontinued_at=discontinued_at,
                price_points=tuple(price_points),
                popularity=1.0 / rank**0.8,
                api_defect=_pick_defect(
                    rng.random(),
                    settings.defect_rate,
                    [(0.015, "null_category"), (0.015, "null_brand"), (0.010, "zero_price")],
                ),
            )
        )
    return products


class World:
    """Customers and products, with lookups by point in time."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.customers = _build_customers(settings)
        self.products = _build_products(settings)
        self.products_by_id = {p.product_id: p for p in self.products}
        self._signups = [c.signup_at for c in self.customers]
        self._loyal = [c for c in self.customers if c.loyal]
        self._loyal_signups = [c.signup_at for c in self._loyal]
        self._sellable_cache: dict[date, tuple[list[Product], list[float]]] = {}

    def customers_as_of(self, as_of: datetime) -> list[Customer]:
        """Customers who had signed up by `as_of` (the list is sorted by sign-up)."""
        return self.customers[: bisect.bisect_right(self._signups, as_of)]

    def products_as_of(self, as_of: datetime) -> list[Product]:
        return [p for p in self.products if p.created_at <= as_of]

    def pick_customer(self, rng, ts: datetime) -> Customer:
        """A customer who already exists at `ts`; loyal customers buy more often."""
        if rng.random() < 0.35:
            loyal_count = bisect.bisect_right(self._loyal_signups, ts)
            if loyal_count:
                return self._loyal[rng.randrange(loyal_count)]
        count = bisect.bisect_right(self._signups, ts)
        if count == 0:
            raise ValueError(f"no customer exists yet at {ts}")
        return self.customers[rng.randrange(count)]

    def sellable_products(self, day: date) -> tuple[list[Product], list[float]]:
        """Products on sale for the whole business day, with cumulative demand weights."""
        cached = self._sellable_cache.get(day)
        if cached is not None:
            return cached
        day_start = datetime.combine(day, time.min, tzinfo=PARIS).astimezone(UTC)
        day_end = datetime.combine(day + timedelta(days=1), time.min, tzinfo=PARIS).astimezone(UTC)
        products = [
            p
            for p in self.products
            if p.created_at <= day_start
            and (p.discontinued_at is None or p.discontinued_at >= day_end)
        ]
        weights = [
            p.popularity * CATEGORY_SEASONALITY[p.category][day.month - 1] for p in products
        ]
        result = (products, list(itertools.accumulate(weights)))
        if len(self._sellable_cache) > 512:
            self._sellable_cache.clear()
        self._sellable_cache[day] = result
        return result
