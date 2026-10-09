"""Runtime configuration, read from environment variables."""

from __future__ import annotations

import os
import random
from dataclasses import dataclass
from datetime import date, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

UTC = timezone.utc
# The shop operates in France: a "business day" is a Europe/Paris day.
PARIS = ZoneInfo("Europe/Paris")


@dataclass(frozen=True)
class Settings:
    seed: int = 42
    # First day of history. Nothing is generated before it.
    start_date: date = date(2025, 10, 1)
    # Average number of orders per day at start_date, all channels together.
    orders_per_day: int = 600
    # Size of the customer pool, spread over four years of sign-ups.
    n_customers: int = 120_000
    # Multiplier on every injected data defect. 0 disables them all.
    defect_rate: float = 1.0
    # Probability that an API call answers 503, to exercise client retries.
    api_error_rate: float = 0.02
    kafka_bootstrap: str = "localhost:9092"
    kafka_topic: str = "shop.orders.v1"
    landing_dir: Path = Path("data/landing/marketplace")
    state_dir: Path = Path("data/state")

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.environ
        default = cls()
        return cls(
            seed=int(env.get("SIM_SEED", default.seed)),
            start_date=date.fromisoformat(
                env.get("SIM_START_DATE", default.start_date.isoformat())
            ),
            orders_per_day=int(env.get("SIM_ORDERS_PER_DAY", default.orders_per_day)),
            n_customers=int(env.get("SIM_CUSTOMERS", default.n_customers)),
            defect_rate=float(env.get("SIM_DEFECT_RATE", default.defect_rate)),
            api_error_rate=float(env.get("SIM_API_ERROR_RATE", default.api_error_rate)),
            kafka_bootstrap=env.get("KAFKA_BOOTSTRAP_SERVERS", default.kafka_bootstrap),
            kafka_topic=env.get("KAFKA_TOPIC", default.kafka_topic),
            landing_dir=Path(env.get("SIM_LANDING_DIR", default.landing_dir)),
            state_dir=Path(env.get("SIM_STATE_DIR", default.state_dir)),
        )


def rng_for(seed: int, *parts: object) -> random.Random:
    """A random generator that depends only on the seed and a stable label.

    Seeding per entity (per day, per event...) instead of sharing one global
    generator is what lets any day be regenerated on its own, in any order.
    """
    return random.Random("|".join([str(seed), *map(str, parts)]))
