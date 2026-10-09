"""Silver layer: each source, cleaned.

What silver does to bronze data:

- types it        text becomes dates, decimals, integers, booleans; local
                  times become UTC
- standardises it French labels become canonical codes, emails are
                  lower-cased, blanks become NULL
- deduplicates it exact duplicate rows and re-delivered events are removed
- validates it    a record that cannot be trusted is not silently dropped:
                  it goes to `silver.rejects` with the reason

Silver is rebuilt entirely from bronze on every run. At this volume that
takes about a minute, and it guarantees silver can never drift from
bronze: a cleaning rule that changes is applied to the whole history.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

TABLES = [
    "customers",
    "products",
    "order_events",
    "web_order_lines",
    "marketplace_order_lines",
    "rejects",
]

ORDER_STATUSES = ["created", "paid", "shipped", "delivered", "cancelled", "returned"]
# Statuses that carry their own timestamp column (paid_at, shipped_at...).
LIFECYCLE_STATUSES = ["paid", "shipped", "delivered", "cancelled", "returned"]

# Both order sources leave silver with exactly these columns, one row per
# order line, so that gold can simply stack them.
ORDER_LINE_COLUMNS = [
    "order_id", "source", "channel", "customer_id", "order_ts", "status", "status_ts",
    "payment_method", "shipping_city", "shipping_postal_code", "shipping_fee",
    "paid_at", "shipped_at", "delivered_at", "cancelled_at", "returned_at",
    "source_total_amount",
    "line_number", "product_id", "quantity", "unit_price", "discount_pct",
]

REJECT_COLUMNS = ["source", "reason", "record", "source_ref", "rejected_at"]


def blank_to_null(column: Column | str) -> Column:
    """Trim a text column; an empty result becomes NULL."""
    trimmed = F.trim(F.col(column) if isinstance(column, str) else column)
    return F.when(trimmed != "", trimmed)


def mapped(column: Column, mapping: dict[str, str]) -> Column:
    """Translate a label through `mapping`. Anything not in it becomes NULL."""
    branches = [F.when(column == source, F.lit(target)) for source, target in mapping.items()]
    return F.coalesce(*branches) if branches else F.lit(None).cast("string")


def first_reason(*rules: tuple[Column, str]) -> Column:
    """The label of the first rule whose condition is true, NULL if none is.

    A NULL condition counts as false, so rules can be written without
    guarding every comparison against missing values.
    """
    chain = None
    for condition, label in rules:
        hit = F.coalesce(condition, F.lit(False))
        chain = F.when(hit, label) if chain is None else chain.when(hit, label)
    return chain


def to_rejects(df: DataFrame, source: str, reason: str = "_reason",
               record: str = "_record", source_ref: str = "_source_ref") -> DataFrame:
    """Shape the rows of `df` that carry a reject reason for `silver.rejects`."""
    return df.where(F.col(reason).isNotNull()).select(
        F.lit(source).alias("source"),
        F.col(reason).alias("reason"),
        F.col(record).cast("string").alias("record"),
        F.col(source_ref).cast("string").alias("source_ref"),
        F.current_timestamp().alias("rejected_at"),
    )


def union_all(frames: list[DataFrame]) -> DataFrame:
    return reduce(lambda left, right: left.unionByName(right), frames)
