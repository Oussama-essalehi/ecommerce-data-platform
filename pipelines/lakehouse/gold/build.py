"""Transformations from silver to gold."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from ..silver import LIFECYCLE_STATUSES

BUSINESS_TIME_ZONE = "Europe/Paris"
AMOUNT = "decimal(12,2)"
UNKNOWN = "Unknown"

ORDER_COLUMNS = [
    "order_id", "source", "channel", "customer_id", "order_ts", "order_date",
    "status", "status_ts", "is_cancelled", "is_returned",
    "paid_at", "shipped_at", "delivered_at", "cancelled_at", "returned_at",
    "payment_method", "shipping_city", "shipping_postal_code",
    "lines_count", "units", "gross_amount", "discount_amount", "net_amount",
    "shipping_fee", "total_amount", "source_total_amount",
]

ORDER_LINE_COLUMNS = [
    "order_id", "line_number", "source", "product_id", "quantity", "unit_price",
    "discount_pct", "gross_amount", "discount_amount", "net_amount",
]


def with_amounts(lines: DataFrame) -> DataFrame:
    """Add the amounts of each line, computed in exact decimals.

    gross = quantity x unit price; net = gross after the line discount.
    """
    gross = (F.col("quantity") * F.col("unit_price")).cast(AMOUNT)
    net = F.round(gross * (F.lit(100) - F.col("discount_pct")) / F.lit(100), 2).cast(AMOUNT)
    return (
        lines.withColumn("gross_amount", gross)
        .withColumn("net_amount", net)
        .withColumn("discount_amount", (F.col("gross_amount") - F.col("net_amount")).cast(AMOUNT))
    )


def build_order_lines(silver_lines: DataFrame) -> DataFrame:
    """gold.order_lines from the stacked silver order lines of every source."""
    return with_amounts(silver_lines).select(*ORDER_LINE_COLUMNS)


def build_orders(silver_lines: DataFrame) -> DataFrame:
    """gold.orders: the lines of each order folded into one row."""
    # Silver repeats the order's facts on each of its lines: any line will do.
    order_facts = [
        "source", "channel", "customer_id", "order_ts", "status", "status_ts",
        *[f"{status}_at" for status in LIFECYCLE_STATUSES],
        "payment_method", "shipping_city", "shipping_postal_code",
        "shipping_fee", "source_total_amount",
    ]
    orders = with_amounts(silver_lines).groupBy("order_id").agg(
        *[F.first(column, ignorenulls=True).alias(column) for column in order_facts],
        F.count("*").cast("int").alias("lines_count"),
        F.sum("quantity").cast("int").alias("units"),
        F.sum("gross_amount").cast(AMOUNT).alias("gross_amount"),
        F.sum("discount_amount").cast(AMOUNT).alias("discount_amount"),
        F.sum("net_amount").cast(AMOUNT).alias("net_amount"),
    )
    shipping_fee = F.coalesce(F.col("shipping_fee"), F.lit(0)).cast("decimal(10,2)")
    return (
        orders.withColumn("shipping_fee", shipping_fee)
        .withColumn("total_amount", (F.col("net_amount") + F.col("shipping_fee")).cast(AMOUNT))
        # The business day is the day in France, not the UTC day: an order
        # placed at 00:30 in Paris belongs to that day, though it is 22:30
        # the day before in UTC.
        .withColumn(
            "order_date", F.to_date(F.from_utc_timestamp("order_ts", BUSINESS_TIME_ZONE))
        )
        .withColumn("is_cancelled", F.col("cancelled_at").isNotNull())
        .withColumn("is_returned", F.col("returned_at").isNotNull())
        .select(*ORDER_COLUMNS)
    )


def build_customers(silver_customers: DataFrame) -> DataFrame:
    """gold.customers: the current version of each customer."""
    return silver_customers.where(F.col("is_current")).select(
        "customer_id", "first_name", "last_name", "email", "phone", "city", "postal_code",
        "country", F.col("created_at").alias("signup_ts"),
        F.to_date(F.from_utc_timestamp("created_at", BUSINESS_TIME_ZONE)).alias("signup_date"),
        "updated_at",
    )


def build_products(silver_products: DataFrame) -> DataFrame:
    """gold.products: the current version of each product."""
    return silver_products.where(F.col("is_current")).select(
        "product_id", "name",
        # A report grouped by category should show a labelled bucket, not a blank.
        F.coalesce("category", F.lit(UNKNOWN)).alias("category"),
        F.coalesce("brand", F.lit(UNKNOWN)).alias("brand"),
        "unit_price", "currency", "is_active", "created_at", "updated_at",
    )
