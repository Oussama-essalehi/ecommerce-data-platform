"""What is checked, table by table.

A suite is a DataFrame to inspect plus a list of checks. The DataFrame is
usually a lake table as it is. A few suites first add helper columns
(prefixed with `_`) so that a rule spanning several columns or tables can be
expressed as a simple "this column must be true".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import great_expectations.expectations as gxe
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .. import lake
from ..config import Config
from . import ERROR, WARNING

BUSINESS_TIME_ZONE = "Europe/Paris"
STATUSES = ["created", "paid", "shipped", "delivered", "cancelled", "returned"]
CHANNELS = ["marketplace", "web", "mobile"]
EMAIL_PATTERN = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"


@dataclass(frozen=True)
class Check:
    name: str
    dimension: str
    description: str
    expectation: gxe.Expectation
    severity: str = ERROR


@dataclass(frozen=True)
class Suite:
    layer: str
    table: str
    prepare: Callable[[SparkSession, Config], DataFrame]
    checks: list[Check]

    @property
    def name(self) -> str:
        return f"{self.layer}.{self.table}"


def _table(layer: str, table: str) -> Callable[[SparkSession, Config], DataFrame]:
    return lambda spark, config: lake.read(spark, config.table(layer, table))


# --- small builders, so the suites below read as a list of rules ------------------


def not_null(column: str, *, at_least: float = 1.0, severity: str = ERROR) -> Check:
    share = "" if at_least == 1.0 else f" on at least {at_least:.1%} of rows"
    return Check(
        f"{column}_not_null", "completeness", f"{column} is filled{share}",
        gxe.ExpectColumnValuesToNotBeNull(column=column, mostly=at_least), severity,
    )


def unique(*columns: str) -> Check:
    label = ", ".join(columns)
    if len(columns) == 1:
        expectation = gxe.ExpectColumnValuesToBeUnique(column=columns[0])
    else:
        expectation = gxe.ExpectCompoundColumnsToBeUnique(column_list=list(columns))
    return Check(f"{'_'.join(columns)}_unique", "uniqueness",
                 f"no two rows share the same {label}", expectation)


def in_set(column: str, values: list[str]) -> Check:
    return Check(f"{column}_known_value", "validity",
                 f"{column} is one of: {', '.join(values)}",
                 gxe.ExpectColumnValuesToBeInSet(column=column, value_set=values))


def between(column: str, low: float | None, high: float | None, *,
            severity: str = ERROR) -> Check:
    bounds = " and ".join(
        part for part in (
            f"at least {low:g}" if low is not None else "",
            f"at most {high:g}" if high is not None else "",
        ) if part
    )
    return Check(f"{column}_in_range", "validity", f"{column} is {bounds}",
                 gxe.ExpectColumnValuesToBeBetween(column=column, min_value=low, max_value=high),
                 severity)


def matches(column: str, pattern: str, what: str) -> Check:
    return Check(f"{column}_format", "validity", f"{column} looks like {what}",
                 gxe.ExpectColumnValuesToMatchRegex(column=column, regex=pattern))


def not_before(later: str, earlier: str) -> Check:
    """`later` is on or after `earlier`, wherever both are known."""
    return Check(
        f"{later}_not_before_{earlier}", "consistency",
        f"{later} is never earlier than {earlier}",
        gxe.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A=later, column_B=earlier, or_equal=True,
            ignore_row_if="either_value_is_missing",
        ),
    )


def holds(flag: str, dimension: str, description: str, *, severity: str = ERROR) -> Check:
    """A helper column computed in `prepare` must be true (NULL rows are skipped)."""
    return Check(flag.lstrip("_"), dimension, description,
                 gxe.ExpectColumnValuesToBeInSet(column=flag, value_set=[True]), severity)


def at_most(column: str, limit: float, dimension: str, description: str, *,
            name: str | None = None, severity: str = ERROR) -> Check:
    """For one-row summary tables: the measured value does not exceed `limit`."""
    return Check(name or f"{column}_at_most_{limit:g}", dimension, description,
                 gxe.ExpectColumnMaxToBeBetween(column=column, max_value=limit), severity)


def has_rows() -> Check:
    return Check("has_rows", "volume", "the table is not empty",
                 gxe.ExpectTableRowCountToBeBetween(min_value=1))


# --- prepared views --------------------------------------------------------------------


def bronze_freshness(spark: SparkSession, config: Config) -> DataFrame:
    """One row: how old the newest data of each source is."""
    def newest(table: str, column: str):
        return lake.read(spark, config.table("bronze", table)).agg(F.max(column).alias("v"))

    def minutes_since(table: str) -> DataFrame:
        age = (F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp("v")) / 60
        return newest(table, "_ingested_at").select(F.round(age, 1).alias("age"))

    today = F.to_date(F.from_utc_timestamp(F.current_timestamp(), BUSINESS_TIME_ZONE))
    events = minutes_since("order_events").withColumnRenamed(
        "age", "order_events_minutes_since_last_ingestion")
    customers = minutes_since("customers").select(
        F.round(F.col("age") / 60, 1).alias("customers_hours_since_last_ingestion"))
    files = newest("marketplace_orders", "_file_date").select(
        F.datediff(today, "v").alias("marketplace_days_since_last_file"))
    return events.crossJoin(customers).crossJoin(files)


def silver_customers(spark: SparkSession, config: Config) -> DataFrame:
    df = lake.read(spark, config.table("silver", "customers"))
    return df.withColumn("_current_customer_id", F.when(F.col("is_current"), F.col("customer_id")))


def silver_products(spark: SparkSession, config: Config) -> DataFrame:
    df = lake.read(spark, config.table("silver", "products"))
    return df.withColumn("_current_product_id", F.when(F.col("is_current"), F.col("product_id")))


def silver_reject_rates(spark: SparkSession, config: Config) -> DataFrame:
    """One row per source: the share of its bronze rows that silver rejected."""
    rejects = lake.read(spark, config.table("silver", "rejects"))
    rows = []
    for source, bronze_table in [
        ("marketplace_orders", "marketplace_orders"),
        ("order_events", "order_events"),
        ("customers", "customers"),
        ("products", "products"),
    ]:
        received = lake.read(spark, config.table("bronze", bronze_table)).count()
        rejected = rejects.where(F.col("source") == source).count()
        rate = round(100.0 * rejected / received, 4) if received else 0.0
        rows.append((source, received, rejected, rate))
    return spark.createDataFrame(
        rows, "source STRING, received BIGINT, rejected BIGINT, reject_rate_pct DOUBLE"
    )


def gold_orders(spark: SparkSession, config: Config) -> DataFrame:
    df = lake.read(spark, config.table("gold", "orders"))
    today = F.to_date(F.from_utc_timestamp(F.current_timestamp(), BUSINESS_TIME_ZONE))
    return (
        df.withColumn("_total_is_net_plus_shipping",
                      F.col("total_amount") == F.col("net_amount") + F.col("shipping_fee"))
        # The shop rounds with floats: allow one cent per line, no more.
        # (0.01 in a SQL expression is an exact decimal, not a float.)
        .withColumn(
            "_total_matches_declared_total",
            F.expr("abs(total_amount - source_total_amount) <= lines_count * 0.01"),
        )
        .withColumn("_order_date_not_in_future", F.col("order_date") <= today)
    )


def gold_orders_freshness(spark: SparkSession, config: Config) -> DataFrame:
    df = lake.read(spark, config.table("gold", "orders"))
    today = F.to_date(F.from_utc_timestamp(F.current_timestamp(), BUSINESS_TIME_ZONE))
    return df.agg(F.datediff(today, F.max("order_date")).alias("days_since_last_order"))


def gold_order_lines(spark: SparkSession, config: Config) -> DataFrame:
    lines = lake.read(spark, config.table("gold", "order_lines"))
    orders = (lake.read(spark, config.table("gold", "orders"))
              .select("order_id").withColumn("_o", F.lit(True)))
    products = (lake.read(spark, config.table("gold", "products"))
                .select("product_id").withColumn("_p", F.lit(True)))
    return (
        lines.join(orders, "order_id", "left").join(F.broadcast(products), "product_id", "left")
        .withColumn("_order_exists", F.coalesce("_o", F.lit(False)))
        .withColumn("_product_exists", F.coalesce("_p", F.lit(False)))
        .withColumn("_gross_is_quantity_times_price",
                    F.col("gross_amount") == F.col("quantity") * F.col("unit_price"))
        .withColumn("_net_is_gross_minus_discount",
                    F.col("net_amount") == F.col("gross_amount") - F.col("discount_amount"))
    )


# --- the suites ----------------------------------------------------------------------------


def _order_lines(table: str) -> Suite:
    """Same rules for both order sources: they share one layout in silver."""
    return Suite("silver", table, _table("silver", table), [
        has_rows(),
        unique("order_id", "line_number"),
        not_null("order_id"),
        not_null("order_ts"),
        not_null("product_id"),
        not_null("customer_id", at_least=0.99, severity=WARNING),
        in_set("status", STATUSES),
        in_set("channel", CHANNELS),
        between("quantity", 1, 1000),
        between("unit_price", 0, 100000),
        between("discount_pct", 0, 100),
        not_before("status_ts", "order_ts"),
    ])


def build_suites() -> list[Suite]:
    return [
        Suite("bronze", "freshness", bronze_freshness, [
            at_most("order_events_minutes_since_last_ingestion", 60, "freshness",
                    "an order event was ingested in the last hour",
                    name="order_events_fresh", severity=WARNING),
            at_most("customers_hours_since_last_ingestion", 26, "freshness",
                    "customers were loaded from the API in the last 26 hours",
                    name="customers_fresh", severity=WARNING),
            at_most("marketplace_days_since_last_file", 1, "freshness",
                    "yesterday's marketplace export has been loaded",
                    name="marketplace_export_fresh", severity=WARNING),
        ]),
        Suite("silver", "customers", silver_customers, [
            has_rows(),
            not_null("customer_id"),
            unique("customer_id", "valid_from"),
            Check("one_current_version", "uniqueness",
                  "each customer has exactly one current version",
                  gxe.ExpectColumnValuesToBeUnique(column="_current_customer_id")),
            not_null("email", at_least=0.99, severity=WARNING),
            matches("email", EMAIL_PATTERN, "an email address"),
            not_null("postal_code", at_least=0.98, severity=WARNING),
            matches("postal_code", r"^[0-9]{5}$", "a five-digit postal code"),
            not_before("valid_to", "valid_from"),
        ]),
        Suite("silver", "products", silver_products, [
            has_rows(),
            not_null("product_id"),
            not_null("name"),
            unique("product_id", "valid_from"),
            Check("one_current_version", "uniqueness",
                  "each product has exactly one current version",
                  gxe.ExpectColumnValuesToBeUnique(column="_current_product_id")),
            between("unit_price", 0.01, 100000),
            not_null("category", at_least=0.95, severity=WARNING),
            not_before("valid_to", "valid_from"),
        ]),
        Suite("silver", "order_events", _table("silver", "order_events"), [
            has_rows(),
            not_null("event_id"),
            unique("event_id"),
            not_null("event_ts"),
            in_set("event_type", ["order_created", "order_status_changed"]),
            matches("order_id", r"^WEB-[0-9]{8}-[0-9]{5}$", "a web shop order number"),
            in_set("status", STATUSES),
        ]),
        _order_lines("web_order_lines"),
        _order_lines("marketplace_order_lines"),
        Suite("silver", "reject_rates", silver_reject_rates, [
            at_most("reject_rate_pct", 1, "volume",
                    "no source has more than 1% of its rows rejected",
                    name="reject_rate_usual", severity=WARNING),
            at_most("reject_rate_pct", 5, "volume",
                    "no source has more than 5% of its rows rejected",
                    name="reject_rate_acceptable"),
        ]),
        Suite("gold", "orders", gold_orders, [
            has_rows(),
            not_null("order_id"),
            unique("order_id"),
            not_null("order_date"),
            not_null("customer_id", at_least=0.99, severity=WARNING),
            in_set("status", STATUSES),
            in_set("channel", CHANNELS),
            between("lines_count", 1, None),
            between("units", 1, None),
            between("total_amount", 0, None),
            holds("_total_is_net_plus_shipping", "consistency",
                  "an order's total is its net amount plus shipping"),
            holds("_total_matches_declared_total", "consistency",
                  "the computed total matches the total declared by the shop, "
                  "within one cent per line"),
            holds("_order_date_not_in_future", "validity", "no order is dated in the future"),
            not_before("shipped_at", "order_ts"),
            not_before("delivered_at", "shipped_at"),
            not_before("returned_at", "delivered_at"),
        ]),
        Suite("gold", "orders_freshness", gold_orders_freshness, [
            at_most("days_since_last_order", 1, "freshness",
                    "gold holds orders from today or yesterday",
                    name="orders_fresh", severity=WARNING),
        ]),
        Suite("gold", "order_lines", gold_order_lines, [
            has_rows(),
            unique("order_id", "line_number"),
            between("quantity", 1, 1000),
            between("net_amount", 0, None),
            holds("_order_exists", "consistency", "every line belongs to an order of gold.orders"),
            holds("_product_exists", "consistency", "every line sells a product of gold.products"),
            holds("_gross_is_quantity_times_price", "consistency",
                  "gross amount is quantity times unit price"),
            holds("_net_is_gross_minus_discount", "consistency",
                  "net amount is gross amount minus the discount"),
        ]),
        Suite("gold", "customers", _table("gold", "customers"), [
            has_rows(),
            not_null("customer_id"),
            unique("customer_id"),
            not_null("signup_date"),
            not_null("email", at_least=0.99, severity=WARNING),
        ]),
        Suite("gold", "products", _table("gold", "products"), [
            has_rows(),
            not_null("product_id"),
            unique("product_id"),
            not_null("category"),
            not_null("unit_price", at_least=0.95, severity=WARNING),
        ]),
    ]
