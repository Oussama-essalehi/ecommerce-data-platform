"""silver.order_events and silver.web_order_lines: the web shop stream, cleaned.

Two steps:

1. `build_events`  parses the JSON payloads of bronze into typed columns,
   rejects what cannot be parsed, and removes re-delivered events (same
   `event_id`). The result is the clean event log.
2. `build_order_lines`  folds that log into the state of each order: the
   `order_created` event gives the order and its lines, the
   `order_status_changed` events give its current status and the date it
   reached each status. Late and out-of-order events need no special
   care here: statuses are ordered by the time they happened
   (`event_ts`), not by the time they arrived.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import (
    LIFECYCLE_STATUSES,
    ORDER_LINE_COLUMNS,
    blank_to_null,
    first_reason,
    to_rejects,
)

SOURCE = "web_shop"
EVENT_TYPES = ["order_created", "order_status_changed"]

# `_corrupt` receives the payload when it is not valid JSON or does not fit
# the schema. Without it Spark would return the fields it managed to read
# before the error, and a truncated order would look like a smaller order.
EVENT_SCHEMA = """
    event_id STRING, event_type STRING, event_ts STRING, schema_version INT,
    order_id STRING, status STRING, customer_id STRING, channel STRING,
    payment_method STRING, currency STRING,
    shipping STRUCT<city: STRING, postal_code: STRING, fee: DECIMAL(10,2)>,
    items ARRAY<STRUCT<line_number: INT, product_id: STRING, quantity: INT,
                       unit_price: DECIMAL(10,2), discount_pct: INT>>,
    total_amount DECIMAL(12,2),
    _corrupt STRING
"""

EVENT_COLUMNS = [
    "event_id", "event_type", "event_ts", "order_id", "status", "customer_id", "channel",
    "payment_method", "shipping_city", "shipping_postal_code", "shipping_fee", "items",
    "total_amount", "kafka_partition", "kafka_offset", "kafka_timestamp",
]


def build_events(bronze: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Returns (silver.order_events, rejected messages, re-delivered events removed)."""
    parsed = bronze.withColumn(
        "e", F.from_json("value", EVENT_SCHEMA, {"columnNameOfCorruptRecord": "_corrupt"})
    )
    typed = parsed.select(
        blank_to_null("e.event_id").alias("event_id"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.event_ts").try_cast("timestamp").alias("event_ts"),
        blank_to_null("e.order_id").alias("order_id"),
        F.col("e.status").alias("status"),
        blank_to_null("e.customer_id").alias("customer_id"),
        F.col("e.channel").alias("channel"),
        F.col("e.payment_method").alias("payment_method"),
        F.col("e.shipping.city").alias("shipping_city"),
        F.col("e.shipping.postal_code").alias("shipping_postal_code"),
        F.col("e.shipping.fee").alias("shipping_fee"),
        F.col("e.items").alias("items"),
        F.col("e.total_amount").alias("total_amount"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("kafka_timestamp"),
        (F.col("e").isNull() | F.col("e._corrupt").isNotNull()).alias("_malformed"),
        F.col("value").alias("_record"),
        F.concat_ws(":", "topic", "partition", "offset").alias("_source_ref"),
    )
    is_created = F.col("event_type") == "order_created"
    is_status = F.col("event_type") == "order_status_changed"
    checked = typed.withColumn(
        "_reason",
        first_reason(
            (F.col("_malformed"), "malformed_json"),
            (
                F.col("event_id").isNull() | F.col("order_id").isNull()
                | F.col("event_ts").isNull() | F.col("event_type").isNull(),
                "missing_field",
            ),
            (~F.col("event_type").isin(EVENT_TYPES), "unknown_event_type"),
            (is_created & (F.col("items").isNull() | (F.size("items") == 0)), "missing_field"),
            (is_status & ~F.coalesce(F.col("status").isin(LIFECYCLE_STATUSES), F.lit(False)),
             "unknown_status"),
        ),
    )
    rejects = to_rejects(checked, "order_events")

    # Kafka delivers at least once: keep the first delivery of each event.
    first_delivery = Window.partitionBy("event_id").orderBy("kafka_partition", "kafka_offset")
    ranked = checked.where(F.col("_reason").isNull()).withColumn(
        "_delivery", F.row_number().over(first_delivery)
    )
    events = ranked.where(F.col("_delivery") == 1).select(*EVENT_COLUMNS)
    redelivered = ranked.where(F.col("_delivery") > 1).select(*EVENT_COLUMNS)
    return events, rejects, redelivered


def build_order_lines(events: DataFrame, product_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (silver.web_order_lines, rejected lines and orphan status events)."""
    # --- orders: one order_created per order ----------------------------------
    earliest = Window.partitionBy("order_id").orderBy("event_ts", "event_id")
    created = (
        events.where(F.col("event_type") == "order_created")
        .withColumn("_rank", F.row_number().over(earliest))
        .where(F.col("_rank") == 1)
        .select(
            "order_id", "customer_id", "channel", F.col("event_ts").alias("order_ts"),
            "payment_method", "shipping_city", "shipping_postal_code", "shipping_fee",
            F.col("total_amount").alias("source_total_amount"), "items",
        )
    )

    # --- status: latest one, and the first time each was reached ---------------
    statuses = events.where(F.col("event_type") == "order_status_changed")
    lifecycle = statuses.groupBy("order_id").agg(
        # max() of a struct compares its fields in order: latest event_ts wins.
        F.max(F.struct("event_ts", "status")).alias("_last"),
        *[
            F.min(F.when(F.col("status") == status, F.col("event_ts"))).alias(f"{status}_at")
            for status in LIFECYCLE_STATUSES
        ],
    )
    orders = (
        created.join(lifecycle, on="order_id", how="left")
        .withColumn("status", F.coalesce(F.col("_last.status"), F.lit("created")))
        .withColumn("status_ts", F.coalesce(F.col("_last.event_ts"), F.col("order_ts")))
        .drop("_last")
    )

    # A status for an order we never saw being created (its order_created
    # was lost or rejected) cannot be attached to anything.
    orphans = (
        statuses.join(created.select("order_id"), on="order_id", how="left_anti")
        .withColumn("_reason", F.lit("orphan_status_event"))
        .withColumn("_record", F.to_json(F.struct("event_id", "order_id", "status", "event_ts")))
        .withColumn("_source_ref", F.concat_ws(":", "kafka_partition", "kafka_offset"))
    )

    # --- lines: one row per item, validated -------------------------------------
    known = product_ids.select("product_id").distinct().withColumn("_known_product", F.lit(True))
    exploded = orders.withColumn("_item", F.explode("items")).select(
        "*",
        F.col("_item.line_number").alias("line_number"),
        blank_to_null("_item.product_id").alias("product_id"),
        F.col("_item.quantity").alias("quantity"),
        F.col("_item.unit_price").alias("unit_price"),
        F.col("_item.discount_pct").alias("discount_pct"),
    )
    checked = exploded.join(F.broadcast(known), on="product_id", how="left").withColumn(
        "_reason",
        first_reason(
            (F.col("line_number").isNull(), "invalid_format"),
            (F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity"),
            (F.col("_known_product").isNull(), "unknown_product"),
            (
                F.col("unit_price").isNull() | (F.col("unit_price") < 0)
                | F.col("discount_pct").isNull() | ~F.col("discount_pct").between(0, 100),
                "invalid_amount",
            ),
        ),
    )
    bad_lines = (
        checked.where(F.col("_reason").isNotNull())
        .withColumn("_record", F.to_json(F.struct("order_id", "_item")))
        .withColumn("_source_ref", F.col("order_id"))
    )
    lines = (
        checked.where(F.col("_reason").isNull())
        .withColumn("source", F.lit(SOURCE))
        .select(*ORDER_LINE_COLUMNS)
    )
    rejects = to_rejects(orphans, "order_events").unionByName(
        to_rejects(bad_lines, "order_events")
    )
    return lines, rejects
