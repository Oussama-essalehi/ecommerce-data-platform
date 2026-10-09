"""silver.marketplace_order_lines: the marketplace CSV exports, cleaned.

Bronze holds one row per order line *per export file*: an order shows up
again each day its status changes. Silver reduces that to one row per
order line, with the order's current status and the date it reached each
status.

Rules:

1. Exact duplicate rows inside a file are dropped.
2. Text is typed: Paris local times become UTC, decimal commas become
   decimals, French labels become status and payment codes.
3. Order-level facts (status, customer, shipping) are read from the most
   recent export row of the order. The customer id is taken from any row
   that has it, since one export may miss it while another does not.
4. Line-level facts (product, quantity, price) are read from the most
   recent row of that line *that passes validation*. A line corrupted in
   one export is therefore repaired by the other exports of the same line.
5. Rows that fail validation go to the rejects table, whether or not
   another export repaired them: the defect is still worth reporting.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from ..bronze.marketplace import CORRUPT_COLUMN, SOURCE_COLUMNS
from . import (
    LIFECYCLE_STATUSES,
    ORDER_LINE_COLUMNS,
    blank_to_null,
    first_reason,
    mapped,
    to_rejects,
)

SOURCE = "marketplace"
LOCAL_TIME_ZONE = "Europe/Paris"
TIMESTAMP_FORMAT = "dd/MM/yyyy HH:mm:ss"

STATUS_CODES = {
    "en attente": "created",
    "payée": "paid",
    "expédiée": "shipped",
    "livrée": "delivered",
    "annulée": "cancelled",
    "retournée": "returned",
}

PAYMENT_CODES = {
    "cb": "card",
    "paypal": "paypal",
    "apple pay": "apple_pay",
    "paiement 3x": "installments",
    "virement": "bank_transfer",
}


def _utc(column: str):
    local = F.try_to_timestamp(F.trim(column), F.lit(TIMESTAMP_FORMAT))
    return F.to_utc_timestamp(local, LOCAL_TIME_ZONE)


def _decimal(column: str):
    return F.regexp_replace(F.trim(column), ",", ".").try_cast("decimal(10,2)")


def parse(bronze: DataFrame) -> DataFrame:
    """Deduplicate and type the raw export rows. One row per export row."""
    deduplicated = bronze.dropDuplicates([*SOURCE_COLUMNS, CORRUPT_COLUMN, "_source_file"])
    return deduplicated.select(
        blank_to_null("numero_commande").alias("order_id"),
        _utc("date_commande").alias("order_ts"),
        _utc("date_maj").alias("updated_ts"),
        mapped(F.lower(F.trim("statut")), STATUS_CODES).alias("status"),
        blank_to_null("id_client").alias("customer_id"),
        F.trim("ligne").try_cast("int").alias("line_number"),
        blank_to_null("ref_produit").alias("product_id"),
        F.trim("quantite").try_cast("int").alias("quantity"),
        _decimal("prix_unitaire").alias("unit_price"),
        F.trim("remise_pct").try_cast("int").alias("discount_pct"),
        _decimal("frais_port").alias("shipping_fee"),
        mapped(F.lower(F.trim("mode_paiement")), PAYMENT_CODES).alias("payment_method"),
        blank_to_null("ville_livraison").alias("shipping_city"),
        blank_to_null("code_postal").alias("shipping_postal_code"),
        F.col("_file_date"),
        F.col(CORRUPT_COLUMN).alias("_corrupt"),
        F.to_json(F.struct(*SOURCE_COLUMNS)).alias("_record"),
        F.col("_source_file").alias("_source_ref"),
    )


def validate(parsed: DataFrame, product_ids: DataFrame) -> DataFrame:
    """Add `_reason` (why the row is rejected, NULL if valid) and `_order_usable`.

    `_order_usable` is true when the row can at least tell us about its
    order (id, dates, status), even if its line data is bad.
    """
    known = product_ids.select("product_id").distinct().withColumn("_known_product", F.lit(True))
    joined = parsed.join(F.broadcast(known), on="product_id", how="left")
    return joined.withColumn(
        "_reason",
        first_reason(
            (F.col("_corrupt").isNotNull(), "malformed_row"),
            (
                F.col("order_id").isNull()
                | F.col("line_number").isNull()
                | F.col("order_ts").isNull()
                | F.col("updated_ts").isNull(),
                "invalid_format",
            ),
            (F.col("status").isNull(), "unknown_status"),
            (F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity"),
            (F.col("_known_product").isNull(), "unknown_product"),
            (
                F.col("unit_price").isNull()
                | (F.col("unit_price") < 0)
                | F.col("discount_pct").isNull()
                | ~F.col("discount_pct").between(0, 100),
                "invalid_amount",
            ),
        ),
    ).withColumn(
        "_order_usable",
        F.col("_corrupt").isNull()
        & F.col("order_id").isNotNull()
        & F.col("order_ts").isNotNull()
        & F.col("updated_ts").isNotNull()
        & F.col("status").isNotNull(),
    )


def build(bronze: DataFrame, product_ids: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (silver.marketplace_order_lines, rejected rows)."""
    checked = validate(parse(bronze), product_ids)
    rejects = to_rejects(checked, "marketplace_orders")

    # --- order level: latest usable row of each order -------------------------
    usable = checked.where(F.col("_order_usable"))
    newest_first = Window.partitionBy("order_id").orderBy(
        F.col("updated_ts").desc(), F.col("_file_date").desc()
    )
    latest = (
        usable.withColumn("_rank", F.row_number().over(newest_first))
        .where(F.col("_rank") == 1)
        .select(
            "order_id", "order_ts", "status", F.col("updated_ts").alias("status_ts"),
            "payment_method", "shipping_city", "shipping_postal_code", "shipping_fee",
        )
    )
    # Facts gathered across every export of the order: the customer, and
    # the first time each status was seen.
    across_exports = usable.groupBy("order_id").agg(
        F.max("customer_id").alias("customer_id"),
        *[
            F.min(F.when(F.col("status") == status, F.col("updated_ts"))).alias(f"{status}_at")
            for status in LIFECYCLE_STATUSES
        ],
    )
    orders = latest.join(across_exports, on="order_id")

    # --- line level: latest valid row of each line ----------------------------
    line_newest_first = Window.partitionBy("order_id", "line_number").orderBy(
        F.col("updated_ts").desc(), F.col("_file_date").desc()
    )
    lines = (
        checked.where(F.col("_reason").isNull())
        .withColumn("_rank", F.row_number().over(line_newest_first))
        .where(F.col("_rank") == 1)
        .select("order_id", "line_number", "product_id", "quantity", "unit_price", "discount_pct")
    )

    result = (
        lines.join(orders, on="order_id")
        .withColumn("source", F.lit(SOURCE))
        .withColumn("channel", F.lit("marketplace"))
        # The export carries no order total to reconcile against.
        .withColumn("source_total_amount", F.lit(None).cast("decimal(12,2)"))
        .select(*ORDER_LINE_COLUMNS)
    )
    return result, rejects
