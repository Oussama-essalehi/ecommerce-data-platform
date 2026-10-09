"""silver.customers and silver.products: typed, cleaned, with their history.

Bronze holds every version of a record the API ever returned. Silver turns
those versions into validity periods (a type 2 slowly changing dimension):

    product_id  unit_price  valid_from   valid_to     is_current
    P0012       48.90       2025-03-01   2026-06-14   false
    P0012       52.90       2026-06-14   NULL         true

`is_current` gives today's catalogue; the periods give the state at any
past date.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from . import blank_to_null, first_reason, to_rejects

EMAIL_PATTERN = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
POSTAL_CODE_PATTERN = r"^[0-9]{5}$"


def _versions(typed: DataFrame, key: str, source: str) -> tuple[DataFrame, DataFrame]:
    """Reject unusable records, keep one row per version, add validity periods."""
    checked = typed.withColumn(
        "_reason",
        first_reason(
            (F.col(key).isNull(), "missing_key"),
            (F.col("updated_at").isNull(), "invalid_timestamp"),
        ),
    )
    rejects = to_rejects(checked, source)

    # The same version can be in bronze twice (overlapping extractions): keep one.
    same_version = Window.partitionBy(key, "updated_at").orderBy(F.col("_ingested_at").desc())
    history = Window.partitionBy(key).orderBy("updated_at")
    versions = (
        checked.where(F.col("_reason").isNull())
        .withColumn("_copy", F.row_number().over(same_version))
        .where(F.col("_copy") == 1)
        # The first version we know is assumed valid since the record was created.
        .withColumn(
            "valid_from",
            F.when(
                F.row_number().over(history) == 1,
                F.least(F.coalesce("created_at", "updated_at"), F.col("updated_at")),
            ).otherwise(F.col("updated_at")),
        )
        .withColumn("valid_to", F.lead("updated_at").over(history))
        .withColumn("is_current", F.col("valid_to").isNull())
        .drop("_reason", "_copy", "_record", "_source_ref", "_ingested_at")
    )
    return versions, rejects


def build_customers(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (silver.customers, rejected records)."""
    email = F.lower(F.trim("email"))
    postal_code = F.trim("postal_code")
    typed = bronze.select(
        blank_to_null("customer_id").alias("customer_id"),
        blank_to_null("first_name").alias("first_name"),
        blank_to_null("last_name").alias("last_name"),
        # An address that is not an email is no better than none.
        F.when(email.rlike(EMAIL_PATTERN), email).alias("email"),
        blank_to_null("phone").alias("phone"),
        blank_to_null("city").alias("city"),
        F.when(postal_code.rlike(POSTAL_CODE_PATTERN), postal_code).alias("postal_code"),
        blank_to_null("country").alias("country"),
        F.col("created_at").try_cast("timestamp").alias("created_at"),
        F.col("updated_at").try_cast("timestamp").alias("updated_at"),
        F.col("_ingested_at"),
        F.col("_raw").alias("_record"),
        F.col("_source_file").alias("_source_ref"),
    )
    return _versions(typed, "customer_id", "customers")


def build_products(bronze: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Returns (silver.products, rejected records)."""
    price = F.col("unit_price").try_cast("decimal(10,2)")
    typed = bronze.select(
        blank_to_null("product_id").alias("product_id"),
        blank_to_null("name").alias("name"),
        blank_to_null("category").alias("category"),
        blank_to_null("brand").alias("brand"),
        # A catalogue price of zero is a missing price, not a free product.
        F.when(price > 0, price).alias("unit_price"),
        blank_to_null("currency").alias("currency"),
        (F.lower(F.trim("is_active")) == "true").alias("is_active"),
        F.col("created_at").try_cast("timestamp").alias("created_at"),
        F.col("updated_at").try_cast("timestamp").alias("updated_at"),
        F.col("_ingested_at"),
        F.col("_raw").alias("_record"),
        F.col("_source_file").alias("_source_ref"),
    )
    return _versions(typed, "product_id", "products")
