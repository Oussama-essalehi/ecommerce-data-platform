from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg
import pytest
from helpers import load_sample_bronze
from psycopg import sql
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse import lake, warehouse
from lakehouse.gold import job as gold_job
from lakehouse.silver import job as silver_job

# --- type mapping (no database needed) ----------------------------------------------


def test_spark_types_map_to_postgres_types():
    schema = StructType([
        StructField("order_id", StringType()),
        StructField("units", IntegerType()),
        StructField("big", LongType()),
        StructField("total_amount", DecimalType(12, 2)),
        StructField("is_cancelled", BooleanType()),
        StructField("order_date", DateType()),
        StructField("order_ts", TimestampType()),
    ])
    assert warehouse.columns_of(schema) == [
        ("order_id", "text"),
        ("units", "integer"),
        ("big", "bigint"),
        ("total_amount", "numeric(12,2)"),
        ("is_cancelled", "boolean"),
        ("order_date", "date"),
        ("order_ts", "timestamp with time zone"),
    ]


def test_an_unmapped_type_is_an_error_not_a_guess():
    with pytest.raises(TypeError, match="array<string>"):
        warehouse.pg_type(ArrayType(StringType()))


def test_create_table_statement_adds_the_load_timestamp():
    statement = warehouse.create_table(
        "lake", "orders", [("order_id", "text"), ("total_amount", "numeric(12,2)")]
    )
    assert statement.as_string(None) == (
        'CREATE TABLE "lake"."orders" ("order_id" text, "total_amount" numeric(12,2), '
        '"_loaded_at" timestamp with time zone NOT NULL DEFAULT now())'
    )


def test_timestamps_are_rendered_as_utc_text(spark):
    df = spark.sql(
        "SELECT TIMESTAMP'2026-10-06 22:30:00.123456' AS order_ts, "
        "CAST(NULL AS TIMESTAMP) AS paid_at, 'x' AS order_id"
    )
    row = warehouse.for_copy(df).first()
    assert row["order_ts"] == "2026-10-06 22:30:00.123456+00"
    assert row["paid_at"] is None and row["order_id"] == "x"


# --- the load, against a real PostgreSQL and real Delta tables ------------------------
#
# These tests write to a schema of their own, created and dropped around each
# test, so they are safe to run against the project's warehouse.


def _connect(config):
    return psycopg.connect(warehouse.connection_string(config), connect_timeout=3)


@pytest.fixture()
def schema(config):
    try:
        connection = _connect(config)
    except psycopg.OperationalError as error:
        pytest.skip(f"no PostgreSQL reachable at {config.warehouse_host}:{config.warehouse_port}"
                    f" ({str(error).strip().splitlines()[0]})")
    name = f"test_lake_{uuid.uuid4().hex[:10]}"
    connection.close()
    yield name
    with _connect(config) as connection:
        connection.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(name)))


@pytest.fixture()
def gold(spark, config):
    """A small gold layer built through the real jobs."""
    load_sample_bronze(spark, config)
    silver_job.run(spark, config)
    gold_job.run(spark, config)


def _query(config, statement, schema):
    with _connect(config) as connection:
        connection.execute("SET TIME ZONE 'UTC'")
        return connection.execute(
            sql.SQL(statement).format(schema=sql.Identifier(schema))
        ).fetchall()


@pytest.mark.delta
def test_gold_tables_arrive_complete_and_typed(spark, config, schema, gold):
    summary = warehouse.run(spark, config, schema=schema)
    assert summary["schema"] == schema
    assert summary["tables"] == {
        "orders": {"rows": 2, "action": "created"},
        "order_lines": {"rows": 3, "action": "created"},
        "customers": {"rows": 1, "action": "created"},
        "products": {"rows": 2, "action": "created"},
    }

    (order,) = _query(config, """
        SELECT order_id, channel, order_ts, order_date, status, is_cancelled, paid_at,
               delivered_at, shipping_postal_code, shipping_fee, total_amount, units
        FROM {schema}.orders WHERE source = 'web_shop'
    """, schema)
    assert order == (
        "WEB-20261006-00001", "web",
        datetime(2026, 10, 6, 8, 0, tzinfo=timezone.utc),      # a UTC instant, with its zone
        date(2026, 10, 6), "paid", False,
        datetime(2026, 10, 6, 8, 3, 10, tzinfo=timezone.utc),
        None,                                                    # NULL stays NULL
        "06000",                                                 # text keeps its leading zero
        Decimal("4.90"), Decimal("94.60"), 3,
    )
    (customer,) = _query(config, "SELECT first_name, last_name, city FROM {schema}.customers", schema)
    assert customer == ("Inès", "Lefèvre", "Nice")               # accents survive the trip

    with _connect(config) as connection, connection.cursor() as cursor:
        types = dict(warehouse.existing_columns(cursor, schema, "orders"))
    assert types["total_amount"] == "numeric(12,2)"
    assert types["order_ts"] == "timestamp with time zone"
    assert types["is_cancelled"] == "boolean"
    assert types["_loaded_at"] == "timestamp with time zone"


@pytest.mark.delta
def test_all_tables_share_one_load_timestamp(spark, config, schema, gold):
    warehouse.run(spark, config, schema=schema)
    stamps = _query(config, """
        SELECT _loaded_at FROM {schema}.orders
        UNION SELECT _loaded_at FROM {schema}.order_lines
        UNION SELECT _loaded_at FROM {schema}.customers
        UNION SELECT _loaded_at FROM {schema}.products
    """, schema)
    assert len(stamps) == 1                                      # one transaction, one instant


@pytest.mark.delta
def test_loading_again_replaces_the_rows_and_keeps_dependent_views(spark, config, schema, gold):
    warehouse.run(spark, config, schema=schema)
    with _connect(config) as connection:
        connection.execute(sql.SQL(
            "CREATE VIEW {schema}.orders_view AS SELECT order_id FROM {schema}.orders"
        ).format(schema=sql.Identifier(schema)))

    again = warehouse.run(spark, config, schema=schema)

    assert {t["action"] for t in again["tables"].values()} == {"truncated"}
    assert _query(config, "SELECT count(*) FROM {schema}.orders", schema) == [(2,)]
    assert _query(config, "SELECT count(*) FROM {schema}.orders_view", schema) == [(2,)]


@pytest.mark.delta
def test_a_changed_gold_schema_recreates_the_table(spark, config, schema, gold):
    warehouse.run(spark, config, schema=schema)
    products = lake.read(spark, config.table("gold", "products"))
    lake.overwrite(products.withColumnRenamed("brand", "maker").localCheckpoint(),
                   config.table("gold", "products"))

    again = warehouse.run(spark, config, schema=schema)

    assert again["tables"]["products"]["action"] == "recreated"
    assert again["tables"]["orders"]["action"] == "truncated"
    assert _query(config, "SELECT count(maker) FROM {schema}.products", schema) == [(2,)]


@pytest.mark.delta
def test_a_failed_load_changes_nothing(spark, config, schema, gold, monkeypatch):
    warehouse.run(spark, config, schema=schema)
    before = _query(config, "SELECT _loaded_at, count(*) FROM {schema}.orders GROUP BY 1", schema)

    real_copy = warehouse.copy_rows

    def fail_on_customers(cursor, schema_name, table, df):
        if table == "customers":
            raise RuntimeError("connection lost in the middle of the load")
        return real_copy(cursor, schema_name, table, df)

    monkeypatch.setattr(warehouse, "copy_rows", fail_on_customers)
    with pytest.raises(RuntimeError, match="middle of the load"):
        warehouse.run(spark, config, schema=schema)

    # orders and order_lines had already been truncated and refilled inside
    # the failed transaction: the rollback brings the previous load back.
    assert _query(config, "SELECT _loaded_at, count(*) FROM {schema}.orders GROUP BY 1", schema) == before
    assert _query(config, "SELECT count(*) FROM {schema}.customers", schema) == [(1,)]


@pytest.mark.delta
def test_the_warehouse_needs_gold_first(spark, config, schema):
    with pytest.raises(warehouse.MissingGoldError, match="Run the gold job first"):
        warehouse.run(spark, config, schema=schema)
