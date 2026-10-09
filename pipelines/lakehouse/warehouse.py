"""Batch job: copy the gold tables into the PostgreSQL data warehouse.

The four gold tables land, unchanged, in the `lake` schema of the
warehouse. dbt takes it from there and builds the star schema.

How the load works:

- Rows go through PostgreSQL's COPY protocol, its bulk-loading path: far
  faster than INSERT statements.
- The four tables are replaced inside a single transaction. Anyone
  querying the warehouse sees either the previous load or the new one,
  never orders without their lines.
- Tables are emptied with TRUNCATE rather than dropped, so the dbt views
  built on top of them keep working.
- Every row gets a `_loaded_at` timestamp, which is what freshness checks
  read to tell when the warehouse was last fed.
"""

from __future__ import annotations

import logging
import time

import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from . import lake
from .config import Config
from .gold import TABLES

log = logging.getLogger(__name__)

SCHEMA = "lake"
LOADED_AT = "_loaded_at"


class MissingGoldError(RuntimeError):
    """The warehouse cannot be loaded before gold has been built."""


def connection_string(config: Config) -> str:
    return make_conninfo(
        host=config.warehouse_host,
        port=config.warehouse_port,
        dbname=config.warehouse_db,
        user=config.warehouse_user,
        password=config.warehouse_password,
    )


def pg_type(data_type: DataType) -> str:
    """The PostgreSQL column type for a Spark column type."""
    if isinstance(data_type, StringType):
        return "text"
    if isinstance(data_type, IntegerType):
        return "integer"
    if isinstance(data_type, LongType):
        return "bigint"
    if isinstance(data_type, DecimalType):
        return f"numeric({data_type.precision},{data_type.scale})"
    if isinstance(data_type, DoubleType):
        return "double precision"
    if isinstance(data_type, BooleanType):
        return "boolean"
    if isinstance(data_type, DateType):
        return "date"
    if isinstance(data_type, TimestampType):
        # The lake stores UTC instants; timestamptz keeps them unambiguous.
        return "timestamp with time zone"
    raise TypeError(f"no PostgreSQL type mapped for Spark type {data_type.simpleString()}")


def columns_of(schema: StructType) -> list[tuple[str, str]]:
    """(column name, PostgreSQL type) for each column of a gold table."""
    return [(field.name, pg_type(field.dataType)) for field in schema.fields]


def create_table(schema: str, table: str, columns: list[tuple[str, str]]) -> sql.Composed:
    definitions = [
        sql.SQL("{} {}").format(sql.Identifier(name), sql.SQL(pg))
        for name, pg in columns
    ]
    definitions.append(
        sql.SQL("{} timestamp with time zone NOT NULL DEFAULT now()").format(
            sql.Identifier(LOADED_AT)
        )
    )
    return sql.SQL("CREATE TABLE {} ({})").format(
        sql.Identifier(schema, table), sql.SQL(", ").join(definitions)
    )


def existing_columns(
    cursor: psycopg.Cursor, schema: str, table: str
) -> list[tuple[str, str]] | None:
    """Columns of the warehouse table as (name, type), or None if it does not exist."""
    cursor.execute(
        """
        SELECT a.attname, format_type(a.atttypid, a.atttypmod)
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s AND a.attnum > 0 AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, table),
    )
    rows = cursor.fetchall()
    return [(name, pg) for name, pg in rows] if rows else None


def prepare_table(
    cursor: psycopg.Cursor, schema: str, table: str, columns: list[tuple[str, str]]
) -> str:
    """Make sure an empty table with the right columns exists.

    Returns what was done: "created", "recreated" (the gold schema changed)
    or "truncated".
    """
    expected = [*columns, (LOADED_AT, "timestamp with time zone")]
    current = existing_columns(cursor, schema, table)
    if current is None:
        cursor.execute(create_table(schema, table, columns))
        return "created"
    if current != expected:
        # CASCADE also drops the dbt views reading this table; the next
        # `dbt build` recreates them.
        log.warning("%s.%s: columns changed, the table is recreated", schema, table)
        cursor.execute(
            sql.SQL("DROP TABLE {} CASCADE").format(sql.Identifier(schema, table))
        )
        cursor.execute(create_table(schema, table, columns))
        return "recreated"
    cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(schema, table)))
    return "truncated"


def for_copy(df: DataFrame) -> DataFrame:
    """Render timestamps as UTC text so they do not depend on any machine's time zone."""
    return df.select(
        *[
            F.date_format(F.col(field.name), "yyyy-MM-dd HH:mm:ss.SSSSSS'+00'").alias(field.name)
            if isinstance(field.dataType, TimestampType)
            else F.col(field.name)
            for field in df.schema.fields
        ]
    )


def copy_rows(cursor: psycopg.Cursor, schema: str, table: str, df: DataFrame) -> int:
    names = [field.name for field in df.schema.fields]
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        sql.Identifier(schema, table), sql.SQL(", ").join(map(sql.Identifier, names))
    )
    rows = 0
    with cursor.copy(statement) as copy:
        # toLocalIterator streams the table partition by partition instead
        # of holding it all in the driver's memory.
        for row in for_copy(df).toLocalIterator():
            copy.write_row(tuple(row))
            rows += 1
    return rows


def run(spark: SparkSession, config: Config, schema: str = SCHEMA) -> dict:
    started = time.monotonic()
    missing = [t for t in TABLES if not lake.exists(spark, config.table("gold", t))]
    if missing:
        raise MissingGoldError(
            "gold tables not built yet: " + ", ".join(missing) + ". Run the gold job first."
        )

    tables: dict[str, dict] = {}
    # One transaction for everything: it commits when the block exits
    # normally and rolls back entirely if anything fails.
    with psycopg.connect(connection_string(config)) as connection, connection.cursor() as cursor:
        cursor.execute("SET TIME ZONE 'UTC'")
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema))
        )
        for table in TABLES:
            df = lake.read(spark, config.table("gold", table))
            action = prepare_table(cursor, schema, table, columns_of(df.schema))
            rows = copy_rows(cursor, schema, table, df)
            tables[table] = {"rows": rows, "action": action}
            log.info("%s.%s: %d rows loaded (%s)", schema, table, rows, action)
        for table in TABLES:
            # Refresh the planner's statistics for the queries dbt runs next.
            cursor.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(schema, table)))

    return {
        "job": "warehouse-load",
        "warehouse": f"{config.warehouse_host}:{config.warehouse_port}/{config.warehouse_db}",
        "schema": schema,
        "tables": tables,
        "seconds": round(time.monotonic() - started, 1),
    }
