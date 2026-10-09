"""The only module that talks to Delta Lake.

Tables are addressed by path (no metastore): <lake>/<layer>/<table>.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession


def exists(spark: SparkSession, path: str) -> bool:
    from delta.tables import DeltaTable

    return DeltaTable.isDeltaTable(spark, path)


def read(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.format("delta").load(path)


def append(df: DataFrame, path: str) -> None:
    df.write.format("delta").mode("append").save(path)


def replace_where(spark: SparkSession, df: DataFrame, path: str, predicate: str) -> None:
    """Atomically replace the rows matching `predicate` with `df`.

    This is what makes a batch load idempotent: running the same day twice
    leaves one copy of that day, not two. Delta checks that every row of
    `df` satisfies the predicate, so a load cannot spill outside its slice.
    """
    if not exists(spark, path):
        append(df, path)
        return
    df.write.format("delta").mode("overwrite").option("replaceWhere", predicate).save(path)


def overwrite(df: DataFrame, path: str) -> None:
    """Atomically replace the whole table (readers see the old or the new version, never a mix)."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
