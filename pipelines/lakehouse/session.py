"""Spark session factory."""

from __future__ import annotations

import os
import sys

import pyspark
from pyspark.sql import SparkSession

from .config import Config


def kafka_package() -> str:
    """Maven coordinate of the Kafka connector matching the installed Spark."""
    return f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"


def build_session(app_name: str, config: Config) -> SparkSession:
    # Python workers must run the same interpreter as the driver. Without
    # this, Spark picks whatever `python3` is first on the PATH, which in a
    # virtual environment may be another version.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(config.spark_master)
        .config("spark.driver.memory", config.driver_memory)
        # Every timestamp in the lake is UTC, whatever the machine's time zone.
        .config("spark.sql.session.timeZone", "UTC")
        # The data is small: 200 shuffle partitions (the default) would mostly be empty.
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if config.ivy_dir:
        builder = builder.config("spark.jars.ivy", config.ivy_dir)

    if not config.offline:
        from delta import configure_spark_with_delta_pip

        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Lets the delta-spark package pick the Delta jar that matches the
        # installed versions, and adds the Kafka connector next to it.
        builder = configure_spark_with_delta_pip(builder, extra_packages=[kafka_package()])

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
