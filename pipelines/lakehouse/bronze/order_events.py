"""Streaming job: Kafka topic of web shop orders -> bronze.order_events.

Spark Structured Streaming reads the topic and appends each message to the
Delta table, untouched: the payload stays a string, and is parsed in
silver. A truncated or malformed message therefore cannot break ingestion.

The checkpoint stores the Kafka offsets already written. Together with
Delta's transactional commits this gives exactly-once delivery into
bronze: after a crash the query restarts from the last committed offsets,
and nothing is lost or written twice.

Two ways to run it:
- default       process everything available, then stop (a scheduled batch)
- continuous    stay up and write a micro-batch every few seconds
"""

from __future__ import annotations

import logging
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..config import Config

log = logging.getLogger(__name__)

TABLE = "order_events"


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Kafka records as bronze rows: payload as text plus the Kafka coordinates."""
    return kafka_df.select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("_ingested_at"),
    )


def read_topic(spark: SparkSession, config: Config, max_offsets: int) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.kafka_bootstrap)
        .option("subscribe", config.kafka_topic)
        # Only used the very first time; afterwards the checkpoint decides.
        .option("startingOffsets", "earliest")
        # Caps each micro-batch so the initial backlog is absorbed in steps.
        .option("maxOffsetsPerTrigger", max_offsets)
        .load()
    )


def run(
    spark: SparkSession,
    config: Config,
    *,
    continuous: bool = False,
    max_offsets: int = 100_000,
    interval_seconds: int = 15,
) -> dict:
    started = time.monotonic()
    table_path = config.table("bronze", TABLE)
    writer = (
        to_bronze(read_topic(spark, config, max_offsets))
        .writeStream.format("delta")
        .outputMode("append")
        .queryName("bronze_order_events")
        .option("checkpointLocation", config.checkpoint("bronze", TABLE))
    )
    if continuous:
        writer = writer.trigger(processingTime=f"{interval_seconds} seconds")
    else:
        writer = writer.trigger(availableNow=True)

    log.info("reading %s from %s into %s (%s)", config.kafka_topic, config.kafka_bootstrap,
             table_path, "continuous" if continuous else "available now")
    query = writer.start(table_path)
    query.awaitTermination()

    rows = sum(progress["numInputRows"] for progress in query.recentProgress)
    return {
        "job": "bronze-events",
        "table": table_path,
        "topic": config.kafka_topic,
        "rows": rows,
        "batches": len(query.recentProgress),
        "seconds": round(time.monotonic() - started, 1),
    }
