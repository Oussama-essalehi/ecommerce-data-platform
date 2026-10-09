from __future__ import annotations

from datetime import datetime

from pyspark.sql.types import (
    BinaryType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from lakehouse.bronze import order_events

# The columns Spark's Kafka source produces.
KAFKA_SCHEMA = StructType(
    [
        StructField("key", BinaryType()),
        StructField("value", BinaryType()),
        StructField("topic", StringType()),
        StructField("partition", IntegerType()),
        StructField("offset", LongType()),
        StructField("timestamp", TimestampType()),
        StructField("timestampType", IntegerType()),
    ]
)


def kafka_rows(spark, messages: list[tuple[str | None, bytes]]):
    rows = [
        (key.encode() if key is not None else None, value, "shop.orders.v1", index % 3, index,
         datetime(2026, 10, 8, 20, 0, index), 0)
        for index, (key, value) in enumerate(messages)
    ]
    return spark.createDataFrame(rows, KAFKA_SCHEMA)


def test_payload_and_kafka_coordinates_are_kept(spark):
    payload = '{"event_type":"order_created","shipping":{"city":"Saint-Étienne"}}'
    df = order_events.to_bronze(kafka_rows(spark, [("WEB-20261008-00001", payload.encode("utf-8"))]))

    assert df.columns == [
        "key", "value", "topic", "partition", "offset", "kafka_timestamp", "_ingested_at",
    ]
    assert dict(df.dtypes)["value"] == "string"
    row = df.first()
    assert row["key"] == "WEB-20261008-00001"
    assert row["value"] == payload
    assert (row["topic"], row["partition"], row["offset"]) == ("shop.orders.v1", 0, 0)
    assert row["kafka_timestamp"] is not None and row["_ingested_at"] is not None


def test_broken_and_empty_messages_pass_through_untouched(spark):
    truncated = b'{"event_id":"abc","event_type":"order_cre'
    df = order_events.to_bronze(
        kafka_rows(spark, [("WEB-1", truncated), (None, b""), ("WEB-3", None)])
    )
    rows = sorted(df.collect(), key=lambda r: r["offset"])
    assert len(rows) == 3  # nothing is filtered in bronze
    assert rows[0]["value"] == truncated.decode()
    assert rows[1]["key"] is None and rows[1]["value"] == ""
    assert rows[2]["value"] is None
