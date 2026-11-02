"""
Bronze: Kafka -> Delta, untouched. One table per source (see sources.py):
crashes.crash3 -> bronze_crash3.

Keeps Kafka's value as a plain string plus Kafka's own metadata. No JSON
parsing, no typing -- that's Silver's job. This preserves the raw envelope
exactly as the producer sent it, which matters if a downstream parsing
failure ever needs diagnosing: the original value has to still exist
somewhere.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.streaming import StreamingQuery

from common import build_spark
from sources import checkpoint_path, kafka_topic, source_names, table_path

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


def start(
    spark: SparkSession, source: str, upstream: Optional[StreamingQuery] = None
) -> StreamingQuery:
    """Start bronze_<source>: its one Kafka topic -> its own Delta table. `upstream` is unused."""
    topic = kafka_topic(source)
    bronze_path = table_path("bronze", source)
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    landed = raw_stream.selectExpr(
        "CAST(key AS STRING) as kafka_key",
        "CAST(value AS STRING) as raw_value",
        "topic",
        "partition",
        "offset",
        "timestamp as kafka_timestamp",
    ).withColumn("ingested_at", current_timestamp())

    query = (
        landed.writeStream.format("delta")
        .queryName(f"bronze_{source}")
        .trigger(processingTime="10 seconds") #Stop RAM from exploding
        .option("checkpointLocation", checkpoint_path("bronze", source))
        .outputMode("append")
        .start(bronze_path)
    )

    print(f"[bronze_{source}] Streaming '{topic}' -> {bronze_path}")
    return query


def main():
    spark = build_spark("BronzeLanding")
    for source in source_names():
        start(spark, source)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
