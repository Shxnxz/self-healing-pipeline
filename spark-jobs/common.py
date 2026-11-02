"""
Shared helpers for the bronze/silver/gold jobs.

Kept deliberately small: a Spark session builder (Delta + Kafka packages
wired in once) and a guard that waits for an upstream Delta table to exist
before a downstream job tries to stream-read it. On a cold `docker compose
up`, silver_job and gold_job can start before their upstream table has been
created by the first micro-batch of the job before them -- this avoids a
crash-loop while that happens.
"""

import os
import time
from typing import Optional

from pyspark.sql import DataFrame

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

# Must match the pinned pyspark version/Scala build in requirements.txt.
SCALA_VERSION = "2.12"
SPARK_VERSION = "3.5.3"

# openlineage-spark reports the Delta streaming sink (Gold) as an output only
# from 1.53.0; earlier versions drop the silver2 -> gold edge. Don't go below it.
OPENLINEAGE_VERSION = "1.54.0"
OPENLINEAGE_LISTENER = "io.openlineage.spark.agent.OpenLineageSparkListener"


def _openlineage_enabled() -> bool:
    return (
        os.environ.get("OPENLINEAGE_ENABLED", "true").lower() == "true"
        and bool(os.environ.get("OPENLINEAGE_KAFKA_BOOTSTRAP_SERVERS"))
    )


def build_spark(app_name: str, driver_memory: str = "1g") -> SparkSession:
    packages = [f"org.apache.spark:spark-sql-kafka-0-10_{SCALA_VERSION}:{SPARK_VERSION}"]
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", driver_memory)
        # Default is 200 tasks per shuffle, which is far too many for micro-batches of a
        # few hundred rows, and every per-source query pays it (see sources.py).
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "8"))
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    if _openlineage_enabled():
        # Lineage events go to Kafka; lineage/consumer.py writes them to Neo4j.
        packages.append(f"io.openlineage:openlineage-spark_{SCALA_VERSION}:{OPENLINEAGE_VERSION}")
        builder = (
            builder.config("spark.extraListeners", OPENLINEAGE_LISTENER)
            .config("spark.openlineage.namespace", os.environ.get("OPENLINEAGE_NAMESPACE", "self-healing-pipeline"))
            .config("spark.openlineage.transport.type", "kafka")
            .config(
                "spark.openlineage.transport.topicName",
                os.environ.get("OPENLINEAGE_TOPIC", "openlineage.events"),
            )
            .config(
                "spark.openlineage.transport.properties.bootstrap.servers",
                os.environ["OPENLINEAGE_KAFKA_BOOTSTRAP_SERVERS"],
            )
            # The transport passes these straight to the Kafka producer, which has no default.
            .config(
                "spark.openlineage.transport.properties.key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            )
            .config(
                "spark.openlineage.transport.properties.value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer",
            )
        )
    spark = configure_spark_with_delta_pip(builder, extra_packages=packages).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# Delta conflicts a concurrent writer can cause on a table several queries append to
# (the shared quarantine / dead letter / audit tables, see sources.py). The first
# writes race to create the table, and mergeSchema writes change its metadata.
_RETRYABLE_DELTA_CONFLICTS = (
    "ConcurrentAppendException",
    "ConcurrentWriteException",
    "MetadataChangedException",
    "ProtocolChangedException",
    "ConcurrentTransactionException",
    "DeltaConcurrentModificationException",
)


def _is_delta_conflict(exc: BaseException) -> bool:
    text = f"{type(exc).__name__} {exc}"
    return any(name in text for name in _RETRYABLE_DELTA_CONFLICTS)


def idempotent_append(
    df: DataFrame,
    path: str,
    app_id: str,
    version: int,
    merge_schema: bool = False,
    attempts: int = 5,
) -> None:
    """
    Append `df` to the Delta table at `path`, at most once per (app_id, version).

    Delta skips a write whose txnVersion is <= the last one it recorded for the same
    txnAppId on that table, so a micro-batch Spark replays after a crash is not
    written twice. That also means two queries writing the same table must never
    share an app_id: their batch ids both start at 0, and one would silently skip
    the other's batches. Callers therefore put the source in app_id.

    A write that loses a race with another writer is retried; a failed commit wrote
    nothing, and the txn check keeps the retry from duplicating rows.
    """
    for attempt in range(1, attempts + 1):
        writer = (
            df.write.format("delta")
            .mode("append")
            .option("txnAppId", app_id)
            .option("txnVersion", version)
        )
        if merge_schema:
            writer = writer.option("mergeSchema", "true")
        try:
            writer.save(path)
            return
        except Exception as exc:
            if attempt == attempts or not _is_delta_conflict(exc):
                raise
            print(f"Concurrent write conflict on {path} ({type(exc).__name__}); "
                  f"retry {attempt}/{attempts - 1}", flush=True)
            time.sleep(0.5 * attempt)


def wait_for_delta_table(
    path: str, poll_seconds: int = 5, upstream: Optional[StreamingQuery] = None
) -> None:
    """
    Block until `path` looks like an initialized Delta table.

    `upstream` is the query that will create the table when it runs in this
    same driver (see run_layers.py). If that query dies first, raise instead
    of waiting forever, so the container exits and gets restarted.
    """
    log_marker = os.path.join(path, "_delta_log")
    waited = 0
    while not os.path.isdir(log_marker):
        if upstream is not None and not upstream.isActive:
            raise RuntimeError(
                f"Upstream query '{upstream.name or upstream.id}' stopped before "
                f"creating {path}"
            ) from upstream.exception()
        print(f"Waiting for upstream Delta table at {path} to exist "
              f"(waited {waited}s)...")
        time.sleep(poll_seconds)
        waited += poll_seconds
