"""A real (small) pipeline for the tools' tests, run by the pipeline's own Spark jobs.

Bronze is written from the first rows of data/Crash1 and data/Crash3 (the real
dirty data), then silver1_job and silver2_job stream it exactly as in Docker. Kafka
is the only thing left out: `deliver` appends messages to Bronze the way the Bronze
job would, and `run` lets Silver 1 and Silver 2 catch up from their checkpoints.
"""
import csv
import importlib
import itertools
import json
import os
import sys
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SPARK_JOBS = os.path.join(REPO, "spark-jobs")
DATA = os.path.join(REPO, "data")
SOURCES = ("crash1", "crash3")
ROWS = 400


def available() -> str:
    """Why the Spark tests cannot run here, or '' when they can."""
    try:
        import delta  # noqa: F401
        import pyspark  # noqa: F401
    except ImportError:
        return "pyspark / delta-spark not installed"
    if not os.path.isfile(os.path.join(DATA, "Crash1", "Crash1-1.csv")):
        return "no data/ folder"
    if os.system("java -version > /dev/null 2>&1") != 0:
        return "no Java"
    return ""


def configure(root: str) -> None:
    """Point the pipeline modules at `root` (they read their paths at import time)."""
    os.environ.update({"DELTA_ROOT": f"{root}/delta", "CHECKPOINT_ROOT": f"{root}/checkpoints",
                       "SOURCES": ",".join(SOURCES), "DAY_FIRST_SOURCES": "crash3",
                       "OPENLINEAGE_ENABLED": "false", "QWEN_ENABLED": "false",
                       "QUARANTINE_PATH": f"{root}/delta/silver2_quarantine",
                       "DEAD_LETTER_PATH": f"{root}/delta/silver2_dead_letter",
                       "SILVER1_DEAD_LETTER_PATH": f"{root}/delta/silver1_dead_letter",
                       "REF_MAPPINGS_PATH": f"{root}/delta/ref_canonical_mappings",
                       "FUZZY_AUDIT_PATH": f"{root}/delta/silver2_fuzzy_audit",
                       "REMEDIATION_AUDIT_PATH": f"{root}/delta/remediation_audit",
                       "FAULT_INJECTIONS_PATH": f"{root}/delta/fault_injections",
                       "PYSPARK_PYTHON": sys.executable})
    from tools import session

    session.pipeline_modules()
    for name in ("sources", "common", "ref_mappings", "schema_guard", "silver1_job", "silver2_job"):
        if name in sys.modules:
            importlib.reload(sys.modules[name])


def _envelopes(source: str):
    folder = source.capitalize()
    with open(os.path.join(DATA, folder, f"{folder}-1.csv"), encoding="utf-8", errors="replace") as fh:
        for row in itertools.islice(csv.DictReader(fh), ROWS):
            yield json.dumps({"record_id": str(uuid.uuid4()), "batch_id": 1, "table_name": folder,
                              "ingestion_ts": "2026-11-14T00:00:00+00:00", "payload": row})


def deliver(spark, source: str, messages) -> None:
    """Append Kafka messages to bronze_<source>, as bronze_job.py lands them."""
    from sources import kafka_topic, table_path

    rows = [(str(i), raw, kafka_topic(source), 0, i) for i, raw in enumerate(messages)]
    (spark.createDataFrame(rows, "kafka_key string, raw_value string, topic string, partition int, offset long")
     .selectExpr("*", "current_timestamp() AS kafka_timestamp", "current_timestamp() AS ingested_at")
     .write.format("delta").mode("append").save(table_path("bronze", source)))


def run(spark) -> None:
    """Silver 1 then Silver 2 for every source, until they have caught up."""
    import silver1_job
    import silver2_job

    for source in SOURCES:
        s1 = silver1_job.start(spark, source)
        s1.processAllAvailable()
        s1.stop()
        s2 = silver2_job.start(spark, source)
        s2.processAllAvailable()
        s2.stop()


def build(root: str):
    configure(root)
    from tools import session

    spark = session.spark("ToolsTest")
    for source in SOURCES:
        deliver(spark, source, list(_envelopes(source)))
    run(spark)
    return spark


class FakeProducer:
    """confluent_kafka.Producer stand-in: keeps what was sent per topic."""

    def __init__(self):
        self.sent = {}

    def produce(self, topic, key=None, value=None, on_delivery=None):
        self.sent.setdefault(topic, []).append(value.decode())

    def flush(self, timeout=None):
        return 0
