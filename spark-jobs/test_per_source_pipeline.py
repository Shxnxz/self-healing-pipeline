"""
End-to-end check of the per-source layout on local Spark (no Kafka, Docker or Neo4j).

Two sources, crash1 and crash3, run side by side in one driver exactly as
run_layers.py starts them:
    bronze_<s> (written here from real CSV rows) -> silver1_<s> -> silver2_<s>
    silver2_crash1 + silver2_crash3 -> gold (UNION ALL)

It checks the things that break silently when several sources share a driver
and shared tables:
  1. no rows cross branches (Silver 1 used to share one temp view per session)
  2. both sources' failures land in the shared quarantine with batch id 0
     (needs a txnAppId per source), each tagged with its target_table
  3. every Silver 1 row ends in its Silver 2 table or the quarantine
  4. Gold is the UNION ALL of the Silver 2 tables, tagged with source_table

Run from spark-jobs/:  python test_per_source_pipeline.py
Needs the repo's data/ folder (DATA_DIR overrides it) and the Delta/Kafka jars.
"""
import csv
import itertools
import json
import os
import shutil
import sys
import tempfile
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = tempfile.mkdtemp(prefix="per-source-test-")
# sources.py reads these at import time, so set them before importing any job.
os.environ["DELTA_ROOT"] = f"{ROOT}/delta"
os.environ["CHECKPOINT_ROOT"] = f"{ROOT}/checkpoints"
os.environ["SOURCES"] = "crash1,crash3"
os.environ["OPENLINEAGE_ENABLED"] = "false"
os.environ["QWEN_ENABLED"] = "false"
sys.path.insert(0, os.path.dirname(HERE))  # repo root, for qwen_classifier

import gold_job  # noqa: E402
import silver1_job  # noqa: E402
import silver2_job  # noqa: E402
from common import build_spark, idempotent_append  # noqa: E402
from sources import source_names, table_path  # noqa: E402

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(HERE), "data"))
ROWS_PER_SOURCE = 40
BAD_WEATHER = "HAILSTORM APOCALYPSE"  # never maps -> quarantine


def _envelopes(folder: str, n: int):
    path = os.path.join(DATA_DIR, folder, f"{folder}-1.csv")
    with open(path, encoding="utf-8", errors="replace") as fh:
        rows = list(itertools.islice(csv.DictReader(fh), n))
    rows[0]["weather_condition"] = BAD_WEATHER
    return [
        json.dumps({
            "record_id": str(uuid.uuid4()),
            "batch_id": 1,
            "table_name": folder,
            "ingestion_ts": "2026-11-01T00:00:00+00:00",
            "payload": row,
        })
        for row in rows
    ]


def _write_bronze(spark, source: str) -> None:
    folder = source.capitalize()  # crash3 -> Crash3, as the producer names it
    rows = [
        (str(i), raw, f"crashes.{source}", 0, i)
        for i, raw in enumerate(_envelopes(folder, ROWS_PER_SOURCE))
    ]
    df = (spark.createDataFrame(rows, "kafka_key string, raw_value string, topic string, partition int, offset long")
          .selectExpr("*", "current_timestamp() AS kafka_timestamp", "current_timestamp() AS ingested_at"))
    df.write.format("delta").save(table_path("bronze", source))


def _drain(queries):
    for q in queries:
        q.processAllAvailable()


def check_shared_app_id_would_lose_rows(spark):
    """Why the source is in every txnAppId: same appId + same batch id = skipped write."""
    path = f"{ROOT}/delta/appid_demo"
    one = spark.range(1)
    idempotent_append(one, path, "silver2_job:quarantine", 0)
    idempotent_append(one, path, "silver2_job:quarantine", 0)  # another source's batch 0
    assert spark.read.format("delta").load(path).count() == 1
    idempotent_append(one, path, "silver2_job_crash3:quarantine", 0)
    assert spark.read.format("delta").load(path).count() == 2
    print("  shared appId drops the second source's batch; per-source appId keeps it: OK")


def run():
    if not os.path.isdir(DATA_DIR):
        print(f"SKIP: no data folder at {DATA_DIR}")
        return
    sources = source_names()
    spark = build_spark("PerSourcePipelineTest")
    try:
        check_shared_app_id_would_lose_rows(spark)

        for s in sources:
            _write_bronze(spark, s)

        silver1 = {s: silver1_job.start(spark, s) for s in sources}
        _drain(silver1.values())
        for s in sources:
            names = {r.table_name for r in spark.read.format("delta").load(table_path("silver1", s))
                     .select("table_name").distinct().collect()}
            n = spark.read.format("delta").load(table_path("silver1", s)).count()
            assert names == {s.capitalize()}, f"silver1_{s} holds rows from {names}"
            assert n == ROWS_PER_SOURCE, f"silver1_{s} has {n} rows"
        print(f"  silver1: {ROWS_PER_SOURCE} rows per source, no rows crossed branches: OK")

        silver2 = {s: silver2_job.start(spark, s, upstream=silver1[s]) for s in sources}
        _drain(silver2.values())
        quarantine = spark.read.format("delta").load(silver2_job.QUARANTINE_PATH)
        for s in sources:
            n_valid = spark.read.format("delta").load(table_path("silver2", s)).count()
            mine = quarantine.filter(quarantine.target_table == f"silver2_{s}")
            n_quarantined = mine.count()
            assert n_quarantined >= 1, f"no quarantine rows for {s}: the shared write was skipped"
            assert {r.table_name for r in mine.select("table_name").distinct().collect()} == {s.capitalize()}
            assert n_valid + n_quarantined == ROWS_PER_SOURCE, (
                f"{s}: {n_valid} valid + {n_quarantined} quarantined != {ROWS_PER_SOURCE}")
            print(f"  silver2_{s}: {n_valid} valid, {n_quarantined} quarantined (target_table set): OK")
        assert not os.path.isdir(silver2_job.DEAD_LETTER_PATH), "a transform failed and was dead-lettered"

        gold = gold_job.start(spark, sources, upstreams=silver2)
        _drain([gold])
        g = spark.read.format("delta").load(gold_job.GOLD_PATH)
        expected = sum(spark.read.format("delta").load(table_path("silver2", s)).count() for s in sources)
        tags = {r.source_table for r in g.select("source_table").distinct().collect()}
        assert g.count() == expected, f"gold has {g.count()} rows, Silver 2 tables {expected}"
        assert tags == {f"silver2_{s}" for s in sources}, tags
        print(f"  gold: UNION ALL of {len(sources)} sources = {expected} rows, source_table set: OK")
        print("ALL PER-SOURCE CHECKS PASSED")
    finally:
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        shutil.rmtree(ROOT, ignore_errors=True)


def test_per_source_pipeline():
    run()


if __name__ == "__main__":
    run()
