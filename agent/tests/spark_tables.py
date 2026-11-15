"""Real pipeline tables for the Validate tests, written by the pipeline's own Spark jobs.

Bronze rows are taken from the repo's data/ CSVs and edited, then silver1_job and
silver2_job run on them exactly as in Docker, so the quarantine, Silver 2 and
ref_canonical_mappings are what the agent meets in production.

  crash1 (month first)  r1-ok-1..3  crash_month "Jule", crash_date in July  -> quarantined
                                    (r1-ok-3 also has weather "Clqudy", fuzzy-corrected to CLOUDY)
                        r1-other    the same, and damage "$1489" too        -> quarantined, 2 failures
                        r1-clean    untouched                               -> Silver 2
  crash3 (day first)    r3-ok-1..2  crash_month "Jule", crash_date 04/07/2021 and 21/07/2021,
                                    July only when read day first           -> quarantined
                        r3-clean    untouched                               -> Silver 2
"""
import csv
import itertools
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
SPARK_JOBS = os.path.join(REPO, "spark-jobs")
DATA = os.path.join(REPO, "data")

CRASH1 = {
    "r1-ok-1": {"crash_month": "Jule", "crash_date": "07/04/2019", "crash_day_of_week": "Th"},
    "r1-ok-2": {"crash_month": "Jule", "crash_date": "07/21/2019", "crash_day_of_week": "Su"},
    "r1-ok-3": {"crash_month": "jule", "crash_date": "07/30/2019", "crash_day_of_week": "Tu",
                "weather_condition": "Clqudy"},
    "r1-other": {"crash_month": "Jule", "crash_date": "07/05/2019", "damage": "$1489", "crash_day_of_week": "Fr"},
    "r1-clean": {},
}
CRASH3 = {
    "r3-ok-1": {"crash_month": "Jule", "crash_date": "04/07/2021", "crash_day_of_week": "Sunday"},
    "r3-ok-2": {"crash_month": "Jule", "crash_date": "21/07/2021", "crash_day_of_week": "Wednesday"},
    "r3-clean": {},
}


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


def _rows(folder: str, edits: dict):
    with open(os.path.join(DATA, folder, f"{folder}-1.csv"), encoding="utf-8", errors="replace") as fh:
        base = list(itertools.islice(csv.DictReader(fh), len(edits)))
    for (record_id, change), row in zip(edits.items(), base):
        yield json.dumps({"record_id": record_id, "batch_id": 1, "table_name": folder,
                          "ingestion_ts": "2026-11-13T00:00:00+00:00", "payload": {**row, **change}})


def build(root: str):
    """Run Bronze -> Silver 1 -> Silver 2 for crash1 and crash3 into `root`. Returns the session."""
    os.environ.update({"DELTA_ROOT": f"{root}/delta", "CHECKPOINT_ROOT": f"{root}/checkpoints",
                       "SOURCES": "crash1,crash3", "DAY_FIRST_SOURCES": "crash3",
                       "OPENLINEAGE_ENABLED": "false", "QWEN_ENABLED": "false",
                       "PYSPARK_PYTHON": sys.executable})
    for path in (SPARK_JOBS, REPO):
        if path not in sys.path:
            sys.path.insert(0, path)
    os.environ["PYTHONPATH"] = os.pathsep.join([SPARK_JOBS, REPO, os.environ.get("PYTHONPATH", "")])

    import silver1_job
    import silver2_job
    from common import build_spark
    from sources import table_path

    spark = build_spark("ValidateTest")
    for source, edits in (("crash1", CRASH1), ("crash3", CRASH3)):
        rows = [(str(i), raw, f"crashes.{source}", 0, i)
                for i, raw in enumerate(_rows(source.capitalize(), edits))]
        (spark.createDataFrame(rows, "kafka_key string, raw_value string, topic string, partition int, offset long")
         .selectExpr("*", "current_timestamp() AS kafka_timestamp", "current_timestamp() AS ingested_at")
         .write.format("delta").save(table_path("bronze", source)))
        silver1 = silver1_job.start(spark, source)
        silver1.processAllAvailable()
        silver2 = silver2_job.start(spark, source, upstream=silver1)
        silver2.processAllAvailable()
        silver1.stop()
        silver2.stop()
    return spark
