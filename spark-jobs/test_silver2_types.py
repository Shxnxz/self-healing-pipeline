"""
Silver 2 stores crash_hour, num_units and the injury counts as INT, and tables
written before that (crash_hour "09.00", num_units 2.0, injuries "0.0") are
converted in place without breaking Gold.

  1. A real Crash4 row (ST CLAIR ST, 2024-09-19, "09 AM", "2 units", "0.0")
     comes out of standardize_silver2 as 9, 2 and 0 with INT columns.
  2. Silver 2 refuses to append to an old table; upgrade_silver2_types.py rewrites
     it and the quarantine once, Gold catches up on rows it missed, restarts at
     the rewrite, gets the new rows once and keeps the old ones as they were.

Run from spark-jobs/:  python -m pytest -rs test_silver2_types.py
Needs the repo's data/ folder (DATA_DIR overrides it) and the Delta jars.
"""
import csv
import os
import shutil
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = tempfile.mkdtemp(prefix="silver2-types-test-")
# sources.py reads these at import time, so set them before importing any job.
os.environ["DELTA_ROOT"] = f"{ROOT}/delta"
os.environ["CHECKPOINT_ROOT"] = f"{ROOT}/checkpoints"
os.environ["SOURCES"] = "crash4"
os.environ["OPENLINEAGE_ENABLED"] = "false"
os.environ["QWEN_ENABLED"] = "false"
os.environ["GOLD_METRICS_ENABLED"] = "false"  # Gold itself is what is checked here
sys.path.insert(0, os.path.dirname(HERE))  # repo root, for qwen_classifier

from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.types import DecimalType, IntegerType, StringType  # noqa: E402

import gold_job  # noqa: E402
import silver1_job  # noqa: E402
import silver2_job  # noqa: E402
import upgrade_silver2_types  # noqa: E402
from common import build_spark  # noqa: E402
from ref_mappings import load_mappings_dict  # noqa: E402
from sources import table_path  # noqa: E402

DATA_DIR = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(HERE), "data"))
FIGURE_CRASH = "3630e8d84d61aef724886f3afe6e7910"  # report Figure 8, ST CLAIR ST


def _silver1_rows(spark, n=400):
    """The Figure 8 crash plus the first n Crash4-1 rows, as Silver 1 rows."""
    path = os.path.join(DATA_DIR, "Crash4", "Crash4-1.csv")
    with open(path, encoding="utf-8", errors="replace") as fh:
        rows = list(csv.DictReader(fh))
    picked = rows[:n] + [r for r in rows if r["crash_record_id"].startswith(FIGURE_CRASH)][:1]
    assert picked[-1]["crash_record_id"].startswith(FIGURE_CRASH), "Figure 8 crash not in Crash4-1.csv"
    cols = ["record_id", "batch_id", "table_name", "ingestion_ts", "topic", "kafka_timestamp",
            *silver1_job.CRASH_COLUMNS]
    data = []
    for i, r in enumerate(picked):
        meta = [f"rec-{i:03d}", "1", "Crash4", "2026-11-01T00:00:00", "crashes.crash4", "2026-11-01T00:00:00"]
        data.append(meta + [(r.get(c) or "").strip() or None for c in silver1_job.CRASH_COLUMNS])
    return spark.createDataFrame(data, ", ".join(f"`{c}` string" for c in cols))


def _old_types(df):
    """Silver 2 as it was written before the INT columns."""
    out = df.withColumn("crash_hour", F.format_string("%02d.00", "crash_hour"))
    out = out.withColumn("num_units", F.col("num_units").cast(DecimalType(10, 1)))
    for c in silver2_job.INJURY_COLUMNS:
        out = out.withColumn(c, F.format_string("%d.0", c))
    return out


def run():
    if not os.path.isdir(DATA_DIR):
        print(f"SKIP: no data folder at {DATA_DIR}")
        return
    spark = build_spark("Silver2TypesTest")
    try:
        silver2_job.init_shared_tables(spark)
        mappings = load_mappings_dict(spark, silver2_job.REF_MAPPINGS_PATH)
        std = silver2_job.standardize_silver2(_silver1_rows(spark), mappings, day_first=False).cache()

        # 1. Types and the Figure 8 record
        types = {f.name: f.dataType for f in std.schema.fields}
        for c in silver2_job.INT_COLUMNS:
            assert isinstance(types[c], IntegerType), f"{c} is {types[c]}"
        fig = std.filter(F.col("crash_record_id").startswith(FIGURE_CRASH)).first()
        assert fig.crash_date == "2024-09-19"
        assert fig.crash_hour == 9, fig.crash_hour
        assert fig.num_units == 2, fig.num_units
        assert [fig[c] for c in silver2_job.INJURY_COLUMNS] == [0, 0, 0, 0, 0, 1, 0]
        assert fig.columns_to_remediate == {}, fig.columns_to_remediate
        print("  Figure 8 record: crash_hour 9, num_units 2, injuries 0/1 as INT: OK")

        # A 1017 typo for 2017 is quarantined (Gold could not write it to Parquet)
        typo = _silver1_rows(spark, 1).limit(1).withColumn("crash_date", F.lit("1017-06-24"))
        typo = silver2_job.standardize_silver2(typo, mappings, day_first=False).first()
        assert typo.crash_date is None and typo.columns_to_remediate.get("crash_date") == "1017-06-24", typo
        print("  crash_date 1017-06-24 quarantined: OK")

        # 2. Old tables, Gold streaming from them, then the upgrade
        valid = std.filter(F.size("columns_to_remediate") == 0)
        failed = std.filter(F.size("columns_to_remediate") > 0)
        n_valid = valid.count()
        assert n_valid >= 10 and failed.count() >= 1, f"need valid and quarantined rows, got {n_valid} valid"
        silver2_path = table_path("silver2", "crash4")
        ids = sorted(r.record_id for r in valid.select("record_id").collect())
        cut = ids[n_valid // 2]
        first, later = valid.filter(F.col("record_id") < cut), valid.filter(F.col("record_id") >= cut)
        s2_cols = [c for c in std.columns if c not in ("columns_to_remediate", "_fuzzy_audit_entries", "raw_record")]
        _old_types(first.select(*s2_cols)).write.format("delta").save(silver2_path)
        (_old_types(failed.drop("_fuzzy_audit_entries"))
         .withColumn("healing_status", F.lit("PENDING"))
         .write.format("delta").save(silver2_job.QUARANTINE_PATH))
        assert isinstance(spark.read.format("delta").load(silver2_path).schema["crash_hour"].dataType, StringType)

        gold = gold_job.start(spark, ["crash4"])
        gold.processAllAvailable()
        for query in spark.streams.active:  # gold and gold_metrics
            query.stop()
        n_first = first.count()
        assert spark.read.format("delta").load(gold_job.GOLD_PATH).count() == n_first

        # One row reached Silver 2 after Gold stopped: the upgrade adds it to Gold. It
        # has a 1017 crash_date, as one old Crash4 row on a real laptop does.
        straggler = later.filter(F.col("record_id") == ids[-1]).withColumn("crash_date", F.lit("1017-06-24"))
        _old_types(straggler.select(*s2_cols)).write.format("delta").mode("append").save(silver2_path)
        later = later.filter(F.col("record_id") != ids[-1])
        n_first += 1

        try:
            silver2_job.require_int_columns(spark, silver2_path)
            raise AssertionError("Silver 2 would append INT rows to an old table")
        except RuntimeError as exc:
            assert "upgrade_silver2_types.py" in str(exc)
        assert upgrade_silver2_types.upgrade(spark, ["crash4"], dry_run=True)
        assert silver2_job.old_type_columns(spark, silver2_path)  # dry run changed nothing
        plan = upgrade_silver2_types.upgrade(spark, ["crash4"])
        assert set(plan["silver2_crash4"]) == set(silver2_job.INT_COLUMNS), plan
        assert set(plan["silver2_quarantine"]) == set(silver2_job.INT_COLUMNS), plan
        assert upgrade_silver2_types.upgrade(spark, ["crash4"]) == {}  # once only
        assert spark.read.format("delta").load(gold_job.GOLD_PATH).filter(
            F.col("record_id") == ids[-1]).first().crash_date is None  # 1017: no date in Gold
        assert spark.read.format("delta").load(gold_job.GOLD_PATH).count() == n_first
        s2 = spark.read.format("delta").load(silver2_path)
        assert s2.count() == n_first
        assert all(isinstance(s2.schema[c].dataType, IntegerType) for c in silver2_job.INT_COLUMNS)
        silver2_job.require_int_columns(spark, silver2_path)
        hours = {r.crash_hour for r in s2.select("crash_hour").collect()}
        assert hours and all(isinstance(h, int) and 0 <= h <= 23 for h in hours if h is not None), hours
        q = spark.read.format("delta").load(silver2_job.QUARANTINE_PATH)
        assert q.count() == failed.count() and "raw_record" in q.columns
        assert isinstance(q.schema["crash_hour"].dataType, IntegerType)
        print("  old Silver 2 and quarantine converted to INT once, rows and raw_record kept: OK")

        # New INT rows append, and Gold picks up only those after the rewrite
        later.select(*s2_cols).write.format("delta").mode("append").save(silver2_path)
        gold = gold_job.start(spark, ["crash4"])
        gold.processAllAvailable()
        g = spark.read.format("delta").load(gold_job.GOLD_PATH)
        assert n_valid - n_first >= 3, "too few new rows to check Gold's restart"
        assert g.count() == n_valid, f"gold has {g.count()} rows, expected {n_valid}"
        assert g.select("record_id").distinct().count() == n_valid, "a row reached Gold twice"
        g_fig = g.filter(F.col("crash_record_id").startswith(FIGURE_CRASH)).first()
        if g_fig is not None:
            assert (g_fig.crash_hour, g_fig.num_units, g_fig.injuries_no_indication) == (9, 2, 1)
        print(f"  gold: {n_first} old + {n_valid - n_first} new rows, no duplicates after the rewrite: OK")
        print("ALL SILVER 2 TYPE CHECKS PASSED")
    finally:
        for query in spark.streams.active:
            query.stop()
        spark.stop()
        shutil.rmtree(ROOT, ignore_errors=True)


def test_silver2_types():
    run()


if __name__ == "__main__":
    run()
