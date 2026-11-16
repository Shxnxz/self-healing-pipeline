"""
Gold's derived fields and the Gold business metrics (gold_metrics.py) on local Spark.

Two small Silver 2 tables (crash1, crash3) stream into Gold, where an "old" Gold
table already exists without the derived fields (as on a machine that ran the
pipeline before them). It checks that:
  1. start() adds the derived columns to the old table, Gold keeps streaming, and the
     derived values are right (weekend, time of day, injury / fatal, injected)
  2. the metrics query refreshes every table once Gold has rows
  3. the aggregates count each crash once (crash_record_id), leave fault-injected
     copies out, and derive the fields again for the old rows (null in Gold)
  4. a record that arrives later (a healed one) lands in its own day on the next refresh

Run from spark-jobs/:  python test_gold_metrics.py     (or python -m pytest test_gold_metrics.py)
Needs Java and the Delta jars (pyspark==3.5.3, delta-spark==3.3.1).
"""
import os
import shutil
import sys
import tempfile
from datetime import date

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = tempfile.mkdtemp(prefix="gold-metrics-test-")
# sources.py reads these at import time, so set them before importing any job.
os.environ["DELTA_ROOT"] = f"{ROOT}/delta"
os.environ["CHECKPOINT_ROOT"] = f"{ROOT}/checkpoints"
os.environ["SOURCES"] = "crash1,crash3"
os.environ["OPENLINEAGE_ENABLED"] = "false"
os.environ["QWEN_ENABLED"] = "false"
os.environ["GOLD_METRICS_INTERVAL"] = "1 second"
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.dirname(HERE))  # repo root, for qwen_classifier

import gold_job  # noqa: E402
import gold_metrics  # noqa: E402
from common import build_spark  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from sources import table_path  # noqa: E402

SILVER2_SCHEMA = (
    "record_id string, crash_record_id string, crash_date date, crash_hour string, crash_day_of_week string, "
    "crash_month string, beat_of_occurrence string, street_direction string, street_name string, "
    "latitude string, longitude string, posted_speed_limit int, traffic_control_device string, "
    "device_condition string, weather_condition string, lighting_condition string, "
    "roadway_surface_cond string, road_defect string, trafficway_type string, alignment string, "
    "intersection_related_i string, crash_type string, first_crash_type string, "
    "prim_contributory_cause string, sec_contributory_cause string, num_units decimal(10,1), "
    "hit_and_run_i boolean, private_property_i string, damage string, most_severe_injury string, "
    "injuries_total string, injuries_fatal string, injuries_incapacitating string, "
    "injuries_non_incapacitating string, injuries_reported_not_evident string, injuries_no_indication string"
)


def _row(rid, crash_id, day, hour, weekday, injuries="0", fatal="0", weather="CLEAR", beat="1234",
         hit_and_run=False, cause="unable to determine"):
    return {
        "record_id": rid, "crash_record_id": crash_id, "crash_date": day, "crash_hour": hour,
        "crash_day_of_week": weekday, "crash_month": day.strftime("%B").upper(), "beat_of_occurrence": beat,
        "street_direction": "S", "street_name": "Drexel Blvd", "latitude": "41.8", "longitude": "-87.6",
        "posted_speed_limit": 30, "traffic_control_device": "NO CONTROLS", "device_condition": "NO CONTROLS",
        "weather_condition": weather, "lighting_condition": "DAYLIGHT", "roadway_surface_cond": "DRY",
        "road_defect": "NO DEFECTS", "trafficway_type": "NOT DIVIDED", "alignment": "STRAIGHT AND LEVEL",
        "intersection_related_i": None, "crash_type": "NO INJURY / DRIVE AWAY", "first_crash_type": "REAR END",
        "prim_contributory_cause": cause, "sec_contributory_cause": cause, "num_units": None,
        "hit_and_run_i": hit_and_run, "private_property_i": None, "damage": "OVER $1,500",
        "most_severe_injury": "NO INDICATION OF INJURY", "injuries_total": injuries, "injuries_fatal": fatal,
        "injuries_incapacitating": "0", "injuries_non_incapacitating": injuries,
        "injuries_reported_not_evident": "0", "injuries_no_indication": "2",
    }


TUE, WED, SAT = date(2021, 6, 1), date(2021, 6, 2), date(2021, 6, 5)

CRASH1 = [
    _row("r1", "CR-1", TUE, "19.00", "TUESDAY", injuries="1", weather="RAIN", hit_and_run=True),
    _row("r2", "CR-2", SAT, "03.00", "SATURDAY"),
    # A fault-injected copy (evaluation stack): in Gold, never in an aggregate.
    _row("r5", "FAULT-jule-1-0001", TUE, "10.00", "TUESDAY", injuries="3"),
]
CRASH3 = [
    _row("r3", "CR-3", WED, "13.00", "WEDNESDAY", injuries="2", fatal="1", cause="speeding"),
    # The same crash twice (a duplicate in the source): counted once.
    _row("r4", "CR-3", WED, "13.00", "WEDNESDAY", injuries="2", fatal="1", cause="speeding"),
]
OLD_GOLD = [_row("r0", "CR-0", SAT, "08.00", "SATURDAY", injuries="1")]
LATE = [_row("r6", "CR-6", TUE, "18.00", "TUESDAY")]


def _write_silver2(spark, source, rows, mode="append"):
    spark.createDataFrame(rows, SILVER2_SCHEMA).write.format("delta").mode(mode).save(table_path("silver2", source))


def _old_gold(spark):
    """A Gold table as the job wrote it before the derived fields existed."""
    spark.createDataFrame(OLD_GOLD, SILVER2_SCHEMA).withColumn(
        "source_table", F.lit("silver2_crash1")).createOrReplaceTempView("silver2_all")
    typed = spark.sql(gold_job.GOLD_TRANSFORM_SQL).drop(*gold_job.DERIVED_COLUMNS)
    typed.write.format("delta").save(gold_job.GOLD_PATH)


def _settle(spark):
    for q in spark.streams.active:
        q.processAllAvailable()
    # The metrics query may need one more trigger after Gold's last batch.
    for q in spark.streams.active:
        q.processAllAvailable()


def _by(rows, key):
    return {r[key]: r for r in rows}


def run_checks(spark):
    _write_silver2(spark, "crash1", CRASH1)
    _write_silver2(spark, "crash3", CRASH3)
    _old_gold(spark)

    gold_query = gold_job.start(spark, ["crash1", "crash3"])
    _settle(spark)
    names = {q.name for q in spark.streams.active}
    assert {"gold", "gold_metrics"} <= names, names

    # 1. derived columns added to the old table, values right
    gold = spark.read.format("delta").load(gold_job.GOLD_PATH)
    for col in gold_job.DERIVED_COLUMNS:
        assert col in gold.columns, f"{col} missing from Gold {gold.columns}"
    g = _by([r.asDict() for r in gold.collect()], "record_id")
    assert len(g) == 6, sorted(g)
    assert g["r0"]["is_injected"] is None and g["r0"]["is_weekend"] is None, "old row should read null"
    assert g["r1"]["time_of_day"] == "EVENING" and g["r1"]["is_injury_crash"] is True
    assert g["r1"]["is_weekend"] is False and g["r1"]["crash_year"] == 2021
    assert g["r2"]["time_of_day"] == "NIGHT" and g["r2"]["is_weekend"] is True
    assert g["r3"]["is_fatal_crash"] is True and g["r3"]["time_of_day"] == "AFTERNOON"
    assert g["r5"]["is_injected"] is True and g["r1"]["is_injected"] is False
    print("  gold: derived columns added to the old table, values right: OK")

    # 2. + 3. every metric table written; dedupe, FAULT- excluded, old rows derived
    daily = _by([r.asDict() for r in spark.read.format("delta").load(
        gold_metrics.metric_path("gold_daily_summary")).collect()], "crash_date")
    assert set(daily) == {TUE, WED, SAT}, sorted(daily)
    assert daily[TUE]["crashes"] == 1 and daily[TUE]["injuries_total"] == 1, daily[TUE]  # r5 left out
    assert daily[TUE]["hit_and_run_crashes"] == 1
    assert daily[WED]["crashes"] == 1 and daily[WED]["fatal_crashes"] == 1, daily[WED]  # CR-3 once
    assert daily[SAT]["crashes"] == 2 and daily[SAT]["is_weekend"] is True, daily[SAT]  # CR-0 (old) + CR-2
    assert daily[SAT]["injury_crashes"] == 1  # CR-0's is_injury_crash derived again
    for name in gold_metrics.METRIC_SQL:
        t = spark.read.format("delta").load(gold_metrics.metric_path(name))
        assert t.count() > 0, f"{name} is empty"
        assert {"gold_version", "refreshed_at"} <= set(t.columns), t.columns
    causes = _by([r.asDict() for r in spark.read.format("delta").load(
        gold_metrics.metric_path("gold_cause_summary")).collect()], "prim_contributory_cause")
    assert causes["speeding"]["crashes"] == 1 and causes["unable to determine"]["crashes"] == 3, causes
    assert abs(sum(c["share_of_crashes"] for c in causes.values()) - 1.0) < 0.01
    beat = spark.read.format("delta").load(gold_metrics.metric_path("gold_beat_monthly")).collect()
    assert len(beat) == 1 and beat[0]["crashes"] == 4, beat  # one beat, all in June 2021
    pattern = spark.read.format("delta").load(gold_metrics.metric_path("gold_time_pattern")).collect()
    assert sum(r["crashes"] for r in pattern) == 4
    print("  metrics: 5 tables, each crash once, injected copies out, old rows derived: OK")

    # 4. a record released later (healed) is counted on the next refresh, in its own day
    _write_silver2(spark, "crash1", LATE)
    _settle(spark)
    daily = _by([r.asDict() for r in spark.read.format("delta").load(
        gold_metrics.metric_path("gold_daily_summary")).collect()], "crash_date")
    assert daily[TUE]["crashes"] == 2, daily[TUE]
    print("  metrics: a late (healed) record lands in its own day on the next refresh: OK")

    # One-off refresh (python gold_metrics.py) gives the same tables.
    written = gold_metrics.refresh(spark)
    assert written["gold_daily_summary"] == 3, written
    gold_query.stop()


def test_gold_metrics():
    spark = build_spark("GoldMetricsTest")
    try:
        run_checks(spark)
    finally:
        for q in spark.streams.active:
            q.stop()
        spark.stop()
        shutil.rmtree(ROOT, ignore_errors=True)


if __name__ == "__main__":
    test_gold_metrics()
    print("All Gold metrics checks passed.")
