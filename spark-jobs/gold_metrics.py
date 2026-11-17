"""
Gold business metrics: the aggregated, business-ready tables built on Gold.

Gold itself (gold_job.py) is one row per crash with typed and derived fields. The
spec's Gold layer also serves two consumers that need aggregates rather than rows:
reports and dashboards for analysts (Power BI / Tableau), and features for machine
learning or forecasting. Those are the tables below, each a Delta table directly
under DELTA_ROOT (so lineage/normalize.py turns it into a (:Table) node fed by gold):

  gold_daily_summary       one row per crash_date: crashes, injury and fatal crashes,
                           injuries, hit and run, weekend flag (trend charts)
  gold_conditions_summary  weather x lighting x road surface: crashes, injury and fatal
                           rates (which conditions are dangerous)
  gold_time_pattern        day of week x hour: crashes and injury rate (heat map)
  gold_cause_summary       primary contributory cause: crashes, share, injury rate
  gold_beat_monthly        police beat x month: counts and shares, the feature table
                           for forecasting crashes per beat

What every table counts:
  - real crashes only: fault-injected copies (is_injected, evaluation stack) are left out
  - each crash once: rows are de-duplicated on crash_record_id (the business key), so a
    record released twice, or a duplicate in the source, is not counted twice
  - rows healed later count where they belong: every refresh recomputes from the whole
    Gold table, so a record the recovery agent releases a day late lands in its own day

How they are kept current: a streaming query on Gold (`gold_metrics`, started by
gold_job.start) wakes every GOLD_METRICS_INTERVAL (60 s) and, when Gold received new
rows, recomputes every table and overwrites it. A Delta overwrite is one commit, so a
dashboard reads the old version or the new one, never half. Gold is small enough for
this (about 1M rows); incremental aggregation would need watermarks that late healed
rows would miss.
At the full dataset's scale that is a full read of Gold plus five overwrites per refresh;
recomputing only the days and beats a batch touched is the next step if it gets slow.
A failed refresh is logged and retried on the next trigger: it never stops the
Silver 2 / Gold queries that share its driver (refresh_logged).

One-off refresh (e.g. after a reset), with the jobs' environment:
    python gold_metrics.py
    docker compose run --rm tools python spark-jobs/gold_metrics.py
"""

import os
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common import build_spark, wait_for_delta_table
from sources import CHECKPOINT_ROOT, shared_path

GOLD_PATH = os.environ.get("GOLD_PATH", shared_path("gold"))
CHECKPOINT_PATH = os.environ.get("GOLD_METRICS_CHECKPOINT_PATH", f"{CHECKPOINT_ROOT}/gold_metrics")
INTERVAL = os.environ.get("GOLD_METRICS_INTERVAL", "60 seconds")

# The columns the aggregates read; nothing else is cached.
_COLUMNS = [
    "crash_record_id", "crash_date", "crash_year", "crash_hour", "crash_day_of_week",
    "is_weekend", "time_of_day", "beat_of_occurrence", "weather_condition",
    "lighting_condition", "roadway_surface_cond", "prim_contributory_cause",
    "posted_speed_limit", "hit_and_run_i", "injuries_total", "injuries_fatal",
    "is_injury_crash", "is_fatal_crash", "is_injected",
]

_COUNTS = """
    COUNT(*) AS crashes,
    SUM(CASE WHEN is_injury_crash THEN 1 ELSE 0 END) AS injury_crashes,
    SUM(CASE WHEN is_fatal_crash THEN 1 ELSE 0 END) AS fatal_crashes,
    COALESCE(SUM(injuries_total), 0) AS injuries_total,
    COALESCE(SUM(injuries_fatal), 0) AS injuries_fatal
"""

_RATES = """
    ROUND(AVG(CASE WHEN is_injury_crash THEN 1.0 ELSE 0.0 END), 4) AS injury_rate,
    ROUND(AVG(CASE WHEN is_fatal_crash THEN 1.0 ELSE 0.0 END), 4) AS fatal_rate
"""

# name -> SQL over the view `gold_crashes` (real crashes, each once).
METRIC_SQL: Dict[str, str] = {
    "gold_daily_summary": f"""
        SELECT crash_date, crash_year, FIRST(is_weekend, TRUE) AS is_weekend,
               {_COUNTS},
               SUM(CASE WHEN hit_and_run_i THEN 1 ELSE 0 END) AS hit_and_run_crashes
        FROM gold_crashes
        WHERE crash_date IS NOT NULL
        GROUP BY crash_date, crash_year
    """,
    "gold_conditions_summary": f"""
        SELECT COALESCE(weather_condition, 'NOT RECORDED') AS weather_condition,
               COALESCE(lighting_condition, 'NOT RECORDED') AS lighting_condition,
               COALESCE(roadway_surface_cond, 'NOT RECORDED') AS roadway_surface_cond,
               {_COUNTS}, {_RATES}
        FROM gold_crashes
        GROUP BY 1, 2, 3
    """,
    "gold_time_pattern": f"""
        SELECT crash_day_of_week, crash_hour, FIRST(time_of_day, TRUE) AS time_of_day,
               {_COUNTS}, {_RATES}
        FROM gold_crashes
        WHERE crash_day_of_week IS NOT NULL AND crash_hour IS NOT NULL
        GROUP BY crash_day_of_week, crash_hour
    """,
    "gold_cause_summary": f"""
        SELECT COALESCE(prim_contributory_cause, 'not recorded') AS prim_contributory_cause,
               {_COUNTS}, {_RATES},
               ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS share_of_crashes
        FROM gold_crashes
        GROUP BY 1
    """,
    "gold_beat_monthly": f"""
        SELECT beat_of_occurrence,
               CAST(DATE_TRUNC('MONTH', crash_date) AS DATE) AS month_start,
               {_COUNTS}, {_RATES},
               ROUND(AVG(CASE WHEN time_of_day = 'NIGHT' THEN 1.0 ELSE 0.0 END), 4) AS night_share,
               ROUND(AVG(CASE WHEN is_weekend THEN 1.0 ELSE 0.0 END), 4) AS weekend_share,
               ROUND(AVG(CASE WHEN weather_condition IN ('RAIN', 'SNOW', 'FOG') THEN 1.0 ELSE 0.0 END), 4)
                   AS adverse_weather_share,
               ROUND(AVG(CASE WHEN hit_and_run_i THEN 1.0 ELSE 0.0 END), 4) AS hit_and_run_share,
               ROUND(AVG(CAST(posted_speed_limit AS DOUBLE)), 1) AS avg_posted_speed_limit
        FROM gold_crashes
        WHERE beat_of_occurrence IS NOT NULL AND crash_date IS NOT NULL
        GROUP BY 1, 2
    """,
}


def enabled() -> bool:
    return os.environ.get("GOLD_METRICS_ENABLED", "true").strip().lower() not in ("0", "false", "no", "off")


def metric_path(name: str) -> str:
    return shared_path(name)


def real_crashes(gold: DataFrame) -> DataFrame:
    """Gold rows the aggregates count: not fault-injected, each crash_record_id once.

    Rows without a crash_record_id cannot be told apart, so they are all kept."""
    cols = [c for c in _COLUMNS if c in gold.columns]
    df = gold.select(*cols)
    if "is_injected" in df.columns:
        df = df.filter(~F.coalesce(F.col("is_injected"), F.lit(False)))
    # Rows written before the derived fields existed have is_injected null: check the key too.
    df = df.filter(~F.coalesce(F.col("crash_record_id"), F.lit("")).startswith("FAULT-"))
    keyed = df.filter(F.col("crash_record_id").isNotNull()).dropDuplicates(["crash_record_id"])
    return keyed.unionByName(df.filter(F.col("crash_record_id").isNull()))


def with_derived(df: DataFrame) -> DataFrame:
    """Fill the derived fields on rows written before gold_job added them (null there)."""
    hour = F.col("crash_hour")
    derived = {
        "crash_year": F.year("crash_date"),
        "is_weekend": F.col("crash_day_of_week").isin("SATURDAY", "SUNDAY"),
        "time_of_day": F.when(hour.isNull(), F.lit(None))
        .when(hour.between(6, 11), "MORNING").when(hour.between(12, 16), "AFTERNOON")
        .when(hour.between(17, 20), "EVENING").otherwise("NIGHT"),
        "is_injury_crash": F.col("injuries_total") > 0,
        "is_fatal_crash": F.col("injuries_fatal") > 0,
    }
    for name, expr in derived.items():
        df = df.withColumn(name, F.coalesce(F.col(name), expr) if name in df.columns else expr)
    return df


def compute(spark: SparkSession, gold: DataFrame) -> Dict[str, DataFrame]:
    """The metric tables as DataFrames (nothing written). Used by refresh and the tests."""
    crashes = with_derived(real_crashes(gold))
    crashes.createOrReplaceTempView("gold_crashes")
    return {name: spark.sql(sql) for name, sql in METRIC_SQL.items()}


def _overwrite(table: DataFrame, path: str, attempts: int = 4) -> None:
    """Overwrite one metric table. The streaming refresh and a one-off refresh can
    overwrite the same table at once; Delta then rejects one of them, and since an
    overwrite is complete on its own, the loser simply writes again."""
    # Delta's Python exceptions are siblings, not subclasses of one base: list them.
    from delta.exceptions import (ConcurrentAppendException, ConcurrentDeleteDeleteException,
                                  ConcurrentDeleteReadException, ConcurrentTransactionException,
                                  ConcurrentWriteException, DeltaConcurrentModificationException,
                                  MetadataChangedException, ProtocolChangedException)
    conflicts = (ConcurrentAppendException, ConcurrentDeleteDeleteException, ConcurrentDeleteReadException,
                 ConcurrentTransactionException, ConcurrentWriteException,
                 DeltaConcurrentModificationException, MetadataChangedException, ProtocolChangedException)

    for attempt in range(1, attempts + 1):
        try:
            table.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path)
            return
        except conflicts:
            if attempt == attempts:
                raise
            time.sleep(attempt)


def refresh(spark: SparkSession, gold_path: str = GOLD_PATH) -> Dict[str, int]:
    """Recompute every metric table from the whole Gold table and overwrite it.

    Returns rows written per table. Each table carries the Gold version it was
    computed from and when, so a dashboard can say how fresh it is."""
    from delta.tables import DeltaTable

    version = DeltaTable.forPath(spark, gold_path).history(1).collect()[0]["version"]
    gold = spark.read.format("delta").option("versionAsOf", version).load(gold_path)
    cached = with_derived(real_crashes(gold)).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        cached.createOrReplaceTempView("gold_crashes")
        refreshed_at = datetime.now(timezone.utc)
        written = {}
        for name, sql in METRIC_SQL.items():
            table = (spark.sql(sql)
                     .withColumn("gold_version", F.lit(version).cast("long"))
                     .withColumn("refreshed_at", F.lit(refreshed_at).cast("timestamp")))
            _overwrite(table, metric_path(name))
            written[name] = spark.read.format("delta").load(metric_path(name)).count()
        return written
    finally:
        cached.unpersist()


# Consecutive failed refreshes, for the log.
_failures = {"count": 0}


def refresh_logged(spark: SparkSession, gold_path: str, batch_id: int, refresh_fn=None) -> bool:
    """One refresh from the streaming query. An error is logged and swallowed: the query
    runs in the silver2-gold-job driver, where an exception escaping foreachBatch would
    stop the query, `awaitAnyTermination()` would end the driver, and a persistent error
    (a full disk, an unreadable table) would crash-loop Silver 2 and Gold ingestion over
    a reporting table. The next trigger with new Gold rows simply tries again, and
    the stale tables still carry the gold_version and refreshed_at they were built from."""
    try:
        written = (refresh_fn or refresh)(spark, gold_path)
    except Exception as exc:  # noqa: BLE001 - reporting must never stop ingestion
        _failures["count"] += 1
        print(f"[gold_metrics] batch {batch_id}: refresh FAILED ({_failures['count']} in a row), "
              f"metric tables left as they were: {type(exc).__name__}: {exc}", flush=True)
        return False
    if _failures["count"]:
        print(f"[gold_metrics] recovered after {_failures['count']} failed refreshes", flush=True)
    _failures["count"] = 0
    print(f"[gold_metrics] batch {batch_id}: refreshed "
          + ", ".join(f"{k}={v}" for k, v in written.items()), flush=True)
    return True


def start(spark: SparkSession, gold_path: str = GOLD_PATH,
          upstream: Optional[StreamingQuery] = None) -> StreamingQuery:
    """Refresh the metric tables whenever Gold received rows, at most every INTERVAL."""
    wait_for_delta_table(gold_path, upstream=upstream)

    def _on_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.isEmpty():
            return
        refresh_logged(batch_df.sparkSession, gold_path, batch_id)

    query = (
        spark.readStream.format("delta").load(gold_path)
        .select("crash_record_id")  # only to notice new rows; refresh reads Gold itself
        .writeStream.queryName("gold_metrics")
        .trigger(processingTime=INTERVAL)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(_on_batch)
        .start()
    )
    print(f"[gold_metrics] Refreshing {', '.join(METRIC_SQL)} from {gold_path} every {INTERVAL}")
    return query


def main() -> None:
    spark = build_spark("GoldMetricsRefresh")
    written = refresh(spark)
    for name, rows in written.items():
        print(f"{name}: {rows} rows -> {metric_path(name)}")


if __name__ == "__main__":
    main()
