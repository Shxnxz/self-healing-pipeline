"""
Gold: business-ready fields, via SQL, over every source.

Gold is the UNION ALL of the per-source Silver 2 tables (sources.py):
silver2_crash1 .. silver2_crash4 -> gold, with `source_table` saying which
branch each row came from. One streaming query reads all of them, so Gold has
one checkpoint holding an offset per source.

Adding or removing a source changes the query's set of inputs, and Spark refuses
to resume a checkpoint whose inputs changed: reset GOLD's checkpoint (and the
Gold table) when SOURCES changes.

Each Gold row also carries a few derived fields (crash_year, is_weekend,
time_of_day, is_injury_crash, is_fatal_crash, is_injected) for reports and ML
features. They were added after the first Gold tables were written: start() adds
them to an existing Gold table before the query starts (older rows read null there;
gold_metrics derives them again for those rows), so no reset is needed.

The business aggregates built on Gold (gold_metrics.py: daily summary, conditions,
time pattern, causes, beat-month features) are refreshed by a second query started
here, after Gold exists. GOLD_METRICS_ENABLED=false leaves them out.
"""

import os

from functools import reduce
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

import gold_metrics
from common import build_spark, has_delta_commit, wait_for_delta_table
from sources import CHECKPOINT_ROOT, shared_path, source_names, table_name, table_path

GOLD_PATH = os.environ.get("GOLD_PATH", shared_path("gold"))
CHECKPOINT_PATH = os.environ.get("GOLD_CHECKPOINT_PATH", f"{CHECKPOINT_ROOT}/gold")

# Business-level transform SQL: the columns an analyst reports on, typed for
# aggregation (Silver keeps counts/coordinates as strings).
#
# Deliberately left out (still available in Silver 2):
#   - pipeline metadata: batch_id, table_name, ingestion_ts, topic, kafka_timestamp
#     (source_table is kept: it names the Silver 2 branch, which lineage and the
#     blast-radius query use to tell which Gold rows a source incident touches)
#   - admin / redundant: idot_control_no, report_type, date_police_notified,
#     street_no, location (same point as latitude/longitude)
#   - rarely populated (<2% of rows, or always 0): crash_date_est_i, photos_taken_i,
#     statements_taken_i, work_zone_i, work_zone_type, workers_present_i, dooring_i,
#     lane_cnt, injuries_unknown
#
# crash_month / crash_day_of_week are kept even though they look derivable from
# crash_date: they come straight from the source, while crash_date is re-parsed in
# Silver 2 and can disagree with them.
#
# Derived fields (outer SELECT), from the typed columns:
#   crash_year       year of crash_date
#   is_weekend       crash_day_of_week is SATURDAY or SUNDAY
#   time_of_day      MORNING 06-11, AFTERNOON 12-16, EVENING 17-20, NIGHT 21-05 (crash_hour)
#   is_injury_crash  at least one injury (injuries_total > 0)
#   is_fatal_crash   at least one fatality (injuries_fatal > 0)
#   is_injected      a fault-injected copy (crash_record_id FAULT-..., evaluation stack
#                    only, tools/README.md); every aggregate in gold_metrics.py leaves
#                    them out
GOLD_TRANSFORM_SQL = """
SELECT
    typed.*,
    YEAR(crash_date) AS crash_year,
    crash_day_of_week IN ('SATURDAY', 'SUNDAY') AS is_weekend,
    CASE
        WHEN crash_hour IS NULL THEN NULL
        WHEN crash_hour BETWEEN 6 AND 11 THEN 'MORNING'
        WHEN crash_hour BETWEEN 12 AND 16 THEN 'AFTERNOON'
        WHEN crash_hour BETWEEN 17 AND 20 THEN 'EVENING'
        ELSE 'NIGHT'
    END AS time_of_day,
    injuries_total > 0 AS is_injury_crash,
    injuries_fatal > 0 AS is_fatal_crash,
    COALESCE(crash_record_id LIKE 'FAULT-%', FALSE) AS is_injected
FROM (
SELECT
    -- Keys: crash_record_id is the business key; record_id traces back through Silver/Bronze
    crash_record_id,
    record_id,
    source_table,

    -- When
    CAST(crash_date AS DATE) AS crash_date,
    CAST(CAST(crash_hour AS DOUBLE) AS INT) AS crash_hour,
    crash_day_of_week,
    crash_month,

    -- Where
    beat_of_occurrence,
    street_direction,
    street_name,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,

    -- Road & environment
    posted_speed_limit,
    traffic_control_device,
    device_condition,
    weather_condition,
    lighting_condition,
    roadway_surface_cond,
    road_defect,
    trafficway_type,
    alignment,
    intersection_related_i,

    -- Crash
    crash_type,
    first_crash_type,
    prim_contributory_cause,
    sec_contributory_cause,
    CAST(num_units AS INT) AS num_units,
    hit_and_run_i,
    private_property_i,
    damage,

    -- Severity
    most_severe_injury,
    CAST(CAST(injuries_total AS DOUBLE) AS INT) AS injuries_total,
    CAST(CAST(injuries_fatal AS DOUBLE) AS INT) AS injuries_fatal,
    CAST(CAST(injuries_incapacitating AS DOUBLE) AS INT) AS injuries_incapacitating,
    CAST(CAST(injuries_non_incapacitating AS DOUBLE) AS INT) AS injuries_non_incapacitating,
    CAST(CAST(injuries_reported_not_evident AS DOUBLE) AS INT) AS injuries_reported_not_evident,
    CAST(CAST(injuries_no_indication AS DOUBLE) AS INT) AS injuries_no_indication
FROM silver2_all
) AS typed
"""


# The derived fields, as added to a Gold table written before they existed.
DERIVED_COLUMNS = {
    "crash_year": "INT",
    "is_weekend": "BOOLEAN",
    "time_of_day": "STRING",
    "is_injury_crash": "BOOLEAN",
    "is_fatal_crash": "BOOLEAN",
    "is_injected": "BOOLEAN",
}


def add_derived_columns(spark: SparkSession, path: str = GOLD_PATH) -> list:
    """Give an existing Gold table the derived columns before any query starts.

    mergeSchema would add them with the first new micro-batch, but a schema change
    under a running stream stops every query reading Gold (gold_metrics), so the
    table is upgraded once, up front. Returns the columns added (none when the table
    is new or already has them)."""
    if not has_delta_commit(path):
        return []
    existing = set(spark.read.format("delta").load(path).columns)
    missing = [(name, kind) for name, kind in DERIVED_COLUMNS.items() if name not in existing]
    if missing:
        cols = ", ".join(f"{name} {kind}" for name, kind in missing)
        spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({cols})")
        print(f"[gold] Added derived columns to {path}: {cols}")
    return [name for name, _ in missing]


def _silver2_stream(spark: SparkSession, source: str) -> DataFrame:
    return (
        spark.readStream.format("delta")
        .load(table_path("silver2", source))
        .withColumn("source_table", F.lit(table_name("silver2", source)))
    )


def start(
    spark: SparkSession,
    sources: Optional[List[str]] = None,
    upstreams: Optional[Dict[str, StreamingQuery]] = None,
) -> StreamingQuery:
    """
    Start the Gold query over every source's Silver 2 table.

    `upstreams` maps a source to its silver2_<source> query when those run in this
    driver, so a dead upstream raises instead of being waited on forever.
    """
    sources = sources or source_names()
    upstreams = upstreams or {}
    for source in sources:
        wait_for_delta_table(table_path("silver2", source), upstream=upstreams.get(source))

    # UNION ALL: the sources hold different crashes (no crash_record_id is in two
    # folders), so no row is dropped or deduplicated here.
    silver2_all = reduce(DataFrame.unionByName, [_silver2_stream(spark, s) for s in sources])
    # Created once, before the query starts, so the shared temp view is not raced.
    silver2_all.createOrReplaceTempView("silver2_all")

    gold = spark.sql(GOLD_TRANSFORM_SQL)
    add_derived_columns(spark, GOLD_PATH)

    query = (
        gold.writeStream.format("delta")
        .queryName("gold")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        # The derived fields were added to an existing table: let Delta add them.
        .option("mergeSchema", "true")
        .outputMode("append")
        .start(GOLD_PATH)
    )

    inputs = ", ".join(table_name("silver2", s) for s in sources)
    print(f"[gold] Streaming UNION ALL of {inputs} -> {GOLD_PATH}")

    if gold_metrics.enabled():
        # Registered in spark.streams, so run_layers' awaitAnyTermination covers it.
        gold_metrics.start(spark, GOLD_PATH, upstream=query)
    return query


def main():
    start(build_spark("GoldCrashTransform")).awaitTermination()


if __name__ == "__main__":
    main()
