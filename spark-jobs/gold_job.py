"""
Gold: business-ready fields, via SQL, over every source.

Gold is the UNION ALL of the per-source Silver 2 tables (sources.py):
silver2_crash1 .. silver2_crash4 -> gold, with `source_table` saying which
branch each row came from. One streaming query reads all of them, so Gold has
one checkpoint holding an offset per source.

Adding or removing a source changes the query's set of inputs, and Spark refuses
to resume a checkpoint whose inputs changed: reset GOLD's checkpoint (and the
Gold table) when SOURCES changes.
"""

import os
from typing import Optional

from functools import reduce
from typing import Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery

from common import build_spark, wait_for_delta_table
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
GOLD_TRANSFORM_SQL = """
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
"""


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

    query = (
        gold.writeStream.format("delta")
        .queryName("gold")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(GOLD_PATH)
    )

    inputs = ", ".join(table_name("silver2", s) for s in sources)
    print(f"[gold] Streaming UNION ALL of {inputs} -> {GOLD_PATH}")
    return query


def main():
    start(build_spark("GoldCrashTransform")).awaitTermination()


if __name__ == "__main__":
    main()
