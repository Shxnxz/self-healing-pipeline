"""
Silver 1: Parse JSON envelope + Initial formatting & cleaning via SQL.

Reads raw JSON strings from Bronze, unpacks the envelope and payload schema,
and registers a temp view 'bronze_flat'. Runs SILVER1_TRANSFORM_SQL which can
be modified for formatting and cleaning logic.
"""

import os

from pyspark.sql.functions import col, coalesce, from_json
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common import build_spark, wait_for_delta_table

BRONZE_PATH = os.environ.get("BRONZE_PATH", "/data/delta/bronze")
SILVER1_PATH = os.environ.get("SILVER1_PATH", "/data/delta/silver1")
CHECKPOINT_PATH = os.environ.get(
    "CHECKPOINT_PATH", "/data/checkpoints/silver1"
)

CRASH_COLUMNS = [
    "crash_record_id",
    "crash_date",
    "posted_speed_limit",
    "traffic_control_device",
    "device_condition",
    "weather_condition",
    "lighting_condition",
    "first_crash_type",
    "trafficway_type",
    "alignment",
    "roadway_surface_cond",
    "road_defect",
    "report_type",
    "crash_type",
    "private_property_i",
    "hit_and_run_i",
    "damage",
    "date_police_notified",
    "prim_contributory_cause",
    "sec_contributory_cause",
    "street_no",
    "street_direction",
    "street_name",
    "beat_of_occurrence",
    "num_units",
    "crash_month",
    "most_severe_injury",
    "injuries_total",
    "injuries_fatal",
    "injuries_incapacitating",
    "injuries_non_incapacitating",
    "injuries_reported_not_evident",
    "injuries_no_indication",
    "injuries_unknown",
    "crash_hour",
    "crash_day_of_week",
    "idot_control_no",
    "latitude",
    "longitude",
    "location",
    "intersection_related_i",
    "crash_date_est_i",
    "photos_taken_i",
    "statements_taken_i",
    "work_zone_i",
    "work_zone_type",
    "lane_cnt",
    "workers_present_i",
    "dooring_i",
]

PAYLOAD_SCHEMA = StructType([StructField(c, StringType(), True) for c in CRASH_COLUMNS])

ENVELOPE_SCHEMA = StructType(
    [
        StructField("record_id", StringType(), True),
        StructField("batch_id", LongType(), True),
        StructField("table_name", StringType(), True),
        StructField("ingestion_ts", StringType(), True),
        StructField("payload", PAYLOAD_SCHEMA, True),
    ]
    + [StructField(c, StringType(), True) for c in CRASH_COLUMNS]
)

# SQL for Silver 1 (Formatting & Cleaning).
# Modify this query to apply any custom type casts, trims, or cleanups.
SILVER1_TRANSFORM_SQL = """
SELECT
    record_id,
    batch_id,
    table_name,
    ingestion_ts,
    topic,
    kafka_timestamp,
    crash_record_id,
    crash_date,
    posted_speed_limit,
    traffic_control_device,
    device_condition,
    weather_condition,
    lighting_condition,
    first_crash_type,
    trafficway_type,
    alignment,
    roadway_surface_cond,
    road_defect,
    report_type,
    crash_type,
    private_property_i,
    hit_and_run_i,
    damage,
    date_police_notified,
    prim_contributory_cause,
    sec_contributory_cause,
    street_no,
    street_direction,
    street_name,
    beat_of_occurrence,
    num_units,
    crash_month,
    most_severe_injury,
    injuries_total,
    injuries_fatal,
    injuries_incapacitating,
    injuries_non_incapacitating,
    injuries_reported_not_evident,
    injuries_no_indication,
    injuries_unknown,
    crash_hour,
    crash_day_of_week,
    idot_control_no,
    latitude,
    longitude,
    location,
    intersection_related_i,
    crash_date_est_i,
    photos_taken_i,
    statements_taken_i,
    work_zone_i,
    work_zone_type,
    lane_cnt,
    workers_present_i,
    dooring_i
FROM bronze_flat
"""


def main():
    spark = build_spark("Silver1CleanTransform")

    wait_for_delta_table(BRONZE_PATH)

    bronze_stream = spark.readStream.format("delta").load(BRONZE_PATH)

    flattened = bronze_stream.select(
        col("topic"),
        col("kafka_timestamp"),
        from_json(col("raw_value"), ENVELOPE_SCHEMA).alias("env"),
    ).select(
        col("topic"),
        col("kafka_timestamp"),
        col("env.record_id").alias("record_id"),
        col("env.batch_id").alias("batch_id"),
        col("env.table_name").alias("table_name"),
        col("env.ingestion_ts").alias("ingestion_ts"),
        *[
            coalesce(col(f"env.payload.{c}"), col(f"env.{c}")).alias(c)
            for c in CRASH_COLUMNS
        ],
    )

    flattened.createOrReplaceTempView("bronze_flat")
    silver1 = spark.sql(SILVER1_TRANSFORM_SQL)

    query = (
        silver1.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(SILVER1_PATH)
    )

    print(f"[silver1] Streaming {BRONZE_PATH} -> {SILVER1_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
