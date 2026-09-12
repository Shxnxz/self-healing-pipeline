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
    TRIM(crash_record_id) AS crash_record_id,
    TO_TIMESTAMP(TRIM(crash_date), 'yyyy-MM-dd HH:mm:ss') AS crash_date,
    CAST(TRIM(posted_speed_limit) AS INT) AS posted_speed_limit,
    UPPER(TRIM(traffic_control_device)) AS traffic_control_device,
    UPPER(TRIM(device_condition)) AS device_condition,
    UPPER(TRIM(weather_condition)) AS weather_condition,
    UPPER(TRIM(lighting_condition)) AS lighting_condition,
    UPPER(TRIM(first_crash_type)) AS first_crash_type,
    UPPER(TRIM(trafficway_type)) AS trafficway_type,
    UPPER(TRIM(alignment)) AS alignment,
    UPPER(TRIM(roadway_surface_cond)) AS roadway_surface_cond,
    UPPER(TRIM(road_defect)) AS road_defect,
    UPPER(TRIM(report_type)) AS report_type,
    UPPER(TRIM(crash_type)) AS crash_type,
    UPPER(TRIM(private_property_i)) AS private_property_i,
    UPPER(TRIM(hit_and_run_i)) AS hit_and_run_i,
    UPPER(TRIM(damage)) AS damage,
    TO_TIMESTAMP(TRIM(date_police_notified), 'yyyy-MM-dd HH:mm:ss') AS date_police_notified,
    CONCAT(
        UPPER(SUBSTR(TRIM(prim_contributory_cause), 1, 1)),
        LOWER(SUBSTR(TRIM(prim_contributory_cause), 2))
    ) AS prim_contributory_cause,
    CONCAT(
        UPPER(SUBSTR(TRIM(sec_contributory_cause), 1, 1)),
        LOWER(SUBSTR(TRIM(sec_contributory_cause), 2))
    ) AS sec_contributory_cause,
    CAST(TRIM(street_no) AS INT) AS street_no,
    UPPER(TRIM(street_direction)) AS street_direction,
    INITCAP(TRIM(street_name)) AS street_name,
    CAST(TRIM(beat_of_occurrence) AS INT) AS beat_of_occurrence,
    CAST(TRIM(num_units) AS INT) AS num_units,
    CAST(TRIM(crash_month) AS INT) AS crash_month,
    UPPER(TRIM(most_severe_injury)) AS most_severe_injury,
    CAST(TRIM(injuries_total) AS INT) AS injuries_total,
    CAST(TRIM(injuries_fatal) AS INT) AS injuries_fatal,
    CAST(TRIM(injuries_incapacitating) AS INT) AS injuries_incapacitating,
    CAST(TRIM(injuries_non_incapacitating) AS INT) AS injuries_non_incapacitating,
    CAST(TRIM(injuries_reported_not_evident) AS INT) AS injuries_reported_not_evident,
    CAST(TRIM(injuries_no_indication) AS INT) AS injuries_no_indication,
    CAST(TRIM(injuries_unknown) AS INT) AS injuries_unknown,
    CAST(TRIM(crash_hour) AS INT) AS crash_hour,
    CAST(TRIM(crash_day_of_week) AS INT) AS crash_day_of_week,
    TRIM(idot_control_no) AS idot_control_no,
    CAST(TRIM(latitude) AS DOUBLE) AS latitude,
    CAST(TRIM(longitude) AS DOUBLE) AS longitude,
    TRIM(location) AS location,
    UPPER(TRIM(intersection_related_i)) AS intersection_related_i,
    UPPER(TRIM(crash_date_est_i)) AS crash_date_est_i,
    UPPER(TRIM(photos_taken_i)) AS photos_taken_i,
    UPPER(TRIM(statements_taken_i)) AS statements_taken_i,
    UPPER(TRIM(work_zone_i)) AS work_zone_i,
    UPPER(TRIM(work_zone_type)) AS work_zone_type,
    CAST(TRIM(lane_cnt) AS INT) AS lane_cnt,
    UPPER(TRIM(workers_present_i)) AS workers_present_i,
    UPPER(TRIM(dooring_i)) AS dooring_i
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
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(SILVER1_PATH)
    )

    print(f"[silver1] Streaming {BRONZE_PATH} -> {SILVER1_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
