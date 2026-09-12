"""
Silver 2: Data standardization via SQL.

Reads from Silver 1's cleaned Delta table, registers a temp view 'silver1',
and runs SILVER2_TRANSFORM_SQL for data standardization.
"""

import os

from common import build_spark, wait_for_delta_table

SILVER1_PATH = os.environ.get("SILVER1_PATH", "/data/delta/silver1")
SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
CHECKPOINT_PATH = os.environ.get(
    "CHECKPOINT_PATH", "/data/checkpoints/silver2"
)

# SQL for Silver 2 (Data Standardization).
# Modify this query to apply standardization rules across sources.
SILVER2_TRANSFORM_SQL = """
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
FROM silver1
"""


def main():
    spark = build_spark("Silver2StandardizeTransform")

    wait_for_delta_table(SILVER1_PATH)

    silver1_stream = spark.readStream.format("delta").load(SILVER1_PATH)
    silver1_stream.createOrReplaceTempView("silver1")

    silver2 = spark.sql(SILVER2_TRANSFORM_SQL)

    query = (
        silver2.writeStream.format("delta")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(SILVER2_PATH)
    )

    print(f"[silver2] Streaming {SILVER1_PATH} -> {SILVER2_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
