"""
Gold: business-ready fields, via SQL.

Reads from Silver 2, registers a temp view 'silver2', and runs
GOLD_TRANSFORM_SQL to produce the business-ready Gold Delta table.
"""

import os

from common import build_spark, wait_for_delta_table

SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
GOLD_PATH = os.environ.get("GOLD_PATH", "/data/delta/gold")
CHECKPOINT_PATH = os.environ.get(
    "CHECKPOINT_PATH", "/data/checkpoints/gold"
)

# Business-level transform SQL.
# Currently selecting all standardized fields from Silver 2.
GOLD_TRANSFORM_SQL = """
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
FROM silver2
"""


def main():
    spark = build_spark("GoldCrashTransform")

    wait_for_delta_table(SILVER2_PATH)

    silver2_stream = spark.readStream.format("delta").load(SILVER2_PATH)
    silver2_stream.createOrReplaceTempView("silver2")

    gold = spark.sql(GOLD_TRANSFORM_SQL)

    query = (
        gold.writeStream.format("delta")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(GOLD_PATH)
    )

    print(f"[gold] Streaming {SILVER2_PATH} -> {GOLD_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
