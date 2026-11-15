"""
Silver 1: Parse JSON envelope + Initial formatting & cleaning via SQL.

Reads raw JSON strings from Bronze, unpacks the envelope and payload schema,
and runs SILVER1_TRANSFORM_SQL over it, which can be modified for formatting and
cleaning logic. One query per source: bronze_crash3 -> silver1_crash3.
"""

import os
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import LongType, StringType, StructField, StructType

from common import build_spark, idempotent_append, wait_for_delta_table
from schema_guard import process_source_batch
from sources import checkpoint_path, shared_path, source_names, table_name, table_path

# Shared by every source; each parked row records its source table.
DEAD_LETTER_PATH = os.environ.get("SILVER1_DEAD_LETTER_PATH", shared_path("silver1_dead_letter"))

# Base for the idempotent-write ids; the source is appended per query (see
# common.idempotent_append). If you ever wipe the checkpoints, change this value.
SILVER1_TXN_APP_ID = os.getenv("SILVER1_TXN_APP_ID", "silver1")

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
# `{bronze_flat}` is the parsed batch, passed to spark.sql() as a DataFrame rather
# than a temp view: temp views are shared by the whole Spark session, so the
# per-source queries running in parallel would overwrite each other's view.
SILVER1_TRANSFORM_SQL = """
SELECT
    record_id,
    batch_id,
    table_name,
    ingestion_ts,
    topic,
    kafka_timestamp,
    NULLIF(TRIM(crash_record_id), '') AS crash_record_id,
    NULLIF(TRIM(crash_date), '') AS crash_date,
    NULLIF(TRIM(posted_speed_limit), '') AS posted_speed_limit,
    NULLIF(UPPER(TRIM(traffic_control_device)), '') AS traffic_control_device,
    NULLIF(UPPER(TRIM(device_condition)), '') AS device_condition,
    NULLIF(UPPER(TRIM(weather_condition)), '') AS weather_condition,
    NULLIF(UPPER(TRIM(lighting_condition)), '') AS lighting_condition,
    NULLIF(UPPER(TRIM(first_crash_type)), '') AS first_crash_type,
    NULLIF(UPPER(TRIM(trafficway_type)), '') AS trafficway_type,
    NULLIF(UPPER(TRIM(alignment)), '') AS alignment,
    NULLIF(UPPER(TRIM(roadway_surface_cond)), '') AS roadway_surface_cond,
    NULLIF(UPPER(TRIM(road_defect)), '') AS road_defect,
    NULLIF(UPPER(TRIM(report_type)), '') AS report_type,
    NULLIF(UPPER(TRIM(crash_type)), '') AS crash_type,
    NULLIF(UPPER(TRIM(private_property_i)), '') AS private_property_i,
    NULLIF(UPPER(TRIM(hit_and_run_i)), '') AS hit_and_run_i,
    NULLIF(UPPER(TRIM(damage)), '') AS damage,
    NULLIF(TRIM(date_police_notified), '') AS date_police_notified,
    NULLIF(LOWER(TRIM(prim_contributory_cause)), '') AS prim_contributory_cause,
    NULLIF(LOWER(TRIM(sec_contributory_cause)), '') AS sec_contributory_cause,
    NULLIF(TRIM(street_no), '') AS street_no,
    NULLIF(UPPER(TRIM(street_direction)), '') AS street_direction,
    NULLIF(INITCAP(TRIM(street_name)), '') AS street_name,
    NULLIF(TRIM(beat_of_occurrence), '') AS beat_of_occurrence,
    NULLIF(TRIM(num_units), '') AS num_units,
    NULLIF(TRIM(crash_month), '') AS crash_month,
    NULLIF(UPPER(TRIM(most_severe_injury)), '') AS most_severe_injury,
    NULLIF(TRIM(injuries_total), '') AS injuries_total,
    NULLIF(TRIM(injuries_fatal), '') AS injuries_fatal,
    NULLIF(TRIM(injuries_incapacitating), '') AS injuries_incapacitating,
    NULLIF(TRIM(injuries_non_incapacitating), '') AS injuries_non_incapacitating,
    NULLIF(TRIM(injuries_reported_not_evident), '') AS injuries_reported_not_evident,
    NULLIF(TRIM(injuries_no_indication), '') AS injuries_no_indication,
    NULLIF(TRIM(injuries_unknown), '') AS injuries_unknown,
    NULLIF(TRIM(crash_hour), '') AS crash_hour,
    NULLIF(TRIM(crash_day_of_week), '') AS crash_day_of_week,
    NULLIF(TRIM(idot_control_no), '') AS idot_control_no,
    NULLIF(TRIM(latitude), '') AS latitude,
    NULLIF(TRIM(longitude), '') AS longitude,
    NULLIF(TRIM(location), '') AS location,
    NULLIF(UPPER(TRIM(intersection_related_i)), '') AS intersection_related_i,
    NULLIF(UPPER(TRIM(crash_date_est_i)), '') AS crash_date_est_i,
    NULLIF(UPPER(TRIM(photos_taken_i)), '') AS photos_taken_i,
    NULLIF(UPPER(TRIM(statements_taken_i)), '') AS statements_taken_i,
    NULLIF(UPPER(TRIM(work_zone_i)), '') AS work_zone_i,
    NULLIF(UPPER(TRIM(work_zone_type)), '') AS work_zone_type,
    NULLIF(TRIM(lane_cnt), '') AS lane_cnt,
    NULLIF(UPPER(TRIM(workers_present_i)), '') AS workers_present_i,
    NULLIF(UPPER(TRIM(dooring_i)), '') AS dooring_i
FROM {bronze_flat}
"""


def clean_batch(batch_df):
    """Bronze rows (raw_value, topic, kafka_timestamp) -> Silver 1 rows, nothing written.

    Also used by tools/fault_injector.py to predict what Silver 1 makes of a record.
    """
    spark = batch_df.sparkSession
    flattened = batch_df.select(
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

    return spark.sql(SILVER1_TRANSFORM_SQL, bronze_flat=flattened)


def write_clean_batch(batch_df, batch_id, silver1_path, txn_app_id):
    """Run the existing cleanup only after source schema validation."""
    idempotent_append(clean_batch(batch_df), silver1_path, txn_app_id + "_clean", batch_id)


def start(
    spark: SparkSession, source: str, upstream: Optional[StreamingQuery] = None
) -> StreamingQuery:
    """Start silver1_<source>: bronze_<source> -> silver1_<source>, bad schemas -> shared dead letter."""
    bronze_path = table_path("bronze", source)
    silver1_path = table_path("silver1", source)
    txn_app_id = f"{SILVER1_TXN_APP_ID}_{source}"
    wait_for_delta_table(bronze_path, upstream=upstream)
    bronze_stream = spark.readStream.format("delta").load(bronze_path)

    def write_clean(batch, batch_id):
        write_clean_batch(batch, batch_id, silver1_path, txn_app_id)

    def process(batch, batch_id):
        process_source_batch(batch, batch_id, CRASH_COLUMNS, write_clean, DEAD_LETTER_PATH,
                             txn_app_id=txn_app_id,
                             affected_dataset=table_name("bronze", source))

    query = (bronze_stream.writeStream.queryName(f"silver1_{source}")
             .trigger(processingTime="10 seconds")
             .option("checkpointLocation", checkpoint_path("silver1", source))
             .foreachBatch(process).start())
    print(f"[silver1_{source}] Validating {bronze_path} -> {silver1_path}", flush=True)
    return query


def main():
    spark = build_spark("Silver1CleanTransform")
    for source in source_names():
        start(spark, source)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
