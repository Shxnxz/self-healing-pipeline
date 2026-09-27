"""
Silver 2: Data standardization via pure PySpark Structured Streaming.

Reads from Silver 1's cleaned Delta table, applies canonical lookup mappings
and native date/numeric parsing chains, isolates failed records into a
physically separate Quarantine table, and appends valid records to Silver 2.
"""

import os
from itertools import chain
from typing import Dict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
)

from common import build_spark, wait_for_delta_table
from ref_mappings import init_ref_mappings, load_mappings_dict

SILVER1_PATH = os.environ.get("SILVER1_PATH", "/data/delta/silver1")
SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints/silver2")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/data/delta/silver2_quarantine")
REF_MAPPINGS_PATH = os.environ.get(
    "REF_MAPPINGS_PATH", "/data/delta/ref_canonical_mappings"
)

# Supported additive date format strings
DATE_FORMATS = [
    "yyyy-MM-dd",
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy/MM/dd",
    "yyyy/MM/dd HH:mm:ss",
    "dd/MM/yyyy",
    "MM/dd/yyyy",
    "MM/dd/yyyy HH:mm:ss",
    "yyyy-MM-dd HH:mm:ss",
    "dd-MM-yyyy",
    "yyyyMMdd",
]

# Supported additive timestamp format strings
TIMESTAMP_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ss",
    "yyyy-MM-dd'T'HH:mm:ss.SSS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "MM/dd/yyyy HH:mm:ss",
    "MM/dd/yyyy hh:mm:ss a",
    "MM/dd/yyyy",
    "yyyy/MM/dd HH:mm:ss",
]


def _build_map_expr(field_name: str, mapping_dict: Dict[str, Dict[str, str]]):
    """Create a Catalyst create_map expression for a given categorical field."""
    field_map = mapping_dict.get(field_name, {})
    if not field_map:
        return F.lit(None).cast(StringType())
    return F.create_map([F.lit(x) for x in chain(*field_map.items())])[
        F.upper(F.trim(F.col(field_name)))
    ]


def standardize_silver2(df: DataFrame, mapping_dict: Dict[str, Dict[str, str]]) -> DataFrame:
    """
    Apply native PySpark parsing chains and canonical reference lookups.
    Tracks all mapping and parsing failures in `unmapped_payload`.
    """
    # 1. Native Date Parsing Chain (try_to_timestamp avoids parser policy exceptions)
    parsed_crash_date = F.coalesce(
        *[F.to_date(F.expr(f"try_to_timestamp(crash_date, \"{fmt}\")")) for fmt in DATE_FORMATS]
    )
    std_crash_date = F.date_format(parsed_crash_date, "yyyy-MM-dd")

    # 2. Native Timestamp Parsing Chain for date_police_notified
    parsed_police_ts = F.coalesce(
        *[F.expr(f"try_to_timestamp(date_police_notified, \"{fmt}\")") for fmt in TIMESTAMP_FORMATS]
    )
    std_date_police_notified = F.date_format(parsed_police_ts, "yyyy-MM-dd HH:mm:ss")

    # 3. Categorical Fields via Reference Mapping Table
    std_weather = _build_map_expr("weather_condition", mapping_dict)
    std_lighting = _build_map_expr("lighting_condition", mapping_dict)
    std_damage = _build_map_expr("damage", mapping_dict)
    std_street_direction = _build_map_expr("street_direction", mapping_dict)
    std_hit_and_run = _build_map_expr("hit_and_run_i", mapping_dict)

    # 4. Numeric & Time Parsing
    # crash_hour: ref lookup, 12h/24h timestamp parse, or direct integer cast
    hour_from_ref = _build_map_expr("crash_hour", mapping_dict)
    hour_from_ts = F.hour(
        F.coalesce(
            F.expr("try_to_timestamp(crash_hour, 'h a')"),
            F.expr("try_to_timestamp(crash_hour, 'hh a')"),
            F.expr("try_to_timestamp(crash_hour, 'HH:mm')"),
            F.expr("try_to_timestamp(crash_hour, 'h:mm a')"),
        )
    )
    hour_from_cast = F.expr("try_cast(crash_hour as int)")
    candidate_hour = F.coalesce(hour_from_ref.cast(IntegerType()), hour_from_ts, hour_from_cast)
    std_crash_hour = F.when(candidate_hour.between(0, 23), candidate_hour).otherwise(None)

    # crash_month: ref lookup, month name timestamp parse, or direct integer cast
    month_from_ref = _build_map_expr("crash_month", mapping_dict)
    month_from_ts = F.month(
        F.coalesce(
            F.expr("try_to_timestamp(crash_month, 'MMMM')"),
            F.expr("try_to_timestamp(crash_month, 'MMM')"),
        )
    )
    month_from_cast = F.expr("try_cast(crash_month as int)")
    candidate_month = F.coalesce(month_from_ref.cast(IntegerType()), month_from_ts, month_from_cast)
    std_crash_month = F.when(candidate_month.between(1, 12), candidate_month).otherwise(None)

    # crash_day_of_week: ref lookup or direct integer cast
    day_from_ref = _build_map_expr("crash_day_of_week", mapping_dict)
    day_from_cast = F.expr("try_cast(crash_day_of_week as int)")
    candidate_day = F.coalesce(day_from_ref.cast(IntegerType()), day_from_cast)
    std_crash_day_of_week = F.when(candidate_day.between(1, 7), candidate_day).otherwise(None)

    # num_units: ref lookup for words ('one' -> 1.0) or regex numeric extract
    units_from_ref = _build_map_expr("num_units", mapping_dict)
    units_from_regex = F.expr("try_cast(regexp_extract(num_units, '(\\\\d+(\\\\.\\\\d+)?)', 1) as decimal(10,1))")
    std_num_units = F.coalesce(units_from_ref.cast(DecimalType(10, 1)), units_from_regex)

    # posted_speed_limit: regex numeric extract
    std_posted_speed_limit = F.expr("try_cast(regexp_extract(posted_speed_limit, '(\\\\d+)', 1) as int)")

    # 5. Multi-field Failure Tracking into unmapped_payload
    # Format: { field_name: (raw_col, std_col, is_mandatory) }
    tracked_fields = {
        "crash_date": (F.col("crash_date"), std_crash_date, True),
        "weather_condition": (F.col("weather_condition"), std_weather, False),
        "lighting_condition": (F.col("lighting_condition"), std_lighting, False),
        "hit_and_run_i": (F.col("hit_and_run_i"), std_hit_and_run, False),
        "damage": (F.col("damage"), std_damage, False),
        "street_direction": (F.col("street_direction"), std_street_direction, False),
        "num_units": (F.col("num_units"), std_num_units, False),
        "crash_month": (F.col("crash_month"), std_crash_month, False),
        "crash_hour": (F.col("crash_hour"), std_crash_hour, False),
        "crash_day_of_week": (F.col("crash_day_of_week"), std_crash_day_of_week, False),
        "date_police_notified": (F.col("date_police_notified"), std_date_police_notified, False),
        "posted_speed_limit": (F.col("posted_speed_limit"), std_posted_speed_limit, False),
    }

    failure_structs = []
    for name, (raw_col, std_col, mandatory) in tracked_fields.items():
        if mandatory:
            # Fatal if standardized value is null
            cond = std_col.isNull()
            val = F.coalesce(raw_col, F.lit("MISSING_MANDATORY_VALUE"))
        else:
            # Failure if raw input existed but could not be parsed/mapped
            cond = raw_col.isNotNull() & (F.trim(raw_col) != "") & std_col.isNull()
            val = raw_col.cast(StringType())

        failure_structs.append(
            F.when(cond, F.struct(F.lit(name).alias("key"), val.alias("val"))).otherwise(None)
        )

    unmapped_payload = F.map_from_entries(F.array_compact(F.array(*failure_structs)))

    # 6. Project standardized columns (exact 55 Silver columns + unmapped_payload)
    return df.select(
        F.col("record_id"),
        F.col("batch_id"),
        F.col("table_name"),
        F.col("ingestion_ts"),
        F.col("topic"),
        F.col("kafka_timestamp"),
        F.col("crash_record_id"),
        std_crash_date.alias("crash_date"),
        std_posted_speed_limit.alias("posted_speed_limit"),
        F.col("traffic_control_device"),
        F.col("device_condition"),
        std_weather.alias("weather_condition"),
        std_lighting.alias("lighting_condition"),
        F.col("first_crash_type"),
        F.col("trafficway_type"),
        F.col("alignment"),
        F.col("roadway_surface_cond"),
        F.col("road_defect"),
        F.col("report_type"),
        F.col("crash_type"),
        F.col("private_property_i"),
        std_hit_and_run.alias("hit_and_run_i"),
        std_damage.alias("damage"),
        std_date_police_notified.alias("date_police_notified"),
        F.col("prim_contributory_cause"),
        F.col("sec_contributory_cause"),
        F.col("street_no"),
        std_street_direction.alias("street_direction"),
        F.col("street_name"),
        F.col("beat_of_occurrence"),
        std_num_units.alias("num_units"),
        std_crash_month.alias("crash_month"),
        F.col("most_severe_injury"),
        F.col("injuries_total"),
        F.col("injuries_fatal"),
        F.col("injuries_incapacitating"),
        F.col("injuries_non_incapacitating"),
        F.col("injuries_reported_not_evident"),
        F.col("injuries_no_indication"),
        F.col("injuries_unknown"),
        std_crash_hour.alias("crash_hour"),
        std_crash_day_of_week.alias("crash_day_of_week"),
        F.col("idot_control_no"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("location"),
        F.col("intersection_related_i"),
        F.col("crash_date_est_i"),
        F.col("photos_taken_i"),
        F.col("statements_taken_i"),
        F.col("work_zone_i"),
        F.col("work_zone_type"),
        F.col("lane_cnt"),
        F.col("workers_present_i"),
        F.col("dooring_i"),
        unmapped_payload.alias("unmapped_payload"),
    )


def make_batch_processor(spark: SparkSession):
    """Returns the foreachBatch function with access to spark session."""

    def process_micro_batch(batch_df: DataFrame, batch_id: int):
        # 1. Reload the latest committed canonical mappings per micro-batch
        mapping_dict = load_mappings_dict(spark, REF_MAPPINGS_PATH)

        # 2. Apply transformations and capture failed fields
        transformed = standardize_silver2(batch_df, mapping_dict)

        # 3. Persist to avoid double evaluation across valid and quarantine splits
        transformed.persist()

        try:
            valid_df = transformed.filter(F.size(F.col("unmapped_payload")) == 0).drop(
                "unmapped_payload"
            )
            quarantine_df = transformed.filter(F.size(F.col("unmapped_payload")) > 0)

            # Append clean rows to Silver 2
            valid_df.write.format("delta").mode("append").save(SILVER2_PATH)

            # Append quarantined rows with lineage metadata if any exist
            if quarantine_df.head(1):
                quarantine_out = (
                    quarantine_df.withColumn("quarantined_at", F.current_timestamp())
                    .withColumn("silver2_batch_id", F.lit(str(batch_id)))
                    .withColumn("healing_status", F.lit("PENDING"))
                )
                quarantine_out.write.format("delta").mode("append").save(QUARANTINE_PATH)
                print(
                    f"[silver2] Batch {batch_id}: captured quarantined records at {QUARANTINE_PATH}"
                )
        finally:
            transformed.unpersist()

    return process_micro_batch


def main():
    spark = build_spark("Silver2StandardizeTransform")

    # Initialize reference canonical mappings if table does not exist
    init_ref_mappings(spark, REF_MAPPINGS_PATH)

    # Wait for upstream Silver 1 Delta table
    wait_for_delta_table(SILVER1_PATH)

    silver1_stream = spark.readStream.format("delta").load(SILVER1_PATH)

    batch_processor = make_batch_processor(spark)

    query = (
        silver1_stream.writeStream.format("delta")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(batch_processor)
        .start()
    )

    print(
        f"[silver2] Streaming {SILVER1_PATH} -> {SILVER2_PATH} (Quarantine: {QUARANTINE_PATH})"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
