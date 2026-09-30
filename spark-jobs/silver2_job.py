"""
Silver 2: Data standardization via pure PySpark Structured Streaming.

Reads from Silver 1's cleaned Delta table, applies canonical lookup mappings
and native date/numeric parsing chains, isolates failed records into a
physically separate Quarantine table, and appends valid records to Silver 2.
"""

import json
import os
import traceback
import urllib.request
from itertools import chain
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
)

from common import build_spark, wait_for_delta_table
from ref_mappings import (
    CANONICAL_ANCHORS,
    fuzzy_canonical,
    init_fuzzy_audit,
    init_ref_mappings,
    load_mappings_dict,
)

SILVER1_PATH = os.environ.get("SILVER1_PATH", "/data/delta/silver1")
SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
CHECKPOINT_PATH = os.environ.get("CHECKPOINT_PATH", "/data/checkpoints/silver2")
QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/data/delta/silver2_quarantine")
DEAD_LETTER_PATH = os.environ.get(
    "DEAD_LETTER_PATH", "/data/delta/silver2_dead_letter"
)
REF_MAPPINGS_PATH = os.environ.get(
    "REF_MAPPINGS_PATH", "/data/delta/ref_canonical_mappings"
)
FUZZY_AUDIT_PATH = os.environ.get(
    "FUZZY_AUDIT_PATH", os.environ.get("AUDIT_PATH", "/data/delta/silver2_fuzzy_audit")
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


def _build_map_expr(
    field_name: str,
    mapping_dict: Dict[str, Dict[str, str]],
    input_col: Optional[Any] = None,
):
    """Create a Catalyst create_map expression for a given categorical field."""
    field_map = mapping_dict.get(field_name, {})
    if not field_map:
        return F.lit(None).cast(StringType())
    c = F.col(field_name) if input_col is None else input_col
    return F.create_map([F.lit(x) for x in chain(*field_map.items())])[
        F.upper(F.trim(c))
    ]


def standardize_silver2(df: DataFrame, mapping_dict: Dict[str, Dict[str, str]]) -> DataFrame:
    """
    Apply native PySpark parsing chains and canonical reference lookups.
    Tracks all mapping and parsing failures in `columns_to_remediate`.
    """
    # Explicit bindings to input DataFrame columns to avoid projection alias shadowing
    raw_crash_date = df["crash_date"]
    raw_date_police_notified = df["date_police_notified"]
    raw_weather = df["weather_condition"]
    raw_lighting = df["lighting_condition"]
    raw_damage = df["damage"]
    raw_hit_and_run = df["hit_and_run_i"]
    raw_street_direction = df["street_direction"]
    raw_crash_hour = df["crash_hour"]
    raw_crash_month = df["crash_month"]
    raw_crash_day = df["crash_day_of_week"]
    raw_num_units = df["num_units"]
    raw_speed_limit = df["posted_speed_limit"]

    # 1. Native Date Parsing Chain (try_to_timestamp avoids parser policy exceptions)
    parsed_crash_date = F.coalesce(
        *[F.to_date(F.expr(f"try_to_timestamp(`crash_date`, \"{fmt}\")")) for fmt in DATE_FORMATS]
    )
    std_crash_date = F.date_format(parsed_crash_date, "yyyy-MM-dd")

    # 2. Native Timestamp Parsing Chain for date_police_notified
    parsed_police_ts = F.coalesce(
        *[F.expr(f"try_to_timestamp(`date_police_notified`, \"{fmt}\")") for fmt in TIMESTAMP_FORMATS]
    )
    std_date_police_notified = F.date_format(parsed_police_ts, "yyyy-MM-dd HH:mm:ss")

    # 3. Categorical Fields via Reference Mapping Table with Fuzzy Canonical Fallback
    # Rule: Runs ONLY after exact lookup fails.
    # Guardrails: length >= 4, distance scaled to length, unique winner margin >= 2,
    # and fixed seed anchors only.

    # weather_condition
    exact_weather = _build_map_expr("weather_condition", mapping_dict, raw_weather)
    fuzzy_weather = fuzzy_canonical(
        raw_weather, CANONICAL_ANCHORS["weather_condition"]
    )
    effective_fuzzy_weather = F.when(exact_weather.isNull(), fuzzy_weather).otherwise(None)
    std_weather = F.coalesce(exact_weather, effective_fuzzy_weather)

    # lighting_condition
    exact_lighting = _build_map_expr("lighting_condition", mapping_dict, raw_lighting)
    fuzzy_lighting = fuzzy_canonical(
        raw_lighting, CANONICAL_ANCHORS["lighting_condition"]
    )
    effective_fuzzy_lighting = F.when(exact_lighting.isNull(), fuzzy_lighting).otherwise(None)
    std_lighting = F.coalesce(exact_lighting, effective_fuzzy_lighting)

    # damage & hit_and_run_i (open/complex categories - no fuzzy canonical matching)
    std_damage = _build_map_expr("damage", mapping_dict, raw_damage)
    std_hit_and_run = _build_map_expr("hit_and_run_i", mapping_dict, raw_hit_and_run)

    # street_direction (fuzzy matches NORTH/SOUTH/EAST/WEST -> maps to N/S/E/W)
    exact_street_direction = _build_map_expr("street_direction", mapping_dict, raw_street_direction)
    fuzzy_dir_anchor = fuzzy_canonical(
        raw_street_direction, CANONICAL_ANCHORS["street_direction"]
    )
    effective_fuzzy_dir = F.when(exact_street_direction.isNull(), fuzzy_dir_anchor).otherwise(None)
    fuzzy_dir_canonical = (
        F.when(effective_fuzzy_dir == "NORTH", "N")
        .when(effective_fuzzy_dir == "SOUTH", "S")
        .when(effective_fuzzy_dir == "EAST", "E")
        .when(effective_fuzzy_dir == "WEST", "W")
        .otherwise(None)
    )
    std_street_direction = F.coalesce(exact_street_direction, fuzzy_dir_canonical)

    # 4. Numeric & Time Parsing
    # crash_hour: ref lookup, 12h/24h timestamp parse, or direct integer cast
    hour_from_ref = _build_map_expr("crash_hour", mapping_dict, raw_crash_hour)
    hour_from_ts = F.hour(
        F.coalesce(
            F.expr("try_to_timestamp(`crash_hour`, 'h a')"),
            F.expr("try_to_timestamp(`crash_hour`, 'hh a')"),
            F.expr("try_to_timestamp(`crash_hour`, 'HH:mm')"),
            F.expr("try_to_timestamp(`crash_hour`, 'h:mm a')"),
        )
    )
    hour_from_cast = F.when(raw_crash_hour.rlike(r"^\d+$"), raw_crash_hour.cast(IntegerType())).otherwise(None)
    candidate_hour = F.coalesce(hour_from_ref.cast(IntegerType()), hour_from_ts, hour_from_cast)
    std_crash_hour = F.when(candidate_hour.between(0, 23), candidate_hour).otherwise(None)

    # crash_month: exact lookup -> direct cast -> fuzzy canonical fallback
    month_from_ref = _build_map_expr("crash_month", mapping_dict, raw_crash_month)
    month_from_cast = F.when(raw_crash_month.rlike(r"^\d+$"), raw_crash_month.cast(IntegerType())).otherwise(None)
    candidate_month = F.coalesce(month_from_ref.cast(IntegerType()), month_from_cast)
    exact_crash_month = F.when(candidate_month.between(1, 12), candidate_month).otherwise(None)

    fuzzy_month_anchor = fuzzy_canonical(
        raw_crash_month, CANONICAL_ANCHORS["crash_month"]
    )
    effective_fuzzy_month = F.when(exact_crash_month.isNull(), fuzzy_month_anchor).otherwise(None)
    fuzzy_month_int = (
        F.when(effective_fuzzy_month == "JANUARY", 1)
        .when(effective_fuzzy_month == "FEBRUARY", 2)
        .when(effective_fuzzy_month == "MARCH", 3)
        .when(effective_fuzzy_month == "APRIL", 4)
        .when(effective_fuzzy_month == "MAY", 5)
        .when(effective_fuzzy_month == "JUNE", 6)
        .when(effective_fuzzy_month == "JULY", 7)
        .when(effective_fuzzy_month == "AUGUST", 8)
        .when(effective_fuzzy_month == "SEPTEMBER", 9)
        .when(effective_fuzzy_month == "OCTOBER", 10)
        .when(effective_fuzzy_month == "NOVEMBER", 11)
        .when(effective_fuzzy_month == "DECEMBER", 12)
        .otherwise(None)
    )
    std_crash_month = F.coalesce(exact_crash_month, fuzzy_month_int)

    # crash_day_of_week: exact lookup -> direct cast -> fuzzy canonical fallback
    day_from_ref = _build_map_expr("crash_day_of_week", mapping_dict, raw_crash_day)
    day_from_cast = F.when(raw_crash_day.rlike(r"^\d+$"), raw_crash_day.cast(IntegerType())).otherwise(None)
    candidate_day = F.coalesce(day_from_ref.cast(IntegerType()), day_from_cast)
    exact_crash_day = F.when(candidate_day.between(1, 7), candidate_day).otherwise(None)

    fuzzy_day_anchor = fuzzy_canonical(
        raw_crash_day, CANONICAL_ANCHORS["crash_day_of_week"]
    )
    effective_fuzzy_day = F.when(exact_crash_day.isNull(), fuzzy_day_anchor).otherwise(None)
    fuzzy_day_int = (
        F.when(effective_fuzzy_day == "SUNDAY", 1)
        .when(effective_fuzzy_day == "MONDAY", 2)
        .when(effective_fuzzy_day == "TUESDAY", 3)
        .when(effective_fuzzy_day == "WEDNESDAY", 4)
        .when(effective_fuzzy_day == "THURSDAY", 5)
        .when(effective_fuzzy_day == "FRIDAY", 6)
        .when(effective_fuzzy_day == "SATURDAY", 7)
        .otherwise(None)
    )
    std_crash_day_of_week = F.coalesce(exact_crash_day, fuzzy_day_int)

    # num_units: ref lookup for words ('one' -> 1.0) or regex numeric extract
    units_from_ref = _build_map_expr("num_units", mapping_dict, raw_num_units)
    units_from_regex = F.regexp_extract(raw_num_units, r"(\d+(\.\d+)?)", 1).cast(DecimalType(10, 1))
    std_num_units = F.coalesce(units_from_ref.cast(DecimalType(10, 1)), units_from_regex)

    # posted_speed_limit: regex numeric extract
    std_posted_speed_limit = F.regexp_extract(raw_speed_limit, r"(\d+)", 1).cast(IntegerType())

    # 5. Multi-field Failure Tracking into columns_to_remediate
    # Format: { field_name: (raw_col, std_col, is_mandatory) }
    tracked_fields = {
        "crash_date": (raw_crash_date, std_crash_date, True),
        "weather_condition": (raw_weather, std_weather, False),
        "lighting_condition": (raw_lighting, std_lighting, False),
        "hit_and_run_i": (raw_hit_and_run, std_hit_and_run, False),
        "damage": (raw_damage, std_damage, False),
        "street_direction": (raw_street_direction, std_street_direction, False),
        "num_units": (raw_num_units, std_num_units, False),
        "crash_month": (raw_crash_month, std_crash_month, False),
        "crash_hour": (raw_crash_hour, std_crash_hour, False),
        "crash_day_of_week": (raw_crash_day, std_crash_day_of_week, False),
        "date_police_notified": (raw_date_police_notified, std_date_police_notified, False),
        "posted_speed_limit": (raw_speed_limit, std_posted_speed_limit, False),
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

    columns_to_remediate = F.map_from_entries(F.array_compact(F.array(*failure_structs)))

    # 6. Audit Trail for Fuzzy Canonical Corrections
    # Captures (field, raw, matched, distance) for all successful fuzzy corrections in batch.
    fuzzy_audit_structs = [
        F.when(
            effective_fuzzy_weather.isNotNull()
            & raw_weather.isNotNull()
            & (F.trim(raw_weather) != ""),
            F.struct(
                F.lit("weather_condition").alias("field"),
                raw_weather.alias("raw"),
                effective_fuzzy_weather.alias("matched"),
                F.levenshtein(
                    F.upper(F.trim(raw_weather)),
                    effective_fuzzy_weather,
                ).alias("distance"),
            ),
        ).otherwise(None),
        F.when(
            effective_fuzzy_lighting.isNotNull()
            & raw_lighting.isNotNull()
            & (F.trim(raw_lighting) != ""),
            F.struct(
                F.lit("lighting_condition").alias("field"),
                raw_lighting.alias("raw"),
                effective_fuzzy_lighting.alias("matched"),
                F.levenshtein(
                    F.upper(F.trim(raw_lighting)),
                    effective_fuzzy_lighting,
                ).alias("distance"),
            ),
        ).otherwise(None),
        F.when(
            effective_fuzzy_dir.isNotNull()
            & raw_street_direction.isNotNull()
            & (F.trim(raw_street_direction) != ""),
            F.struct(
                F.lit("street_direction").alias("field"),
                raw_street_direction.alias("raw"),
                effective_fuzzy_dir.alias("matched"),
                F.levenshtein(
                    F.upper(F.trim(raw_street_direction)),
                    effective_fuzzy_dir,
                ).alias("distance"),
            ),
        ).otherwise(None),
        F.when(
            effective_fuzzy_month.isNotNull()
            & raw_crash_month.isNotNull()
            & (F.trim(raw_crash_month) != ""),
            F.struct(
                F.lit("crash_month").alias("field"),
                raw_crash_month.alias("raw"),
                effective_fuzzy_month.alias("matched"),
                F.levenshtein(
                    F.upper(F.trim(raw_crash_month)),
                    effective_fuzzy_month,
                ).alias("distance"),
            ),
        ).otherwise(None),
        F.when(
            effective_fuzzy_day.isNotNull()
            & raw_crash_day.isNotNull()
            & (F.trim(raw_crash_day) != ""),
            F.struct(
                F.lit("crash_day_of_week").alias("field"),
                raw_crash_day.alias("raw"),
                effective_fuzzy_day.alias("matched"),
                F.levenshtein(
                    F.upper(F.trim(raw_crash_day)),
                    effective_fuzzy_day,
                ).alias("distance"),
            ),
        ).otherwise(None),
    ]
    fuzzy_audit_entries = F.array_compact(F.array(*fuzzy_audit_structs))

    # 7. Project standardized columns (exact 55 Silver columns + failure/audit lineage)
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
        columns_to_remediate.alias("columns_to_remediate"),
        fuzzy_audit_entries.alias("_fuzzy_audit_entries"),
    )


def make_batch_processor(spark: SparkSession):
    """Returns the foreachBatch function with access to spark session."""

    def process_micro_batch(batch_df: DataFrame, batch_id: int):
        # 1. Reload the latest committed canonical mappings per micro-batch
        mapping_dict = load_mappings_dict(spark, REF_MAPPINGS_PATH)

        # 2. Apply transformations and capture failed fields
        transformed = standardize_silver2(batch_df, mapping_dict)

        # 3. Persist to avoid double evaluation across valid, quarantine, and audit splits
        transformed.persist()

        try:
            # Clean rows: drop lineage/audit tracking columns before writing to Silver 2
            # Adds zero extra columns to Silver 2
            valid_df = transformed.filter(
                F.size(F.col("columns_to_remediate")) == 0
            ).drop("columns_to_remediate", "_fuzzy_audit_entries")

            # Quarantined rows: drop audit tracking column
            quarantine_df = transformed.filter(
                F.size(F.col("columns_to_remediate")) > 0
            ).drop("_fuzzy_audit_entries")

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

            # Log fuzzy corrections to small audit Delta table: (field, raw, matched, distance)
            fuzzy_records_df = transformed.filter(
                F.size(F.col("_fuzzy_audit_entries")) > 0
            )
            if fuzzy_records_df.head(1):
                audit_df = (
                    fuzzy_records_df.select(
                        F.explode("_fuzzy_audit_entries").alias("entry"),
                        F.lit(str(batch_id)).alias("batch_id"),
                        F.current_timestamp().alias("corrected_at"),
                    ).select(
                        F.col("entry.field").alias("field"),
                        F.col("entry.raw").alias("raw"),
                        F.col("entry.matched").alias("matched"),
                        F.col("entry.distance").cast(IntegerType()).alias("distance"),
                        F.col("corrected_at"),
                        F.col("batch_id"),
                    )
                )
                audit_df.write.format("delta").mode("append").save(FUZZY_AUDIT_PATH)
                print(
                    f"[silver2] Batch {batch_id}: logged fuzzy corrections to {FUZZY_AUDIT_PATH}"
                )
        finally:
            transformed.unpersist()

    return process_micro_batch


def main():
    spark = build_spark("Silver2StandardizeTransform")

    # Initialize reference canonical mappings if table does not exist
    init_ref_mappings(spark, REF_MAPPINGS_PATH)

    # Initialize fuzzy audit Delta table if not already present
    init_fuzzy_audit(spark, FUZZY_AUDIT_PATH)

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
        f"[silver2] Streaming {SILVER1_PATH} -> {SILVER2_PATH} "
        f"(Quarantine: {QUARANTINE_PATH}, Audit: {FUZZY_AUDIT_PATH})"
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()

