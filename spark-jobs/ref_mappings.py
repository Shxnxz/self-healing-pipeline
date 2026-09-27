"""
Reference Canonical Mappings Management.

Handles seed definitions, Delta table initialization, and dictionary lookups
for categorical standardization in Silver 2 and self-healing LLM loops.
"""

import os
from datetime import datetime, timezone
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

REF_MAPPINGS_SCHEMA = StructType(
    [
        StructField("field_name", StringType(), False),
        StructField("raw_synonym", StringType(), False),
        StructField("canonical_value", StringType(), False),
        StructField("added_by", StringType(), False),
        StructField("added_at", TimestampType(), False),
        StructField("confidence", DoubleType(), False),
        StructField("source_batch_id", StringType(), True),
    ]
)

# Initial seed vocabulary extracted from original Silver 2 SQL rules
SEED_ENTRIES = [
    # weather_condition
    ("weather_condition", "CLEAR", "CLEAR"),
    ("weather_condition", "C", "CLEAR"),
    ("weather_condition", "CLEAR WEATHER", "CLEAR"),
    ("weather_condition", "RAIN", "RAIN"),
    ("weather_condition", "R", "RAIN"),
    ("weather_condition", "RAINY", "RAIN"),
    ("weather_condition", "UNKNOWN", "UNKNOWN"),
    ("weather_condition", "U", "UNKNOWN"),
    ("weather_condition", "NOT KNOWN", "UNKNOWN"),
    ("weather_condition", "CLOUDY/OVERCAST", "CLOUDY"),
    ("weather_condition", "CLOUDY", "CLOUDY"),
    ("weather_condition", "O", "CLOUDY"),
    ("weather_condition", "OVERCAST", "CLOUDY"),
    # lighting_condition
    ("lighting_condition", "DAYLIGHT", "DAYLIGHT"),
    ("lighting_condition", "D", "DAYLIGHT"),
    ("lighting_condition", "MORNING", "DAYLIGHT"),
    ("lighting_condition", "DAY", "DAYLIGHT"),
    ("lighting_condition", "DARKNESS, LIGHTED ROAD", "DARKNESS, LIGHTED ROAD"),
    ("lighting_condition", "N, L", "DARKNESS, LIGHTED ROAD"),
    ("lighting_condition", "NIGHT, LIGHTED", "DARKNESS, LIGHTED ROAD"),
    ("lighting_condition", "NIGHT WITH LIGHT", "DARKNESS, LIGHTED ROAD"),
    ("lighting_condition", "DARKNESS", "DARKNESS"),
    ("lighting_condition", "N", "DARKNESS"),
    ("lighting_condition", "NIGHT", "DARKNESS"),
    ("lighting_condition", "UNKNOWN", "UNKNOWN"),
    ("lighting_condition", "U", "UNKNOWN"),
    ("lighting_condition", "NOT KNOWN", "UNKNOWN"),
    # hit_and_run_i
    ("hit_and_run_i", "Y", "Y"),
    ("hit_and_run_i", "TRUE", "Y"),
    ("hit_and_run_i", "YES", "Y"),
    ("hit_and_run_i", "RIGHT", "Y"),
    ("hit_and_run_i", "N", "N"),
    ("hit_and_run_i", "FALSE", "N"),
    ("hit_and_run_i", "NO", "N"),
    ("hit_and_run_i", "WRONG", "N"),
    # damage
    ("damage", "OVER $1,500", "OVER $1,500"),
    ("damage", "> $1500", "OVER $1,500"),
    ("damage", "HIGH", "OVER $1,500"),
    ("damage", "$501 - $1,500", "$501 - $1,500"),
    ("damage", "$501 - $1500", "$501 - $1,500"),
    ("damage", "MEDIUM", "$501 - $1,500"),
    ("damage", "$500 OR LESS", "$500 OR LESS"),
    ("damage", "<= $500", "$500 OR LESS"),
    ("damage", "≤ $500", "$500 OR LESS"),
    ("damage", "LOW", "$500 OR LESS"),
    # street_direction
    ("street_direction", "S", "S"),
    ("street_direction", "SOUTH", "S"),
    ("street_direction", "SOU", "S"),
    ("street_direction", "N", "N"),
    ("street_direction", "NORTH", "N"),
    ("street_direction", "NOR", "N"),
    ("street_direction", "E", "E"),
    ("street_direction", "EAST", "E"),
    ("street_direction", "EAS", "E"),
    ("street_direction", "W", "W"),
    ("street_direction", "WEST", "W"),
    ("street_direction", "WES", "W"),
    # num_units words
    ("num_units", "ONE", "1.0"),
    ("num_units", "TWO", "2.0"),
    ("num_units", "THREE", "3.0"),
    ("num_units", "FOUR", "4.0"),
    ("num_units", "FIVE", "5.0"),
    ("num_units", "SIX", "6.0"),
    # crash_month names/abbreviations
    ("crash_month", "JANUARY", "1"),
    ("crash_month", "JAN", "1"),
    ("crash_month", "JA", "1"),
    ("crash_month", "FEBRUARY", "2"),
    ("crash_month", "FEB", "2"),
    ("crash_month", "FE", "2"),
    ("crash_month", "MARCH", "3"),
    ("crash_month", "MAR", "3"),
    ("crash_month", "MA", "3"),
    ("crash_month", "APRIL", "4"),
    ("crash_month", "APR", "4"),
    ("crash_month", "AP", "4"),
    ("crash_month", "MAY", "5"),
    ("crash_month", "MY", "5"),
    ("crash_month", "JUNE", "6"),
    ("crash_month", "JUN", "6"),
    ("crash_month", "JU", "6"),
    ("crash_month", "JULY", "7"),
    ("crash_month", "JUL", "7"),
    ("crash_month", "JL", "7"),
    ("crash_month", "AUGUST", "8"),
    ("crash_month", "AUG", "8"),
    ("crash_month", "AU", "8"),
    ("crash_month", "SEPTEMBER", "9"),
    ("crash_month", "SEP", "9"),
    ("crash_month", "SE", "9"),
    ("crash_month", "OCTOBER", "10"),
    ("crash_month", "OCT", "10"),
    ("crash_month", "OC", "10"),
    ("crash_month", "NOVEMBER", "11"),
    ("crash_month", "NOV", "11"),
    ("crash_month", "NO", "11"),
    ("crash_month", "DECEMBER", "12"),
    ("crash_month", "DEC", "12"),
    ("crash_month", "DE", "12"),
    # crash_day_of_week
    ("crash_day_of_week", "SUNDAY", "1"),
    ("crash_day_of_week", "SUN", "1"),
    ("crash_day_of_week", "SU", "1"),
    ("crash_day_of_week", "MONDAY", "2"),
    ("crash_day_of_week", "MON", "2"),
    ("crash_day_of_week", "MO", "2"),
    ("crash_day_of_week", "TUESDAY", "3"),
    ("crash_day_of_week", "TUE", "3"),
    ("crash_day_of_week", "TU", "3"),
    ("crash_day_of_week", "WEDNESDAY", "4"),
    ("crash_day_of_week", "WED", "4"),
    ("crash_day_of_week", "WE", "4"),
    ("crash_day_of_week", "THURSDAY", "5"),
    ("crash_day_of_week", "THU", "5"),
    ("crash_day_of_week", "TH", "5"),
    ("crash_day_of_week", "FRIDAY", "6"),
    ("crash_day_of_week", "FRI", "6"),
    ("crash_day_of_week", "FR", "6"),
    ("crash_day_of_week", "SATURDAY", "7"),
    ("crash_day_of_week", "SAT", "7"),
    ("crash_day_of_week", "SA", "7"),
]


def init_ref_mappings(spark: SparkSession, ref_path: str) -> None:
    """Initialize the ref_canonical_mappings Delta table if not already present."""
    log_marker = os.path.join(ref_path, "_delta_log")
    if not os.path.isdir(log_marker):
        print(f"[ref_mappings] Initializing canonical reference table at {ref_path}...")
        now = datetime.now(timezone.utc)
        rows = [
            (
                field_name,
                raw_synonym.strip().upper(),
                canonical_val,
                "seed",
                now,
                1.0,
                None,
            )
            for field_name, raw_synonym, canonical_val in SEED_ENTRIES
        ]
        df = spark.createDataFrame(rows, schema=REF_MAPPINGS_SCHEMA)
        df.write.format("delta").mode("overwrite").save(ref_path)
        print(f"[ref_mappings] Initialized with {len(rows)} seed rules.")


def load_mappings_dict(spark: SparkSession, ref_path: str) -> Dict[str, Dict[str, str]]:
    """
    Load the latest committed canonical mappings from Delta into a nested dictionary:
    { field_name: { UPPER_RAW_SYNONYM: CANONICAL_VALUE } }
    """
    ref_df = spark.read.format("delta").load(ref_path)
    collected = ref_df.select("field_name", "raw_synonym", "canonical_value").collect()
    mapping_dict: Dict[str, Dict[str, str]] = {}
    for row in collected:
        field = row.field_name
        synonym = (row.raw_synonym or "").strip().upper()
        if not field or not synonym:
            continue
        if field not in mapping_dict:
            mapping_dict[field] = {}
        mapping_dict[field][synonym] = row.canonical_value
    return mapping_dict
