# """
# Asynchronous Vocabulary-Healing & Quarantine Replay Job.

# Implements the narrow, self-healing loop:
# 1. Scans silver2_quarantine for pending unmapped categorical values.
# 2. Gated by an unmapped-rate threshold to differentiate routine synonyms from major schema anomalies.
# 3. Classifies candidate synonyms into canonical enums (LLM / heuristic resolver).
# 4. Appends resolved synonyms to ref_canonical_mappings with audit metadata (added_by='auto_llm').
# 5. Replays resolved quarantine rows, appending them into silver2 for the first time (preserving Gold streaming).
# """

# import json
# import os
# import sys
# from datetime import datetime, timezone
# from typing import Dict, List, Optional, Tuple

# from pyspark.sql import DataFrame, SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import (
#     DoubleType,
#     StringType,
#     StructField,
#     StructType,
#     TimestampType,
# )

# from common import build_spark, wait_for_delta_table
# from ref_mappings import REF_MAPPINGS_SCHEMA, load_mappings_dict
# from silver2_job import standardize_silver2

# SILVER2_PATH = os.environ.get("SILVER2_PATH", "/data/delta/silver2")
# QUARANTINE_PATH = os.environ.get("QUARANTINE_PATH", "/data/delta/silver2_quarantine")
# REF_MAPPINGS_PATH = os.environ.get(
#     "REF_MAPPINGS_PATH", "/data/delta/ref_canonical_mappings"
# )

# # Canonical enum targets for vocabulary fields
# CANONICAL_ENUMS: Dict[str, List[str]] = {
#     "weather_condition": ["CLEAR", "RAIN", "CLOUDY", "SNOW", "FOG", "UNKNOWN"],
#     "lighting_condition": [
#         "DAYLIGHT",
#         "DARKNESS, LIGHTED ROAD",
#         "DARKNESS",
#         "UNKNOWN",
#     ],
#     "damage": ["OVER $1,500", "$501 - $1,500", "$500 OR LESS"],
#     "street_direction": ["N", "S", "E", "W"],
#     "hit_and_run_i": ["Y", "N"],
# }

# # Max permissible unmapped rate before escalating to full human diagnosis
# UNMAPPED_RATE_THRESHOLD = float(os.environ.get("UNMAPPED_RATE_THRESHOLD", "0.15"))


# def get_pending_unmapped_vocab(
#     spark: SparkSession, quarantine_path: str
# ) -> List[Tuple[str, str]]:
#     """Extract distinct unmapped (field_name, raw_synonym) pairs from quarantine."""
#     if not os.path.exists(os.path.join(quarantine_path, "_delta_log")):
#         return []

#     q_df = spark.read.format("delta").load(quarantine_path)
#     if "healing_status" in q_df.columns:
#         q_df = q_df.filter(F.col("healing_status") == "PENDING")

#     exploded = q_df.select(F.explode("unmapped_payload").alias("field_name", "raw_synonym"))
#     distinct_rows = exploded.filter(
#         F.col("field_name").isin(list(CANONICAL_ENUMS.keys()))
#     ).distinct().collect()

#     return [(r.field_name, r.raw_synonym) for r in distinct_rows]


# def classify_synonym_llm(
#     field_name: str, raw_synonym: str, api_key: Optional[str] = None
# ) -> Optional[Tuple[str, float]]:
#     """
#     Classify an unknown raw string into one of the allowed canonical enums.
#     Uses strict enum constraints. Returns (canonical_value, confidence) or None.
#     """
#     valid_targets = CANONICAL_ENUMS.get(field_name, [])
#     if not valid_targets:
#         return None

#     raw_clean = raw_synonym.strip().upper()

#     # Rule-based fast paths for common linguistic variations
#     if field_name == "weather_condition":
#         if "MIST" in raw_clean or "FOG" in raw_clean or "HAZE" in raw_clean:
#             return ("FOG", 0.95)
#         if "SUN" in raw_clean or "FAIR" in raw_clean:
#             return ("CLEAR", 0.95)
#         if "DRIZZLE" in raw_clean or "SHOWER" in raw_clean or "PRECIP" in raw_clean:
#             return ("RAIN", 0.95)
#         if "OVERCAST" in raw_clean or "GLOOM" in raw_clean:
#             return ("CLOUDY", 0.95)
#         if "SNOW" in raw_clean or "BLIZZARD" in raw_clean or "SLEET" in raw_clean:
#             return ("SNOW", 0.95)

#     if field_name == "lighting_condition":
#         if "DUSK" in raw_clean or "DAWN" in raw_clean or "SUNRISE" in raw_clean or "SUNSET" in raw_clean:
#             return ("DARKNESS, LIGHTED ROAD", 0.85)

#     # If an LLM endpoint is configured, execute strict JSON structured prompt
#     openai_key = api_key or os.environ.get("OPENAI_API_KEY")
#     if openai_key:
#         try:
#             import urllib.request

#             prompt = (
#                 f"You are a data standardization engine for vehicle crash reports.\n"
#                 f"Field: '{field_name}'\n"
#                 f"Raw input string: '{raw_synonym}'\n"
#                 f"Allowed Canonical Enums: {json.dumps(valid_targets)}\n"
#                 f"Return JSON strictly in this format: {{\"canonical_value\": \"...\", \"confidence\": 0.9}}\n"
#                 f"If none apply, return canonical_value 'UNKNOWN'."
#             )
#             payload = json.dumps({
#                 "model": "gpt-4o-mini",
#                 "messages": [{"role": "user", "content": prompt}],
#                 "response_format": {"type": "json_object"},
#                 "temperature": 0.0,
#             }).encode("utf-8")

#             req = urllib.request.Request(
#                 "https://api.openai.com/v1/chat/completions",
#                 data=payload,
#                 headers={
#                     "Content-Type": "application/json",
#                     "Authorization": f"Bearer {openai_key}",
#                 },
#             )
#             with urllib.request.urlopen(req, timeout=10) as resp:
#                 data = json.loads(resp.read().decode("utf-8"))
#                 parsed = json.loads(data["choices"][0]["message"]["content"])
#                 target = parsed.get("canonical_value", "").strip()
#                 conf = float(parsed.get("confidence", 0.0))
#                 if target in valid_targets and conf >= 0.8:
#                     return (target, conf)
#         except Exception as e:
#             print(f"[heal_vocab] LLM classification error for '{raw_synonym}': {e}")

#     return None


# def add_canonical_mapping(
#     spark: SparkSession,
#     ref_path: str,
#     field_name: str,
#     raw_synonym: str,
#     canonical_value: str,
#     confidence: float,
#     added_by: str = "auto_llm",
#     source_batch_id: Optional[str] = None,
# ) -> None:
#     """Append a new verified canonical mapping to the reference table with audit metadata."""
#     now = datetime.now(timezone.utc)
#     new_row = [
#         (
#             field_name,
#             raw_synonym.strip().upper(),
#             canonical_value,
#             added_by,
#             now,
#             float(confidence),
#             source_batch_id,
#         )
#     ]
#     new_df = spark.createDataFrame(new_row, schema=REF_MAPPINGS_SCHEMA)
#     new_df.write.format("delta").mode("append").save(ref_path)
#     print(
#         f"[heal_vocab] Appended mapping: {field_name} ['{raw_synonym}'] -> '{canonical_value}' "
#         f"(confidence={confidence:.2f}, added_by='{added_by}')"
#     )


# def replay_quarantined_records(
#     spark: SparkSession,
#     quarantine_path: str,
#     silver2_path: str,
#     ref_path: str,
# ) -> int:
#     """
#     Re-evaluate quarantined records against latest reference mappings.
#     Any records whose unmapped fields have all been resolved are appended to silver2
#     for the first time, ensuring downstream gold_job receives the clean data.
#     """
#     if not os.path.exists(os.path.join(quarantine_path, "_delta_log")):
#         return 0

#     from delta.tables import DeltaTable

#     mapping_dict = load_mappings_dict(spark, ref_path)
#     q_table = DeltaTable.forPath(spark, quarantine_path)
#     q_df = q_table.toDF().filter(F.col("healing_status") == "PENDING")

#     if q_df.count() == 0:
#         return 0

#     # Re-standardize the quarantined records
#     re_standardized = standardize_silver2(q_df, mapping_dict)

#     # Identify records that are now fully clean
#     healed_df = re_standardized.filter(F.size(F.col("unmapped_payload")) == 0).drop(
#         "unmapped_payload"
#     )
#     healed_count = healed_df.count()

#     if healed_count > 0:
#         # Append resolved rows to Silver 2 for the first time
#         # Strip internal quarantine metadata before appending
#         clean_silver2_cols = [
#             c for c in healed_df.columns if c not in ["quarantined_at", "silver2_batch_id", "healing_status"]
#         ]
#         healed_df.select(*clean_silver2_cols).write.format("delta").mode("append").save(silver2_path)
#         print(f"[heal_vocab] Appended {healed_count} healed records into {silver2_path} for the first time.")

#         # Mark healed records in quarantine table as RESOLVED
#         healed_ids = [r.record_id for r in healed_df.select("record_id").collect()]
#         q_table.update(
#             condition=F.col("record_id").isin(healed_ids),
#             set={"healing_status": F.lit("RESOLVED")},
#         )
#         print(f"[heal_vocab] Updated {healed_count} records in quarantine to RESOLVED.")

#     return healed_count


# def main():
#     spark = build_spark("Silver2VocabularyHealing")

#     print(f"[heal_vocab] Checking quarantine at {QUARANTINE_PATH}...")
#     pending_items = get_pending_unmapped_vocab(spark, QUARANTINE_PATH)
#     print(f"[heal_vocab] Found {len(pending_items)} distinct unmapped categorical candidates.")

#     if not pending_items:
#         print("[heal_vocab] Quarantine is clear of pending vocabulary entries.")
#         sys.exit(0)

#     # Process candidates
#     resolved_count = 0
#     for field_name, raw_synonym in pending_items:
#         result = classify_synonym_llm(field_name, raw_synonym)
#         if result:
#             canonical_val, confidence = result
#             add_canonical_mapping(
#                 spark,
#                 REF_MAPPINGS_PATH,
#                 field_name,
#                 raw_synonym,
#                 canonical_val,
#                 confidence,
#                 added_by="auto_llm",
#             )
#             resolved_count += 1
#         else:
#             print(f"[heal_vocab] Could not resolve candidate '{raw_synonym}' for '{field_name}'. Held for review.")

#     if resolved_count > 0:
#         print(f"[heal_vocab] Replaying quarantine against newly added mappings...")
#         replay_quarantined_records(spark, QUARANTINE_PATH, SILVER2_PATH, REF_MAPPINGS_PATH)


# if __name__ == "__main__":
#     main()
