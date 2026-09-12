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
SELECT * FROM silver2
"""


def main():
    spark = build_spark("GoldCrashTransform")

    wait_for_delta_table(SILVER2_PATH)

    silver2_stream = spark.readStream.format("delta").load(SILVER2_PATH)
    silver2_stream.createOrReplaceTempView("silver2")

    gold = spark.sql(GOLD_TRANSFORM_SQL)

    query = (
        gold.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(GOLD_PATH)
    )

    print(f"[gold] Streaming {SILVER2_PATH} -> {GOLD_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
