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
SELECT * FROM silver1
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
