"""
One-off: convert Silver 2 tables written before the INT columns, in place.

Silver 2 now stores crash_hour (9), num_units (2) and the injury counts (0) as INT.
Tables written before held "09.00", 2.0 and "0.0". New rows cannot be appended to
them, so this script converts them once, without reprocessing anything:

  1. Gold catches up: any Silver 2 row not in Gold yet is added to it with Gold's
     own transform (normally none, when the pipeline was stopped after draining).
  2. Each silver2_<source> table and the shared quarantine are rewritten with the
     same casts Gold always applied ("09.00" -> 9, 2.0 -> 2, "0.0" -> 0), so Gold's
     values do not change. Row counts, raw_record and healing status are kept.
  3. Gold's checkpoint is moved aside and Gold is told to start each Silver 2 table
     at its rewrite (gold_job.start_versions). A Delta stream cannot carry on across
     a column type change, and Gold already holds every row from before.

Stop the pipeline and the agent first (docker compose stop), then:

    docker compose run --rm silver2-gold-job python upgrade_silver2_types.py

and start everything again. Running it again finds nothing to convert and changes
nothing. --dry-run only reports what it would convert.
"""
import argparse
import json
import os
import shutil
import time
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

import gold_job
import silver2_job
from common import build_spark, has_delta_commit
from sources import source_names, table_name, table_path


def convert_table(spark: SparkSession, path: str) -> List[str]:
    """Rewrite one table with its old-typed INT_COLUMNS cast to INT. Returns them."""
    todo = silver2_job.old_type_columns(spark, path)
    if not todo:
        return []
    df = spark.read.format("delta").load(path)
    converted = df.select(*[
        silver2_job._whole_count(F.col(c).cast(StringType())).alias(c) if c in todo else F.col(c)
        for c in df.columns
    ])
    before = df.count()
    (converted.write.format("delta").mode("overwrite")
     .option("overwriteSchema", "true").save(path))
    after = spark.read.format("delta").load(path).count()
    if after != before:
        raise RuntimeError(f"{path}: {before} rows before the conversion, {after} after")
    print(f"[upgrade] {path}: {', '.join(todo)} -> INT ({after} rows)")
    return todo


def _latest_version(spark: SparkSession, path: str) -> int:
    from delta.tables import DeltaTable
    return int(DeltaTable.forPath(spark, path).history(1).first()["version"])


def catch_up_gold(spark: SparkSession, sources: List[str]) -> int:
    """Add to Gold the Silver 2 rows it does not hold yet. Returns how many."""
    tables = {s: table_path("silver2", s) for s in sources if has_delta_commit(table_path("silver2", s))}
    if not tables:
        return 0
    silver2_all: DataFrame = None
    for s, path in tables.items():
        df = spark.read.format("delta").load(path).withColumn("source_table", F.lit(table_name("silver2", s)))
        silver2_all = df if silver2_all is None else silver2_all.unionByName(df)
    if has_delta_commit(gold_job.GOLD_PATH):
        in_gold = spark.read.format("delta").load(gold_job.GOLD_PATH).select("record_id", "source_table")
        silver2_all = silver2_all.join(in_gold, ["record_id", "source_table"], "left_anti")
    missing = silver2_all.count()
    if missing:
        silver2_all.createOrReplaceTempView("silver2_all")
        gold_job.add_derived_columns(spark, gold_job.GOLD_PATH)
        (spark.sql(gold_job.GOLD_TRANSFORM_SQL).write.format("delta").mode("append")
         .option("mergeSchema", "true").save(gold_job.GOLD_PATH))
    print(f"[upgrade] Gold: added {missing} Silver 2 row(s) it did not hold yet")
    return missing


def upgrade(spark: SparkSession, sources: List[str], dry_run: bool = False) -> Dict[str, List[str]]:
    """Convert every old-typed Silver 2 table and the quarantine; see the module doc."""
    paths = {table_name("silver2", s): table_path("silver2", s) for s in sources}
    paths["silver2_quarantine"] = silver2_job.QUARANTINE_PATH
    plan = {name: silver2_job.old_type_columns(spark, path) for name, path in paths.items()}
    plan = {name: cols for name, cols in plan.items() if cols}
    if not plan:
        print("[upgrade] Every Silver 2 table already has INT columns: nothing to do")
        return {}
    for name, cols in plan.items():
        print(f"[upgrade] {name}: {', '.join(cols)} still old types")
    if dry_run:
        return plan

    catch_up_gold(spark, sources)
    starts = gold_job.start_versions()
    for name in plan:
        convert_table(spark, paths[name])
        if name != "silver2_quarantine":
            starts[name] = _latest_version(spark, paths[name])

    os.makedirs(os.path.dirname(gold_job.START_VERSIONS_PATH) or ".", exist_ok=True)
    with open(gold_job.START_VERSIONS_PATH, "w", encoding="utf-8") as fh:
        json.dump(starts, fh, indent=2, sort_keys=True)
    if os.path.isdir(gold_job.CHECKPOINT_PATH):
        aside = f"{gold_job.CHECKPOINT_PATH}.before-int-{time.strftime('%Y%m%d-%H%M%S')}"
        shutil.move(gold_job.CHECKPOINT_PATH, aside)
        print(f"[upgrade] Gold checkpoint moved to {aside}")
    print(f"[upgrade] Gold starts each Silver 2 table at its rewrite: {starts} ({gold_job.START_VERSIONS_PATH})")
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="only report what would be converted")
    args = parser.parse_args()
    spark = build_spark("UpgradeSilver2Types")
    try:
        upgrade(spark, source_names(), dry_run=args.dry_run)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
