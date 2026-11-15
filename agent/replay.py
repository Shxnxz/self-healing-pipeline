"""Re-run Silver 2 on a staged group, and release the records that pass (Spark).

Validate (spec VD2) must use "the same standardization logic that originally
detected the failure", so this module runs the pipeline's own
`silver2_job.standardize_silver2` from spark-jobs/, in a small local Spark session
inside the agent, and nothing re-implements it:

  1. read the staged records from silver2_quarantine (record_id + target_table),
     every row of them, whatever their healing_status
  2. replay their `raw_record` (the untouched Silver 1 row) through
     standardize_silver2 with ref_canonical_mappings + the staged overlay
     (ref_mappings.overlay_mappings), in the source's own date order
     (sources.is_day_first, so Crash3 is read day first as in the pipeline)
  3. report per record what it failed before and after, the standardized value of
     the targeted column and the standardized crash_date

agent/validation.py decides from that report. `Replay.release()` then writes the
records that passed, from the same replayed rows:

  - append them to the source's Silver 2 table (silver2_crashN), exactly the columns
    the Silver 2 job writes. The write is idempotent (txnAppId = the execution), so a
    step that runs again after a crash never writes them twice. Gold streams them
    from there like any other Silver 2 row.
  - then mark their quarantine rows healing_status = RESOLVED, with resolved_by =
    the execution id and resolved_at. Two columns the agent adds to the quarantine
    the first time; the Silver 2 job's appends leave them empty.

The append comes first: if the agent stops in between, the rows are in Silver 2 and
still PENDING in quarantine, and the next run skips the append (Delta remembers the
txnAppId) and only marks them.
"""
import os
import re
import sys
import time
from typing import Dict, List, Optional

from agent import config

WAITING = ("PENDING", "IN_PROGRESS")
RESOLVED = "RESOLVED"

# The Silver 2 columns that are standardized (silver2_job.standardize_silver2). A
# column outside this list is never interpolated into a query.
TRACKED_COLUMNS = (
    "crash_date", "weather_condition", "lighting_condition", "hit_and_run_i", "damage",
    "street_direction", "num_units", "crash_month", "crash_hour", "crash_day_of_week",
    "date_police_notified", "posted_speed_limit",
)
# Only a per-source Silver 2 table can be written: silver2_<source> (sources.py), not
# one of the shared tables next to them.
_TARGET_TABLE = re.compile(r"^silver2_[a-z][a-z0-9]*$")
_SHARED_TABLES = ("silver2_quarantine",)
# silver2_job drops these before it writes a valid row to Silver 2.
_NOT_IN_SILVER2 = ("columns_to_remediate", "_fuzzy_audit_entries", "raw_record")
_ATTEMPTS = 5

_spark = None


class ReplayError(Exception):
    """The group cannot be replayed as asked (bad table or column name)."""


def _spark_jobs():
    """Make the pipeline's job modules importable, here and in Spark's Python workers
    (the fuzzy-match UDF of ref_mappings runs in a worker)."""
    path = config.SPARK_JOBS_DIR
    if not os.path.isfile(os.path.join(path, "silver2_job.py")):
        raise RuntimeError(f"spark-jobs not found at {path} (SPARK_JOBS_DIR)")
    if path not in sys.path:
        sys.path.insert(0, path)
    parts = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    if path not in parts:
        os.environ["PYTHONPATH"] = os.pathsep.join([path] + parts)


def spark():
    """The agent's Spark session: started on first use and kept for the next ones."""
    global _spark
    if _spark is not None and _spark.sparkContext._jsc is not None:
        return _spark
    _spark_jobs()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("recovery-agent-validate")
        .master(config.VALIDATE_SPARK_MASTER)
        .config("spark.driver.memory", config.VALIDATE_SPARK_DRIVER_MEMORY)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        # The pipeline's Spark settings that change what standardize_silver2 returns.
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    _spark = configure_spark_with_delta_pip(builder).getOrCreate()
    _spark.sparkContext.setLogLevel("WARN")
    return _spark


def _retry(what: str, fn):
    """Run a Delta write, again when it lost a race with the Silver 2 job's appends."""
    from common import _is_delta_conflict

    for attempt in range(1, _ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == _ATTEMPTS or not _is_delta_conflict(exc):
                raise
            print(f"[agent] {what}: concurrent Delta write ({type(exc).__name__}), retry {attempt}", flush=True)
            time.sleep(0.5 * attempt)


class Replay:
    """One replay of a staged group: the report for validation.evaluate() and the
    replayed rows a release writes."""

    def __init__(self, session, target_table: str, record_ids: List[str], column: str, overlay: dict,
                 quarantine_path: str, ref_path: str, silver2_path: str):
        from delta.tables import DeltaTable
        from pyspark.sql import functions as F

        from ref_mappings import load_mappings_dict, overlay_mappings
        from silver2_job import standardize_silver2
        from sources import is_day_first

        self.spark, self.target_table, self.column = session, target_table, column
        self.quarantine_path, self.silver2_path = quarantine_path, silver2_path
        ids = sorted(set(record_ids))

        base = load_mappings_dict(session, ref_path)
        field = base.get(column, {})
        (raw_key,) = overlay[column]  # validation.evaluate() checks the overlay's shape
        self.canonical_values = sorted(set(field.values()))
        self.existing_mapping = field.get(raw_key)

        quarantine = session.read.format("delta").load(quarantine_path)
        self.quarantine_version = DeltaTable.forPath(session, quarantine_path).history(1).first()["version"]
        group = quarantine.filter((F.col("target_table") == target_table) & F.col("record_id").isin(ids))
        status = F.col("resolved_by") if "resolved_by" in quarantine.columns else F.lit(None).cast("string")
        meta = group.select("record_id", "healing_status", status.alias("resolved_by"),
                            F.map_keys("columns_to_remediate").alias("before")).collect()

        replayed = standardize_silver2(group.select("raw_record.*"), overlay_mappings(base, overlay),
                                       day_first=is_day_first(target_table)).cache()
        self.replayed = replayed
        after = {row["record_id"]: row for row in replayed.select(
            "record_id", F.col("columns_to_remediate").alias("after"),
            F.col(column).cast("string").alias("value"), F.col("crash_date").alias("crash_date")).collect()}

        self.rows = []
        for row in meta:
            out = after.get(row["record_id"])
            self.rows.append({
                "record_id": row["record_id"], "healing_status": row["healing_status"],
                "resolved_by": row["resolved_by"], "before": list(row["before"] or []),
                "after": dict(out["after"] or {}) if out else {column: "NOT REPLAYED"},
                "value": out["value"] if out else None, "crash_date": out["crash_date"] if out else None,
            })

        self.in_silver2: List[str] = []
        self.released_before = False
        if os.path.isdir(os.path.join(silver2_path, "_delta_log")):
            self.in_silver2 = [r["record_id"] for r in session.read.format("delta").load(silver2_path)
                               .filter(F.col("record_id").isin(ids)).select("record_id").distinct().collect()]

    def report(self) -> dict:
        return {"rows": self.rows, "canonical_values": self.canonical_values,
                "existing_mapping": self.existing_mapping, "in_silver2": self.in_silver2,
                "released_before": self.released_before, "quarantine_version": self.quarantine_version}

    def mark_released_before(self, execution_id: str) -> None:
        """Did this execution already append its records to Silver 2 (a step run again)?"""
        from deltalake import DeltaTable as Table

        if self.in_silver2:
            version = Table(self.silver2_path).transaction_version(release_app_id(execution_id))
            self.released_before = version is not None

    def release(self, execution_id: str, record_ids: List[str]) -> dict:
        """Append the replayed rows of `record_ids` to Silver 2, then mark them RESOLVED.

        Returns what this call really wrote: `appended` is 0 when this execution's
        append had already landed (the step is running again), `marked` counts the
        quarantine rows this call set to RESOLVED.

        The replay's fuzzy corrections (`_fuzzy_audit_entries`) are not written to
        silver2_fuzzy_audit: the Silver 2 job already logged them for these records
        when it quarantined them (it logs every row of a micro-batch, quarantined ones
        too), and ref_mappings.promote_fuzzy_variants counts rows, so logging them again
        would count each correction twice. A replay cannot produce a correction the
        first pass did not: the fuzzy anchors are fixed seeds, and the only mapping
        added (the overlay) turns the targeted column into an exact match.
        """
        from delta.tables import DeltaTable
        from deltalake import DeltaTable as Table
        from pyspark.sql import functions as F

        from common import idempotent_append

        if not record_ids:
            return {"appended": 0, "marked": 0}
        ids = sorted(set(record_ids))
        app_id = release_app_id(execution_id)
        rows = self.replayed.filter(F.col("record_id").isin(ids) & (F.size("columns_to_remediate") == 0))
        rows = rows.drop(*_NOT_IN_SILVER2)
        landed = os.path.isdir(os.path.join(self.silver2_path, "_delta_log")) and \
            Table(self.silver2_path).transaction_version(app_id) is not None
        appended = 0 if landed else rows.count()
        # Idempotent: Delta skips the append if this execution's txnAppId already wrote.
        idempotent_append(rows, self.silver2_path, app_id, 0)

        quarantine = DeltaTable.forPath(self.spark, self.quarantine_path)
        if "resolved_by" not in quarantine.toDF().columns:
            _retry("add resolved_by to the quarantine", lambda: self.spark.sql(
                f"ALTER TABLE delta.`{self.quarantine_path}` "
                "ADD COLUMNS (resolved_by STRING, resolved_at TIMESTAMP)"))
            quarantine = DeltaTable.forPath(self.spark, self.quarantine_path)
        condition = ((F.col("target_table") == self.target_table) & F.col("record_id").isin(ids)
                     & F.col("healing_status").isin(*WAITING))
        marked = quarantine.toDF().filter(condition).count()
        _retry("mark quarantine rows RESOLVED", lambda: quarantine.update(condition, {
            "healing_status": F.lit(RESOLVED), "resolved_by": F.lit(execution_id),
            "resolved_at": F.current_timestamp()}))
        return {"appended": appended, "marked": marked}

    def close(self) -> None:
        self.replayed.unpersist()


def release_app_id(execution_id: str) -> str:
    return f"recovery_agent_release:{execution_id}"


def replay_group(target_table: Optional[str], record_ids: List[str], column: Optional[str], overlay: Dict,
                 quarantine_path: Optional[str] = None, ref_path: Optional[str] = None,
                 delta_root: Optional[str] = None) -> Replay:
    """Replay one staged group. Raises ReplayError for a request it must not run."""
    if not target_table or not _TARGET_TABLE.match(target_table) or target_table in _SHARED_TABLES:
        raise ReplayError(f"{target_table!r} is not a per-source Silver 2 table (silver2_<source>)")
    if column not in TRACKED_COLUMNS:
        raise ReplayError(f"{column!r} is not a column Silver 2 standardizes")
    if not isinstance(overlay, dict) or list(overlay) != [column] or len(overlay[column]) != 1:
        raise ReplayError(f"the overlay {overlay!r} is not one value of {column}")
    root = (delta_root or config.DELTA_ROOT).rstrip("/")
    return Replay(spark(), target_table, record_ids, column, overlay,
                  quarantine_path or config.QUARANTINE_PATH, ref_path or config.REF_MAPPINGS_PATH,
                  f"{root}/{target_table}")
