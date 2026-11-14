"""Replay tool: reprocess one failed micro-batch or one quarantine group (spec RM1).

The pipeline never has to be re-run from the start to fix a few records: every
record that did not reach Silver 2 is still kept somewhere, with what is needed to
run it again.

  where it waits           what is kept                  replayed through
  silver1_dead_letter      the raw Bronze row (envelope)  Silver 1's own SQL -> silver1_<source>,
                                                          Silver 2's stream takes it from there
  silver2_dead_letter      the Silver 1 row (raw_json)    Silver 2's own standardize + write stage
                                                          -> silver2_<source> / quarantine
  silver2_quarantine       the Silver 1 row (raw_record)  standardize_silver2 with the CURRENT
                                                          ref_canonical_mappings; rows that pass
                                                          go to silver2_<source>, marked RESOLVED

Commands (dry run unless --apply):

  python -m tools.replay list
  python -m tools.replay dead-letter silver1 --source crash1 --batch 12 --rename CrashDate=crash_date
  python -m tools.replay dead-letter silver2 --source crash1 --batch 7
  python -m tools.replay quarantine --source crash1 --column weather_condition --value "FREEZING RAIN"

Every write is idempotent (Delta txnAppId, version 0, one id per replayed batch or
group), so a replay that is run again after a crash does not write a row twice, and
a batch or row that was replayed is marked (dead letter: healing_status REPLAYED,
quarantine: RESOLVED with resolved_by "replay:<id>"), so it is not picked up again.
"""
import argparse
import hashlib
import json
import os
import sys
import time
from typing import Dict, List, Optional

from tools import session

REPLAYED = "REPLAYED"
RESOLVED = "RESOLVED"
WAITING = ("PENDING", "IN_PROGRESS")
_ATTEMPTS = 5
# silver2_job drops these before it writes a valid row to Silver 2.
_NOT_IN_SILVER2 = ("columns_to_remediate", "_fuzzy_audit_entries", "raw_record")
# The Bronze columns Silver 1 reads (bronze_job.py), kept on every silver1_dead_letter row.
_BRONZE_COLUMNS = ("kafka_key", "raw_value", "topic", "partition", "offset", "kafka_timestamp", "ingested_at")


class ReplayRefused(Exception):
    """The replay must not run as asked; nothing was written."""


# --------------------------------------------------------------------------- helpers
def _paths() -> Dict[str, str]:
    session.pipeline_modules()
    import silver1_job
    import silver2_job

    return {"silver1_dead_letter": silver1_job.DEAD_LETTER_PATH,
            "silver2_dead_letter": silver2_job.DEAD_LETTER_PATH,
            "quarantine": silver2_job.QUARANTINE_PATH,
            "ref": silver2_job.REF_MAPPINGS_PATH}


def _source(name: str) -> str:
    session.pipeline_modules()
    from sources import source_names

    (source,) = source_names(name)  # validates the name
    return source


def _retry(what: str, fn):
    """A Delta update that lost a race with the streaming jobs' appends is tried again."""
    from common import _is_delta_conflict

    for attempt in range(1, _ATTEMPTS + 1):
        try:
            return fn()
        except Exception as exc:
            if attempt == _ATTEMPTS or not _is_delta_conflict(exc):
                raise
            print(f"[replay] {what}: concurrent Delta write ({type(exc).__name__}), retry {attempt}", flush=True)
            time.sleep(0.5 * attempt)


def _add_columns(spark, path: str, columns: Dict[str, str]) -> None:
    """Add bookkeeping columns to a shared table the first time (same as Validate does)."""
    from delta.tables import DeltaTable

    existing = set(DeltaTable.forPath(spark, path).toDF().columns)
    missing = {name: kind for name, kind in columns.items() if name not in existing}
    if missing:
        cols = ", ".join(f"{name} {kind}" for name, kind in missing.items())
        _retry(f"add {cols}", lambda: spark.sql(f"ALTER TABLE delta.`{path}` ADD COLUMNS ({cols})"))


def _mark_dead_letter(spark, path: str, condition, replay_id: str) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    _add_columns(spark, path, {"replayed_by": "STRING", "replayed_at": "TIMESTAMP"})
    table = DeltaTable.forPath(spark, path)
    _retry("mark dead letter rows REPLAYED", lambda: table.update(
        condition & (F.col("healing_status") != REPLAYED),
        {"healing_status": F.lit(REPLAYED), "replayed_by": F.lit(replay_id),
         "replayed_at": F.current_timestamp()}))


def _short_hash(*parts) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _parse_renames(pairs: Optional[List[str]]) -> Dict[str, str]:
    renames = {}
    for pair in pairs or []:
        old, sep, new = pair.partition("=")
        if not sep or not old.strip() or not new.strip():
            raise ReplayRefused(f"--rename {pair!r}: use OLD=NEW, e.g. CrashDate=crash_date")
        renames[old.strip()] = new.strip()
    return renames


# --------------------------------------------------------------------------- list
def list_waiting(spark, source: Optional[str] = None, top: int = 30) -> dict:
    """What is waiting to be replayed: dead-lettered batches and quarantine groups."""
    from pyspark.sql import functions as F

    from sources import kafka_topic, table_name

    paths = _paths()
    out = {"silver1_dead_letter": [], "silver2_dead_letter": [], "quarantine": []}
    if session.is_delta(paths["silver1_dead_letter"]):
        dl = spark.read.format("delta").load(paths["silver1_dead_letter"])
        if source:
            dl = dl.filter(F.col("topic") == kafka_topic(source))
        out["silver1_dead_letter"] = [r.asDict() for r in dl.groupBy(
            "topic", "silver1_batch_id", "error_class", "healing_status",
            F.concat_ws(",", "_expected_only").alias("expected_only"),
            F.concat_ws(",", "_source_only").alias("source_only"),
        ).count().orderBy("topic", "silver1_batch_id").collect()]
    if session.is_delta(paths["silver2_dead_letter"]):
        dl = spark.read.format("delta").load(paths["silver2_dead_letter"])
        if source:
            dl = dl.filter(F.col("target_table") == table_name("silver2", source))
        out["silver2_dead_letter"] = [r.asDict() for r in dl.groupBy(
            "target_table", "silver2_batch_id", "error_type", "healing_status",
        ).count().orderBy("target_table", "silver2_batch_id").collect()]
    if session.is_delta(paths["quarantine"]):
        q = spark.read.format("delta").load(paths["quarantine"]).filter(F.col("healing_status") == "PENDING")
        if source:
            q = q.filter(F.col("target_table") == table_name("silver2", source))
        groups = (q.select("target_table", "silver2_batch_id", F.explode("columns_to_remediate").alias("column", "value"))
                  .groupBy("target_table", "column", F.upper(F.trim("value")).alias("value"))
                  .agg(F.count(F.lit(1)).alias("count"), F.countDistinct("silver2_batch_id").alias("batches"))
                  .orderBy(F.desc("count")).limit(top))
        out["quarantine"] = [r.asDict() for r in groups.collect()]
    return out


# --------------------------------------------------------------------------- dead letter: Silver 1
def _rewrite_envelope(raw_value: str, renames: Dict[str, str], drops: List[str]):
    """The raw envelope with the payload's keys renamed / dropped, and the payload's keys."""
    envelope = json.loads(raw_value)
    if not isinstance(envelope, dict):
        raise ValueError("not a JSON object")
    payload = envelope.get("payload") if isinstance(envelope.get("payload"), dict) else None
    target = payload if payload is not None else envelope
    for old, new in renames.items():
        if old in target:
            if new in target:
                raise ValueError(f"both {old!r} and {new!r} are present")
            target[new] = target.pop(old)
    for name in drops:
        target.pop(name, None)
    metadata = {"record_id", "batch_id", "table_name", "ingestion_ts", "payload"}
    keys = set(target) if payload is not None else set(target) - metadata
    return json.dumps(envelope, ensure_ascii=False), keys


def replay_silver1_dead_letter(spark, source: str, batch: int, renames: Dict[str, str],
                               drops: List[str], apply: bool) -> dict:
    """Re-run Silver 1 on a batch its schema guard parked, after the cause is fixed.

    `renames` / `drops` fix the payload keys first (the engineer's RENAME_COLUMN, which
    has no automated executor). The whole batch must then match the column contract,
    or nothing is written. Rows that pass are cleaned by Silver 1's own SQL into
    silver1_<source>; the Silver 2 stream standardizes them like any other row.
    """
    from pyspark.sql import functions as F

    import silver1_job
    from schema_guard import schema_difference
    from sources import kafka_topic, table_path

    path = _paths()["silver1_dead_letter"]
    if not session.is_delta(path):
        raise ReplayRefused(f"no Silver 1 dead letter at {path}")
    where = (F.col("topic") == kafka_topic(source)) & (F.col("silver1_batch_id") == int(batch))
    parked = spark.read.format("delta").load(path).filter(where)
    pending = parked.filter(F.col("healing_status") != REPLAYED).select(*_BRONZE_COLUMNS, "record_id").collect()
    result = {"layer": "silver1", "source": source, "batch": batch, "renames": renames, "drops": drops,
              "parked": parked.count(), "to_replay": len(pending), "applied": False}
    if not pending:
        result["note"] = "nothing waiting (no such batch, or already REPLAYED)"
        return result

    optional = json.loads(os.getenv("SILVER1_OPTIONAL_COLUMNS", "[]"))
    expected = [c for c in silver1_job.CRASH_COLUMNS if c not in optional]
    rows, problems = [], []
    for row in pending:
        try:
            raw_value, keys = _rewrite_envelope(row["raw_value"], renames, drops)
        except ValueError as exc:
            problems.append({"record_id": row["record_id"], "problem": f"unreadable: {exc}"})
            continue
        expected_only, source_only = schema_difference(keys, expected)
        expected_only = [c for c in expected_only if c not in optional]
        if expected_only or source_only:
            problems.append({"record_id": row["record_id"], "expected_only": expected_only,
                             "source_only": source_only})
            continue
        rows.append({**row.asDict(), "raw_value": raw_value})
    result["problems"] = problems[:20]
    result["problem_count"] = len(problems)
    if problems:
        result["note"] = ("refused: these records still do not match the column contract; "
                          "fix them with --rename OLD=NEW / --drop COLUMN (nothing written)")
        return result

    replay_id = f"replay:silver1:{source}:{batch}"
    result["replay_id"] = replay_id
    if not apply:
        result["note"] = "dry run: add --apply to write"
        return result
    bronze = spark.createDataFrame([{c: r[c] for c in _BRONZE_COLUMNS} for r in rows],
                                   parked.select(*_BRONZE_COLUMNS).schema)
    # Same function the stream runs; idempotent on (replay id + "_clean", 0).
    silver1_job.write_clean_batch(bronze, 0, table_path("silver1", source), replay_id)
    _mark_dead_letter(spark, path, where, replay_id)
    result["applied"] = True
    result["written"] = len(rows)
    return result


# --------------------------------------------------------------------------- dead letter: Silver 2
def replay_silver2_dead_letter(spark, source: str, batch: int, apply: bool) -> dict:
    """Re-run Silver 2 on a micro-batch whose transform failed (after the cause is fixed).

    The parked Silver 1 rows go through the same two stages as the stream
    (silver2_job.standardize_and_count + write_standardized): valid rows to
    silver2_<source>, failing rows to the quarantine (labelled with the original
    batch id), fuzzy fixes to the fuzzy audit. If the transform fails again nothing is
    written and the batch stays parked.
    """
    from pyspark.sql import functions as F

    import silver2_job
    from ref_mappings import load_mappings_dict
    from sources import is_day_first, table_name, table_path

    path = _paths()["silver2_dead_letter"]
    if not session.is_delta(path):
        raise ReplayRefused(f"no Silver 2 dead letter at {path}")
    target_table = table_name("silver2", source)
    where = (F.col("target_table") == target_table) & (F.col("silver2_batch_id") == str(batch))
    parked = spark.read.format("delta").load(path).filter(where)
    pending = parked.filter(F.col("healing_status") != REPLAYED)
    result = {"layer": "silver2", "source": source, "batch": batch, "parked": parked.count(),
              "to_replay": pending.count(), "applied": False}
    if not result["to_replay"]:
        result["note"] = "nothing waiting (no such batch, or already REPLAYED)"
        return result

    schema = spark.read.format("delta").load(table_path("silver1", source)).schema
    batch_df = pending.select(F.from_json("raw_json", schema).alias("r")).select("r.*")
    mappings = load_mappings_dict(spark, silver2_job.REF_MAPPINGS_PATH)
    try:
        transformed, stats = silver2_job.standardize_and_count(batch_df, mappings,
                                                               day_first=is_day_first(source))
    except Exception as exc:
        result["note"] = f"refused: the transform still fails ({type(exc).__name__}: {str(exc)[:300]})"
        return result
    try:
        total, quarantined, fuzzy = stats
        result.update({"valid": total - quarantined, "quarantined": quarantined, "fuzzy_fixed": fuzzy})
        replay_id = f"replay:silver2:{source}:{batch}"
        result["replay_id"] = replay_id
        if not apply:
            result["note"] = "dry run: add --apply to write"
            return result
        silver2_job.write_standardized(transformed, stats, silver2_path=table_path("silver2", source),
                                       txn_app_id=replay_id, txn_version=0, target_table=target_table,
                                       batch_label=str(batch))
    finally:
        transformed.unpersist()
    _mark_dead_letter(spark, path, where, replay_id)
    result["applied"] = True
    return result


# --------------------------------------------------------------------------- quarantine
def agent_held(spark, target_table: str) -> Dict[str, str]:
    """record_id -> execution_id of every record of `target_table` that Remediate staged
    with a SUCCESS (remediation_audit). Waiting rows among them belong to a workflow."""
    from pyspark.sql import functions as F

    from sources import shared_path

    path = os.environ.get("REMEDIATION_AUDIT_PATH", shared_path("remediation_audit"))
    if not session.is_delta(path):
        return {}
    rows = (spark.read.format("delta").load(path)
            .filter((F.col("status") == "SUCCESS") & (F.col("target_table") == target_table))
            .select("execution_id", F.explode("record_ids").alias("record_id")).collect())
    return {r["record_id"]: r["execution_id"] for r in rows}


def replay_quarantine(spark, source: str, column: Optional[str] = None, value: Optional[str] = None,
                      batch: Optional[str] = None, record_ids: Optional[List[str]] = None,
                      include_in_progress: bool = False, apply: bool = False, force: bool = False) -> dict:
    """Re-run waiting quarantine rows with the current reference table and release the
    ones that now pass.

    For rows that became fixable without the agent: a variant Learn promoted into
    ref_canonical_mappings (rows that failed two columns stay quarantined until both
    are known), canonical values an engineer added, or a code fix. No overlay is used
    and nothing is promoted: the replay trusts ref_canonical_mappings as the Silver 2
    job does. Rows that still fail stay PENDING, untouched.

    Records the agent is working on are left out: a row that Remediate staged (a
    SUCCESS execution in remediation_audit lists it) and that is still waiting belongs
    to an open workflow (being validated, waiting for an engineer's approval, or a fix
    that did not hold). Releasing it here would make that workflow's Validate stop at
    rows_conserved and open another round for rows already fixed. `force` takes them
    anyway (e.g. after the engineer dismissed the case).
    """
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from common import idempotent_append
    from ref_mappings import load_mappings_dict
    from silver2_job import standardize_silver2, write_fuzzy_audit
    from sources import is_day_first, table_name, table_path

    paths = _paths()
    if not session.is_delta(paths["quarantine"]):
        raise ReplayRefused(f"no quarantine at {paths['quarantine']}")
    if value is not None and not column:
        raise ReplayRefused("--value needs --column")
    target_table = table_name("silver2", source)
    statuses = list(WAITING) if include_in_progress else ["PENDING"]
    q = spark.read.format("delta").load(paths["quarantine"])
    selected = q.filter((F.col("target_table") == target_table) & F.col("healing_status").isin(*statuses))
    if column:
        selected = selected.filter(F.array_contains(F.map_keys("columns_to_remediate"), column))
    if value is not None:
        selected = selected.filter(
            F.upper(F.trim(F.element_at("columns_to_remediate", F.lit(column)))) == value.strip().upper())
    if batch is not None:
        selected = selected.filter(F.col("silver2_batch_id") == str(batch))
    if record_ids:
        selected = selected.filter(F.col("record_id").isin(*record_ids))

    held = agent_held(spark, target_table)
    held_selected = sorted({r["record_id"] for r in selected.select("record_id").collect()} & set(held))
    if held_selected and not force:
        selected = selected.filter(~F.col("record_id").isin(*held_selected))

    mappings = load_mappings_dict(spark, paths["ref"])
    replayed = standardize_silver2(selected.select("raw_record.*"), mappings,
                                   day_first=is_day_first(source)).cache()
    try:
        report = replayed.select("record_id", F.map_keys("columns_to_remediate").alias("after")).collect()
        passing = sorted(r["record_id"] for r in report if not r["after"])
        still: Dict[str, int] = {}
        for r in report:
            for name in r["after"] or []:
                still[name] = still.get(name, 0) + 1
        silver2_path = table_path("silver2", source)
        in_silver2 = set()
        if passing and session.is_delta(silver2_path):
            in_silver2 = {r["record_id"] for r in spark.read.format("delta").load(silver2_path)
                          .filter(F.col("record_id").isin(*passing)).select("record_id").distinct().collect()}
        result = {"source": source, "target_table": target_table, "column": column, "value": value,
                  "batch": batch, "selected": len(report), "now_pass": len(passing),
                  "still_failing": len(report) - len(passing), "still_failing_by_column": still,
                  "already_in_silver2": len(in_silver2), "applied": False,
                  "held_by_agent": 0 if force else len(held_selected),
                  "held_by_executions": sorted({held[r] for r in held_selected})[:10]}
        if held_selected and not force:
            result["held_note"] = ("left out: staged by the agent and still waiting "
                                   "(python -m agent.audit <execution_id>); --force takes them")
        if not passing:
            result["note"] = "nothing to release: no selected row passes with the current reference table"
            return result
        replay_id = f"replay:{_short_hash(target_table, passing)}"
        result["replay_id"] = replay_id
        if not apply:
            result["note"] = "dry run: add --apply to write"
            return result

        to_append = [rid for rid in passing if rid not in in_silver2]
        if to_append:
            rows = replayed.filter(F.col("record_id").isin(*to_append)).drop(*_NOT_IN_SILVER2)
            # Idempotent: Delta skips the append if this replay id already wrote it.
            idempotent_append(rows, silver2_path, f"{replay_id}:release", 0)
            # Another column fixed by the fuzzy match on the way: logged like the stream does.
            fuzzy = replayed.filter(F.col("record_id").isin(*to_append))
            if fuzzy.filter(F.size("_fuzzy_audit_entries") > 0).limit(1).count():
                write_fuzzy_audit(fuzzy, txn_app_id=replay_id, txn_version=0, batch_label=replay_id,
                                  target_table=target_table)
        _add_columns(spark, paths["quarantine"], {"resolved_by": "STRING", "resolved_at": "TIMESTAMP"})
        table = DeltaTable.forPath(spark, paths["quarantine"])
        condition = ((F.col("target_table") == target_table) & F.col("record_id").isin(*passing)
                     & F.col("healing_status").isin(*WAITING))
        _retry("mark quarantine rows RESOLVED", lambda: table.update(condition, {
            "healing_status": F.lit(RESOLVED), "resolved_by": F.lit(replay_id),
            "resolved_at": F.current_timestamp()}))
        result.update({"applied": True, "appended": len(to_append), "marked_resolved": len(passing)})
        return result
    finally:
        replayed.unpersist()


# --------------------------------------------------------------------------- CLI
def _print_table(title: str, rows: List[dict]) -> None:
    print(f"\n{title}: {len(rows) or 'none'}")
    if not rows:
        return
    cols = list(rows[0])
    widths = {c: max(len(c), *(len(str(r[c])) for r in rows)) for c in cols}
    print("  " + "  ".join(c.ljust(widths[c]) for c in cols))
    for r in rows:
        print("  " + "  ".join(str(r[c]).ljust(widths[c]) for c in cols))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tools.replay", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="dead-lettered batches and quarantine groups waiting")
    p_list.add_argument("--source", help="only this source, e.g. crash1")
    p_list.add_argument("--top", type=int, default=30, help="quarantine groups to show (largest first)")

    p_dl = sub.add_parser("dead-letter", help="reprocess one dead-lettered micro-batch")
    p_dl.add_argument("layer", choices=["silver1", "silver2"])
    p_dl.add_argument("--source", required=True, help="e.g. crash1")
    p_dl.add_argument("--batch", type=int, required=True, help="the parked micro-batch id (see list)")
    p_dl.add_argument("--rename", action="append", metavar="OLD=NEW",
                      help="silver1 only: rename a payload column before replaying (repeatable)")
    p_dl.add_argument("--drop", action="append", metavar="COLUMN",
                      help="silver1 only: drop an unexpected payload column (repeatable)")
    p_dl.add_argument("--apply", action="store_true", help="write (default: dry run)")

    p_q = sub.add_parser("quarantine", help="re-run waiting quarantine rows with the current reference table")
    p_q.add_argument("--source", required=True, help="e.g. crash1")
    p_q.add_argument("--column", help="only rows failing this column")
    p_q.add_argument("--value", help="only rows whose failing value is this (needs --column)")
    p_q.add_argument("--batch", help="only this silver2_batch_id")
    p_q.add_argument("--record-id", action="append", dest="record_ids", help="only these records (repeatable)")
    p_q.add_argument("--include-in-progress", action="store_true",
                     help="also rows the recovery trigger marked IN_PROGRESS")
    p_q.add_argument("--force", action="store_true",
                     help="also records the agent staged and is still working on (see agent.audit)")
    p_q.add_argument("--apply", action="store_true", help="write (default: dry run)")
    p_q.add_argument("--json", action="store_true")
    for p in (p_list, p_dl):
        p.add_argument("--json", action="store_true")

    args = parser.parse_args(argv)
    try:
        source = _source(args.source) if getattr(args, "source", None) else None
        spark = session.spark(f"Replay-{args.command}")
        if args.command == "list":
            result = list_waiting(spark, source, args.top)
            if not args.json:
                _print_table("Silver 1 dead letter (python -m tools.replay dead-letter silver1 ...)",
                             result["silver1_dead_letter"])
                _print_table("Silver 2 dead letter (python -m tools.replay dead-letter silver2 ...)",
                             result["silver2_dead_letter"])
                _print_table("Quarantine groups still PENDING (python -m tools.replay quarantine ...)",
                             result["quarantine"])
                return 0
        elif args.command == "dead-letter":
            if args.layer == "silver1":
                result = replay_silver1_dead_letter(spark, source, args.batch, _parse_renames(args.rename),
                                                    args.drop or [], args.apply)
            else:
                if args.rename or args.drop:
                    raise ReplayRefused("--rename / --drop only apply to the silver1 dead letter")
                result = replay_silver2_dead_letter(spark, source, args.batch, args.apply)
        else:
            result = replay_quarantine(spark, source, args.column, args.value, args.batch, args.record_ids,
                                       args.include_in_progress, args.apply, args.force)
    except ReplayRefused as exc:
        print(f"[replay] refused: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, default=str, ensure_ascii=False))
    refused = str(result.get("note", "")).startswith("refused")
    return 2 if refused else 0


if __name__ == "__main__":
    sys.exit(main())
