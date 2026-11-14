"""Fault injector: reproduce the pipeline's failures on demand, with the right answer kept.

It takes records that went through the pipeline cleanly, corrupts one thing in each
(tools/faults.py: typos, ambiguous values, unseen variants and formats, unknown
values, a missing mandatory value, schema changes, broken messages), and publishes
them to the source's Kafka topic exactly as the producer does. From there they meet
the real Bronze, Silver 1, Silver 2, quarantine, lookup and recovery agent.

Every injected record is written to the Delta table `fault_injections` first, with
its ground truth: the original value, the value Silver 2 should end with, the fix a
correct recovery makes (faults.expected_fix), where the record should end up
(intended_path) and where the pipeline's own code says it will (predicted_path,
computed with Silver 1's SQL and Silver 2's standardize_silver2 before publishing).
`status` follows each record through the pipeline's tables afterwards. That pair is
what the evaluation (Task 111) measures from.

  python -m tools.fault_injector kinds                     # the catalog
  python -m tools.fault_injector inject jule --dry-run     # plan + prediction, nothing sent
  python -m tools.fault_injector inject jule               # data/faults/jule.json
  python -m tools.fault_injector inject --kind ambiguous --column crash_month --value Jule --base JUNE --count 5
  python -m tools.fault_injector runs
  python -m tools.fault_injector status <run_id> [--records] [--json]

Injected records are marked: crash_record_id = "FAULT-<run_id>-<seq>", record_id a
uuid5 of (run_id, seq). They are copies of real crashes, so anything that counts
crashes (Gold metrics) should leave out crash_record_id LIKE 'FAULT-%'.
"""
import argparse
import json
import os
import random
import re
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from tools import faults, session

FAULTS_DIR = os.environ.get("FAULTS_DIR", os.path.join(session.REPO, "data", "faults"))
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DEFAULT_INTERVAL = float(os.environ.get("FAULT_BATCH_INTERVAL_SECONDS", "15"))
_RUN_ID = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_NAMESPACE = uuid.UUID("6f1d3c3e-6a52-4e0b-9a51-5e1f7c0b1080")
_METADATA = {"record_id", "batch_id", "table_name", "ingestion_ts", "payload"}

# Observed outcomes (status), in the order a record can move through them.
NOT_ARRIVED = "NOT_ARRIVED"            # not in Silver 1 or a dead letter yet
IN_SILVER1 = "IN_SILVER1"              # cleaned, Silver 2 has not taken it yet
DEAD_LETTER = "DEAD_LETTER"            # parked by Silver 1's schema guard
QUARANTINED = "QUARANTINED"            # waiting in silver2_quarantine (PENDING / IN_PROGRESS)
HEALED = "HEALED"                      # released into Silver 2 by the agent (Validate)
HEALED_BY_REPLAY = "HEALED_BY_REPLAY"  # released by tools.replay quarantine
REPLAYED = "REPLAYED"                  # a dead-lettered record re-run by tools.replay, now in Silver 2
FUZZY_FIXED = "FUZZY_FIXED"            # Silver 2 corrected it on the way in
CLEAN = "CLEAN"                        # in Silver 2 without quarantine or fuzzy (e.g. a learned variant)

MANIFEST_SCHEMA = (
    "run_id string, scenario string, fault_index int, kind string, source string, folder string, "
    "target_table string, kafka_topic string, column string, renamed_to string, batch int, seq int, "
    "record_id string, crash_record_id string, base_record_id string, original_raw string, "
    "injected_raw string, original_value string, expected_value string, expected_action string, "
    "expected_params string, intended_path string, predicted_path string, predicted_value string, "
    "predicted_failures string, as_intended boolean, planned_at timestamp, published_at timestamp"
)


def manifest_path() -> str:
    session.pipeline_modules()
    from sources import shared_path

    return os.environ.get("FAULT_INJECTIONS_PATH", shared_path("fault_injections"))


# --------------------------------------------------------------------------- scenario
def load_scenario(name: str) -> dict:
    """A scenario file: data/faults/<name>.json, or a path."""
    path = name if os.path.isfile(name) else os.path.join(FAULTS_DIR, f"{name}.json")
    if not os.path.isfile(path):
        known = sorted(f[:-5] for f in os.listdir(FAULTS_DIR) if f.endswith(".json")) \
            if os.path.isdir(FAULTS_DIR) else []
        raise faults.FaultSpecError(f"no scenario {name!r} (looked for {path}; known: {known})")
    with open(path, encoding="utf-8") as fh:
        scenario = json.load(fh)
    scenario.setdefault("name", os.path.splitext(os.path.basename(path))[0])
    if not scenario.get("faults"):
        raise faults.FaultSpecError(f"scenario {path} has no faults")
    return scenario


def adhoc_scenario(args) -> dict:
    fault = {k: v for k, v in {
        "kind": args.kind, "source": args.source, "column": args.column, "value": args.value,
        "base": args.base, "to": args.to, "count": args.count, "batches": args.batches}.items() if v is not None}
    return {"name": f"adhoc-{args.kind}", "faults": [fault]}


def new_run_id(scenario_name: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%S")
    base = re.sub(r"[^a-z0-9-]+", "-", scenario_name.lower()).strip("-")[:40] or "run"
    return f"{base}-{stamp}"


# --------------------------------------------------------------------------- plan
def _base_rows(spark, spec: dict, n: int, seed: int) -> List[dict]:
    """Records of this source that reached Silver 2 straight from the stream (never
    quarantined, not injected before), with their Silver 1 row and Silver 2 value."""
    from pyspark.sql import functions as F

    from sources import shared_path, table_name, table_path

    source, column = spec["source"], spec.get("column")
    s1_path, s2_path = table_path("silver1", source), table_path("silver2", source)
    for path in (s1_path, s2_path):
        if not session.is_delta(path):
            raise faults.FaultSpecError(f"{path} does not exist: run the pipeline for {source} first")
    s2 = spark.read.format("delta").load(s2_path)
    truth = F.col(column) if column and column in s2.columns else F.lit(None)
    cand = s2.select("record_id", truth.cast("string").alias("_truth_text"), truth.alias("_truth"))
    if faults.KINDS[spec["kind"]]["needs_value"] or spec.get("base") or spec.get("base_any"):
        cand = cand.filter(F.col("_truth").isNotNull())
    if spec.get("base"):
        cand = cand.filter(F.upper("_truth_text") == spec["base"])
    elif spec.get("base_any"):
        cand = cand.filter(F.upper("_truth_text").isin(*spec["base_any"]))
    quarantine = shared_path("silver2_quarantine")
    if session.is_delta(quarantine):
        released = (spark.read.format("delta").load(quarantine)
                    .filter(F.col("target_table") == table_name("silver2", source)).select("record_id"))
        cand = cand.join(released, "record_id", "left_anti")
    s1 = spark.read.format("delta").load(s1_path).filter(
        ~F.coalesce(F.col("crash_record_id"), F.lit("")).startswith("FAULT-"))
    rows = (cand.dropDuplicates(["record_id"]).join(s1.dropDuplicates(["record_id"]), "record_id")
            .orderBy(F.rand(seed)).limit(n).collect())
    return [r.asDict() for r in rows]


def fuzzy_fixes(column: str, value: str) -> bool:
    """Would Silver 2's fuzzy match correct `value` (ref_mappings.fuzzy_canonical's
    guardrails: at least 4 characters, a close best anchor, a runner-up 2 edits further)?
    Used to pick typos that are typos; the prediction then runs the real thing."""
    from ref_mappings import CANONICAL_ANCHORS, _score_anchors

    key = value.strip().upper()
    if len(key) < 4 or column not in CANONICAL_ANCHORS:
        return False
    best, _, second = _score_anchors(key, CANONICAL_ANCHORS[column])
    second = 1000 if second is None else second
    return best <= max(1, int(len(key) * 0.25)) and second - best >= 2


def plan(spark, scenario: dict, run_id: str, seed: int = 7) -> List[dict]:
    """The records to inject: manifest row + envelope, nothing sent or written."""
    import silver1_job
    from sources import kafka_topic, source_names, table_name

    known = set(source_names())
    planned_at = datetime.now(timezone.utc)
    records, seq = [], 0
    for index, entry in enumerate(scenario["faults"]):
        spec = faults.normalize_spec(entry, index, set(silver1_job.CRASH_COLUMNS))
        if spec["source"] not in known:
            raise faults.FaultSpecError(f"fault {index}: source {spec['source']!r} is not in SOURCES {sorted(known)}")
        wanted = spec["count"] * spec["batches"]
        rng = random.Random(f"{seed}:{run_id}:{index}")
        made = 0
        for row in _base_rows(spark, spec, wanted * 4, seed + index):
            if made == wanted:
                break
            column = spec.get("column")
            original_value = faults.canonical(column, row.get("_truth")) if column else None
            injected = faults.corrupt(spec, original_value, rng, accept_typo=fuzzy_fixes)
            if faults.KINDS[spec["kind"]]["needs_value"] and injected is None:
                continue
            if spec["kind"] == "unknown_value" and injected is None:
                continue
            seq += 1
            batch = made // spec["count"] + 1
            made += 1
            record_id = str(uuid.uuid5(_NAMESPACE, f"{run_id}:{seq}"))
            payload = faults.build_payload(row, silver1_job.CRASH_COLUMNS)
            payload["crash_record_id"] = f"FAULT-{run_id}-{seq:04d}"
            payload = faults.apply_fault(spec, payload, injected)
            envelope = {"record_id": record_id, "batch_id": batch, "table_name": row.get("table_name"),
                        "ingestion_ts": planned_at.isoformat(), "payload": payload}
            fix = faults.expected_fix(spec, original_value)
            records.append({
                "run_id": run_id, "scenario": scenario.get("name"), "fault_index": index,
                "kind": spec["kind"], "source": spec["source"], "folder": row.get("table_name"),
                "target_table": table_name("silver2", spec["source"]), "kafka_topic": kafka_topic(spec["source"]),
                "column": column, "renamed_to": spec.get("to"), "batch": batch, "seq": seq,
                "record_id": record_id, "crash_record_id": f"FAULT-{run_id}-{seq:04d}",
                "base_record_id": row["record_id"],
                "original_raw": None if not column or row.get(column) is None else str(row.get(column)),
                "injected_raw": injected if spec["kind"] not in ("schema_rename", "schema_missing", "corrupt_record")
                else None,
                "original_value": original_value,
                "expected_value": None if spec["kind"] in ("unknown_value", "corrupt_record") else original_value,
                "expected_action": fix["action"], "expected_params": json.dumps(fix["params"], ensure_ascii=False),
                "intended_path": faults.KINDS[spec["kind"]]["intended"],
                "predicted_path": None, "predicted_value": None, "predicted_failures": None, "as_intended": None,
                "planned_at": planned_at, "published_at": None,
                "_message": faults.message(envelope, spec),
            })
        if made < wanted:
            print(f"[inject] fault {index} ({spec['kind']} {spec.get('column') or ''}): only {made} of "
                  f"{wanted} suitable clean records in {spec['source']}", file=sys.stderr, flush=True)
    return records


def _schema_ok(message: str, expected) -> bool:
    """What Silver 1's schema guard decides (schema_guard.process_source_batch)."""
    try:
        envelope = json.loads(message)
    except ValueError:
        return False
    if not isinstance(envelope, dict):
        return False
    payload = envelope.get("payload")
    keys = set(payload) if isinstance(payload, dict) else set(envelope) - _METADATA
    return keys == set(expected)


def predict(spark, records: List[dict]) -> None:
    """Fill predicted_* with what the pipeline's own code makes of each record now:
    Silver 1's schema guard and SQL, then standardize_silver2 with the current
    reference table, in the source's date order."""
    from pyspark.sql import functions as F

    import silver1_job
    from ref_mappings import load_mappings_dict
    from silver2_job import REF_MAPPINGS_PATH, standardize_silver2
    from sources import is_day_first

    optional = json.loads(os.getenv("SILVER1_OPTIONAL_COLUMNS", "[]"))
    expected = [c for c in silver1_job.CRASH_COLUMNS if c not in optional]
    mappings = load_mappings_dict(spark, REF_MAPPINGS_PATH)
    by_source: Dict[str, List[dict]] = {}
    for rec in records:
        if not _schema_ok(rec["_message"], expected):
            rec.update({"predicted_path": faults.DEAD_LETTER, "predicted_failures": None})
        else:
            by_source.setdefault(rec["source"], []).append(rec)
    for source, recs in by_source.items():
        bronze = spark.createDataFrame([(r["_message"], r["kafka_topic"]) for r in recs],
                                       "raw_value string, topic string").withColumn(
            "kafka_timestamp", F.current_timestamp())
        std = standardize_silver2(silver1_job.clean_batch(bronze), mappings, day_first=is_day_first(source))
        out = {row["record_id"]: row.asDict(recursive=True) for row in std.collect()}
        for rec in recs:
            row = out.get(rec["record_id"], {})
            failures = row.get("columns_to_remediate") or {}
            fuzzy = {e["field"] for e in row.get("_fuzzy_audit_entries") or []}
            column = rec["column"]
            if failures:
                path = faults.QUARANTINED
            elif column in fuzzy:
                path = faults.FUZZY_FIXED
            else:
                path = faults.CLEAN
            rec.update({"predicted_path": path,
                        "predicted_value": faults.canonical(column, row.get(column)) if column else None,
                        "predicted_failures": json.dumps(failures, ensure_ascii=False)})
    for rec in records:
        rec["as_intended"] = rec["predicted_path"] == rec["intended_path"]


# --------------------------------------------------------------------------- manifest + publish
def _manifest_rows(records: List[dict]) -> List[dict]:
    return [{k: v for k, v in r.items() if not k.startswith("_")} for r in records]


def run_exists(spark, run_id: str) -> bool:
    from pyspark.sql import functions as F

    path = manifest_path()
    return session.is_delta(path) and spark.read.format("delta").load(path).filter(
        F.col("run_id") == run_id).limit(1).count() > 0


def write_manifest(spark, records: List[dict]) -> None:
    spark.createDataFrame(_manifest_rows(records), MANIFEST_SCHEMA).write.format("delta") \
        .mode("append").save(manifest_path())


def mark_published(spark, run_id: str, batch: int, at: datetime) -> None:
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    DeltaTable.forPath(spark, manifest_path()).update(
        (F.col("run_id") == run_id) & (F.col("batch") == batch),
        {"published_at": F.lit(at).cast("timestamp")})


def _producer():
    from confluent_kafka import Producer

    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "enable.idempotence": True, "acks": "all"})


def publish(spark, records: List[dict], interval: float, producer=None) -> None:
    """Send batch 1, wait `interval` seconds, send batch 2, ... so a fault that must
    recur across micro-batches (promotion: N occurrences in M batches) lands in
    separate Silver 2 micro-batches (they trigger every 10 s)."""
    producer = producer or _producer()
    batches = sorted({r["batch"] for r in records})
    for i, batch in enumerate(batches):
        errors = []
        part = [r for r in records if r["batch"] == batch]
        for rec in part:
            producer.produce(rec["kafka_topic"], key=rec["record_id"].encode(), value=rec["_message"].encode(),
                             on_delivery=lambda err, _msg: errors.append(err) if err else None)
        remaining = producer.flush(30)
        if remaining or errors:
            raise RuntimeError(f"Kafka delivery failed for batch {batch} ({remaining} pending, {errors[:1]})")
        at = datetime.now(timezone.utc)
        mark_published(spark, records[0]["run_id"], batch, at)
        print(f"[inject] batch {batch}: published {len(part)} record(s) at {at.isoformat()}", flush=True)
        if i + 1 < len(batches):
            time.sleep(interval)


def inject(spark, scenario: dict, run_id: Optional[str] = None, seed: int = 7, dry_run: bool = False,
           interval: float = DEFAULT_INTERVAL, producer=None) -> List[dict]:
    run_id = run_id or new_run_id(scenario.get("name", "run"))
    if not _RUN_ID.match(run_id):
        raise faults.FaultSpecError(f"run id {run_id!r}: lower-case letters, digits and '-' only")
    if not dry_run and run_exists(spark, run_id):
        raise faults.FaultSpecError(f"run {run_id!r} was already injected; pick another --run-id")
    records = plan(spark, scenario, run_id, seed)
    if not records:
        raise faults.FaultSpecError("no record could be planned (no suitable clean records)")
    predict(spark, records)
    if not dry_run:
        write_manifest(spark, records)  # first, so nothing is ever sent without its ground truth
        publish(spark, records, interval, producer)
    return records


# --------------------------------------------------------------------------- status
def _rows_by_id(df, key="record_id") -> Dict[str, dict]:
    return {r[key]: r.asDict(recursive=True) for r in df.collect()}


def status(spark, run_id: str) -> List[dict]:
    """Where every record of a run is now, and whether it ended with the right value."""
    from pyspark.sql import functions as F

    from sources import shared_path, table_path

    path = manifest_path()
    if not session.is_delta(path):
        raise faults.FaultSpecError(f"no fault_injections table at {path}")
    manifest = [r.asDict() for r in spark.read.format("delta").load(path)
                .filter(F.col("run_id") == run_id).orderBy("seq").collect()]
    if not manifest:
        raise faults.FaultSpecError(f"no run {run_id!r} (python -m tools.fault_injector runs)")
    ids = [m["record_id"] for m in manifest]
    ids_df = spark.createDataFrame([(i,) for i in ids], "rid string")

    def load(p):
        return spark.read.format("delta").load(p) if session.is_delta(p) else None

    dead = {}
    dl = load(shared_path("silver1_dead_letter"))
    if dl is not None:
        # A corrupt message has no readable record_id: find it inside the raw value.
        cols = ["healing_status", "error_class"] + [c for c in ("replayed_by",) if c in dl.columns]
        hits = dl.join(F.broadcast(ids_df), F.expr("instr(raw_value, rid) > 0")).select("rid", *cols)
        dead = _rows_by_id(hits, "rid")
    quarantine = {}
    q = load(shared_path("silver2_quarantine"))
    if q is not None:
        extra = [c for c in ("resolved_by", "resolved_at") if c in q.columns]
        quarantine = _rows_by_id(q.filter(F.col("record_id").isin(*ids)).select(
            "record_id", "healing_status", "quarantined_at", "silver2_batch_id",
            F.to_json("columns_to_remediate").alias("failures"), *extra))
    fuzzy = {}
    fz = load(shared_path("silver2_fuzzy_audit"))
    if fz is not None:
        for r in fz.filter(F.col("record_id").isin(*ids)).select("record_id", "field").collect():
            fuzzy.setdefault(r["record_id"], set()).add(r["field"])
    silver1, silver2 = set(), {}
    for source in sorted({m["source"] for m in manifest}):
        s1 = load(table_path("silver1", source))
        if s1 is not None:
            silver1 |= {r["record_id"] for r in s1.filter(F.col("record_id").isin(*ids)).select("record_id").collect()}
        s2 = load(table_path("silver2", source))
        if s2 is not None:
            silver2.update(_rows_by_id(s2.filter(F.col("record_id").isin(*ids))))
    executions = {}
    audit = load(os.environ.get("REMEDIATION_AUDIT_PATH", shared_path("remediation_audit")))
    resolved_by = {q_["resolved_by"] for q_ in quarantine.values() if q_.get("resolved_by")}
    if audit is not None and resolved_by:
        for r in audit.filter(F.col("execution_id").isin(*resolved_by) & (F.col("status") == "SUCCESS")).select(
                "execution_id", "failure_id", "action", "action_params", "action_source", "confidence",
                "executed_at").collect():
            executions.setdefault(r["execution_id"], r.asDict())

    out = []
    for m in manifest:
        rid, column = m["record_id"], m["column"]
        qrow, s2row, dlrow = quarantine.get(rid), silver2.get(rid), dead.get(rid)
        resolver = (qrow or {}).get("resolved_by")
        execution = executions.get(resolver) if resolver else None
        if s2row is not None:
            if qrow and qrow["healing_status"] == "RESOLVED":
                outcome = HEALED_BY_REPLAY if str(resolver or "").startswith("replay:") else HEALED
            elif dlrow:
                outcome = REPLAYED
            elif column in fuzzy.get(rid, set()):
                outcome = FUZZY_FIXED
            else:
                outcome = CLEAN
        elif qrow:
            outcome = QUARANTINED
        elif dlrow:
            outcome = DEAD_LETTER
        elif rid in silver1:
            outcome = IN_SILVER1
        else:
            outcome = NOT_ARRIVED
        value = faults.canonical(column, s2row.get(column)) if (s2row is not None and column) else None
        correct = None
        if s2row is not None and m["expected_value"] is not None and column:
            correct = value == m["expected_value"]
        healed_at = (qrow or {}).get("resolved_at")
        published = m["published_at"]
        out.append({
            **{k: m[k] for k in ("seq", "kind", "source", "column", "batch", "record_id", "injected_raw",
                                 "expected_value", "expected_action", "intended_path", "predicted_path")},
            "outcome": outcome, "value_in_silver2": value, "correct": correct,
            "quarantine_status": (qrow or {}).get("healing_status"),
            "quarantine_failures": (qrow or {}).get("failures"),
            "dead_letter_status": (dlrow or {}).get("healing_status"),
            "resolved_by": resolver,
            "fix_source": (execution or {}).get("action_source") or ("REPLAY" if outcome in (
                HEALED_BY_REPLAY, REPLAYED) else None),
            "fix_action": (execution or {}).get("action"),
            "fix_params": (execution or {}).get("action_params"),
            "failure_id": (execution or {}).get("failure_id"),
            "published_at": published, "quarantined_at": (qrow or {}).get("quarantined_at"),
            "healed_at": healed_at,
            "seconds_to_heal": (healed_at - published).total_seconds() if healed_at and published else None,
        })
    return out


def summarize(rows: List[dict]) -> dict:
    by_kind: Dict[str, Dict[str, int]] = {}
    for r in rows:
        by_kind.setdefault(r["kind"], {}).setdefault(r["outcome"], 0)
        by_kind[r["kind"]][r["outcome"]] += 1
    return {
        "records": len(rows),
        "outcomes": {o: sum(1 for r in rows if r["outcome"] == o) for o in sorted({r["outcome"] for r in rows})},
        "by_kind": by_kind,
        "correct": sum(1 for r in rows if r["correct"] is True),
        "wrong": sum(1 for r in rows if r["correct"] is False),
        "not_as_predicted_on_entry": sum(1 for r in rows if r["predicted_path"] != r["intended_path"]),
    }


def runs(spark) -> List[dict]:
    from pyspark.sql import functions as F

    path = manifest_path()
    if not session.is_delta(path):
        return []
    return [r.asDict() for r in spark.read.format("delta").load(path).groupBy("run_id", "scenario").agg(
        F.count(F.lit(1)).alias("records"), F.min("planned_at").alias("planned_at"),
        F.max("published_at").alias("last_published_at"),
        F.sum(F.when(F.col("published_at").isNull(), 1).otherwise(0)).alias("unpublished"),
    ).orderBy("planned_at").collect()]


# --------------------------------------------------------------------------- CLI
def _print_rows(rows: List[dict], cols: List[str]) -> None:
    if not rows:
        print("  none")
        return
    widths = {c: min(40, max(len(c), *(len(str(r.get(c))) for r in rows))) for c in cols}
    print("  " + "  ".join(c.ljust(widths[c]) for c in cols))
    for r in rows:
        print("  " + "  ".join(str(r.get(c))[:40].ljust(widths[c]) for c in cols))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m tools.fault_injector", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("kinds", help="the fault catalog")
    p_inj = sub.add_parser("inject", help="inject a scenario (data/faults/<name>.json) or one ad-hoc fault")
    p_inj.add_argument("scenario", nargs="?", help="scenario name in data/faults/ or a path")
    p_inj.add_argument("--kind", choices=sorted(faults.KINDS))
    p_inj.add_argument("--source")
    p_inj.add_argument("--column")
    p_inj.add_argument("--value", help="the corrupted value to inject (default: from the catalog)")
    p_inj.add_argument("--base", help="only corrupt records whose true value is this, e.g. JUNE")
    p_inj.add_argument("--to", help="schema_rename: the new column name (default CamelCase)")
    p_inj.add_argument("--count", type=int, help="records per batch (default 5)")
    p_inj.add_argument("--batches", type=int, help="batches, sent --interval seconds apart (default 1)")
    p_inj.add_argument("--run-id")
    p_inj.add_argument("--seed", type=int, default=7)
    p_inj.add_argument("--interval", type=float, default=DEFAULT_INTERVAL)
    p_inj.add_argument("--dry-run", action="store_true", help="plan and predict only: nothing sent or written")
    p_inj.add_argument("--json", action="store_true")
    sub.add_parser("runs", help="injected runs")
    p_st = sub.add_parser("status", help="where every record of a run is now")
    p_st.add_argument("run_id")
    p_st.add_argument("--records", action="store_true", help="one line per record")
    p_st.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)

    if args.command == "kinds":
        for kind, info in faults.KINDS.items():
            cols = ", ".join(info["columns"]) if info["columns"] else "any column"
            print(f"{kind:18} -> {info['intended']:12} columns: {cols}")
        return 0
    try:
        if args.command == "inject":
            if bool(args.scenario) == bool(args.kind):
                raise faults.FaultSpecError("give a scenario name or --kind (not both)")
            scenario = load_scenario(args.scenario) if args.scenario else adhoc_scenario(args)
            spark = session.spark("FaultInjector")
            records = inject(spark, scenario, args.run_id, args.seed, args.dry_run, args.interval)
            rows = _manifest_rows(records)
            if args.json:
                print(json.dumps(rows, indent=2, default=str, ensure_ascii=False))
            else:
                _print_rows(rows, ["seq", "kind", "source", "column", "batch", "original_raw", "injected_raw",
                                   "expected_value", "intended_path", "predicted_path"])
                odd = [r for r in rows if not r["as_intended"]]
                if odd:
                    print(f"\n{len(odd)} record(s) will not take the intended path with today's code and "
                          "reference table (see predicted_path).")
                verb = "planned (dry run, nothing sent)" if args.dry_run else "injected"
                print(f"\nrun {rows[0]['run_id']}: {len(rows)} record(s) {verb}.")
                if not args.dry_run:
                    print(f"Follow it: python -m tools.fault_injector status {rows[0]['run_id']}")
            return 0
        spark = session.spark("FaultInjector")
        if args.command == "runs":
            result = runs(spark)
            print(json.dumps(result, indent=2, default=str)) if args.json else _print_rows(
                result, ["run_id", "scenario", "records", "unpublished", "planned_at", "last_published_at"])
            return 0
        rows = status(spark, args.run_id)
        if args.json:
            print(json.dumps({"summary": summarize(rows), "records": rows}, indent=2, default=str,
                             ensure_ascii=False))
            return 0
        if args.records:
            _print_rows(rows, ["seq", "kind", "column", "injected_raw", "expected_value", "outcome",
                               "value_in_silver2", "correct", "fix_source", "seconds_to_heal"])
        print(json.dumps(summarize(rows), indent=2))
        return 0
    except faults.FaultSpecError as exc:
        print(f"[fault_injector] {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
