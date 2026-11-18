"""Quarantine trigger: silver2_quarantine (Delta) -> Kafka `recovery.failures`.

Spec section 3: the recovery process "picks up PENDING records, grouped by issue and
ordered by occurrence count, and marks the group IN_PROGRESS so it is not picked up
twice". Each scan, with the `deltalake` package (no Spark/JVM):

  1. read every PENDING row (one Delta snapshot);
  2. group them into one StructuredFailure per (folder, column, value), largest first;
  3. publish the groups and wait until Kafka acknowledged all of them;
  4. claim the rows that were read: healing_status PENDING -> IN_PROGRESS.

The claim comes after the publish, so a crash in between republishes the group
rather than losing it (the agent deduplicates on failure_id, or attaches it to the
workflow already open for the same problem). The claim touches only the rows that
were read: PENDING rows of the (target_table, silver2_batch_id) micro-batches in the
snapshot. A micro-batch's quarantine rows are one Delta commit, so a batch committed
after the snapshot stays PENDING and is picked up by the next scan, even when its
`quarantined_at` is older (the four Silver 2 queries commit concurrently, so a
timestamp watermark could skip a batch for good).

IN_PROGRESS rows are never picked up again: not by this scanner and not by
`tools/replay.py quarantine` unless asked (--include-in-progress). Validate sets the
rows it releases to RESOLVED. Rows an engineer dismissed, or that still fail another
column after a fix, stay IN_PROGRESS: they wait for that other fix, a promotion and
the replay tool, or an engineer, instead of starting a new workflow every scan.

A later micro-batch with the same problem is a new group with its own failure_id;
the recovery agent attaches it to the workflow that is still open for the same
signature (agent/consumer.py) instead of starting a second one.

Upgrade from the watermark scanner: when SCANNER_STATE_PATH still holds the old
`quarantined_at` watermark, the PENDING rows up to it were published already, so
they are claimed once without being published again.
"""
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from confluent_kafka import Producer

import config
from failures import group_quarantine_rows

PENDING, IN_PROGRESS = "PENDING", "IN_PROGRESS"
COLUMNS = ["record_id", "table_name", "target_table", "silver2_batch_id", "columns_to_remediate",
           "quarantined_at", "healing_status"]
# A claim that loses a race with another writer (Validate marking rows RESOLVED, the
# replay tool) is tried again on a fresh snapshot.
CLAIM_ATTEMPTS = 5


def _utc(ts: Optional[datetime]) -> Optional[datetime]:
    if ts is None:
        return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def load_watermark(path: str) -> Optional[datetime]:
    """The old scanner's progress, if this volume still has it (see the module doc)."""
    try:
        return datetime.fromisoformat(json.loads(Path(path).read_text())["watermark"])
    except (OSError, ValueError, KeyError, TypeError):
        return None


def save_state(path: str, state: dict) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(state))
    os.replace(tmp, target)


def _open(table_path: str):
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        return DeltaTable(table_path)
    except TableNotFoundError:
        return None


def read_pending(table_path: str) -> List[dict]:
    """Every PENDING row of the quarantine (one snapshot), [] when it does not exist."""
    table = _open(table_path)
    if table is None:
        return []
    import pyarrow as pa
    import pyarrow.dataset as ds

    available = {f.name for f in table.schema().fields}
    # The claim's UPDATE rewrites files with Arrow string_view columns; a filter on a
    # table mixing both kinds fails unless the column is cast first.
    rows = table.to_pyarrow_dataset().to_table(
        columns=[c for c in COLUMNS if c in available],
        filter=ds.field("healing_status").cast(pa.string()) == PENDING, use_threads=False,
    ).to_pylist()
    for row in rows:
        row["quarantined_at"] = _utc(row.get("quarantined_at"))
    return rows


def _literal(value) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def batches_of(rows: List[dict]) -> Dict[Optional[str], List[str]]:
    """target_table -> the silver2_batch_id values the rows came from."""
    found = defaultdict(set)
    for row in rows:
        if row.get("silver2_batch_id") is not None:
            found[row.get("target_table")].add(str(row["silver2_batch_id"]))
    return {table: sorted(ids) for table, ids in found.items()}


def unbatched_ids(rows: List[dict]) -> List[str]:
    """record_ids of rows without a silver2_batch_id (none since the per-folder split)."""
    return sorted({r["record_id"] for r in rows if r.get("silver2_batch_id") is None and r.get("record_id")})


def claim_predicate(batches: Dict[Optional[str], List[str]], until: Optional[datetime] = None,
                    record_ids: Optional[List[str]] = None) -> Optional[str]:
    """SQL predicate selecting the PENDING rows of exactly these micro-batches (and of
    these record_ids, for rows that carry no batch id)."""
    parts = []
    for table, ids in sorted(batches.items(), key=lambda kv: str(kv[0])):
        where_table = "target_table IS NULL" if table is None else f"target_table = {_literal(table)}"
        parts.append(f"({where_table} AND silver2_batch_id IN ({', '.join(_literal(i) for i in ids)}))")
    if record_ids:
        parts.append(f"(silver2_batch_id IS NULL AND record_id IN ({', '.join(_literal(i) for i in record_ids)}))")
    if not parts:
        return None
    predicate = f"healing_status = {_literal(PENDING)} AND ({' OR '.join(parts)})"
    if until is not None:
        predicate += f" AND quarantined_at <= {_literal(until.isoformat())}"
    return predicate


def claim(table_path: str, rows: List[dict], until: Optional[datetime] = None) -> int:
    """Mark the PENDING rows of the micro-batches `rows` came from IN_PROGRESS.
    Returns how many rows changed."""
    predicate = claim_predicate(batches_of(rows), until, unbatched_ids(rows))
    if predicate is None:
        return 0
    delay = 1.0
    for attempt in range(1, CLAIM_ATTEMPTS + 1):
        try:
            result = _open(table_path).update(predicate=predicate, updates={"healing_status": _literal(IN_PROGRESS)})
            return int(result.get("num_updated_rows", 0))
        except Exception as exc:   # a concurrent commit: read the table again and retry
            if attempt == CLAIM_ATTEMPTS:
                raise
            print(f"[scanner] claim attempt {attempt} failed ({type(exc).__name__}: {exc}); retrying", flush=True)
            time.sleep(delay)
            delay *= 2
    return 0


def publish(producer: Producer, failures) -> None:
    errors = []
    for f in failures:
        producer.produce(
            config.FAILURE_TOPIC, key=f.failure_id.encode(),
            value=json.dumps(f.to_dict(), ensure_ascii=False).encode(),
            on_delivery=lambda err, _msg: errors.append(err) if err else None,
        )
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"[scanner] Kafka delivery failed ({remaining} pending, {errors[:1]})")


def order_by_occurrence(failures):
    """Largest group first (spec: "ordered by occurrence count"); ties keep their order."""
    return sorted(failures, key=lambda f: -(f.rows_affected or 0))


def migrate_watermark(table_path: str, state_path: str) -> int:
    """Claim, without publishing, the rows the old watermark scanner already published."""
    watermark = load_watermark(state_path)
    if watermark is None:
        return 0
    seen = [r for r in read_pending(table_path) if r["quarantined_at"] and r["quarantined_at"] <= _utc(watermark)]
    claimed = claim(table_path, seen, until=_utc(watermark)) if seen else 0
    save_state(state_path, {"mode": "claim", "migrated_watermark": watermark.isoformat(),
                            "migrated_rows": claimed})
    print(f"[scanner] Upgraded from watermark {watermark.isoformat()}: {claimed} row(s) already "
          f"published are marked IN_PROGRESS", flush=True)
    return claimed


def scan_once(producer: Producer, table_path: Optional[str] = None) -> Tuple[int, int]:
    """One scan. Returns (rows claimed, failures published)."""
    path = table_path or config.QUARANTINE_PATH
    rows = read_pending(path)
    if not rows:
        return 0, 0
    failures = order_by_occurrence(group_quarantine_rows(rows, sample_size=config.SAMPLE_RECORD_IDS))
    publish(producer, failures)
    claimed = claim(path, rows)
    print(f"[scanner] {len(rows)} PENDING quarantine rows -> {len(failures)} failures on "
          f"{config.FAILURE_TOPIC}; {claimed} row(s) marked IN_PROGRESS", flush=True)
    return claimed, len(failures)


def main() -> None:
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "enable.idempotence": True, "acks": "all",
    })
    print(f"[scanner] Watching {config.QUARANTINE_PATH} every {config.SCAN_INTERVAL_SECONDS}s "
          f"(PENDING groups are published, then claimed IN_PROGRESS)", flush=True)
    migrate_watermark(config.QUARANTINE_PATH, config.SCANNER_STATE_PATH)
    while True:
        scan_once(producer)
        time.sleep(config.SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
