"""Quarantine scanner: silver2_quarantine (Delta) -> Kafka `recovery.failures`.

Reads new PENDING rows with the `deltalake` package (no Spark/JVM), groups them
into one StructuredFailure per (folder, column, value) and publishes each group.

Progress is a `quarantined_at` watermark in SCANNER_STATE_PATH, saved only after
Kafka acknowledged the batch, so a crash republishes rather than loses failures.
silver2_job.py stamps one current_timestamp() per micro-batch write and Delta
commits are atomic, so a write is never half-read.
"""
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from confluent_kafka import Producer

import config
from failures import group_quarantine_rows

COLUMNS = ["record_id", "table_name", "target_table", "silver2_batch_id", "columns_to_remediate",
           "quarantined_at", "healing_status"]


def _utc(ts: Optional[datetime]) -> Optional[datetime]:
    if ts is None:
        return None
    return ts.replace(tzinfo=timezone.utc) if ts.tzinfo is None else ts.astimezone(timezone.utc)


def load_watermark(path: str) -> Optional[datetime]:
    try:
        return datetime.fromisoformat(json.loads(Path(path).read_text())["watermark"])
    except (OSError, ValueError, KeyError):
        return None


def save_watermark(path: str, watermark: datetime) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps({"watermark": watermark.isoformat()}))
    os.replace(tmp, target)


def read_new_rows(table_path: str, watermark: Optional[datetime]) -> List[dict]:
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        table = DeltaTable(table_path)
    except TableNotFoundError:
        return []
    available = {f.name for f in table.schema().fields}
    rows = table.to_pyarrow_table(
        columns=[c for c in COLUMNS if c in available],
        filters=[("healing_status", "=", "PENDING")],
    ).to_pylist()
    out = []
    for row in rows:
        row["quarantined_at"] = _utc(row.get("quarantined_at"))
        if watermark is None or (row["quarantined_at"] and row["quarantined_at"] > watermark):
            out.append(row)
    return out


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


def scan_once(producer: Producer, watermark: Optional[datetime]) -> Optional[datetime]:
    rows = read_new_rows(config.QUARANTINE_PATH, watermark)
    if not rows:
        return watermark
    failures = group_quarantine_rows(rows, sample_size=config.SAMPLE_RECORD_IDS)
    publish(producer, failures)
    newest = max(r["quarantined_at"] for r in rows if r["quarantined_at"])
    save_watermark(config.SCANNER_STATE_PATH, newest)
    print(f"[scanner] {len(rows)} new quarantine rows -> {len(failures)} failures "
          f"on {config.FAILURE_TOPIC} (watermark {newest.isoformat()})", flush=True)
    return newest


def main() -> None:
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "enable.idempotence": True, "acks": "all",
    })
    watermark = load_watermark(config.SCANNER_STATE_PATH)
    print(f"[scanner] Watching {config.QUARANTINE_PATH} every {config.SCAN_INTERVAL_SECONDS}s "
          f"(watermark {watermark})")
    while True:
        watermark = scan_once(producer, watermark)
        time.sleep(config.SCAN_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
