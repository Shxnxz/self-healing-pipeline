"""The quarantine trigger claims what it publishes (Task 109): python -m pytest lookup/tests"""
import json
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

import scanner

SCHEMA = pa.schema([
    ("record_id", pa.string()), ("table_name", pa.string()), ("target_table", pa.string()),
    ("silver2_batch_id", pa.string()), ("columns_to_remediate", pa.map_(pa.string(), pa.string())),
    ("quarantined_at", pa.timestamp("us", tz="UTC")), ("healing_status", pa.string()),
])
T0 = datetime(2026, 11, 16, 8, 0, tzinfo=timezone.utc)


def row(record_id, batch, value="Jule", table="silver2_crash1", at=T0, status="PENDING", column="crash_month"):
    return {"record_id": record_id, "table_name": "Crash1", "target_table": table, "silver2_batch_id": batch,
            "columns_to_remediate": [(column, value)], "quarantined_at": at, "healing_status": status}


def append(path, rows):
    write_deltalake(str(path), pa.Table.from_pylist(rows, schema=SCHEMA), mode="append")


def statuses(path):
    return {r["record_id"]: r["healing_status"]
            for r in DeltaTable(str(path)).to_pyarrow_table(columns=["record_id", "healing_status"]).to_pylist()}


class FakeProducer:
    def __init__(self, fail=False):
        self.sent, self.fail, self._pending = [], fail, []

    def produce(self, topic, key, value, on_delivery):
        self._pending.append((json.loads(value), on_delivery))

    def flush(self, timeout):
        for value, callback in self._pending:
            callback(RuntimeError("broker down") if self.fail else None, None)
            if not self.fail:
                self.sent.append(value)
        self._pending = []
        return 0


@pytest.fixture
def table(tmp_path):
    return tmp_path / "silver2_quarantine"


def test_published_groups_are_claimed_in_progress_and_not_published_again(table):
    append(table, [row("a1", "7"), row("a2", "7"), row("b1", "7", value="Ju"), row("c1", "8", value="Ju")])
    producer = FakeProducer()
    assert scanner.scan_once(producer, str(table)) == (4, 2)
    published = {(f["signature_value"], f["rows_affected"]) for f in producer.sent}
    assert published == {("Jule", 2), ("Ju", 2)}
    assert set(statuses(table).values()) == {"IN_PROGRESS"}
    assert scanner.scan_once(producer, str(table)) == (0, 0) and len(producer.sent) == 2


def test_groups_are_published_largest_first(table):
    append(table, [row("a1", "7", value="Ju")] + [row(f"j{n}", "7") for n in range(3)])
    producer = FakeProducer()
    scanner.scan_once(producer, str(table))
    assert [f["signature_value"] for f in producer.sent] == ["Jule", "Ju"]


def test_nothing_is_claimed_when_kafka_did_not_take_the_groups(table):
    append(table, [row("a1", "7")])
    with pytest.raises(RuntimeError):
        scanner.scan_once(FakeProducer(fail=True), str(table))
    assert statuses(table) == {"a1": "PENDING"}             # published on the next scan, not lost


def test_a_later_batch_is_a_new_group_with_its_own_failure_id(table):
    append(table, [row("a1", "7")])
    producer = FakeProducer()
    scanner.scan_once(producer, str(table))
    append(table, [row("a2", "8")])
    scanner.scan_once(producer, str(table))
    first, later = producer.sent
    assert first["batch_id"] == "7" and later["batch_id"] == "8"
    assert first["failure_id"] != later["failure_id"]       # the agent attaches it to the open workflow
    assert statuses(table) == {"a1": "IN_PROGRESS", "a2": "IN_PROGRESS"}


def test_claim_touches_only_the_batches_that_were_read(table):
    """A Silver 2 query can commit a batch with an OLDER timestamp after the snapshot was
    read (the four sources commit concurrently). It must stay PENDING, not be claimed unseen."""
    append(table, [row("a1", "7", at=T0)])
    rows = scanner.read_pending(str(table))
    append(table, [row("late", "3", table="silver2_crash3", at=T0 - timedelta(seconds=5))])
    assert scanner.claim(str(table), rows) == 1
    assert statuses(table) == {"a1": "IN_PROGRESS", "late": "PENDING"}


def test_resolved_and_in_progress_rows_are_left_alone(table):
    append(table, [row("a1", "7", status="RESOLVED"), row("a2", "7", status="IN_PROGRESS"), row("a3", "7")])
    producer = FakeProducer()
    assert scanner.scan_once(producer, str(table)) == (1, 1)
    assert producer.sent[0]["rows_affected"] == 1
    assert statuses(table) == {"a1": "RESOLVED", "a2": "IN_PROGRESS", "a3": "IN_PROGRESS"}


def test_values_with_quotes_are_claimed(table):
    append(table, [row("o'1", "7", value="O'NEIL", table="silver2_crash1")])
    assert scanner.scan_once(FakeProducer(), str(table)) == (1, 1)


def test_missing_table_is_not_an_error(table):
    assert scanner.scan_once(FakeProducer(), str(table)) == (0, 0)


def test_upgrade_from_the_watermark_scanner_claims_what_it_had_published(table, tmp_path):
    state = tmp_path / "state" / "quarantine_scanner.json"
    state.parent.mkdir()
    state.write_text(json.dumps({"watermark": T0.isoformat()}))
    append(table, [row("old", "7", at=T0), row("new", "9", at=T0 + timedelta(minutes=1))])
    assert scanner.migrate_watermark(str(table), str(state)) == 1
    assert statuses(table) == {"old": "IN_PROGRESS", "new": "PENDING"}
    assert "watermark" not in json.loads(state.read_text())
    assert scanner.migrate_watermark(str(table), str(state)) == 0          # only once
    producer = FakeProducer()
    scanner.scan_once(producer, str(table))
    assert [f["sample_record_ids"] for f in producer.sent] == [["new"]]


def test_compose_lets_the_scanner_write_only_the_quarantine():
    """The claim writes healing_status, so the quarantine needs a writable mount; the
    rest of ./delta stays read only for the scanner (main and evaluation stack)."""
    from pathlib import Path

    import yaml

    root = Path(__file__).resolve().parents[2]
    for name in ("docker-compose.yml", "docker-compose.eval.yml"):
        service = yaml.safe_load((root / name).read_text().replace("!override", ""))["services"]["lookup-scanner"]
        mounts = {}
        for entry in service["volumes"]:
            parts = entry.split(":")
            mounts[parts[1]] = len(parts) > 2 and "ro" in parts[2].split(",")
        writable = [m for m, ro in mounts.items() if m.startswith("/data/delta") and not ro]
        assert mounts["/data/delta"] is True, name
        assert writable == ["/data/delta/silver2_quarantine"], name
