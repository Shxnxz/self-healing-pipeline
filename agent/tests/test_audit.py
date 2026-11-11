"""The remediation audit table: python -m pytest agent/tests   (a real Delta table in a temp folder)"""
import json
from datetime import datetime, timedelta, timezone

import pytest
from deltalake import DeltaTable

from agent import audit, config

NOW = datetime(2026, 11, 11, 7, 0, tzinfo=timezone.utc)


def record(execution="e1", status="SUCCESS", at=NOW, **more):
    return {"execution_id": execution, "failure_id": "f1", "lookup_id": "L1", "action": "ADD_VARIANT",
            "action_params": {"canonical_value": "JULY"}, "action_source": "DIAGNOSIS", "incident_id": None,
            "confidence": 0.93, "status": status, "reasons": [], "target_table": "silver2_crash1",
            "column_name": "crash_month", "raw_value": "Jule", "overlay": {"crash_month": {"JULE": "JULY"}},
            "rows_expected": 2, "rows_affected": 2, "rows_with_other_failures": 0, "record_ids": ["r1", "r2"],
            "quarantine_version": 4, "started_at": at, "executed_at": at, **more}


# --- the idempotency key -------------------------------------------------------------
def test_same_fix_for_the_same_failure_has_the_same_id():
    one = audit.execution_id("f1", "ADD_VARIANT", {"canonical_value": "JULY"})
    assert one == audit.execution_id("f1", "ADD_VARIANT", {"canonical_value": "JULY"})
    assert len(one) == 32
    renames = {"renames": {"A": "a", "B": "b"}}
    assert audit.execution_id("s1", "RENAME_COLUMN", renames) == \
        audit.execution_id("s1", "RENAME_COLUMN", {"renames": {"B": "b", "A": "a"}})   # key order is not identity


def test_another_failure_action_or_parameter_is_another_execution():
    ids = {audit.execution_id("f1", "ADD_VARIANT", {"canonical_value": "JULY"}),
           audit.execution_id("f2", "ADD_VARIANT", {"canonical_value": "JULY"}),
           audit.execution_id("f1", "ADD_VARIANT", {"canonical_value": "JUNE"}),
           audit.execution_id("f1", "ESCALATE", {}),
           audit.execution_id("f1", "ESCALATE", None),      # no parameters = {}
           audit.execution_id(None, None, None)}
    assert len(ids) == 5


# --- the table -----------------------------------------------------------------------
def test_a_record_comes_back_as_it_was_written(tmp_path):
    path = str(tmp_path / "audit")
    audit.write(record(), path)
    assert audit.find("e1", path) == record()
    assert audit.find("other", path) is None


def test_the_configured_table_is_used_by_default():
    audit.write(record())
    assert audit.find("e1")["status"] == "SUCCESS"
    assert DeltaTable(config.REMEDIATION_AUDIT_PATH).version() == 0


def test_a_table_that_does_not_exist_yet_has_no_records(tmp_path):
    assert audit.find("e1", str(tmp_path / "nothing")) is None
    (tmp_path / "empty").mkdir()      # the folder Docker creates for the mount before the first write
    assert audit.find("e1", str(tmp_path / "empty")) is None and audit.latest(path=str(tmp_path / "empty")) == []
    audit.write(record(), str(tmp_path / "empty"))
    assert audit.find("e1", str(tmp_path / "empty")) is not None


def test_a_table_that_cannot_be_read_is_an_error_not_an_empty_answer(tmp_path):
    path = tmp_path / "audit"
    audit.write(record(), str(path))
    for log in (path / "_delta_log").glob("*.json"):
        log.write_text("not a delta log")
    with pytest.raises(Exception):
        audit.find("e1", str(path))


def test_every_execution_is_one_appended_row_readable_by_any_delta_reader(tmp_path):
    path = str(tmp_path / "audit")
    audit.write(record("e1"), path)
    audit.write(record("e2", status="FAILED", reasons=["no rows"], record_ids=[], overlay=None, rows_affected=0,
                       target_table=None, at=NOW + timedelta(minutes=1)), path)
    table = DeltaTable(path)
    assert table.version() == 1                                   # one commit per record
    assert table.protocol().min_reader_version == 1               # plain Delta: Spark 3.5 reads it
    rows = {row["execution_id"]: row for row in table.to_pyarrow_dataset().to_table(use_threads=False).to_pylist()}
    assert json.loads(rows["e1"]["action_params"]) == {"canonical_value": "JULY"}
    assert rows["e1"]["record_ids"] == ["r1", "r2"] and rows["e1"]["executed_at"] == NOW
    assert rows["e2"]["overlay"] is None and rows["e2"]["reasons"] == ["no rows"]


def test_latest_is_newest_first_and_leaves_the_record_ids_out(tmp_path):
    path = str(tmp_path / "audit")
    for n in range(3):
        audit.write(record(f"e{n}", at=NOW + timedelta(minutes=n)), path)
    newest = audit.latest(limit=2, path=path)
    assert [row["execution_id"] for row in newest] == ["e2", "e1"]
    assert "record_ids" not in newest[0] and newest[0]["overlay"] == {"crash_month": {"JULE": "JULY"}}
    assert [row["execution_id"] for row in audit.for_failure("f1", path)] == ["e0", "e1", "e2"]
    assert audit.for_failure("other", path) == []


def test_if_an_id_is_ever_written_twice_the_first_execution_counts(tmp_path):
    path = str(tmp_path / "audit")
    audit.write(record("e1", rows_affected=2), path)
    audit.write(record("e1", rows_affected=9, at=NOW + timedelta(minutes=5)), path)
    assert audit.find("e1", path)["rows_affected"] == 2


def test_command_line_prints_the_executions(tmp_path, monkeypatch, capsys):
    audit.write(record())
    monkeypatch.setattr("sys.argv", ["agent.audit"])
    audit.main()
    assert json.loads(capsys.readouterr().out)[0]["execution_id"] == "e1"
    for key in ("e1", "f1"):
        monkeypatch.setattr("sys.argv", ["agent.audit", key])
        audit.main()
        printed = json.loads(capsys.readouterr().out)
        assert printed[0]["record_ids"] == ["r1", "r2"] and printed[0]["executed_at"] == NOW.isoformat()
    monkeypatch.setattr("sys.argv", ["agent.audit", "unknown"])
    with pytest.raises(SystemExit, match="No execution recorded"):
        audit.main()
