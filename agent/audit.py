"""Remediation audit table (spec RD1, RD2, TA2): one row per execution.

A Delta table written with `deltalake` (no Spark), next to the pipeline's own tables
so Spark jobs and verify_delta.py can read it. Only the agent writes it.

It does two jobs with one row:

  audit record        action, parameters, affected records and the execution
                      timestamp, for a failed execution as much as for a successful one
  idempotency check   `execution_id` is a hash of (failure, action, parameters). A row
                      with that id means this exact fix was already executed for this
                      failure, whatever happened to the agent afterwards

The row is written in one Delta commit and is the only thing Remediate writes, so an
execution is either recorded completely or did not happen. There is no state in
which records were staged but the audit record is missing.

Print it:

  docker compose exec recovery-agent python -m agent.audit                  # latest executions
  docker compose exec recovery-agent python -m agent.audit <execution_id>   # one, with its record ids
  docker compose exec recovery-agent python -m agent.audit <failure_id>
"""
import hashlib
import json
import sys
from datetime import datetime
from typing import List, Optional

from agent import config

# Columns of the table. Maps are stored as JSON text so the schema stays the same
# whatever an action's parameters look like.
JSON_COLUMNS = ("action_params", "overlay")


def _schema():
    import pyarrow as pa

    timestamp = pa.timestamp("us", tz="UTC")
    return pa.schema([
        ("execution_id", pa.string()),             # idempotency key, see execution_id()
        ("failure_id", pa.string()),
        ("lookup_id", pa.string()),
        ("action", pa.string()),
        ("action_params", pa.string()),            # JSON
        ("action_source", pa.string()),            # DIAGNOSIS (passed the gate) | INCIDENT (reused fix)
        ("incident_id", pa.string()),              # the past incident a reused fix came from
        ("confidence", pa.float64()),              # the model's confidence, for DIAGNOSIS
        ("status", pa.string()),                   # SUCCESS | FAILED
        ("reasons", pa.list_(pa.string())),        # why it failed; empty on SUCCESS
        ("target_table", pa.string()),             # Silver 2 table the records belong to
        ("column_name", pa.string()),
        ("raw_value", pa.string()),
        ("overlay", pa.string()),                  # JSON {column: {RAW VALUE: canonical value}}
        ("rows_expected", pa.int64()),             # rows the failure reported
        ("rows_affected", pa.int64()),             # quarantine rows staged by this execution
        ("rows_with_other_failures", pa.int64()),  # of those, rows that also fail another column
        ("record_ids", pa.list_(pa.string())),     # the affected records (distinct record_id)
        ("quarantine_version", pa.int64()),        # Delta version of the quarantine table that was read
        ("started_at", timestamp),
        ("executed_at", timestamp),                # execution timestamp
    ])


def execution_id(failure_id: Optional[str], action: Optional[str], params) -> str:
    """The idempotency key: the same fix for the same failure always gets the same id."""
    identity = json.dumps({"failure": failure_id, "action": action, "params": params or {}},
                          sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(identity.encode()).hexdigest()[:32]


def _rows(path: str, column: Optional[str] = None, value: Optional[str] = None,
          columns: Optional[List[str]] = None) -> List[dict]:
    """Rows of the audit table, [] when it does not exist yet. Other errors are raised:
    a table that cannot be read is not the same as a table without the record."""
    import pyarrow.dataset as ds
    from deltalake import DeltaTable
    from deltalake.exceptions import TableNotFoundError

    try:
        table = DeltaTable(path)
    except TableNotFoundError:
        return []
    # Single-threaded for the same reason as agent/reference.py.
    return table.to_pyarrow_dataset().to_table(
        columns=columns, filter=None if column is None else ds.field(column) == value, use_threads=False,
    ).to_pylist()


def _decode(row: dict) -> dict:
    for name in JSON_COLUMNS:
        if isinstance(row.get(name), str):
            row[name] = json.loads(row[name])
    return row


def find(execution: str, path: Optional[str] = None) -> Optional[dict]:
    """The audit record of an execution, or None when it never ran."""
    rows = _rows(path or config.REMEDIATION_AUDIT_PATH, "execution_id", execution)
    if not rows:
        return None
    # One row per id is the rule; if it is ever broken, the first execution counts.
    return _decode(min(rows, key=lambda row: row["executed_at"]))


def write(record: dict, path: Optional[str] = None) -> None:
    """Append one audit record in one Delta commit. Raises when it cannot be written."""
    import pyarrow as pa
    from deltalake import write_deltalake

    schema = _schema()
    row = {name: record.get(name) for name in schema.names}
    for name in JSON_COLUMNS:
        row[name] = None if row[name] is None else json.dumps(row[name], sort_keys=True, ensure_ascii=False)
    for name in ("reasons", "record_ids"):
        row[name] = list(row[name] or [])
    # "merge" lets a later task add a column without breaking tables that already exist.
    write_deltalake(path or config.REMEDIATION_AUDIT_PATH, pa.Table.from_pylist([row], schema=schema),
                    mode="append", schema_mode="merge")


def latest(limit: int = 20, path: Optional[str] = None) -> List[dict]:
    """The most recent executions, newest first, without their record ids."""
    names = [name for name in _schema().names if name != "record_ids"]
    rows = _rows(path or config.REMEDIATION_AUDIT_PATH, columns=names)
    rows.sort(key=lambda row: row["executed_at"], reverse=True)
    return [_decode(row) for row in rows[:limit]]


def for_failure(failure_id: str, path: Optional[str] = None) -> List[dict]:
    """Every execution recorded for a failure, oldest first, with their record ids."""
    rows = _rows(path or config.REMEDIATION_AUDIT_PATH, "failure_id", failure_id)
    return [_decode(row) for row in sorted(rows, key=lambda row: row["executed_at"])]


def _json_default(value):
    return value.isoformat() if isinstance(value, datetime) else str(value)


def main() -> None:
    if len(sys.argv) > 2:
        sys.exit("usage: python -m agent.audit [execution_id | failure_id]")
    if len(sys.argv) == 2:
        one = find(sys.argv[1])
        found = [one] if one else for_failure(sys.argv[1])
        if not found:
            sys.exit(f"No execution recorded for {sys.argv[1]}")
    else:
        found = latest()
    print(json.dumps(found, indent=2, ensure_ascii=False, default=_json_default))


if __name__ == "__main__":
    main()
