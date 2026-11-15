"""Remediate (spec RD1, RD2, RD3, TA2): execute the approved action, once.

Reads   failure, action (and diagnosis.confidence for the audit record)
Writes  execution = {"status": "SUCCESS" | "FAILED", "execution_id", "action",
                     "action_params", "source", "target_table", "column_name",
                     "raw_value", "overlay", "rows_expected", "rows_affected",
                     "rows_with_other_failures", "sample_record_ids",
                     "quarantine_version", "executed_at", "idempotent_replay",
                     "audit": {"path", "written"}, "reasons"}

The fixed sequence of the spec:

  1. Idempotency check   `execution_id` = hash of (failure, action, parameters). If
                         the audit table already holds a SUCCESS for it, the fix was
                         executed before (the agent stopped before LangGraph saved
                         this node and the workflow resumed here, the graph retried
                         the step, or the consumer ran the failure again because a
                         later step was a stub). The recorded result is returned
                         and nothing is executed again. A FAILED
                         attempt staged nothing, so it does not count as executed:
                         the fix runs again, and the new attempt is audited too.
  2. Execute             the action's executor (agent/executors.py). For ADD_VARIANT:
                         stage the failure's quarantined records with the overlay.
  3. Audit record        one row in the audit table (agent/audit.py): action,
                         parameters, affected records, timestamp. Written for every
                         attempt, failed ones too.

SUCCESS is claimed only after the audit record is committed: an execution that
cannot be recorded did not happen. The audit row is also the only thing written, in
one Delta commit, so stopping the agent at any point leaves either nothing or the
complete record.

Remediate never raises. Whatever goes wrong becomes FAILED with the reasons, and the
graph sends everything that is not SUCCESS to an engineer.

Validate (Task 105) reads `execution.overlay` and the staged records: the full list
is `agent.audit.find(execution_id)["record_ids"]` (the state keeps only a sample).
"""
from datetime import datetime, timezone
from typing import Optional

from agent import audit, config
from agent.executors import EXECUTORS, RemediationError
from agent.state import EXECUTION_FAILED, EXECUTION_SUCCESS, RecoveryState, step

NAME = "remediate"
SAMPLE_IDS = 5

# Copied from the audit record into the state as they are.
_SHARED = ("execution_id", "status", "action", "action_params", "target_table", "column_name", "raw_value",
           "overlay", "rows_expected", "rows_affected", "rows_with_other_failures", "quarantine_version")


def _execution(record: dict, path: str, written: bool, replay: bool) -> dict:
    """The `execution` state entry of an audit record."""
    executed_at = record.get("executed_at")
    return {
        **{name: record.get(name) for name in _SHARED},
        "source": record.get("action_source"),
        "sample_record_ids": list(record.get("record_ids") or [])[:SAMPLE_IDS],
        "executed_at": executed_at.isoformat() if isinstance(executed_at, datetime) else executed_at,
        "idempotent_replay": replay,
        "audit": {"path": path, "written": written},
        "reasons": list(record.get("reasons") or []),
    }


def _note(execution: dict) -> str:
    if execution["status"] != EXECUTION_SUCCESS:
        return f"failed: {execution['action']} not executed: " + "; ".join(execution["reasons"])
    expected = "" if execution["rows_expected"] is None else f" of {execution['rows_expected']}"
    done = (f"{execution['action']} staged {execution['rows_affected']}{expected} record(s) "
            f"of {execution['target_table'] or 'the quarantine'}")
    if execution["idempotent_replay"]:
        return f"already executed at {execution['executed_at']}, not run again: {done}"
    return f"executed: {done}"


def make_remediate(audit_path: Optional[str] = None):
    """Build the node. `audit_path` replaces the configured audit table (tests)."""

    def remediate(state: RecoveryState) -> dict:
        failure = state.get("failure") or {}
        action = state.get("action") or {}
        name = action.get("action")
        params = action.get("action_params") if isinstance(action.get("action_params"), dict) else {}
        path = audit_path or config.REMEDIATION_AUDIT_PATH
        started = datetime.now(timezone.utc)
        record = {
            "execution_id": audit.execution_id(failure.get("failure_id"), name, params),
            "failure_id": failure.get("failure_id"), "lookup_id": state.get("lookup_id"),
            "action": name, "action_params": params, "action_source": action.get("source"),
            "incident_id": action.get("incident_id"),
            "confidence": (state.get("diagnosis") or {}).get("confidence")
            if action.get("source") == "DIAGNOSIS" else None,
            "status": EXECUTION_FAILED, "reasons": [],
            "target_table": None, "column_name": failure.get("column_name"), "raw_value": failure.get("raw_value"),
            "overlay": None, "rows_expected": failure.get("rows_affected"), "rows_affected": 0,
            "rows_with_other_failures": 0, "record_ids": [], "quarantine_version": None,
            "started_at": started, "executed_at": started,
        }

        def done(written: bool, replay: bool = False) -> dict:
            execution = _execution(record, path, written, replay)
            return {"execution": execution, "trace": [step(NAME, _note(execution))]}

        # 1. Idempotency check, before anything else.
        try:
            earlier = audit.find(record["execution_id"], path)
        except Exception as exc:
            # Not knowing whether it already ran is a reason not to run it.
            record["reasons"] = [f"idempotency check failed, the audit table could not be read: {exc}"]
            return done(written=False)
        if earlier is not None:
            record = earlier
            return done(written=True, replay=True)

        # 2. Execute.
        executor = EXECUTORS.get(name)
        try:
            if executor is None:
                raise RemediationError([f"no automatic execution exists for {name!r}; an engineer has to apply it"])
            record.update(executor(failure, params))
            record["status"] = EXECUTION_SUCCESS
        except RemediationError as exc:
            record["reasons"] = exc.reasons
        except Exception as exc:  # a bug in an executor must not stop the agent
            record["reasons"] = [f"{name} raised {type(exc).__name__}: {exc}"]
        record["executed_at"] = datetime.now(timezone.utc)

        # 3. Audit record. Without it the execution does not count.
        try:
            audit.write(record, path)
        except Exception as exc:
            try:  # the commit may have landed before the error: the table decides, not the exception
                landed = audit.find(record["execution_id"], path)
            except Exception:
                landed = None
            if landed is not None:
                record = landed
                return done(written=True)
            record["status"] = EXECUTION_FAILED
            record["reasons"] = record["reasons"] + [
                f"the audit record could not be written, so nothing is staged: {exc}"]
            return done(written=False)
        return done(written=True)

    return remediate


remediate = make_remediate()
