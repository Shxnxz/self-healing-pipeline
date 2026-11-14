"""Validate (spec VD1 to VD4): does the staged fix hold for the whole group? Then release it.

Reads   failure, execution (and the audit record of the execution)
Writes  validation = {"status": PASSED | FAILED, "passed", "reasons", "checks",
                      "execution_id", "target_table", "column", "canonical_value",
                      "rows_staged", "rows_valid", "rows_remaining", "rows_released",
                      "rows_already_released", "crash_date", "quarantine_version",
                      "validated_at"}

  1. The group      the records Remediate staged: `audit.find(execution_id)["record_ids"]`
                    with its overlay and target table (VD1). The state only keeps a
                    sample of the ids, the audit record has all of them.
  2. Replay         Silver 2's own standardize_silver2 re-runs once on those records,
                    with ref_canonical_mappings + the overlay (VD2, agent/replay.py).
                    The mapping is not promoted for this: that is Learn's decision.
  3. Checks         agent/validation.py (VD3): the targeted column is resolved in every
                    record to a canonical value, no conflicting mapping exists, row
                    counts are conserved, no new failure appears, and for crash_month /
                    crash_day_of_week the value agrees with the crash_date of every record.
  4. Release        only when every check passed: the records with nothing left to fix
                    are appended to the source's Silver 2 table and their quarantine
                    rows marked RESOLVED (resolved_by = execution id). Records that still
                    fail another column stay quarantined for that failure.

The graph sends a passed validation to Learn and anything else to Escalate (VD4).

A fix that does not hold is a result (FAILED with reasons), not an exception. Spark or
Delta failing to read or write raises, so the graph retries the step and then escalates
it as STEP_FAILED. Running the step again is safe: the replay changes nothing, and a
release that already happened is recognised (Delta txnAppId on Silver 2, resolved_by
in the quarantine) and not written twice.
"""
from datetime import datetime, timezone
from typing import Callable, Optional

from agent import actions, audit, config, replay as replay_module, validation
from agent.replay import ReplayError
from agent.state import RecoveryState, step

NAME = "validate"
PASSED, FAILED = "PASSED", "FAILED"


def _note(result: dict) -> str:
    if result["status"] != PASSED:
        return "failed: " + "; ".join(result["reasons"])
    note = (f"passed: {result['column']} resolves to {result['canonical_value']} in all "
            f"{result['rows_staged']} record(s); {result['rows_released']} released to {result['target_table']}")
    if result["rows_already_released"]:
        note += f", {result['rows_already_released']} were already released by this execution"
    if result["rows_remaining"]:
        note += f", {result['rows_remaining']} record(s) stay quarantined for another column"
    return note


def make_validate(replay: Optional[Callable] = None, audit_path: Optional[str] = None):
    """Build the node. `replay` replaces agent.replay.replay_group (tests run without Spark)."""

    def validate(state: RecoveryState) -> dict:
        failure = state.get("failure") or {}
        execution = state.get("execution") or {}
        result = {
            "status": FAILED, "passed": False, "reasons": [], "checks": [],
            "execution_id": execution.get("execution_id"), "target_table": execution.get("target_table"),
            "column": failure.get("column_name"), "canonical_value": None,
            "rows_staged": 0, "rows_valid": 0, "rows_remaining": 0, "rows_released": 0,
            "rows_already_released": 0, "crash_date": None, "quarantine_version": None,
            "validated_at": datetime.now(timezone.utc).isoformat(),
        }

        def done(reasons=None) -> dict:
            if reasons:
                result["reasons"] = list(reasons)
            return {"validation": result, "trace": [step(NAME, _note(result))]}

        # 1. The staged group, from the audit record (raises if the table is unreadable).
        record = audit.find(result["execution_id"], audit_path or config.REMEDIATION_AUDIT_PATH) \
            if result["execution_id"] else None
        if record is None:
            return done([f"no successful execution {result['execution_id']} in the audit table, "
                         "so there is no staged group to validate"])
        if record.get("action") != actions.ADD_VARIANT:
            return done([f"Validate has no check for {record.get('action')!r}; an engineer has to verify it"])
        canonical = (record.get("action_params") or {}).get("canonical_value")
        record_ids = list(record.get("record_ids") or [])
        result.update(target_table=record.get("target_table"), column=record.get("column_name"),
                      canonical_value=canonical)
        if not record_ids:
            return done(["the execution staged no records"])

        # 2. Replay through Silver 2's own standardization.
        try:
            group = (replay or replay_module.replay_group)(record.get("target_table"), record_ids, record.get("column_name"),
                           record.get("overlay"))
        except ReplayError as exc:
            return done([str(exc)])
        try:
            group.mark_released_before(result["execution_id"])
            report = group.report()
            result["quarantine_version"] = report.get("quarantine_version")

            # 3. Every check of VD3.
            verdict = validation.evaluate(
                execution_id=result["execution_id"], column=record.get("column_name"),
                raw_value=record.get("raw_value"), canonical=canonical, overlay=record.get("overlay"),
                staged_ids=record_ids, replay=report)
            result.update(checks=verdict["checks"], crash_date=verdict["crash_date"],
                          rows_staged=verdict["rows_staged"], rows_valid=verdict["rows_valid"],
                          rows_remaining=verdict["rows_remaining"],
                          rows_already_released=len(verdict["already_released"]))
            if not verdict["passed"]:
                return done(verdict["reasons"])

            # 4. Release the records that are fixed.
            released = group.release(result["execution_id"], verdict["to_release"])
            result.update(status=PASSED, passed=True, reasons=[], rows_released=released["appended"])
            return done()
        finally:
            group.close()

    return validate


validate = make_validate()
