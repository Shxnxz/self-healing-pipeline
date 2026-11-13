"""What Remediate does for each action (spec RD2).

One function per action of agent/actions.py. An executor prepares the fix and returns
what it covers; the Remediate node (agent/nodes/remediate.py) does the idempotency
check before it and writes the audit record after it. An executor writes nothing.

ADD_VARIANT stages the fix, it does not release it:

  - it collects the failure's group: every quarantined record still waiting with the
    same folder, column and raw value, from the micro-batches the failure came from.
    If records the failure reported cannot be found, nothing is staged
  - it builds the overlay {column: {RAW VALUE: canonical value}}, the mapping that
    Validate applies on top of ref_canonical_mappings when it re-runs the
    standardization on exactly these records (ref_mappings.overlay_mappings takes
    this shape)

Nothing in Silver 2, the quarantine or ref_canonical_mappings changes here, so a
wrong fix is undone by discarding the staged group. The records reach Silver 2 only
after Validate passes, and the mapping reaches the reference table only through
Learn's promotion. That is what makes ADD_VARIANT a safe action.

An action without an executor cannot run automatically, even when a past incident
hands it over as a verified fix: it fails here and goes to an engineer.
"""
from typing import Callable, Dict, List

from agent import actions, reference


class RemediationError(Exception):
    """The action could not be applied. `reasons` is what the engineer reads."""

    def __init__(self, reasons: List[str]):
        super().__init__("; ".join(reasons))
        self.reasons = list(reasons)


def add_variant(failure: dict, params) -> dict:
    column = failure.get("column_name")
    # Checked again at the moment of execution: the reference table may have changed
    # since the policy gate (or, for a reused past fix, deterministic_lookup) checked
    # them, and a workflow can resume here long after.
    try:
        canonical = reference.read_canonical_values(column)
    except Exception as exc:
        raise RemediationError([f"the reference table could not be read: {exc}"]) from exc
    clean, problems = actions.check_params(actions.ADD_VARIANT, params, failure, canonical)
    if problems:
        raise RemediationError(problems)

    try:
        group = reference.failure_records(failure)
    except Exception as exc:
        raise RemediationError([f"the quarantine table could not be read: {exc}"]) from exc

    rows = group["rows"]
    if not rows:
        raise RemediationError(["no quarantined record of this failure is waiting for a fix "
                                "(already resolved, or the failure does not match the quarantine)"])
    # The group is staged whole or not at all. Rows that were fixed in the meantime
    # are accounted for; rows that cannot be found are not, and a fix applied to part
    # of a group would leave the rest in quarantine with nobody looking for it.
    expected = failure.get("rows_affected")
    found = len(rows) + group.get("not_waiting", 0)
    if isinstance(expected, int) and found < expected:
        raise RemediationError([f"only {found} of the {expected} records the failure reported were found "
                                f"in the quarantine, so the group is not staged in part"])
    tables = sorted({row["target_table"] for row in rows if row.get("target_table")})
    if len(tables) > 1:
        raise RemediationError([f"the records of one failure belong to several tables: {', '.join(tables)}"])

    raw = failure["raw_value"].strip().upper()
    return {
        "action_params": clean,
        "target_table": tables[0] if tables else None,
        "overlay": {column: {raw: clean["canonical_value"]}},
        "record_ids": sorted({row["record_id"] for row in rows if row.get("record_id")}),
        "rows_affected": len(rows),
        "rows_with_other_failures": sum(1 for row in rows if row.get("other_failures")),
        "quarantine_version": group["version"],
    }


# action -> executor(failure, action_params) -> what was staged. Every safe action
# needs one (a test checks it), or the policy gate would pass fixes that can only fail.
EXECUTORS: Dict[str, Callable[[dict, dict], dict]] = {
    actions.ADD_VARIANT: add_variant,
}
