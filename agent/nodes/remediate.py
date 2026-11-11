"""Remediate (spec RD1, RD2, RD3, TA2). STUB: execution is built in Task 104.

Reads   failure, action
Writes  execution = {"status": "SUCCESS" | "FAILED", ...}

It must check idempotency first, run the action, then write the audit record.
Anything other than SUCCESS goes to an engineer. The stub changes nothing and
reports that, so a reused fix is escalated, never assumed applied.
"""
from agent.state import NOT_IMPLEMENTED, RecoveryState, step

NAME = "remediate"


def remediate(state: RecoveryState) -> dict:
    action = (state.get("action") or {}).get("action")
    return {
        "execution": {"status": NOT_IMPLEMENTED, "action": action,
                      "reasons": ["remediate is not built yet (Task 104); nothing was changed"]},
        "stubbed": [NAME],
        "trace": [step(NAME, f"stub: {action} not executed (Task 104)")],
    }
