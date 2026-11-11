"""Policy gate (spec AP2, AP3, RS1). STUB: the real checks are built in Task 103.

Reads   diagnosis
Writes  policy = {"passed": bool, "reasons": [str]}
        action = the proposed action, with "source": "DIAGNOSIS", only when it passes

It must pass only when confidence >= threshold AND the action is in the safe set.
The stub fails closed: it rejects everything.
"""
from agent.state import NOT_IMPLEMENTED, RecoveryState, step

NAME = "policy_gate"


def policy_gate(state: RecoveryState) -> dict:
    return {
        "policy": {"status": NOT_IMPLEMENTED, "passed": False,
                   "reasons": ["policy gate is not built yet (Task 103)"]},
        "stubbed": [NAME],
        "trace": [step(NAME, "stub: rejected (Task 103)")],
    }
