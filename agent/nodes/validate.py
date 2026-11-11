"""Validate (spec VD1 to VD4). STUB: the checks are built in Task 105.

Reads   failure, action, execution
Writes  validation = {"passed": bool, "reasons": [str]}

It must re-run the standardization on the staged group with the resolution as an
overlay, and pass only when every check holds. The stub fails closed.
"""
from agent.state import NOT_IMPLEMENTED, RecoveryState, step

NAME = "validate"


def validate(state: RecoveryState) -> dict:
    return {
        "validation": {"status": NOT_IMPLEMENTED, "passed": False,
                       "reasons": ["validate is not built yet (Task 105)"]},
        "trace": [step(NAME, "stub: not validated (Task 105)")],
    }
