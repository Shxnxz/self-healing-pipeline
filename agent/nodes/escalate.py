"""Escalate (spec EL1). STUB: the engineer review is built in Task 106.

Reads   everything: an engineer gets the whole state, not a bare alert
Writes  escalation = {"status", "reason", "details"}, status = "ESCALATED"

The stub records why the failure was escalated and moves on; nobody is asked yet.
Task 106 makes the workflow wait here for the engineer's decision.
"""
from agent.state import EXECUTION_SUCCESS, STATUS_ESCALATED, RecoveryState, step

NAME = "escalate"

POLICY_GATE_REJECTED = "POLICY_GATE_REJECTED"
REMEDIATION_FAILED = "REMEDIATION_FAILED"
VALIDATION_FAILED = "VALIDATION_FAILED"


def escalation_reason(state: RecoveryState) -> tuple:
    """Which step sent the failure here: the last one that ran and did not succeed."""
    validation = state.get("validation")
    if validation is not None and validation.get("passed") is not True:
        return VALIDATION_FAILED, validation.get("reasons") or []
    execution = state.get("execution")
    if execution is not None and execution.get("status") != EXECUTION_SUCCESS:
        return REMEDIATION_FAILED, execution.get("reasons") or []
    return POLICY_GATE_REJECTED, (state.get("policy") or {}).get("reasons") or []


def escalate(state: RecoveryState) -> dict:
    reason, details = escalation_reason(state)
    return {
        "escalation": {"status": "PENDING_REVIEW", "reason": reason, "details": details},
        "status": STATUS_ESCALATED,
        "trace": [step(NAME, f"stub: {reason}, no engineer asked yet (Task 106)")],
    }
