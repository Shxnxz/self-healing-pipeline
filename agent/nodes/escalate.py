"""Escalate (spec EL1). STUB: the engineer review is built in Task 106.

Reads   everything: an engineer gets the whole state, not a bare alert
Writes  escalation = {"status", "reason", "details"}, status = "ESCALATED"

The stub records why the failure was escalated and moves on; nobody is asked yet.
Task 106 makes the workflow wait here for the engineer's decision.
"""
from agent.state import EXECUTION_SUCCESS, STATUS_ESCALATED, RecoveryState, step

NAME = "escalate"

STEP_FAILED = "STEP_FAILED"
POLICY_GATE_REJECTED = "POLICY_GATE_REJECTED"
GATE_PASSED_WITHOUT_ACTION = "GATE_PASSED_WITHOUT_ACTION"
REMEDIATION_FAILED = "REMEDIATION_FAILED"
VALIDATION_FAILED = "VALIDATION_FAILED"


def escalation_reason(state: RecoveryState) -> tuple:
    """Which step sent the failure here: the last one that ran and did not succeed."""
    error = state.get("error")
    if error:  # the step itself kept raising, so it left no result to read
        return STEP_FAILED, [f"{error.get('node')}: {error.get('error')} ({error.get('attempts')} attempts)"]
    validation = state.get("validation")
    if validation is not None and validation.get("passed") is not True:
        return VALIDATION_FAILED, validation.get("reasons") or []
    execution = state.get("execution")
    if execution is not None and execution.get("status") != EXECUTION_SUCCESS:
        return REMEDIATION_FAILED, execution.get("reasons") or []
    policy = state.get("policy") or {}
    if policy.get("passed") is True:  # approved, but there is nothing to execute
        return GATE_PASSED_WITHOUT_ACTION, ["the policy gate passed but set no action"]
    return POLICY_GATE_REJECTED, policy.get("reasons") or []


def escalate(state: RecoveryState) -> dict:
    reason, details = escalation_reason(state)
    return {
        "escalation": {"status": "PENDING_REVIEW", "reason": reason, "details": details},
        "status": STATUS_ESCALATED,
        "stubbed": [NAME],
        "trace": [step(NAME, f"stub: {reason}, no engineer asked yet (Task 106)")],
    }
