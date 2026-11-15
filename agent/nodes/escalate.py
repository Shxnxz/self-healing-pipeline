"""Escalate (spec EL1): open a case for an engineer.

Reads   everything: an engineer gets the whole picture, not a bare alert
Writes  escalation = {"status": "WAITING", "escalation_id", "round", "reason", "details",
                      "case", "escalated_at", "decision": None, "history"}
        status = "ESCALATED"

The case (agent/escalation.py: build_case) is saved in the workflow's state: the
failure and its table, the blast radius and lineage, the closest past incidents and
earlier failed attempts, the model's hypothesis and confidence, what the gate,
Remediate or Validate said, sample records, the column's canonical values and the
fix that `approve` would apply.

The next node, engineer_review, pauses the workflow until the engineer decides
(`python -m agent.review`). Escalate itself only writes the state, so the graph can
retry it safely. A failure that comes back to an engineer (their fix did not validate,
a retry was escalated again) gets a new round; the earlier rounds are kept in
`history`.
"""
from agent import escalation as esc
from agent.state import STATUS_ESCALATED, RecoveryState, step

NAME = "escalate"

# Kept here too: earlier tasks and tests import the reasons from this module.
STEP_FAILED, POLICY_GATE_REJECTED = esc.STEP_FAILED, esc.POLICY_GATE_REJECTED
ENGINEER_REQUESTED, NO_DIAGNOSIS = esc.ENGINEER_REQUESTED, esc.NO_DIAGNOSIS
GATE_PASSED_WITHOUT_ACTION = esc.GATE_PASSED_WITHOUT_ACTION
REMEDIATION_FAILED, VALIDATION_FAILED = esc.REMEDIATION_FAILED, esc.VALIDATION_FAILED
APPROVAL_REQUIRED = esc.APPROVAL_REQUIRED
escalation_reason = esc.escalation_reason


def escalate(state: RecoveryState) -> dict:
    previous = state.get("escalation") or {}
    history = list(previous.get("history") or [])
    if previous.get("escalation_id"):
        history.append({k: previous.get(k) for k in ("escalation_id", "round", "reason", "details",
                                                     "escalated_at", "decision")})
    round_ = int(previous.get("round") or 0) + 1
    reason, details = esc.escalation_reason(state)
    case = esc.build_case(state, reason, details, round_)
    escalation = {
        "status": esc.WAITING, "escalation_id": case["escalation_id"], "round": round_,
        "reason": reason, "details": details, "case": case, "escalated_at": esc.now(),
        "decision": None, "history": history,
    }
    note = f"{reason}: waiting for an engineer ({case['escalation_id']}); proposed fix {esc.fix_text(case['proposed_fix'])}"
    return {"escalation": escalation, "status": STATUS_ESCALATED, "trace": [step(NAME, note)]}
