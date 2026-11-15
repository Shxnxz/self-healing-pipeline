"""Engineer review (spec EL1, "Resolved By Engineer"): wait for the engineer's decision.

Reads   escalation (the open case), failure
Writes  escalation = {..., "status": "DECIDED", "decision": {...}}, status, and
          APPLY_FIX  action = the engineer's fix ("source": "ENGINEER"); execution,
                     validation and error cleared, so Remediate and Validate run afresh
          RETRY      route, action, diagnosis, policy, execution, validation and error
                     cleared, so the automated path runs again from deterministic_lookup

The workflow pauses here with LangGraph's interrupt(): the state is saved in the
checkpoints and the agent moves on to the next failure. Nothing waits in memory, so a
restart of the agent loses nothing. The engineer answers with `python -m agent.review`,
which publishes the decision to Kafka `recovery.decisions`; the consumer checks it and
resumes the workflow with it (agent/consumer.py).

  APPLY_FIX          -> remediate      (then validate; a failed fix comes back here)
  RETRY              -> deterministic_lookup
  RESOLVED_MANUALLY  -> learn          status RESOLVED_BY_ENGINEER
  DISMISS            -> learn          status DISMISSED

On resume LangGraph runs this node again from the top and interrupt() returns the
decision. A decision that does not hold (made on an older round, a value that is not
canonical) is checked again here, and the node asks again instead of acting on it.
"""
from langgraph.types import interrupt

from agent import escalation as esc
from agent.state import (STATUS_DISMISSED, STATUS_RESOLVED_BY_ENGINEER, RecoveryState, step)

NAME = "engineer_review"

# Keys the automated steps write. They are cleared before those steps run again, so a
# result of an earlier round (an error, a failed validation) cannot route this one.
_EXECUTION_KEYS = ("execution", "validation", "error")
_RETRY_KEYS = ("route", "action", "diagnosis", "policy") + _EXECUTION_KEYS


def request(state: RecoveryState, refused=None) -> dict:
    """The interrupt's value: enough to find the case (the case itself is in the state)."""
    escalation = state.get("escalation") or {}
    failure = state.get("failure") or {}
    asked = {"kind": "ENGINEER_REVIEW", "failure_id": state.get("failure_id"),
             "escalation_id": escalation.get("escalation_id"), "reason": escalation.get("reason"),
             "folder": failure.get("folder"), "column_name": failure.get("column_name"),
             "raw_value": failure.get("raw_value"),
             "proposed_fix": (escalation.get("case") or {}).get("proposed_fix")}
    if refused:
        asked["refused"] = refused
    return asked


def apply(state: RecoveryState, decision: dict) -> dict:
    """The state update for a decision that check_decision accepted."""
    escalation = {**(state.get("escalation") or {}), "status": esc.DECIDED, "decision": decision}
    kind = decision["decision"]
    update = {"escalation": escalation}
    by = f"{kind} by {decision['engineer']}"
    if kind == esc.APPLY_FIX:
        proposed = (escalation.get("case") or {}).get("proposed_fix") or {}
        same = proposed.get("action") == decision["action"] and proposed.get("action_params") == decision["action_params"]
        update.update({key: None for key in _EXECUTION_KEYS})
        update["action"] = {"action": decision["action"], "action_params": decision["action_params"],
                            "source": esc.SOURCE_ENGINEER,
                            "incident_id": proposed.get("incident_id") if same else None,
                            "approved_by": decision["engineer"]}
        update["status"] = STATUS_RESOLVED_BY_ENGINEER   # until Validate says otherwise
        note = f"{by}: {esc.fix_text(update['action'])}"
    elif kind == esc.RETRY:
        update.update({key: None for key in _RETRY_KEYS})
        update["status"] = None   # the automated path sets it again
        note = f"{by}: the automated path runs again"
    elif kind == esc.RESOLVED_MANUALLY:
        update["status"] = STATUS_RESOLVED_BY_ENGINEER
        note = f"{by}: {decision.get('note')}"
    else:
        update["status"] = STATUS_DISMISSED
        note = f"{by}: {decision.get('note')}"
    update["trace"] = [step(NAME, note)]
    return update


def engineer_review(state: RecoveryState) -> dict:
    answer = interrupt(request(state))
    decision, problems = esc.check_decision(state, answer)
    while problems:  # the consumer checks first, so this only catches a change in between
        answer = interrupt(request(state, refused={"decision": answer, "problems": problems}))
        decision, problems = esc.check_decision(state, answer)
    return apply(state, decision)
