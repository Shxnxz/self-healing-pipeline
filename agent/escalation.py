"""Escalation and the engineer's decision (spec EL1, "Escalate and Human-in-the-Loop").

Three parts, used by the two nodes of Escalate and by the review CLI:

  escalation_reason(state)    which step sent the failure to an engineer, and why
  build_case(state, ...)      what the engineer gets: the failure, its lineage and blast
                              radius, the closest past incidents, the model's hypothesis
                              and confidence, what the gate / Remediate / Validate said,
                              sample records and the fix that "approve" would apply.
                              Not a bare alert.
  check_decision / apply      the engineer's answer, checked against the failure, and
                              what the workflow does with it

The engineer's decisions:

  APPLY_FIX          run this fix (the proposed one, or the engineer's own canonical
                     value) through Remediate and Validate like any other fix. If
                     Validate passes the records are released and Learn may promote the
                     variant at once (spec EL2: approved by an engineer). If not, the
                     failure comes back to the engineer with the new reason.
  RETRY              run the automated path again from Deterministic Lookup, e.g. after
                     Ollama or a table was down (NO_DIAGNOSIS, STEP_FAILED)
  RESOLVED_MANUALLY  the engineer fixed it outside the agent; nothing is executed, the
                     outcome goes to Learn ("Resolved By Engineer")
  DISMISS            not something to fix (or not now); nothing is executed, the records
                     stay quarantined, the outcome goes to Learn

Every decision names the engineer and the escalation it answers (`escalation_id` =
failure_id:round), so a decision made on an older round of the same failure is refused.
"""
import json
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from agent import actions, reference
from agent.state import EXECUTION_SUCCESS, ROUTE_ESCALATE

# --- why a failure is escalated (escalation["reason"]) ---------------------------
STEP_FAILED = "STEP_FAILED"                     # a step kept raising
POLICY_GATE_REJECTED = "POLICY_GATE_REJECTED"   # a proposed fix was stopped by a check
ENGINEER_REQUESTED = "ENGINEER_REQUESTED"       # Diagnose itself proposed ESCALATE
NO_DIAGNOSIS = "NO_DIAGNOSIS"                   # model unreachable or its reply unusable
POLICY_OUTCOME_REASONS = {"ENGINEER_REQUESTED": ENGINEER_REQUESTED, "NO_DIAGNOSIS": NO_DIAGNOSIS}
GATE_PASSED_WITHOUT_ACTION = "GATE_PASSED_WITHOUT_ACTION"
REMEDIATION_FAILED = "REMEDIATION_FAILED"
VALIDATION_FAILED = "VALIDATION_FAILED"
APPROVAL_REQUIRED = "APPROVAL_REQUIRED"         # deterministic_lookup asks before reusing a fix

# --- escalation["status"] ---------------------------------------------------------
WAITING = "WAITING"    # the case is open, the workflow is paused in engineer_review
DECIDED = "DECIDED"    # the engineer answered (escalation["decision"])

# --- the engineer's decisions --------------------------------------------------
APPLY_FIX = "APPLY_FIX"
RETRY = "RETRY"
RESOLVED_MANUALLY = "RESOLVED_MANUALLY"
DISMISS = "DISMISS"
DECISIONS = (APPLY_FIX, RETRY, RESOLVED_MANUALLY, DISMISS)
NEEDS_NOTE = (RESOLVED_MANUALLY, DISMISS)   # the corpus should say what was done or why not

SOURCE_ENGINEER = "ENGINEER"   # action["source"] of a fix an engineer decided
PAST_INCIDENTS_SHOWN = 3


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def escalation_id(failure_id: Optional[str], round_: int) -> str:
    return f"{failure_id}:{round_}"


# ----------------------------------------------------------------------------
# Why
# ----------------------------------------------------------------------------
def escalation_reason(state: dict) -> Tuple[str, List[str]]:
    """Which step sent the failure here: the last one that ran and did not succeed.

    An engineer's APPLY_FIX or RETRY clears error, execution and validation before the
    automated steps run again, so what is left in the state is from this round."""
    error = state.get("error")
    if error:  # the step itself kept raising, so it left no result to read
        return STEP_FAILED, [f"{error.get('node')}: {error.get('error')} ({error.get('attempts')} attempts)"]
    validation = state.get("validation")
    if validation is not None and validation.get("passed") is not True:
        return VALIDATION_FAILED, validation.get("reasons") or []
    execution = state.get("execution")
    if execution is not None and execution.get("status") != EXECUTION_SUCCESS:
        return REMEDIATION_FAILED, execution.get("reasons") or []
    if state.get("route") == ROUTE_ESCALATE:
        notes = [e.get("note") for e in state.get("trace") or [] if e.get("node") == "deterministic_lookup"]
        return APPROVAL_REQUIRED, [notes[-1] if notes else "a reused fix needs an engineer's approval"]
    policy = state.get("policy") or {}
    if policy.get("passed") is True:  # approved, but there is nothing to execute
        return GATE_PASSED_WITHOUT_ACTION, ["the policy gate passed but set no action"]
    # The gate says whether it stopped a fix or there was none to stop (policy.outcome).
    reason = POLICY_OUTCOME_REASONS.get(policy.get("outcome"), POLICY_GATE_REJECTED)
    return reason, policy.get("reasons") or []


# ----------------------------------------------------------------------------
# What the engineer gets
# ----------------------------------------------------------------------------
def _is_fix(action: Optional[str]) -> bool:
    return bool(action) and action != actions.ESCALATE


def proposed_fix(state: dict) -> Optional[dict]:
    """The fix `approve` applies: the one that was about to run or ran (a reused fix, a
    fix that failed in Remediate or Validate), else the one the gate stopped."""
    action = state.get("action") or {}
    if _is_fix(action.get("action")):
        return {"action": action["action"], "action_params": action.get("action_params") or {},
                "source": action.get("source"), "incident_id": action.get("incident_id")}
    diagnosis = state.get("diagnosis") or {}
    proposal = diagnosis.get("proposed_action") or {}
    if _is_fix(proposal.get("action")):
        return {"action": proposal["action"], "action_params": proposal.get("action_params") or {},
                "source": "DIAGNOSIS", "incident_id": None, "confidence": diagnosis.get("confidence")}
    return None


def _failed_checks(checks) -> List[dict]:
    if isinstance(checks, dict):
        checks = [{"check": k, **(v if isinstance(v, dict) else {"passed": v})} for k, v in checks.items()]
    return [c for c in checks or [] if isinstance(c, dict) and c.get("passed") is False]


def _canonical(failure: dict, diagnosis: dict) -> List[str]:
    known = (diagnosis.get("evidence") or {}).get("canonical_values")
    if known:
        return list(known)
    try:
        return reference.canonical_values(failure.get("column_name")) if failure.get("column_name") else []
    except Exception:  # a case is still worth opening without them
        return []


def _samples(failure: dict, diagnosis: dict) -> List[dict]:
    known = (diagnosis.get("evidence") or {}).get("sample_records")
    if known:
        return list(known)
    try:
        return reference.sample_records(failure)
    except Exception:
        return []


def build_case(state: dict, reason: str, details: List[str], round_: int) -> dict:
    """Everything an engineer needs to decide without opening another tool (spec section 8)."""
    failure = state.get("failure") or {}
    lineage = state.get("lineage_context") or {}
    incidents = state.get("incident_context") or {}
    diagnosis = state.get("diagnosis") or {}
    policy = state.get("policy") or {}
    execution = state.get("execution")
    validation = state.get("validation")
    radius = lineage.get("blast_radius") or {}
    return {
        "escalation_id": escalation_id(state.get("failure_id"), round_),
        "round": round_,
        "reason": reason,
        "details": list(details),
        "failure": {k: failure.get(k) for k in (
            "failure_id", "source", "failure_type", "family", "folder", "column_name", "raw_value",
            "signature_value", "rows_affected", "batch_id", "sample_record_ids", "trusted", "mandatory",
            "expected_only", "source_only") if failure.get(k) not in (None, [], "")},
        "table": lineage.get("detected_table"),
        "priority": state.get("priority") or {},
        "blast_radius": {k: radius.get(k) for k in (
            "score", "tier", "affected_tables", "affected_layers", "column_reaches", "blocks_failures",
            "lineage_known") if k in radius},
        "lineage": {"origin_table": lineage.get("origin_table"), "detected_table": lineage.get("detected_table"),
                    "downstream": [d.get("name") for d in lineage.get("downstream") or [] if isinstance(d, dict)]},
        "past_incidents": [{k: c.get(k) for k in (
            "incident_id", "matched_by", "score", "raw_value", "column_name", "table_name", "action",
            "action_params", "diagnosis", "resolved_by", "transferable", "reason")}
            for c in (incidents.get("candidates") or [])[:PAST_INCIDENTS_SHOWN]],
        "previous_attempts": incidents.get("previous_attempts") or [],
        "lookup": {"route": state.get("lookup_route"), "reason": state.get("route_reason")},
        "diagnosis": {k: diagnosis.get(k) for k in (
            "status", "hypothesis", "confidence", "proposed_action", "model", "reasons")} if diagnosis else None,
        "policy": {"outcome": policy.get("outcome"), "reasons": policy.get("reasons") or [],
                   "failed_checks": _failed_checks(policy.get("checks"))} if policy else None,
        "execution": {k: execution.get(k) for k in (
            "status", "execution_id", "action", "action_params", "source", "target_table", "rows_expected",
            "rows_affected", "rows_with_other_failures", "reasons")} if execution else None,
        "validation": {"status": validation.get("status"), "reasons": validation.get("reasons") or [],
                       "failed_checks": _failed_checks(validation.get("checks")),
                       "crash_date": validation.get("crash_date"),
                       "rows_staged": validation.get("rows_staged")} if validation else None,
        "error": state.get("error"),
        "sample_records": _samples(failure, diagnosis),
        "canonical_values": _canonical(failure, diagnosis),
        "proposed_fix": proposed_fix(state),
    }


# ----------------------------------------------------------------------------
# The engineer's answer
# ----------------------------------------------------------------------------
def make_decision(failure_id: str, escalation_id_: str, decision: str, engineer: str,
                  note: Optional[str] = None, action: Optional[str] = None,
                  action_params: Optional[dict] = None) -> dict:
    """A decision message, as the review CLI publishes it to Kafka recovery.decisions."""
    message = {"schema_version": "1.0", "failure_id": failure_id, "escalation_id": escalation_id_,
               "decision": decision, "engineer": engineer, "note": note, "decided_at": now()}
    if decision == APPLY_FIX:
        message.update(action=action, action_params=action_params or {})
    return message


def check_decision(state: dict, decision) -> Tuple[Optional[dict], List[str]]:
    """Is this a decision the waiting workflow can act on? Returns (clean decision, [])
    or (None, [problems]). Called by the CLI before it sends, by the consumer before it
    resumes the workflow, and by engineer_review itself."""
    if not isinstance(decision, dict):
        return None, ["a decision must be a JSON object"]
    escalation = state.get("escalation") or {}
    if escalation.get("status") != WAITING:
        return None, [f"failure {state.get('failure_id')} is not waiting for an engineer"]
    problems = []
    if decision.get("escalation_id") != escalation.get("escalation_id"):
        problems.append(f"the decision answers escalation {decision.get('escalation_id')!r}, "
                        f"the open one is {escalation.get('escalation_id')!r}")
    kind = decision.get("decision")
    if kind not in DECISIONS:
        problems.append(f"decision {kind!r} is not one of {', '.join(DECISIONS)}")
    engineer = decision.get("engineer")
    if not isinstance(engineer, str) or not engineer.strip():
        problems.append("the decision must name the engineer who made it")
    note = decision.get("note")
    if note is not None and not isinstance(note, str):
        problems.append("note must be text")
    elif kind in NEEDS_NOTE and not (note or "").strip():
        problems.append(f"{kind} needs a note: what was done, or why it is not fixed")

    clean = {"decision": kind, "engineer": engineer.strip()[:100] if isinstance(engineer, str) else None,
             "note": note.strip()[:2000] or None if isinstance(note, str) else None,
             "escalation_id": decision.get("escalation_id"), "decided_at": decision.get("decided_at") or now()}
    if kind == APPLY_FIX:
        params, fix_problems = _check_fix(state, decision.get("action"), decision.get("action_params"))
        problems += fix_problems
        clean.update(action=decision.get("action"), action_params=params)
    return (None, problems) if problems else (clean, [])


def _check_fix(state: dict, action, params) -> Tuple[Optional[dict], List[str]]:
    from agent.executors import EXECUTORS  # executors imports reference/audit; keep this module light

    if not _is_fix(action) or action not in actions.CATALOGUE:
        return None, [f"APPLY_FIX needs a fix: one of {', '.join(sorted(EXECUTORS))}"]
    if action not in EXECUTORS:
        return None, [f"the agent cannot execute {action} yet; apply it by hand and send RESOLVED_MANUALLY"]
    failure = state.get("failure") or {}
    try:
        canonical = reference.read_canonical_values(failure.get("column_name")) \
            if action == actions.ADD_VARIANT else []
    except Exception as exc:  # retry later rather than act on a table we cannot read
        return None, [f"cannot read the canonical values to check the fix ({type(exc).__name__}: {exc})"]
    return actions.check_params(action, params, failure, canonical)


def fix_text(fix: Optional[dict]) -> str:
    if not fix:
        return "none"
    params = fix.get("action_params")
    return f"{fix.get('action')} {json.dumps(params, ensure_ascii=False)}" if params else str(fix.get("action"))
