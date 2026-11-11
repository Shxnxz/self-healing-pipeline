"""Policy gate (spec AP2, AP3, RS1): may this diagnosis run without an engineer?

Reads   diagnosis, failure, incident_context
Writes  policy = {"passed", "outcome", "reasons", "checks", "threshold",
                  "unverified_threshold", "safe_actions"}
        action = the proposal, with "source": "DIAGNOSIS", only when it passes

Deterministic: no model is asked. The two conditions of the spec must hold together:

  safe_action   the action is in the safe set (agent/actions.py)
  confidence    the model's confidence is at least POLICY_CONFIDENCE_THRESHOLD

A confident model can still be wrong, and a proposal is only safe for the failure
it is made for. Four more checks tie it to that failure and to the data. They can
only turn a pass into an escalation, never the reverse:

  trusted_failure   the failure itself is not waiting for review (Qwen classification)
  parameters        the parameters fit the failure, e.g. the canonical value exists
                    for that column. The model's wording is not trusted for this.
  evidence          the records must not contradict the value (agent/evidence.py:
                    crash_month and crash_day_of_week against crash_date). Where
                    nothing in a record can confirm it, the confidence must reach
                    the stricter POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD.
  not_tried_before  the same fix has not already failed for this failure

`outcome` says what happened, so an escalation can be worded correctly:

  PASSED              every check passed
  REJECTED            a fix was proposed and a check stopped it
  ENGINEER_REQUESTED  Diagnose itself asked for an engineer (ESCALATE); no fix was blocked
  NO_DIAGNOSIS        there was no usable diagnosis to judge

Every check is recorded, passed or not, so the engineer sees why a proposal was
stopped and the audit shows why one went through. Anything missing or malformed
fails closed.
"""
import json
import math
from typing import Callable, List, Optional

from agent import actions, config, evidence, reference
from agent.state import RecoveryState, step

NAME = "policy_gate"

PASSED, REJECTED = "PASSED", "REJECTED"
ENGINEER_REQUESTED, NO_DIAGNOSIS = "ENGINEER_REQUESTED", "NO_DIAGNOSIS"

# diagnosis["status"] values that carry a proposal (a diagnosis without a status,
# as an engineer or a test might write it, counts as one).
_USABLE = (None, "OK", "SKIPPED")


def _same_fix(attempt: dict, action: str, params: dict) -> bool:
    return attempt.get("action") == action and \
        json.dumps(attempt.get("action_params") or {}, sort_keys=True) == json.dumps(params, sort_keys=True)


def _stopped(outcome: str, name: str, detail: str) -> dict:
    return {"passed": False, "outcome": outcome, "reasons": [detail], "params": None,
            "checks": [{"check": name, "passed": False, "detail": detail}]}


def evaluate(diagnosis: dict, failure: dict, previous_attempts: List[dict], canonical: List[str],
             records: List[dict], threshold: float, unverified_threshold: float, safe: frozenset) -> dict:
    """Pure decision: returns the `policy` dict plus the checked parameters."""
    proposed = diagnosis.get("proposed_action")
    if diagnosis.get("status") not in _USABLE or not isinstance(proposed, dict) \
            or not isinstance(proposed.get("action"), str):
        why = "; ".join(diagnosis.get("reasons") or []) or "it proposes no action"
        return _stopped(NO_DIAGNOSIS, "diagnosis",
                        f"no usable diagnosis ({diagnosis.get('status') or 'missing'}): {why}")

    action = proposed["action"]
    if action == actions.ESCALATE:
        who = "Diagnose" if diagnosis.get("status") == "SKIPPED" else "the model"
        return _stopped(ENGINEER_REQUESTED, "proposal",
                        f"{who} asked for an engineer: {diagnosis.get('hypothesis') or 'no reason given'}")

    known = actions.CATALOGUE.get(action)
    checks = []

    def check(name: str, passed: bool, detail: str) -> None:
        checks.append({"check": name, "passed": bool(passed), "detail": detail})

    if known is None:
        check("safe_action", False, f"{action!r} is not a pre-approved action")
    elif action in safe:
        check("safe_action", True, f"{action} is in the safe-action set")
    elif known.safe:
        check("safe_action", False, f"{action} is switched off by POLICY_SAFE_ACTIONS")
    else:
        check("safe_action", False, f"{action} is not in the safe-action set: {known.why}")

    confidence = diagnosis.get("confidence")
    is_probability = isinstance(confidence, (int, float)) and not isinstance(confidence, bool) \
        and not math.isnan(confidence) and 0 <= confidence <= 1
    if not is_probability:
        check("confidence", False, f"confidence {confidence!r} is not a number from 0 to 1")
    elif confidence >= threshold:
        check("confidence", True, f"confidence {confidence:.2f} reaches the threshold {threshold:.2f}")
    else:
        check("confidence", False, f"confidence {confidence:.2f} is below the threshold {threshold:.2f}")

    trusted = failure.get("trusted") is True
    check("trusted_failure", trusted, "the failure comes from a trusted detection" if trusted
          else "the failure's own classification needs review, so no fix runs automatically")

    params, problems = actions.check_params(action, proposed.get("action_params"), failure, canonical)
    check("parameters", not problems, "the parameters fit the failure" if not problems else "; ".join(problems))

    if params is not None and action == actions.ADD_VARIANT:
        found = evidence.check(failure.get("column_name"), params["canonical_value"], records)
        if found["verdict"] == evidence.AGREES:
            check("evidence", True, found["detail"])
        elif found["verdict"] == evidence.CONTRADICTS:
            check("evidence", False, found["detail"])
        else:
            limit = "an engineer is required" if math.isinf(unverified_threshold) else \
                f"the stricter threshold {unverified_threshold:.2f} applies"
            ok = is_probability and confidence >= unverified_threshold
            check("evidence", ok, f"{found['detail']}, so {limit}" + (
                "" if ok or not is_probability or math.isinf(unverified_threshold)
                else f" and confidence {confidence:.2f} is below it"))

    if params is not None:
        repeated = [a for a in previous_attempts if _same_fix(a, action, params)]
        check("not_tried_before", not repeated,
              f"this fix already ended {repeated[0].get('outcome')} for the same failure" if repeated
              else "this fix has not failed before for this failure")

    passed = all(c["passed"] for c in checks)
    return {"passed": passed, "outcome": PASSED if passed else REJECTED,
            "reasons": [c["detail"] for c in checks if not c["passed"]],
            "checks": checks, "params": params if passed else None}


def make_policy_gate(canonical_values: Optional[Callable] = None, sample_records: Optional[Callable] = None,
                     threshold: Optional[float] = None, unverified_threshold: Optional[float] = None,
                     safe: Optional[frozenset] = None):
    """Build the node. The arguments replace the Delta readers and the settings (tests)."""

    def policy_gate(state: RecoveryState) -> dict:
        diagnosis = state.get("diagnosis") or {}
        failure = state.get("failure") or {}
        limit = config.POLICY_CONFIDENCE_THRESHOLD if threshold is None else threshold
        strict = config.POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD if unverified_threshold is None \
            else unverified_threshold
        allowed = actions.safe_actions(config.POLICY_SAFE_ACTIONS) if safe is None else safe
        # Read again here, not taken from the diagnosis: the gate does not rely on
        # anything the Diagnose step wrote down about what is valid or what the data says.
        canonical = (canonical_values or reference.canonical_values)(failure.get("column_name"))
        ids = failure.get("sample_record_ids") or []
        records = (sample_records or reference.sample_records)(failure, limit=len(ids)) if ids else []
        attempts = (state.get("incident_context") or {}).get("previous_attempts") or []

        verdict = evaluate(diagnosis, failure, attempts, canonical, records, limit, max(strict, limit), allowed)
        params = verdict.pop("params")
        policy = {**verdict, "threshold": limit,
                  "unverified_threshold": None if math.isinf(strict) else max(strict, limit),
                  "safe_actions": sorted(allowed)}
        if not policy["passed"]:
            return {"policy": policy, "action": None,
                    "trace": [step(NAME, f"{policy['outcome'].lower()}: " + "; ".join(policy["reasons"]))]}
        action = {"action": diagnosis["proposed_action"]["action"], "action_params": params,
                  "source": "DIAGNOSIS", "incident_id": None}
        note = f"passed: {action['action']} at confidence {diagnosis['confidence']:.2f}"
        return {"policy": policy, "action": action, "trace": [step(NAME, note)]}

    return policy_gate


policy_gate = make_policy_gate()
