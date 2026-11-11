"""Policy gate (spec AP2, AP3, RS1): may this diagnosis run without an engineer?

Reads   diagnosis, failure, incident_context
Writes  policy = {"passed", "reasons", "checks", "threshold", "safe_actions"}
        action = the proposal, with "source": "DIAGNOSIS", only when it passes

Deterministic: no model is asked. The two conditions of the spec must hold together:

  safe_action   the action is in the safe set (agent/actions.py)
  confidence    the model's confidence is at least POLICY_CONFIDENCE_THRESHOLD

A proposal is only safe for the failure it is made for, so three more checks tie it
to that failure. They can only turn a pass into an escalation, never the reverse:

  trusted_failure   the failure itself is not waiting for review (Qwen classification)
  parameters        the parameters fit the failure, e.g. the canonical value exists
                    for that column. The model's wording is not trusted for this.
  not_tried_before  the same fix has not already failed for this failure

Every check is recorded, passed or not, so the engineer sees why a proposal was
stopped and the audit shows why one went through. Anything missing or malformed
fails closed.
"""
import json
import math
from typing import Callable, List, Optional

from agent import actions, config, reference
from agent.state import RecoveryState, step

NAME = "policy_gate"

# diagnosis["status"] values that carry a proposal (a diagnosis without a status,
# as an engineer or a test might write it, counts as one).
_USABLE = (None, "OK", "SKIPPED")


def _same_fix(attempt: dict, action: str, params: dict) -> bool:
    return attempt.get("action") == action and \
        json.dumps(attempt.get("action_params") or {}, sort_keys=True) == json.dumps(params, sort_keys=True)


def evaluate(diagnosis: dict, failure: dict, previous_attempts: List[dict], canonical: List[str],
             threshold: float, safe: frozenset) -> dict:
    """Pure decision: returns the `policy` dict plus the checked parameters."""
    proposed = diagnosis.get("proposed_action")
    if diagnosis.get("status") not in _USABLE or not isinstance(proposed, dict) \
            or not isinstance(proposed.get("action"), str):
        why = "; ".join(diagnosis.get("reasons") or []) or "it proposes no action"
        detail = f"no usable diagnosis ({diagnosis.get('status') or 'missing'}): {why}"
        return {"passed": False, "reasons": [detail], "params": None,
                "checks": [{"check": "diagnosis", "passed": False, "detail": detail}]}

    action = proposed["action"]
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
    is_number = isinstance(confidence, (int, float)) and not isinstance(confidence, bool) \
        and not math.isnan(confidence)
    if not is_number or not 0 <= confidence <= 1:
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

    if params is not None:
        repeated = [a for a in previous_attempts if _same_fix(a, action, params)]
        check("not_tried_before", not repeated,
              f"this fix already ended {repeated[0].get('outcome')} for the same failure" if repeated
              else "this fix has not failed before for this failure")

    passed = all(c["passed"] for c in checks)
    return {"passed": passed, "reasons": [c["detail"] for c in checks if not c["passed"]],
            "checks": checks, "params": params if passed else None}


def make_policy_gate(canonical_values: Optional[Callable] = None, threshold: Optional[float] = None,
                     safe: Optional[frozenset] = None):
    """Build the node. The arguments replace the Delta reader and the settings (tests)."""

    def policy_gate(state: RecoveryState) -> dict:
        diagnosis = state.get("diagnosis") or {}
        failure = state.get("failure") or {}
        limit = config.POLICY_CONFIDENCE_THRESHOLD if threshold is None else threshold
        allowed = actions.safe_actions(config.POLICY_SAFE_ACTIONS) if safe is None else safe
        # Read again here, not taken from the diagnosis: the gate does not rely on
        # anything the Diagnose step wrote down about what is valid.
        canonical = (canonical_values or reference.canonical_values)(failure.get("column_name"))
        attempts = (state.get("incident_context") or {}).get("previous_attempts") or []

        verdict = evaluate(diagnosis, failure, attempts, canonical, limit, allowed)
        params = verdict.pop("params")
        policy = {**verdict, "threshold": limit, "safe_actions": sorted(allowed)}
        if not policy["passed"]:
            return {"policy": policy, "action": None,
                    "trace": [step(NAME, "rejected: " + "; ".join(policy["reasons"]))]}
        action = {"action": diagnosis["proposed_action"]["action"], "action_params": params,
                  "source": "DIAGNOSIS", "incident_id": None}
        note = f"passed: {action['action']} at confidence {diagnosis['confidence']:.2f}"
        return {"policy": policy, "action": action, "trace": [step(NAME, note)]}

    return policy_gate


policy_gate = make_policy_gate()
