"""Deterministic Lookup: where the lookup result enters the graph (spec DL2, DL3).

The lookup itself (blast radius, incident pattern, ranking) runs in the lookup/
service before the graph starts. This node reads its verdict and sets the route:

  remediate   a verified past fix can be reused; it becomes `action`
  escalate    the same, but the fix is borrowed from a similar (not exact) incident,
              or from an exact one whose fix was itself borrowed and never approved
              (`borrowed`, written by Learn), and the failure is in a tier of
              APPROVE_BORROWED_FIX_TIERS (default P1): it becomes `action` and waits
              for an engineer's approval
              (reason APPROVAL_REQUIRED; `python -m agent.review approve` runs it)
  diagnose    everything else

Remediate cannot run without an action, so a "remediate" verdict that carries no
past fix is sent to diagnose instead of being trusted. A past fix is also checked
against the failure with the same function the policy gate uses
(actions.check_params): a fix that no longer fits, for example because its canonical
value was removed from the reference table since, is sent to diagnose too.
"""
from agent import actions, config, reference
from agent.state import ROUTE_DIAGNOSE, ROUTE_ESCALATE, ROUTE_REMEDIATE, RecoveryState, step

NAME = "deterministic_lookup"


def is_borrowed(match: dict) -> bool:
    """A fix taken from a similar (not exact) incident, or from an exact one whose own
    fix was borrowed without an engineer's approval (Learn records `borrowed` on it, so
    reusing a borrowed fix again and again never turns it into an exact one)."""
    return (match.get("matched_by") or "UNKNOWN") != "EXACT" or bool(match.get("borrowed"))


def deterministic_lookup(state: RecoveryState) -> dict:
    match = (state.get("incident_context") or {}).get("match") or {}
    note = state.get("route_reason") or "no reusable incident"
    if state.get("lookup_route") == ROUTE_REMEDIATE and match.get("action"):
        failure = state.get("failure") or {}
        canonical = reference.canonical_values(failure.get("column_name")) \
            if match["action"] == actions.ADD_VARIANT else []
        params, problems = actions.check_params(match["action"], match.get("action_params"), failure, canonical)
        if match["action"] == actions.ESCALATE:
            params, problems = None, ["ESCALATE is not a fix"]
        if params is not None:
            action = {"action": match["action"], "action_params": params,
                      "source": "INCIDENT", "incident_id": match.get("incident_id")}
            note = state.get("route_reason") or f"reuse {match['action']}"
            tier = (state.get("priority") or {}).get("tier")
            matched_by = match.get("matched_by") or "UNKNOWN"
            if is_borrowed(match) and tier in config.APPROVE_BORROWED_FIX_TIERS:
                origin = (f"a {matched_by.lower()} match" if matched_by != "EXACT" else
                          "an exact match whose fix was itself borrowed and never approved")
                note = (f"{tier} failure: past fix {match['action']} borrowed from {origin} "
                        f"(incident {match.get('incident_id')}, score {match.get('score')}) "
                        "needs an engineer's approval before it runs")
                return {"route": ROUTE_ESCALATE, "action": action, "trace": [step(NAME, note)]}
            return {"route": ROUTE_REMEDIATE, "action": action, "trace": [step(NAME, note)]}
        note = f"past fix {match['action']} no longer fits ({'; '.join(problems)}); sent to diagnose"
    elif state.get("lookup_route") == ROUTE_REMEDIATE:
        note = "lookup said remediate but gave no past fix; sent to diagnose"
    return {"route": ROUTE_DIAGNOSE, "action": None, "trace": [step(NAME, note)]}
