"""Deterministic Lookup: where the lookup result enters the graph (spec DL2, DL3).

The lookup itself (blast radius, incident pattern, ranking) runs in the lookup/
service before the graph starts. This node reads its verdict and sets the route:

  remediate   a verified past fix can be reused; it becomes `action`
  diagnose    everything else

Remediate cannot run without an action, so a "remediate" verdict that carries no
past fix is sent to diagnose instead of being trusted.
"""
from agent.state import ROUTE_DIAGNOSE, ROUTE_REMEDIATE, RecoveryState, step

NAME = "deterministic_lookup"


def deterministic_lookup(state: RecoveryState) -> dict:
    match = (state.get("incident_context") or {}).get("match") or {}
    if state.get("lookup_route") == ROUTE_REMEDIATE and match.get("action"):
        action = {
            "action": match["action"],
            "action_params": match.get("action_params") or {},
            "source": "INCIDENT",
            "incident_id": match.get("incident_id"),
        }
        note = state.get("route_reason") or f"reuse {match['action']}"
        return {"route": ROUTE_REMEDIATE, "action": action, "trace": [step(NAME, note)]}

    if state.get("lookup_route") == ROUTE_REMEDIATE:
        note = "lookup said remediate but gave no past fix; sent to diagnose"
    else:
        note = state.get("route_reason") or "no reusable incident"
    return {"route": ROUTE_DIAGNOSE, "action": None, "trace": [step(NAME, note)]}
