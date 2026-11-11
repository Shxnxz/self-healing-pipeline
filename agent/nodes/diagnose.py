"""Diagnose (spec AP1). STUB: the reasoning LLM is built in Task 103.

Reads   failure, lineage_context, incident_context
Writes  diagnosis = {"hypothesis": str, "confidence": 0..1,
                     "proposed_action": {"action": str, "action_params": dict}}

The stub proposes nothing, so the policy gate rejects it and the failure goes to
an engineer.
"""
from agent.state import NOT_IMPLEMENTED, RecoveryState, step

NAME = "diagnose"


def diagnose(state: RecoveryState) -> dict:
    return {
        "diagnosis": {"status": NOT_IMPLEMENTED, "hypothesis": None, "confidence": 0.0,
                      "proposed_action": None},
        "stubbed": [NAME],
        "trace": [step(NAME, "stub: no diagnosis (Task 103)")],
    }
