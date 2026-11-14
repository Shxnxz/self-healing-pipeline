"""Learn (spec EL2, EL3, KG2, TA3). STUB: the incident write is built in Task 107.

Reads   everything. After a passed validation the healed records are already in
        Silver 2 and RESOLVED in quarantine (Validate releases them); what is left
        here is the incident in Neo4j and the promotion into ref_canonical_mappings.
Writes  learned = {"written": bool, ...}, status

It must write the outcome to Neo4j with agent.memory.incidents.write_incident()
for automated and engineer resolutions alike, and decide promotion into
ref_canonical_mappings. The stub writes nothing.

An engineer's outcome arrives here from engineer_review (Task 106):
  status RESOLVED_BY_ENGINEER with escalation.decision.decision APPLY_FIX: the fix
      (action, approved_by) passed Validate, so it is approved by an engineer and may
      be promoted at once (spec EL2)
  status RESOLVED_BY_ENGINEER with RESOLVED_MANUALLY: fixed outside the agent, the
      engineer's note says how; nothing was executed or released
  status DISMISSED: the engineer decided not to fix it; the records stay quarantined
  status FAILED: escalate or engineer_review kept raising (error), nobody decided
escalation.history holds the earlier rounds when a fix came back to the engineer.
"""
from agent.state import NOT_IMPLEMENTED, STATUS_RESOLVED, RecoveryState, step

NAME = "learn"


def learn(state: RecoveryState) -> dict:
    return {
        "learned": {"status": NOT_IMPLEMENTED, "written": False,
                    "reasons": ["learn is not built yet (Task 107); no incident was stored"]},
        # Escalate has already set the status when the failure went to an engineer.
        "status": state.get("status") or STATUS_RESOLVED,
        "stubbed": [NAME],
        "trace": [step(NAME, "stub: nothing stored (Task 107)")],
    }
