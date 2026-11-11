"""Learn (spec EL2, EL3, KG2, TA3). STUB: the incident write is built in Task 107.

Reads   everything
Writes  learned = {"written": bool, ...}, status

It must write the outcome to Neo4j with agent.memory.incidents.write_incident()
for automated and engineer resolutions alike, and decide promotion into
ref_canonical_mappings. The stub writes nothing.
"""
from agent.state import NOT_IMPLEMENTED, STATUS_RESOLVED, RecoveryState, step

NAME = "learn"


def learn(state: RecoveryState) -> dict:
    return {
        "learned": {"status": NOT_IMPLEMENTED, "written": False,
                    "reasons": ["learn is not built yet (Task 107); no incident was stored"]},
        # Escalate has already set the status when the failure went to an engineer.
        "status": state.get("status") or STATUS_RESOLVED,
        "trace": [step(NAME, "stub: nothing stored (Task 107)")],
    }
