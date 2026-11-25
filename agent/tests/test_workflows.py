"""agent.workflows: a workflow's state reduced to the fields the evaluation reads."""
from agent.workflows import summarize


def _trace(*nodes):
    return [{"node": n, "at": f"2026-11-16T08:00:{i:02d}+00:00", "note": ""} for i, n in enumerate(nodes)]


def test_reused_fix_without_the_model():
    state = {
        "failure_id": "f1", "status": "RESOLVED", "route": "remediate", "lookup_route": "remediate",
        "failure": {"folder": "Crash1", "column_name": "crash_month", "raw_value": "JULE", "rows_affected": 4},
        "incident_context": {"match": {"matched_by": "EXACT", "score": 1.0, "incident_id": "old"}},
        "action": {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JUNE"}, "source": "INCIDENT",
                   "incident_id": "old"},
        "execution": {"status": "SUCCESS", "execution_id": "e1", "executed_at": "t2"},
        "validation": {"status": "PASSED", "rows_released": 4, "validated_at": "t3"},
        "learned": {"status": "STORED", "outcome": "SUCCESS", "resolved_by": "LOOKUP",
                    "promotion": {"status": "PROMOTED", "route": "ENGINEER_APPROVED"}, "learned_at": "t4"},
        "trace": _trace("deterministic_lookup", "remediate", "validate", "learn"),
    }
    s = summarize(state, [])
    assert s["action_source"] == "INCIDENT" and s["reused_incident_id"] == "old"
    assert s["model_called"] is False and s["diagnose_runs"] == 0
    assert s["escalated"] is False and s["escalation_rounds"] == 0
    assert s["promotion_status"] == "PROMOTED" and s["ended_at"] == "t4"
    assert s["started_at"] == "2026-11-16T08:00:00+00:00" and s["waiting"] is False


def test_escalated_twice_and_waiting():
    state = {
        "failure_id": "f2", "status": "ESCALATED", "route": "diagnose",
        "failure": {"folder": "Crash3", "column_name": "weather_condition", "raw_value": "RAINING"},
        "diagnosis": {"status": "OK", "confidence": 0.7, "model": "qwen3:8b",
                      "proposed_action": {"action": "ADD_VARIANT", "action_params": {"canonical_value": "RAIN"}}},
        "policy": {"outcome": "REJECTED", "reasons": ["confidence"]},
        "escalation": {"status": "WAITING", "reason": "VALIDATION_FAILED", "escalated_at": "t9",
                       "history": [{"reason": "POLICY_GATE_REJECTED", "escalated_at": "t5"}]},
        "trace": _trace("deterministic_lookup", "diagnose", "policy_gate", "escalate", "engineer_review",
                        "remediate", "validate", "escalate"),
    }
    s = summarize(state, ["engineer_review"])
    assert s["waiting"] is True and s["ended_at"] is None
    assert s["model_called"] is True and s["diagnosis_params"] == {"canonical_value": "RAIN"}
    assert s["escalation_rounds"] == 2
    assert s["escalation_reasons"] == ["POLICY_GATE_REJECTED", "VALIDATION_FAILED"]
    assert s["escalated_at"] == "t5"


def test_skipped_diagnosis_is_not_a_model_call():
    state = {"failure_id": "f3", "diagnosis": {"status": "SKIPPED"}, "trace": _trace("deterministic_lookup", "diagnose")}
    assert summarize(state, [])["model_called"] is False
