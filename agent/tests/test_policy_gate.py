"""The policy gate: python -m pytest agent/tests   (deterministic, no model)"""
import pytest

from agent import config
from agent.graph import build_graph
from agent.nodes.diagnose import make_diagnose
from agent.nodes.policy_gate import make_policy_gate
from agent.state import initial_state
from agent.tests.helpers import (JULY_REPLY, FakeModel, canonical_values, diagnose_july, july_samples, path,
                                 remediate_ok, schema_item, stack_item, validate_ok)

JULY = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"}}


def gate(item=None, proposed=JULY, confidence=0.93, status="OK", **settings):
    state = initial_state("L1", item or stack_item())
    state["diagnosis"] = {"status": status, "hypothesis": "typo of July", "confidence": confidence,
                          "proposed_action": proposed, "reasons": []}
    return make_policy_gate(canonical_values=canonical_values, **settings)(state)


def failed(out):
    return [c["check"] for c in out["policy"]["checks"] if not c["passed"]]


# --- the two conditions of the spec (AP2) ---------------------------------------------
def test_confident_and_safe_passes_and_becomes_the_action():
    out = gate()
    assert out["policy"]["passed"] is True and out["policy"]["reasons"] == []
    assert out["action"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                             "source": "DIAGNOSIS", "incident_id": None}
    assert [c["check"] for c in out["policy"]["checks"]] == [
        "safe_action", "confidence", "trusted_failure", "parameters", "not_tried_before"]
    assert out["policy"]["threshold"] == 0.85 and out["policy"]["safe_actions"] == ["ADD_VARIANT"]
    assert out["trace"][0]["note"] == "passed: ADD_VARIANT at confidence 0.93"


def test_safe_but_not_confident_enough_is_rejected():
    out = gate(confidence=0.84)
    assert out["policy"]["passed"] is False and out["action"] is None and failed(out) == ["confidence"]
    assert out["policy"]["reasons"] == ["confidence 0.84 is below the threshold 0.85"]


def test_confidence_equal_to_the_threshold_passes():
    assert gate(confidence=0.85)["policy"]["passed"] is True
    assert gate(confidence=0.5, threshold=0.5)["policy"]["passed"] is True
    assert gate(confidence=0.99, threshold=1.0)["policy"]["passed"] is False


def test_confident_but_not_safe_is_rejected():
    proposed = {"action": "RENAME_COLUMN", "action_params": {
        "renames": {"CrashDate": "crash_date", "Weather": "weather_condition"}}}
    out = gate(schema_item(), proposed, confidence=0.99)
    assert out["action"] is None and failed(out) == ["safe_action"]
    assert out["policy"]["reasons"][0].startswith("RENAME_COLUMN is not in the safe-action set")


def test_neither_condition_holds_lists_both_reasons():
    proposed = {"action": "RENAME_COLUMN", "action_params": {"renames": {"CrashDate": "crash_date"}}}
    out = gate(schema_item(), proposed, confidence=0.3)
    assert failed(out) == ["safe_action", "confidence"] and len(out["policy"]["reasons"]) == 2


@pytest.mark.parametrize("confidence", ["0.93", None, True, float("nan"), 1.5, -0.1])
def test_a_confidence_that_is_not_a_probability_is_rejected(confidence):
    out = gate(confidence=confidence)
    assert out["action"] is None and failed(out) == ["confidence"]


# --- the proposal must fit the failure ------------------------------------------------
def test_asking_for_an_engineer_is_never_executed():
    out = gate(proposed={"action": "ESCALATE", "action_params": {}}, confidence=1.0)
    assert out["action"] is None and failed(out) == ["safe_action"]


def test_an_action_outside_the_pre_approved_set_is_rejected():
    out = gate(proposed={"action": "DROP_TABLE", "action_params": {}}, confidence=1.0)
    assert out["action"] is None and failed(out) == ["safe_action", "parameters"]


def test_a_value_that_is_not_canonical_for_the_column_is_rejected():
    out = gate(proposed={"action": "ADD_VARIANT", "action_params": {"canonical_value": "CLEAR"}})
    assert out["action"] is None and failed(out) == ["parameters"]
    assert out["policy"]["reasons"] == ["'CLEAR' is not a canonical value of crash_month"]


def test_unreadable_canonical_values_fail_closed():
    item = stack_item()
    item["failure"]["column_name"] = "crash_hour"   # canonical_values() answers [] for it
    out = gate(item)
    assert out["action"] is None and failed(out) == ["parameters"]


def test_only_checked_parameters_reach_remediate():
    proposed = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY", "table": "gold"}}
    assert gate(proposed=proposed)["action"]["action_params"] == {"canonical_value": "JULY"}


def test_a_failure_whose_classification_needs_review_is_never_fixed_automatically():
    item = stack_item()
    item["failure"]["trusted"] = False
    out = gate(item, confidence=0.99)
    assert out["action"] is None and failed(out) == ["trusted_failure"]


def test_a_fix_that_already_failed_for_this_failure_is_not_repeated():
    item = stack_item()
    item["incident_context"]["previous_attempts"] = [
        {"incident_id": "i9", "action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
         "outcome": "FAILED_VALIDATION"}]
    out = gate(item)
    assert out["action"] is None and failed(out) == ["not_tried_before"]
    assert "already ended FAILED_VALIDATION" in out["policy"]["reasons"][0]
    # the other month was not tried, so it may run
    assert gate(item, {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JUNE"}})["policy"]["passed"]


# --- no usable diagnosis ----------------------------------------------------------------
@pytest.mark.parametrize("status", ["UNAVAILABLE", "INVALID_OUTPUT", "NOT_IMPLEMENTED"])
def test_a_diagnosis_that_failed_is_rejected_whatever_it_carries(status):
    out = gate(status=status, confidence=1.0)
    assert out["action"] is None and failed(out) == ["diagnosis"]
    assert out["policy"]["reasons"][0].startswith(f"no usable diagnosis ({status})")


@pytest.mark.parametrize("proposed", [None, {}, {"action": None}, "ADD_VARIANT", {"action": ["ADD_VARIANT"]}])
def test_a_diagnosis_without_a_proposal_is_rejected(proposed):
    out = gate(proposed=proposed, confidence=1.0)
    assert out["action"] is None and failed(out) == ["diagnosis"]


def test_a_missing_diagnosis_is_rejected():
    out = make_policy_gate(canonical_values=canonical_values)(initial_state("L1", stack_item()))
    assert out["policy"]["passed"] is False and out["action"] is None


# --- settings ---------------------------------------------------------------------------
def test_threshold_and_safe_set_come_from_the_settings(monkeypatch):
    monkeypatch.setattr(config, "POLICY_CONFIDENCE_THRESHOLD", 0.95)
    assert gate(confidence=0.93)["policy"]["reasons"] == ["confidence 0.93 is below the threshold 0.95"]

    monkeypatch.setattr(config, "POLICY_CONFIDENCE_THRESHOLD", 0.85)
    monkeypatch.setattr(config, "POLICY_SAFE_ACTIONS", [])   # POLICY_SAFE_ACTIONS=NONE
    out = gate()
    assert out["action"] is None and out["policy"]["safe_actions"] == []
    assert out["policy"]["reasons"] == ["ADD_VARIANT is switched off by POLICY_SAFE_ACTIONS"]


# --- through the graph, with the real Diagnose and gate ----------------------------------
def run(item, reply, **nodes):
    steps = {"diagnose": make_diagnose(chat=FakeModel(reply), canonical_values=canonical_values,
                                       sample_records=july_samples),
             "policy_gate": make_policy_gate(canonical_values=canonical_values), **nodes}
    return build_graph(nodes=steps).invoke(initial_state("L1", item))


def test_a_confident_safe_diagnosis_reaches_remediate_with_the_checked_action():
    state = run(stack_item(), JULY_REPLY, remediate=remediate_ok, validate=validate_ok)
    assert path(state) == ["deterministic_lookup", "diagnose", "policy_gate", "remediate", "validate", "learn"]
    assert state["action"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                               "source": "DIAGNOSIS", "incident_id": None}
    assert state["status"] == "RESOLVED"


def test_a_rejected_diagnosis_reaches_the_engineer_with_the_reasons():
    state = run(stack_item(), {**JULY_REPLY, "confidence": 0.6}, remediate=remediate_ok, validate=validate_ok)
    assert path(state) == ["deterministic_lookup", "diagnose", "policy_gate", "escalate", "learn"]
    assert state["action"] is None and "execution" not in state and state["status"] == "ESCALATED"
    assert state["escalation"]["reason"] == "POLICY_GATE_REJECTED"
    assert state["escalation"]["details"] == ["confidence 0.60 is below the threshold 0.85"]
    assert state["diagnosis"]["hypothesis"].startswith("Typo of July")   # the engineer still gets it


def test_the_gate_works_with_a_diagnosis_written_without_a_status():
    """Task 102's fake Diagnose has no `status`; an engineer's note would not either."""
    state = build_graph(nodes={"diagnose": diagnose_july, "remediate": remediate_ok, "validate": validate_ok,
                               "policy_gate": make_policy_gate(canonical_values=canonical_values)}
                        ).invoke(initial_state("L1", stack_item()))
    assert state["policy"]["passed"] is True and state["status"] == "RESOLVED"


@pytest.mark.parametrize("value, expected", [
    (None, None), ("", None), ("  ", None),              # unset or empty: the safe set of actions.py
    ("NONE", []), ("none", []),                           # every diagnosis is reviewed
    ("ADD_VARIANT", ["ADD_VARIANT"]), ("ADD_VARIANT, RENAME_COLUMN", ["ADD_VARIANT", " RENAME_COLUMN"]),
])
def test_policy_safe_actions_setting(monkeypatch, value, expected):
    import importlib

    if value is None:
        monkeypatch.delenv("POLICY_SAFE_ACTIONS", raising=False)
    else:
        monkeypatch.setenv("POLICY_SAFE_ACTIONS", value)
    try:
        assert importlib.reload(config).POLICY_SAFE_ACTIONS == expected
    finally:
        monkeypatch.undo()
        importlib.reload(config)
