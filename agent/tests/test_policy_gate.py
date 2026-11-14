"""The policy gate: python -m pytest agent/tests   (deterministic, no model)"""
import importlib

import pytest

from agent import config
from agent.graph import build_graph
from agent.nodes.diagnose import make_diagnose
from agent.nodes.policy_gate import make_policy_gate
from agent.state import initial_state
from agent.tests.helpers import (JULY_REPLY, FakeModel, canonical_values, diagnose_july, july_samples, path,
                                 remediate_ok, schema_item, stack_item, validate_ok)

JULY = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"}}
JUNE = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JUNE"}}
IDS = ["r1", "r2"]


def no_samples(failure, limit=None, path=None):
    return []


def gate(item=None, proposed=JULY, confidence=0.93, status="OK", samples=july_samples, **settings):
    """The gate on one diagnosis. By default two quarantined records dated in July back it."""
    state = initial_state("L1", item or stack_item())
    state["failure"].setdefault("sample_record_ids", IDS)
    state["diagnosis"] = {"status": status, "hypothesis": "typo of July", "confidence": confidence,
                          "proposed_action": proposed, "reasons": []}
    return make_policy_gate(canonical_values=canonical_values, sample_records=samples, **settings)(state)


def failed(out):
    return [c["check"] for c in out["policy"]["checks"] if not c["passed"]]


# --- the two conditions of the spec (AP2) ---------------------------------------------
def test_confident_and_safe_passes_and_becomes_the_action():
    out = gate()
    assert out["policy"]["passed"] is True and out["policy"]["reasons"] == []
    assert out["policy"]["outcome"] == "PASSED"
    assert out["action"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                             "source": "DIAGNOSIS", "incident_id": None}
    assert [c["check"] for c in out["policy"]["checks"]] == [
        "safe_action", "confidence", "trusted_failure", "parameters", "evidence", "not_tried_before"]
    assert out["policy"]["threshold"] == 0.85 and out["policy"]["unverified_threshold"] == 0.95
    assert out["policy"]["safe_actions"] == ["ADD_VARIANT"]
    assert out["trace"][0]["note"] == "passed: ADD_VARIANT at confidence 0.93"


def test_safe_but_not_confident_enough_is_rejected():
    out = gate(confidence=0.84)
    assert out["policy"]["passed"] is False and out["action"] is None and failed(out) == ["confidence"]
    assert out["policy"]["outcome"] == "REJECTED"
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
def test_asking_for_an_engineer_is_never_executed_and_is_not_called_a_rejection():
    out = gate(proposed={"action": "ESCALATE", "action_params": {}}, confidence=1.0)
    assert out["action"] is None and out["policy"]["passed"] is False
    assert out["policy"]["outcome"] == "ENGINEER_REQUESTED" and failed(out) == ["proposal"]
    assert out["policy"]["reasons"] == ["the model asked for an engineer: typo of July"]
    assert out["trace"][0]["note"].startswith("engineer_requested: the model asked for an engineer")


def test_an_empty_mandatory_field_is_reported_as_diagnose_asking_for_an_engineer():
    out = gate(proposed={"action": "ESCALATE", "action_params": {}}, confidence=1.0, status="SKIPPED")
    assert out["policy"]["outcome"] == "ENGINEER_REQUESTED"
    assert out["policy"]["reasons"][0].startswith("Diagnose asked for an engineer")


def test_an_action_outside_the_pre_approved_set_is_rejected():
    out = gate(proposed={"action": "DROP_TABLE", "action_params": {}}, confidence=1.0)
    assert out["action"] is None and failed(out) == ["safe_action", "parameters"]
    assert out["policy"]["outcome"] == "REJECTED"


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
    # the other month was not tried, so it may run when the records back it
    assert gate(item, JUNE, samples=june_samples)["policy"]["passed"]


# --- the records must back the value (agent/evidence.py) -----------------------------------
def june_samples(failure, limit=None, path=None):
    return [{"crash_date": "2024-06-22", "crash_month": "Jule"}, {"crash_date": "22/06/2024", "crash_month": "Jule"}]


def test_a_confident_value_that_the_records_contradict_is_rejected():
    """The reviewer's case: Jule -> JULY at 0.97, but every crash_date is in June."""
    out = gate(confidence=0.97, samples=june_samples)
    assert out["action"] is None and failed(out) == ["evidence"]
    assert out["policy"]["reasons"] == [
        "2 of 2 sample records rule out JULY: crash_date '2024-06-22' gives JUNE"]


def test_one_contradicting_record_is_enough():
    def mixed(failure, limit=None, path=None):
        return july_samples(failure) + [{"crash_date": "06/30/2019", "crash_month": "Jule"}]

    out = gate(confidence=0.99, samples=mixed)
    assert failed(out) == ["evidence"] and "1 of 3 sample records rule out JULY" in out["policy"]["reasons"][0]


def test_records_that_confirm_the_value_let_the_normal_threshold_apply():
    out = gate(confidence=0.85)
    assert out["policy"]["passed"] is True
    evidence = [c for c in out["policy"]["checks"] if c["check"] == "evidence"][0]
    assert evidence["detail"] == "crash_date of 1 sample record(s) gives JULY, none says otherwise"


def test_the_weekday_is_checked_against_the_date_too():
    item = stack_item(value="Thrusday")
    item["failure"]["column_name"] = "crash_day_of_week"

    def samples(failure, limit=None, path=None):
        return [{"crash_date": "2024-06-20", "crash_day_of_week": "Thrusday"}]   # a Thursday

    thursday = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "THURSDAY"}}
    tuesday = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "TUESDAY"}}
    assert gate(item, thursday, confidence=0.86, samples=samples)["policy"]["passed"] is True
    assert failed(gate(item, tuesday, confidence=0.99, samples=samples)) == ["evidence"]


def weather_item():
    item = stack_item(value="misty dark")
    item["failure"]["column_name"] = "weather_condition"
    return item


FOG = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "FOG"}}


def test_a_value_nothing_can_confirm_needs_the_stricter_threshold():
    out = gate(weather_item(), FOG, confidence=0.93)
    assert out["action"] is None and failed(out) == ["evidence"]
    assert out["policy"]["reasons"] == ["no other field of a record determines weather_condition, so the "
                                        "stricter threshold 0.95 applies and confidence 0.93 is below it"]
    assert gate(weather_item(), FOG, confidence=0.95)["policy"]["passed"] is True


def test_a_month_without_a_readable_date_is_unconfirmed_too():
    for samples in (no_samples, lambda failure, limit=None, path=None: [{"crash_date": "07/04/2019"},
                                                                         {"crash_date": "yesterday"}, {}]):
        out = gate(confidence=0.93, samples=samples)
        assert failed(out) == ["evidence"] and "no sample record has a crash_date that decides" in \
            out["policy"]["reasons"][0]
        assert gate(confidence=0.96, samples=samples)["policy"]["passed"] is True


def test_never_sends_every_unconfirmed_value_to_an_engineer():
    out = gate(weather_item(), FOG, confidence=1.0, unverified_threshold=float("inf"))
    assert out["action"] is None and failed(out) == ["evidence"]
    assert out["policy"]["reasons"] == ["no other field of a record determines weather_condition, "
                                        "so an engineer is required"]
    assert out["policy"]["unverified_threshold"] is None
    # a value the records confirm is not affected
    assert gate(confidence=0.9, unverified_threshold=float("inf"))["policy"]["passed"] is True


def test_the_stricter_threshold_is_never_below_the_normal_one():
    out = gate(weather_item(), FOG, confidence=0.93, threshold=0.9, unverified_threshold=0.6)
    assert out["policy"]["passed"] is True and out["policy"]["unverified_threshold"] == 0.9
    assert failed(gate(weather_item(), FOG, confidence=0.89, threshold=0.9, unverified_threshold=0.6)) == [
        "confidence", "evidence"]


def test_the_gate_reads_the_records_itself_and_asks_for_all_the_failure_has():
    asked = []

    def samples(failure, limit=None, path=None):
        asked.append(limit)
        return july_samples(failure)

    item = stack_item()
    item["failure"]["sample_record_ids"] = ["r1", "r2", "r3", "r4", "r5"]
    assert gate(item, samples=samples)["policy"]["passed"] is True and asked == [5]


# --- no usable diagnosis ----------------------------------------------------------------
@pytest.mark.parametrize("status", ["UNAVAILABLE", "INVALID_OUTPUT", "NOT_IMPLEMENTED"])
def test_a_diagnosis_that_failed_is_rejected_whatever_it_carries(status):
    out = gate(status=status, confidence=1.0)
    assert out["action"] is None and failed(out) == ["diagnosis"]
    assert out["policy"]["outcome"] == "NO_DIAGNOSIS"
    assert out["policy"]["reasons"][0].startswith(f"no usable diagnosis ({status})")


@pytest.mark.parametrize("proposed", [None, {}, {"action": None}, "ADD_VARIANT", {"action": ["ADD_VARIANT"]}])
def test_a_diagnosis_without_a_proposal_is_rejected(proposed):
    out = gate(proposed=proposed, confidence=1.0)
    assert out["action"] is None and failed(out) == ["diagnosis"]


def test_a_missing_diagnosis_is_rejected():
    out = make_policy_gate(canonical_values=canonical_values)(initial_state("L1", stack_item()))
    assert out["policy"]["passed"] is False and out["action"] is None
    assert out["policy"]["outcome"] == "NO_DIAGNOSIS"


# --- settings ---------------------------------------------------------------------------
def test_threshold_and_safe_set_come_from_the_settings(monkeypatch):
    monkeypatch.setattr(config, "POLICY_CONFIDENCE_THRESHOLD", 0.95)
    assert gate(confidence=0.93)["policy"]["reasons"] == ["confidence 0.93 is below the threshold 0.95"]

    monkeypatch.setattr(config, "POLICY_CONFIDENCE_THRESHOLD", 0.85)
    monkeypatch.setattr(config, "POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD", 0.9)
    assert gate(weather_item(), FOG, confidence=0.93)["policy"]["passed"] is True
    monkeypatch.setattr(config, "POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD", float("inf"))   # NEVER
    assert failed(gate(weather_item(), FOG, confidence=1.0)) == ["evidence"]

    monkeypatch.setattr(config, "POLICY_CONFIDENCE_THRESHOLD", 0.85)
    monkeypatch.setattr(config, "POLICY_SAFE_ACTIONS", [])   # POLICY_SAFE_ACTIONS=NONE
    out = gate()
    assert out["action"] is None and out["policy"]["safe_actions"] == []
    assert out["policy"]["reasons"] == ["ADD_VARIANT is switched off by POLICY_SAFE_ACTIONS"]


# --- through the graph, with the real Diagnose and gate ----------------------------------
def run(item, reply, **nodes):
    steps = {"diagnose": make_diagnose(chat=FakeModel(reply), canonical_values=canonical_values,
                                       sample_records=july_samples),
             "policy_gate": make_policy_gate(canonical_values=canonical_values, sample_records=july_samples),
             **nodes}
    item["failure"].setdefault("sample_record_ids", IDS)
    return build_graph(nodes=steps).invoke(initial_state("L1", item))


def test_a_confident_safe_diagnosis_reaches_remediate_with_the_checked_action():
    state = run(stack_item(), JULY_REPLY, remediate=remediate_ok, validate=validate_ok)
    assert path(state) == ["deterministic_lookup", "diagnose", "policy_gate", "remediate", "validate", "learn"]
    assert state["action"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                               "source": "DIAGNOSIS", "incident_id": None}
    assert state["status"] == "RESOLVED"


def test_a_rejected_diagnosis_reaches_the_engineer_with_the_reasons():
    state = run(stack_item(), {**JULY_REPLY, "confidence": 0.6}, remediate=remediate_ok, validate=validate_ok)
    assert path(state) == ["deterministic_lookup", "diagnose", "policy_gate", "escalate"]   # waits for the engineer
    assert state["action"] is None and "execution" not in state and state["status"] == "ESCALATED"
    assert state["escalation"]["reason"] == "POLICY_GATE_REJECTED"
    assert state["escalation"]["details"] == ["confidence 0.60 is below the threshold 0.85"]
    assert state["diagnosis"]["hypothesis"].startswith("Typo of July")   # the engineer still gets it


def test_the_gate_works_with_a_diagnosis_written_without_a_status():
    """Task 102's fake Diagnose has no `status`; an engineer's note would not either."""
    item = stack_item()
    item["failure"]["sample_record_ids"] = IDS
    gate_node = make_policy_gate(canonical_values=canonical_values, sample_records=july_samples)
    state = build_graph(nodes={"diagnose": diagnose_july, "remediate": remediate_ok, "validate": validate_ok,
                               "policy_gate": gate_node}).invoke(initial_state("L1", item))
    assert state["policy"]["passed"] is True and state["status"] == "RESOLVED"


def test_a_model_that_asks_for_an_engineer_reaches_one_without_a_blocked_fix():
    reply = {"hypothesis": "The records point to June and July alike.", "action": "ESCALATE",
             "canonical_value": None, "confidence": 0.4}
    state = run(stack_item(), reply)
    assert path(state) == ["deterministic_lookup", "diagnose", "policy_gate", "escalate"]   # waits for the engineer
    assert state["policy"]["outcome"] == "ENGINEER_REQUESTED"
    assert state["escalation"]["reason"] == "ENGINEER_REQUESTED"
    assert state["escalation"]["details"] == [
        "the model asked for an engineer: The records point to June and July alike."]


# --- the settings themselves (agent/config.py) ---------------------------------------------
def settings(monkeypatch, **env):
    """agent.config as it would load with these environment variables."""
    for name in ("POLICY_SAFE_ACTIONS", "POLICY_CONFIDENCE_THRESHOLD", "POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD",
                 "DIAGNOSE_THINK"):
        monkeypatch.delenv(name, raising=False)
    for name, value in env.items():
        monkeypatch.setenv(name, value)
    try:
        loaded = importlib.reload(config)
        return {name: getattr(loaded, name) for name in dir(loaded) if name.isupper()}
    finally:
        monkeypatch.undo()
        importlib.reload(config)


@pytest.mark.parametrize("value, expected", [
    (None, None), ("", None), ("  ", None), (" , ", None),   # unset or empty: the safe set of actions.py
    ("NONE", []), ("none", []),                               # every diagnosis is reviewed
    ("ADD_VARIANT,NONE", []), ("none, add_variant", []),      # NONE wins wherever it stands
    ("ADD_VARIANT", ["ADD_VARIANT"]), ("add_variant, RENAME_COLUMN", ["ADD_VARIANT", "RENAME_COLUMN"]),
])
def test_policy_safe_actions_setting(monkeypatch, value, expected):
    env = {} if value is None else {"POLICY_SAFE_ACTIONS": value}
    assert settings(monkeypatch, **env)["POLICY_SAFE_ACTIONS"] == expected


def test_threshold_defaults():
    assert config.POLICY_CONFIDENCE_THRESHOLD == 0.85
    assert config.POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD == 0.95


@pytest.mark.parametrize("env, normal, unverified", [
    ({"POLICY_CONFIDENCE_THRESHOLD": "0.5"}, 0.5, 0.95),
    ({"POLICY_CONFIDENCE_THRESHOLD": "1"}, 1.0, 1.0),
    ({"POLICY_CONFIDENCE_THRESHOLD": "0.97"}, 0.97, 0.97),         # the stricter one follows it up
    ({"POLICY_CONFIDENCE_THRESHOLD": " "}, 0.85, 0.95),            # empty in compose = not set
    ({"POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD": "0.9"}, 0.85, 0.9),
    ({"POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD": "0.6"}, 0.85, 0.85),   # never below the normal one
    ({"POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD": "never"}, 0.85, float("inf")),
])
def test_thresholds_inside_the_allowed_range(monkeypatch, env, normal, unverified):
    loaded = settings(monkeypatch, **env)
    assert loaded["POLICY_CONFIDENCE_THRESHOLD"] == normal
    assert loaded["POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD"] == unverified


@pytest.mark.parametrize("env, problem", [
    ({"POLICY_CONFIDENCE_THRESHOLD": "0"}, "must be between 0.5 and 1.0"),      # would pass everything
    ({"POLICY_CONFIDENCE_THRESHOLD": "0.49"}, "must be between 0.5 and 1.0"),
    ({"POLICY_CONFIDENCE_THRESHOLD": "85"}, "must be between 0.5 and 1.0"),     # a percentage
    ({"POLICY_CONFIDENCE_THRESHOLD": "-1"}, "must be between"),
    ({"POLICY_CONFIDENCE_THRESHOLD": "nan"}, "must be between"),
    ({"POLICY_CONFIDENCE_THRESHOLD": "high"}, "is not a number"),
    ({"POLICY_CONFIDENCE_THRESHOLD": "NEVER"}, "is not a number"),              # only the stricter one has it
    ({"POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD": "0"}, "must be between 0.5 and 1.0"),
    ({"POLICY_UNVERIFIED_CONFIDENCE_THRESHOLD": "2"}, "must be between"),
])
def test_a_threshold_outside_the_range_stops_the_agent_at_start(monkeypatch, env, problem):
    with pytest.raises(ValueError, match=problem):
        settings(monkeypatch, **env)


@pytest.mark.parametrize("value, expected", [(None, False), ("", False), ("false", False), ("FALSE", False),
                                             ("true", True), ("default", None), ("maybe", False)])
def test_diagnose_think_setting(monkeypatch, value, expected):
    env = {} if value is None else {"DIAGNOSE_THINK": value}
    assert settings(monkeypatch, **env)["DIAGNOSE_THINK"] is expected
