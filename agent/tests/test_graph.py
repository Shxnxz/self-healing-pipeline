"""The recovery graph: python -m pytest agent/tests   (no Kafka, no Neo4j)"""
import sys
from pathlib import Path

import pytest

from agent import reference
from agent.graph import build_graph
from agent.state import NOT_IMPLEMENTED, initial_state
from agent.tests.helpers import (PAST_FIX, canonical_values, diagnose_july, gate_passes,
                                 gate_passes_without_action, jule_quarantine, path, remediate_ok, stack_item,
                                 validate_fails, validate_ok)


def run(item, **nodes):
    return build_graph(nodes=nodes).invoke(initial_state("L1", item))


def test_graph_has_the_edges_of_the_spec():
    edges = {(e.source, e.target) for e in build_graph().get_graph().edges}
    assert edges == {
        ("__start__", "deterministic_lookup"),
        ("deterministic_lookup", "remediate"), ("deterministic_lookup", "diagnose"),
        ("diagnose", "policy_gate"),
        ("policy_gate", "remediate"), ("policy_gate", "escalate"),
        ("remediate", "validate"), ("remediate", "escalate"),
        ("validate", "learn"), ("validate", "escalate"),
        ("escalate", "learn"),
        ("learn", "__end__"),
    }


# --- with the steps built so far: nothing is released, everything reaches an engineer
def test_new_failure_is_diagnosed_then_escalated_by_the_gate():
    state = run(stack_item(route="diagnose"))
    assert path(state) == ["deterministic_lookup", "diagnose", "policy_gate", "escalate", "learn"]
    assert state["status"] == "ESCALATED"
    assert state["escalation"]["reason"] == "POLICY_GATE_REJECTED"
    assert state["action"] is None and "execution" not in state


def test_known_failure_skips_the_llm_and_carries_the_past_fix():
    state = run(stack_item(route="remediate", match=PAST_FIX))
    assert path(state) == ["deterministic_lookup", "remediate", "escalate", "learn"]
    assert state["action"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                               "source": "INCIDENT", "incident_id": "i1"}
    assert "diagnosis" not in state


def test_remediate_that_cannot_execute_never_claims_a_fix():
    # Nothing is readable in this test, so the reused fix cannot be checked or staged.
    state = run(stack_item(route="remediate", match=PAST_FIX))
    assert state["execution"]["status"] == "FAILED" and state["execution"]["rows_affected"] == 0
    assert state["escalation"]["reason"] == "REMEDIATION_FAILED"
    assert state["escalation"]["details"] == state["execution"]["reasons"] != []
    assert state["status"] == "ESCALATED" and state["learned"]["written"] is False


def test_executed_fix_goes_to_validate_which_is_still_a_stub(tmp_path, monkeypatch):
    monkeypatch.setattr(reference, "canonical_values", canonical_values)
    jule_quarantine(tmp_path / "silver2_quarantine")
    state = run(stack_item(route="remediate", match=PAST_FIX))
    assert path(state) == ["deterministic_lookup", "remediate", "validate", "escalate", "learn"]
    assert state["execution"]["status"] == "SUCCESS" and state["execution"]["rows_affected"] == 10
    assert state["validation"]["status"] == NOT_IMPLEMENTED
    assert state["escalation"]["reason"] == "VALIDATION_FAILED" and state["status"] == "ESCALATED"


def test_remediate_verdict_without_a_past_fix_is_diagnosed():
    state = run(stack_item(route="remediate", match=None))
    assert state["route"] == "diagnose"
    assert path(state)[:2] == ["deterministic_lookup", "diagnose"]


# --- with fake nodes: the routes the later tasks will use -------------------------
def test_reused_fix_that_validates_is_learned_without_escalation():
    state = run(stack_item(route="remediate", match=PAST_FIX), remediate=remediate_ok, validate=validate_ok)
    assert path(state) == ["deterministic_lookup", "remediate", "validate", "learn"]
    assert state["status"] == "RESOLVED" and "escalation" not in state


def test_diagnosis_that_passes_the_gate_is_remediated():
    state = run(stack_item(route="diagnose"), diagnose=diagnose_july, policy_gate=gate_passes,
                remediate=remediate_ok, validate=validate_ok)
    assert path(state) == ["deterministic_lookup", "diagnose", "policy_gate", "remediate", "validate", "learn"]
    assert state["action"]["source"] == "DIAGNOSIS" and state["status"] == "RESOLVED"


def test_gate_that_passes_without_an_action_still_escalates():
    state = run(stack_item(route="diagnose"), diagnose=diagnose_july,
                policy_gate=gate_passes_without_action, remediate=remediate_ok)
    assert "remediate" not in path(state) and state["status"] == "ESCALATED"


def test_failed_validation_is_escalated():
    state = run(stack_item(route="remediate", match=PAST_FIX), remediate=remediate_ok, validate=validate_fails)
    assert path(state) == ["deterministic_lookup", "remediate", "validate", "escalate", "learn"]
    assert state["escalation"] == {"status": "PENDING_REVIEW", "reason": "VALIDATION_FAILED",
                                   "details": ["new failure in weather_condition"]}


def test_unknown_node_name_is_rejected():
    with pytest.raises(ValueError, match="remediat"):
        build_graph(nodes={"remediat": remediate_ok})


# --- contract with the lookup service -----------------------------------------------
def test_accepts_a_stack_built_by_the_lookup_service():
    """Build the message with lookup/ itself, so a change of its shape fails here."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lookup"))
    from blast_radius import LineageView
    from failures import StructuredFailure
    from patterns import evaluate
    from stack import build_stack, envelope

    jule = StructuredFailure(source="QUARANTINE", failure_type="UNRESOLVED_VALUE", origin_layer="silver1",
                             detected_layer="silver2", folder="Crash1", column_name="crash_month",
                             raw_value="Jule", rows_affected=10)
    misty = StructuredFailure(source="QUARANTINE", failure_type="UNRESOLVED_VALUE", origin_layer="silver1",
                              detected_layer="silver2", folder="Crash1", column_name="weather_condition",
                              raw_value="misty dark", rows_affected=3)
    lineage = LineageView("silver1_crash1", "silver2_crash1", True,
                          [{"name": "gold", "kind": "delta", "depth": 2, "columns": ["crash_month"]}])
    past = {"incident_id": "i1", "failure_type": "UNRESOLVED_VALUE", "table_name": "silver2_crash1",
            "column_name": "crash_month", "raw_value": "JULE", "action": "ADD_VARIANT",
            "action_params": '{"canonical_value": "JULY"}', "outcome": "SUCCESS"}
    contexts = [evaluate(jule, "silver2_crash1", past, [], []), evaluate(misty, "silver2_crash1", None, [], [])]
    message = envelope(build_stack([jule, misty], [lineage, lineage], contexts))

    states = {it["failure"]["column_name"]: build_graph().invoke(initial_state(message["lookup_id"], it))
              for it in message["stack"]}
    known, new = states["crash_month"], states["weather_condition"]
    assert known["failure_id"] == jule.failure_id and known["route"] == "remediate"
    assert known["action"]["action_params"] == {"canonical_value": "JULY"}
    assert new["route"] == "diagnose" and "diagnose" in path(new)
    assert known["priority"]["tier"] in {"P1", "P2", "P3"} and known["priority"]["rank"] in {1, 2}
