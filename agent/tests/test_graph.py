"""The recovery graph: python -m pytest agent/tests   (no Kafka, no Neo4j)"""
import sys
from pathlib import Path

import pytest

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import interrupt

from agent.graph import build_graph
from agent.state import NOT_IMPLEMENTED, initial_state
from agent.tests.helpers import (PAST_FIX, diagnose_july, escalate_ok, gate_passes,
                                 gate_passes_without_action, learn_ok, path, remediate_ok, stack_item,
                                 trace, validate_fails, validate_ok)


def run(item, **nodes):
    return build_graph(nodes=nodes, retry_seconds=0).invoke(initial_state("L1", item))


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
        # a step that keeps raising goes to the engineer from anywhere
        ("deterministic_lookup", "escalate"), ("diagnose", "escalate"),
    }


# --- with the stubs: nothing is fixed, everything reaches an engineer --------------
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


def test_stub_remediate_never_claims_a_fix():
    state = run(stack_item(route="remediate", match=PAST_FIX))
    assert state["execution"]["status"] == NOT_IMPLEMENTED
    assert state["escalation"]["reason"] == "REMEDIATION_FAILED"
    assert state["status"] == "ESCALATED" and state["learned"]["written"] is False


def test_stubs_mark_the_workflow_so_it_is_not_taken_for_a_real_result():
    state = run(stack_item(route="diagnose"))
    assert state["stubbed"] == ["escalate", "learn"]   # Diagnose and the gate are real since Task 103
    built = run(stack_item(route="remediate", match=PAST_FIX), remediate=remediate_ok,
                validate=validate_ok, learn=learn_ok)
    assert built["stubbed"] == []


def test_remediate_verdict_without_a_past_fix_is_diagnosed():
    state = run(stack_item(route="remediate", match=None))
    assert state["route"] == "diagnose"
    assert path(state)[:2] == ["deterministic_lookup", "diagnose"]


@pytest.mark.parametrize("past, problem", [
    ({"action": "ADD_VARIANT", "action_params": {"canonical_value": "SMARCH"}}, "'SMARCH' is not a canonical value"),
    ({"action": "ADD_VARIANT", "action_params": None}, "None is not a canonical value"),
    ({"action": "RENAME_COLUMN", "action_params": {"renames": {"Month": "crash_month"}}}, "needs a schema failure"),
    ({"action": "DROP_TABLE", "action_params": {}}, "not a pre-approved action"),
    ({"action": "ESCALATE", "action_params": {}}, "ESCALATE is not a fix"),
])
def test_a_past_fix_that_no_longer_fits_the_failure_is_diagnosed(past, problem):
    """The gate checks a fresh proposal against the failure; a reused one gets the same check."""
    state = run(stack_item(route="remediate", match={**PAST_FIX, **past}))
    assert state["route"] == "diagnose" and path(state)[:2] == ["deterministic_lookup", "diagnose"]
    note = state["trace"][0]["note"]
    assert note.startswith(f"past fix {past['action']} no longer fits") and problem in note
    assert state["action"] is None


def test_a_past_fix_is_not_reused_when_the_canonical_values_cannot_be_read(monkeypatch):
    from agent import reference
    monkeypatch.setattr(reference, "canonical_values", lambda column, path=None: [])
    state = run(stack_item(route="remediate", match=PAST_FIX))
    assert state["route"] == "diagnose" and "no canonical values are known" in state["trace"][0]["note"]


def test_only_the_checked_parameters_of_a_past_fix_are_reused():
    match = {**PAST_FIX, "action_params": {"canonical_value": "JULY", "table": "gold"}}
    state = run(stack_item(route="remediate", match=match), remediate=remediate_ok, validate=validate_ok)
    assert state["action"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                               "source": "INCIDENT", "incident_id": "i1"}


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
    assert state["escalation"]["reason"] == "GATE_PASSED_WITHOUT_ACTION"  # not "rejected"


def test_failed_validation_is_escalated():
    state = run(stack_item(route="remediate", match=PAST_FIX), remediate=remediate_ok, validate=validate_fails)
    assert path(state) == ["deterministic_lookup", "remediate", "validate", "escalate", "learn"]
    assert state["escalation"] == {"status": "PENDING_REVIEW", "reason": "VALIDATION_FAILED",
                                   "details": ["new failure in weather_condition"]}


# --- a step that raises must not stop the agent ---------------------------------------
def test_step_that_keeps_raising_is_retried_then_escalated():
    calls = []

    def remediate_broken(state):
        calls.append(1)
        raise RuntimeError("Spark is down")

    state = run(stack_item(route="remediate", match=PAST_FIX), remediate=remediate_broken)
    assert len(calls) == 3
    assert path(state) == ["deterministic_lookup", "remediate", "escalate", "learn"]
    assert state["error"] == {"node": "remediate", "error": "RuntimeError: Spark is down", "attempts": 3}
    assert state["escalation"]["reason"] == "STEP_FAILED"
    assert "remediate: RuntimeError: Spark is down" in state["escalation"]["details"][0]
    assert state["status"] == "ESCALATED" and "execution" not in state


def test_step_that_raises_once_then_works_is_not_escalated():
    calls = []

    def remediate_flaky(state):
        calls.append(1)
        if len(calls) == 1:
            raise ConnectionError("Neo4j restarting")
        return remediate_ok(state)

    state = run(stack_item(route="remediate", match=PAST_FIX), remediate=remediate_flaky,
                validate=validate_ok, learn=learn_ok)
    assert len(calls) == 2 and "error" not in state and state["status"] == "RESOLVED"


def test_broken_diagnose_goes_to_the_engineer_not_to_the_gate():
    def diagnose_broken(state):
        raise TimeoutError("LLM did not answer")

    state = run(stack_item(route="diagnose"), diagnose=diagnose_broken)
    assert path(state) == ["deterministic_lookup", "diagnose", "escalate", "learn"]
    assert state["escalation"]["reason"] == "STEP_FAILED"


def test_broken_escalate_ends_the_workflow_as_failed():
    """Nothing is left to catch it, but the workflow still ends so the agent moves on."""
    def escalate_broken(state):
        raise RuntimeError("cannot reach the review queue")

    state = run(stack_item(route="diagnose"), escalate=escalate_broken, learn=learn_ok)
    assert state["status"] == "FAILED" and state["error"]["node"] == "escalate"
    assert path(state)[-2:] == ["escalate", "learn"]


def test_interrupt_in_a_step_still_pauses_the_workflow():
    """Task 106 will wait for the engineer with interrupt(); the retry guard must not eat it."""
    def escalate_waits(state):
        decision = interrupt({"ask": "approve?"})
        return {**escalate_ok(state), "escalation": {"status": "RESOLVED", "decision": decision}}

    graph = build_graph(InMemorySaver(), nodes={"escalate": escalate_waits, "learn": learn_ok}, retry_seconds=0)
    thread = {"configurable": {"thread_id": "f1"}}
    paused = graph.invoke(initial_state("L1", stack_item("f1")), thread)
    assert "__interrupt__" in paused and graph.get_state(thread).next == ("escalate",)
    assert "error" not in graph.get_state(thread).values


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
