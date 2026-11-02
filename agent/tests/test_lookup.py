"""Tests for agent/nodes/lookup.py, the LangGraph adapter over lookup/.

The scoring and matching logic is tested in lookup/tests; these tests cover input
conversion, the state the node returns, and walking the stack.
Run from the repo root:  python -m pytest agent/tests lookup/tests
"""
from unittest.mock import MagicMock, patch

from agent.nodes.lookup import (
    DONE,
    lookup_node,
    next_failure,
    route_after_lookup,
    to_failure,
)
from blast_radius import LineageView
from patterns import evaluate
from stack import build_stack, envelope


def _envelope(failures, incidents_by_index=None):
    """Build a real recovery.lookup message without Neo4j."""
    incidents_by_index = incidents_by_index or {}
    parsed = [to_failure(f) for f in failures]
    lineages = [LineageView(f.origin_layer, f.detected_layer, False) for f in parsed]
    ctxs = [evaluate(f, l.detected_table, incidents_by_index.get(i), [], [])
            for i, (f, l) in enumerate(zip(parsed, lineages))]
    return envelope(build_stack(parsed, lineages, ctxs))


LEGACY = {"table_name": "silver2_crash1", "column_name": "crash_month", "raw_value": "Jule",
          "rows_affected": 3}
QWEN = {"incident_id": "q1", "failure_type": "SCHEMA_MISMATCH", "table": "Crash4",
        "job_name": "Silver1SchemaValidation", "classification_status": "OK", "needs_review": False,
        "affected_dataset": "bronze", "affected_record_count": 40,
        "affected_columns": {"expected_only": ["crash_date"], "source_only": ["CRASH_DATE"]}}
PAST_FIX = {"incident_id": "inc-1", "failure_type": "UNRESOLVED_VALUE", "table_name": "silver2",
            "column_name": "crash_month", "raw_value": "Jule", "action": "ADD_VARIANT",
            "action_params": '{"canonical_value": "JULY"}'}


def test_legacy_group_is_converted_with_folder_and_layers():
    f = to_failure(LEGACY)
    assert (f.origin_layer, f.detected_layer, f.folder) == ("silver1", "silver2", "crash1")
    assert f.failure_type == "UNRESOLVED_VALUE" and f.rows_affected == 3
    assert to_failure({"table_name": "/data/delta/silver2", "column_name": "x"}).folder is None


def test_qwen_message_and_structured_dict_are_accepted():
    q = to_failure(QWEN)
    assert q.source == "QWEN" and q.origin_layer == "bronze"
    assert to_failure(q.to_dict()) == q


def test_node_uses_precomputed_stack_from_kafka_without_neo4j():
    result = _envelope([LEGACY, QWEN])
    updates = lookup_node({"lookup_result": result}, driver=MagicMock())
    assert len(updates["stack"]) == 2
    assert updates["current"]["rank"] == 1
    assert updates["failure"]["source"] == "QWEN"  # schema failure outranks one bad value
    assert updates["route"] == "diagnose"
    assert updates["diagnosis_context"] is updates["current"]
    assert "blast_radius" in updates["lineage_context"]


def test_remediate_item_exposes_action_for_remediate_node():
    result = _envelope([LEGACY], {0: PAST_FIX})
    updates = lookup_node({"lookup_result": result})
    assert route_after_lookup(updates) == "remediate"
    assert updates["action"] == "ADD_VARIANT"
    assert updates["action_params"] == {"canonical_value": "JULY"}
    assert updates["source_incident"] == "inc-1" and updates["confidence"] == 1.0


@patch("agent.nodes.lookup.run_lookup")
def test_node_runs_lookup_for_single_legacy_failure(mock_run):
    mock_run.return_value = _envelope([LEGACY])
    driver = MagicMock()
    updates = lookup_node({"failure_group": LEGACY}, driver=driver)
    (called_driver, failures, embed), _ = mock_run.call_args
    assert called_driver is driver and embed is None
    assert [f.column_name for f in failures] == ["crash_month"]
    assert updates["route"] == "diagnose"


def test_next_failure_walks_the_stack_until_done():
    state = lookup_node({"lookup_result": _envelope([LEGACY, QWEN])})
    state = {**state, **next_failure(state)}
    assert state["failure"]["source"] == "QUARANTINE"
    state = {**state, **next_failure(state)}
    assert state["current"] is None and route_after_lookup(state) == DONE


def test_empty_input_routes_to_done():
    with patch("agent.nodes.lookup.run_lookup", return_value=envelope([])):
        assert route_after_lookup(lookup_node({"failures": []}, driver=MagicMock())) == DONE
