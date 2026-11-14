"""Incident memory against a real Neo4j: the Cypher Learn and the lookup run.

Skipped unless NEO4J_TEST_URI points at a Neo4j 5 it may write to (the test deletes
the incidents it creates, and only those):

  docker run -d --name neo4j-test -p 7687:7687 -e NEO4J_AUTH=neo4j/selfhealing neo4j:5.26-community
  NEO4J_TEST_URI=bolt://localhost:7687 python -m pytest agent/tests/test_incidents_neo4j.py
"""
import importlib
import os
import sys
from pathlib import Path

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from agent import config, reference
from agent.graph import build_graph
from agent.memory import incidents
from agent.state import initial_state
from agent.tests.helpers import canonical_values, diagnose_july, gate_passes, jule_quarantine, stack_item

URI = os.environ.get("NEO4J_TEST_URI")
pytestmark = [pytest.mark.neo4j, pytest.mark.skipif(not URI, reason="NEO4J_TEST_URI not set")]
PREFIX = "t107-"


@pytest.fixture
def driver(monkeypatch):
    from neo4j import GraphDatabase

    drv = GraphDatabase.driver(URI, auth=(os.environ.get("NEO4J_TEST_USER", "neo4j"),
                                          os.environ.get("NEO4J_TEST_PASSWORD", "selfhealing")))
    incidents.apply_schema(drv)
    clean = "MATCH (i:Incident) WHERE i.incident_id STARTS WITH $p OR i.failure_id STARTS WITH $p DETACH DELETE i"
    drv.execute_query(clean, p=PREFIX)
    monkeypatch.setattr(incidents, "shared_driver", lambda: drv)
    yield drv
    drv.execute_query(clean, p=PREFIX)
    drv.close()


def write(drv, incident_id, *, action="ADD_VARIANT", canonical="JULY", outcome="SUCCESS", embedding=None, **extra):
    return incidents.write_incident(
        drv, incident_id=PREFIX + incident_id, table_name="silver2_crash1", column_name="crash_month",
        raw_value="Jule", failure_type="UNRESOLVED_VALUE", diagnosis="d", action=action,
        action_params={"canonical_value": canonical} if action == "ADD_VARIANT" else {}, outcome=outcome,
        resolved_by="LLM", confidence=0.9, verified=outcome == "SUCCESS", rows_affected=4, embedding=embedding,
        signature_text=incidents.signature_text("silver2_crash1", "crash_month", "Jule", "UNRESOLVED_VALUE"),
        extra={"failure_id": PREFIX + incident_id, "batch_ids": ["silver2_crash1:7"], **extra})


def test_an_incident_without_a_vector_is_stored_and_found_exactly(driver):
    write(driver, "a", borrowed=False)
    hit = incidents.find_exact(driver, "silver2_crash3", "crash_month", " JULE ")
    assert hit["incident_id"] == PREFIX + "a" and hit["borrowed"] is False and hit["score"] == 1.0
    records, _, _ = driver.execute_query(
        "MATCH (i:Incident {incident_id: $id})-[:AFFECTS]->(t:Table) RETURN i.embedding AS e, i.batch_ids AS b, t.name AS t",
        id=PREFIX + "a")
    assert records[0]["e"] is None and records[0]["b"] == ["silver2_crash1:7"] and records[0]["t"] == "silver2_crash1"


def test_an_incident_with_a_vector_is_found_by_similarity(driver):
    vector = [0.0] * 767 + [1.0]
    write(driver, "v", embedding=vector)
    hits = incidents.find_similar(driver, vector, k=50)
    assert PREFIX + "v" in [h["incident_id"] for h in hits]


def test_exact_lookup_prefers_an_approved_fix_and_skips_manual_ones(driver):
    write(driver, "borrowed", borrowed=True)
    assert incidents.find_exact(driver, "silver2_crash1", "crash_month", "Jule")["borrowed"] is True
    write(driver, "approved", borrowed=False, approved_by="thay")
    write(driver, "manual", action="MANUAL")   # the newest, but there is nothing to repeat
    hit = incidents.find_exact(driver, "silver2_crash1", "crash_month", "Jule")
    assert hit["incident_id"] == PREFIX + "approved" and hit["approved_by"] == "thay"


def test_writing_again_updates_and_none_removes_a_property(driver):
    write(driver, "a", engineer_note="first")
    write(driver, "a", engineer_note=None, rows=1)
    records, _, _ = driver.execute_query(
        "MATCH (i:Incident {incident_id: $id}) RETURN count(*) AS n, i.engineer_note AS note", id=PREFIX + "a")
    assert records[0]["n"] == 1 and records[0]["note"] is None


def test_history_and_promotion_marks(driver):
    write(driver, "ok")
    write(driver, "bad", canonical="JUNE", outcome="FAILED_VALIDATION")
    history = incidents.signature_history(driver, "silver2|crash_month|JULE")
    ours = {h["incident_id"]: h for h in history if h["incident_id"].startswith(PREFIX)}
    assert ours[PREFIX + "ok"]["action_params"] == {"canonical_value": "JULY"}
    assert ours[PREFIX + "ok"]["batch_ids"] == ["silver2_crash1:7"]
    assert ours[PREFIX + "bad"]["outcome"] == "FAILED_VALIDATION"
    incidents.mark_promotion(driver, PREFIX + "ok", "PROMOTED", "approved")
    records, _, _ = driver.execute_query(
        "MATCH (i:Incident {incident_id: $id}) RETURN i.promoted AS p, i.promotion AS s", id=PREFIX + "ok")
    assert records[0]["p"] is True and records[0]["s"] == "PROMOTED"


def test_the_lookup_reads_what_learn_wrote(driver):
    """lookup/patterns.py on the incidents Learn stores: exact, with `borrowed` carried."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lookup"))
    patterns = importlib.import_module("patterns")
    failures = importlib.import_module("failures")
    write(driver, "b", borrowed=True)
    failure = failures.StructuredFailure(source="QUARANTINE", failure_type="UNRESOLVED_VALUE", origin_layer="silver1",
                                         detected_layer="silver2", folder="Crash3", column_name="crash_month",
                                         raw_value="jule", rows_affected=3)
    exact, candidates, attempts = patterns.fetch_candidates(driver, failure, "silver2_crash3")
    context = patterns.evaluate(failure, "silver2_crash3", exact, candidates, attempts)
    assert context["match"]["incident_id"] == PREFIX + "b"
    assert context["match"]["matched_by"] == "EXACT" and context["match"]["borrowed"] is True


def test_a_workflow_stores_its_incident_in_neo4j(driver, monkeypatch):
    """The real graph with the real Learn writing to Neo4j (promotion stays in memory)."""
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)
    jule_quarantine(config.QUARANTINE_PATH, rows=3)
    graph = build_graph(InMemorySaver(), nodes={"diagnose": diagnose_july, "policy_gate": gate_passes},
                        retry_seconds=0)
    item = stack_item(PREFIX + "wf", rows=3)
    out = graph.invoke(initial_state("L1", item), {"configurable": {"thread_id": PREFIX + "wf"}})
    assert out["learned"]["status"] == "STORED", out["trace"][-1]
    hit = incidents.find_exact(driver, "silver2_crash2", "crash_month", "JULE")
    records, _, _ = driver.execute_query(
        "MATCH (i:Incident {incident_id: $id}) RETURN i.outcome AS o, i.resolved_by AS r, i.promotion AS p, "
        "i.fix_source AS s, i.borrowed AS b", id=PREFIX + "wf")
    assert dict(records[0]) == {"o": "SUCCESS", "r": "LLM", "p": "BELOW_THRESHOLD", "s": "DIAGNOSIS", "b": False}
    assert hit is not None
