"""Learn: the incident in memory and promotion into ref_canonical_mappings.

python -m pytest agent/tests/test_learn.py   (no Neo4j: the `memory` fixture is in memory)
"""
import importlib

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from agent import config, escalation as esc, promotion, reference
from agent.consumer import process_lookup, run_workflow
from agent.graph import build_graph
from agent.state import initial_state
from agent.tests.helpers import (PAST_FIX, canonical_values, diagnose_july, gate_passes, lookup_message, path,
                                 quarantine_row, stack_item, stub, trace, write_quarantine)

learn_node = importlib.import_module("agent.nodes.learn")   # agent.nodes exports the function as `learn`


@pytest.fixture(autouse=True)
def vocabulary(monkeypatch):
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)
    monkeypatch.setattr(config, "PROMOTE_MIN_OCCURRENCES", 5)
    monkeypatch.setattr(config, "PROMOTE_MIN_BATCHES", 3)


def thread(failure_id):
    return {"configurable": {"thread_id": failure_id}}


def graph(**nodes):
    return build_graph(InMemorySaver(), nodes=nodes, retry_seconds=0)


def item_in_batch(failure_id, batch, rows=2, value="Jule", route="diagnose", match=None, tier="P2", table="silver2_crash1"):
    """A failure of `rows` quarantined records in micro-batch `batch`, and those records."""
    item = stack_item(failure_id, route=route, match=match, value=value, rows=rows)
    item["failure"]["batch_id"] = str(batch)
    item["failure"]["evidence"] = {"quarantine_batches": [str(batch)]}
    item["priority"] = {"score": 80.0 if tier == "P1" else 42.0, "tier": tier}
    write_quarantine(config.QUARANTINE_PATH, [
        quarantine_row(f"{failure_id}-r{n}", {"crash_month": value}, batch=str(batch), table=table)
        for n in range(rows)])
    return item


AUTO = {"diagnose": diagnose_july, "policy_gate": gate_passes}   # the model's JULY passes the gate


def gate_rejects(state):
    return {"policy": {"passed": False, "outcome": "REJECTED", "reasons": ["confidence 0.60 is below 0.85"]},
            "trace": trace("policy_gate")}


def decide(g, failure_id, kind, note=None, action=None, params=None, promote=True):
    state = g.get_state(thread(failure_id)).values
    message = esc.make_decision(failure_id, state["escalation"]["escalation_id"], kind, "thay", note,
                                action, params, promote=promote)
    return g.invoke(Command(resume=message), thread(failure_id))


# --- what is stored ------------------------------------------------------------------
def test_an_automatic_fix_is_stored_as_a_verified_incident(memory):
    g = graph(**AUTO)
    out = g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    assert path(out) == ["deterministic_lookup", "diagnose", "policy_gate", "remediate", "validate", "learn"]
    assert out["status"] == "RESOLVED" and out["stubbed"] == []

    stored = memory.incidents["f1"]
    assert stored["signature_key"] == "silver2|crash_month|JULE"
    assert stored["outcome"] == "SUCCESS" and stored["verified"] is True and stored["resolved_by"] == "LLM"
    assert stored["action"] == "ADD_VARIANT" and stored["action_params"] == {"canonical_value": "JULY"}
    assert stored["diagnosis"] == "typo of July" and stored["confidence"] == 0.93
    assert stored["rows_affected"] == 2 and stored["batch_ids"] == ["silver2_crash1:7"]
    assert stored["table_name"] == "silver2_crash1" and stored["fix_source"] == "DIAGNOSIS"
    assert stored["borrowed"] is False and stored["execution_id"] == out["execution"]["execution_id"]
    assert stored["signature_text"] == "table=silver2_crash1 column=crash_month value=JULE failure=UNRESOLVED_VALUE"
    assert stored["embedding"] is None   # no LOOKUP_EMBED_MODEL in tests

    learned = out["learned"]
    assert learned["status"] == "STORED" and learned["incident_id"] == "f1" and learned["verified"] is True
    assert learned["promotion"]["status"] == "BELOW_THRESHOLD"
    assert "healed 2 rows in 1 batches so far, needs 5 rows in 3 batches" in learned["promotion"]["reasons"][0]
    assert memory.mappings == {} and stored["promotion"] == "BELOW_THRESHOLD"


def test_learn_can_run_twice_without_storing_twice(memory):
    g = graph(**AUTO)
    out = g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    again = learn_node.learn(out)
    assert again["learned"]["incident_id"] == "f1" and list(memory.incidents) == ["f1"]
    assert again["learned"]["promotion"]["occurrences"] == 2   # counted once


# --- promotion by recurrence (Thay, 2026-11-14: 5 rows across 3 batches) --------------
def test_a_fix_the_agent_made_is_promoted_once_it_recurs_in_three_batches(memory):
    g = graph(**AUTO)
    outs = [g.invoke(initial_state("L1", item_in_batch(f"f{b}", b, rows=2)), thread(f"f{b}")) for b in (7, 8, 9)]
    assert [o["learned"]["promotion"]["status"] for o in outs] == ["BELOW_THRESHOLD", "BELOW_THRESHOLD", "PROMOTED"]
    third = outs[-1]["learned"]["promotion"]
    assert (third["occurrences"], third["batches"], third["route"]) == (6, 3, "RECURRENCE")
    assert memory.promoted == [{"column": "crash_month", "raw": "JULE", "canonical": "JULY",
                                "added_by": "recovery_agent:recurrence", "source": "recovery_agent:f9"}]
    assert memory.incidents["f9"]["promoted"] is True

    # the next one finds the variant in the table already
    fourth = g.invoke(initial_state("L1", item_in_batch("f10", 10)), thread("f10"))
    assert fourth["learned"]["promotion"]["status"] == "ALREADY_IN_TABLE"


def test_three_batches_are_not_enough_without_five_rows(memory):
    g = graph(**AUTO)
    outs = [g.invoke(initial_state("L1", item_in_batch(f"f{b}", b, rows=1)), thread(f"f{b}")) for b in (7, 8, 9)]
    assert outs[-1]["learned"]["promotion"]["status"] == "BELOW_THRESHOLD"
    assert memory.mappings == {}


def test_the_same_batch_number_in_two_sources_is_two_batches():
    assert promotion.batch_key("silver2_crash1", "7") != promotion.batch_key("silver2_crash3", "7")


def test_never_turns_automatic_promotion_off(memory, monkeypatch):
    monkeypatch.setattr(config, "PROMOTE_MIN_OCCURRENCES", 0)
    g = graph(**AUTO)
    outs = [g.invoke(initial_state("L1", item_in_batch(f"f{b}", b, rows=5)), thread(f"f{b}")) for b in (7, 8, 9)]
    assert outs[-1]["learned"]["promotion"]["status"] == "BELOW_THRESHOLD"
    assert "automatic promotion is off" in outs[-1]["learned"]["promotion"]["reasons"][0]
    assert memory.mappings == {}


@pytest.mark.parametrize("raw, expected", [("", 5), ("3", 3), ("NEVER", 0), ("never", 0)])
def test_promotion_settings(monkeypatch, raw, expected):
    monkeypatch.setenv("PROMOTE_MIN_OCCURRENCES", raw)
    assert config._count("PROMOTE_MIN_OCCURRENCES", 5) == expected


@pytest.mark.parametrize("raw", ["0", "-1", "five"])
def test_a_promotion_setting_that_would_promote_everything_stops_the_agent(monkeypatch, raw):
    monkeypatch.setenv("PROMOTE_MIN_BATCHES", raw)
    with pytest.raises(ValueError):
        config._count("PROMOTE_MIN_BATCHES", 3)


# --- never for an ambiguous value ---------------------------------------------------
def past(memory, incident_id, canonical, outcome="SUCCESS", batch="1", rows=50, **extra):
    memory.write_incident(None, incident_id=incident_id, table_name="silver2_crash2", column_name="crash_month",
                          raw_value="JULE", failure_type="UNRESOLVED_VALUE", diagnosis="past", action="ADD_VARIANT",
                          action_params={"canonical_value": canonical}, outcome=outcome, resolved_by="LLM",
                          confidence=0.9, verified=outcome == "SUCCESS", rows_affected=rows, embedding=None,
                          signature_text="t", extra={"batch_ids": [f"silver2_crash2:{batch}"], **extra})


def test_a_value_fixed_to_two_different_months_is_never_promoted(memory):
    past(memory, "old-june", "JUNE")
    g = graph(**AUTO)
    outs = [g.invoke(initial_state("L1", item_in_batch(f"f{b}", b, rows=5)), thread(f"f{b}")) for b in (7, 8, 9)]
    promotion_ = outs[-1]["learned"]["promotion"]
    assert promotion_["status"] == "BLOCKED"
    assert "incident old-june fixed the same value to JUNE" in promotion_["reasons"]
    assert memory.mappings == {}


def test_a_fix_that_failed_validation_before_is_not_promoted_by_recurrence(memory):
    past(memory, "old-fail", "JULY", outcome="FAILED_VALIDATION")
    g = graph(**AUTO)
    outs = [g.invoke(initial_state("L1", item_in_batch(f"f{b}", b, rows=5)), thread(f"f{b}")) for b in (7, 8, 9)]
    assert outs[-1]["learned"]["promotion"]["status"] == "BLOCKED"
    assert "the same fix failed validation before (incident old-fail)" in outs[-1]["learned"]["promotion"]["reasons"]


def test_a_table_that_maps_the_value_elsewhere_blocks_promotion(memory):
    memory.mappings[("crash_month", "JULE")] = "JUNE"
    decided = promotion.decide(
        incident={"incident_id": "x", "action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                  "outcome": "SUCCESS", "verified": True, "column_name": "crash_month", "raw_key": "JULE",
                  "canonical_value": "JULY"},
        history=[], existing="JUNE", engineer_approved=True)
    assert decided["status"] == "BLOCKED" and "maps it to JUNE, not JULY" in decided["reasons"][0]


def test_only_a_validated_add_variant_is_promoted():
    base = {"incident_id": "x", "outcome": "SUCCESS", "verified": True, "column_name": "c", "raw_key": "R",
            "canonical_value": "V"}
    for incident in ({**base, "action": "RENAME_COLUMN"}, {**base, "action": "ADD_VARIANT", "verified": False}):
        assert promotion.decide(incident=incident, history=[], existing=None,
                                engineer_approved=True)["status"] == "NOT_APPLICABLE"


# --- an engineer's fix ----------------------------------------------------------------
def test_an_engineers_fix_that_validated_is_promoted_at_once(memory):
    g = graph(diagnose=diagnose_july, policy_gate=gate_rejects)
    g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    out = decide(g, "f1", "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JULY"})
    assert path(out)[-4:] == ["engineer_review", "remediate", "validate", "learn"]
    assert out["status"] == "RESOLVED_BY_ENGINEER"

    stored = memory.incidents["f1"]
    assert stored["resolved_by"] == "ENGINEER" and stored["approved_by"] == "thay" and stored["verified"] is True
    assert stored["escalation_reasons"] == ["POLICY_GATE_REJECTED"] and stored["engineer"] == "thay"
    assert out["learned"]["promotion"]["status"] == "PROMOTED"
    assert out["learned"]["promotion"]["route"] == "ENGINEER_APPROVED"
    assert memory.promoted[0]["added_by"] == "recovery_agent:engineer:thay"
    assert memory.mappings == {("crash_month", "JULE"): "JULY"}


def test_no_promote_heals_the_records_only_and_keeps_recurrence_from_promoting_it(memory):
    g = graph(diagnose=diagnose_july, policy_gate=gate_rejects)
    g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    out = decide(g, "f1", "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JULY"}, promote=False)
    assert out["escalation"]["decision"]["promote"] is False
    assert out["learned"]["promotion"]["status"] == "DECLINED" and memory.mappings == {}
    assert memory.incidents["f1"]["promote_declined"] is True

    auto = graph(**AUTO)
    outs = [auto.invoke(initial_state("L1", item_in_batch(f"g{b}", b, rows=5)), thread(f"g{b}")) for b in (8, 9, 10)]
    assert outs[-1]["learned"]["promotion"]["status"] == "BLOCKED"
    assert "an engineer declined promoting this fix (incident f1)" in outs[-1]["learned"]["promotion"]["reasons"]


def test_a_fix_that_failed_validation_is_stored_as_a_failed_attempt(memory):
    """Round 1: the model's JULY fails Validate. Round 2: the engineer's JUNE holds."""
    calls = []

    def validate_july_fails(state):
        calls.append(state["action"]["action_params"]["canonical_value"])
        if state["action"]["action_params"]["canonical_value"] == "JULY":
            return {"validation": {"status": "FAILED", "passed": False, "reasons": ["record r3 is dated June"]},
                    "trace": trace("validate")}
        return {"validation": {"status": "PASSED", "passed": True, "reasons": [], "rows_staged": 2,
                               "column": "crash_month", "canonical_value": "JUNE",
                               "target_table": "silver2_crash1"}, "trace": trace("validate")}

    g = graph(**AUTO, validate=validate_july_fails)
    g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    out = decide(g, "f1", "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JUNE"})
    assert calls == ["JULY", "JUNE"] and out["status"] == "RESOLVED_BY_ENGINEER"

    attempt = memory.incidents["f1:attempt:1"]
    assert attempt["outcome"] == "FAILED_VALIDATION" and attempt["verified"] is False
    assert attempt["action_params"] == {"canonical_value": "JULY"} and attempt["diagnosis"] == "record r3 is dated June"
    assert memory.incidents["f1"]["action_params"] == {"canonical_value": "JUNE"}
    assert out["learned"]["attempts"] == [{"incident_id": "f1:attempt:1", "round": 1,
                                           "fix": 'ADD_VARIANT {"canonical_value": "JULY"}'}]
    assert out["learned"]["promotion"]["status"] == "PROMOTED"   # a failed JULY does not cloud JUNE


def test_a_manual_resolution_is_stored_as_context_not_as_a_fix_to_repeat(memory):
    g = graph(diagnose=diagnose_july, policy_gate=gate_rejects)
    g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    out = decide(g, "f1", "RESOLVED_MANUALLY", note="re-sent the source file with the right months")
    stored = memory.incidents["f1"]
    assert stored["outcome"] == "SUCCESS" and stored["action"] == "MANUAL" and stored["verified"] is True
    assert stored["diagnosis"] == "re-sent the source file with the right months"
    assert out["learned"]["promotion"] is None and memory.mappings == {}


def test_a_dismissal_is_stored_and_never_reused(memory):
    g = graph(diagnose=diagnose_july, policy_gate=gate_rejects)
    g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    out = decide(g, "f1", "DISMISS", note="test data")
    stored = memory.incidents["f1"]
    assert out["status"] == "DISMISSED"
    assert stored["outcome"] == "DISMISSED" and stored["verified"] is False and stored["action"] == "DISMISS"
    assert stored["dismissed_fix"] == 'ADD_VARIANT {"canonical_value": "JULY"}'


def test_nothing_is_stored_when_nobody_decided(memory):
    state = initial_state("L1", stack_item("f1"))
    out = learn_node.learn({**state, "status": "FAILED", "error": {"node": "escalate", "error": "boom"}})
    assert out["learned"]["status"] == "SKIPPED" and out["status"] == "FAILED" and memory.incidents == {}


# --- a borrowed fix (Thay, 2026-11-14) ------------------------------------------------
BORROWED = {**PAST_FIX, "matched_by": "STRUCTURED", "score": 0.9}


def test_a_borrowed_fix_nobody_approved_is_stored_as_borrowed_and_does_not_count(memory):
    g = graph()
    outs = [g.invoke(initial_state("L1", item_in_batch(f"f{b}", b, rows=5, route="remediate", match=BORROWED)),
                     thread(f"f{b}")) for b in (7, 8, 9)]
    assert [o["status"] for o in outs] == ["RESOLVED"] * 3        # P2: reused without approval
    assert all(memory.incidents[f"f{b}"]["borrowed"] is True for b in (7, 8, 9))
    assert memory.incidents["f7"]["matched_by"] == "STRUCTURED" and memory.incidents["f7"]["resolved_by"] == "LOOKUP"
    promotion_ = outs[-1]["learned"]["promotion"]
    assert promotion_["status"] == "BELOW_THRESHOLD" and promotion_["occurrences"] == 0
    assert "borrowed from a similar incident without approval" in promotion_["reasons"][0]
    assert memory.mappings == {}


def test_an_exact_match_on_a_borrowed_fix_still_waits_for_approval_on_p1(memory):
    """Reusing a borrowed fix again must not turn it into an exact one on P1."""
    exact_on_borrowed = {**PAST_FIX, "matched_by": "EXACT", "borrowed": True}
    g = graph()
    out = g.invoke(initial_state("L1", item_in_batch("f1", 7, route="remediate", match=exact_on_borrowed,
                                                     tier="P1")), thread("f1"))
    assert out["escalation"]["reason"] == "APPROVAL_REQUIRED"
    assert "exact match whose fix was itself borrowed" in out["escalation"]["details"][0]

    p2 = g.invoke(initial_state("L1", item_in_batch("f2", 8, route="remediate", match=exact_on_borrowed)),
                  thread("f2"))
    assert p2["status"] == "RESOLVED" and memory.incidents["f2"]["borrowed"] is True   # stays borrowed


def test_a_borrowed_fix_an_engineer_approved_is_not_borrowed_and_is_promoted(memory):
    g = graph()
    g.invoke(initial_state("L1", item_in_batch("f1", 7, route="remediate", match=BORROWED, tier="P1")), thread("f1"))
    out = decide(g, "f1", "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JULY"})
    stored = memory.incidents["f1"]
    assert out["status"] == "RESOLVED_BY_ENGINEER"
    assert stored["borrowed"] is False and stored["approved_by"] == "thay"
    assert stored["matched_by"] == "STRUCTURED" and stored["reused_incident_id"] == "i1"
    assert out["learned"]["promotion"]["status"] == "PROMOTED"


# --- embedding ---------------------------------------------------------------------------
def test_the_incident_is_embedded_with_the_lookups_model(memory, monkeypatch):
    monkeypatch.setattr(config, "EMBED_MODEL", "nomic-embed-text")
    seen = []
    monkeypatch.setattr(learn_node, "embed", lambda text: seen.append(text) or [0.1] * 768)
    out = graph(**AUTO).invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    assert seen == ["table=silver2_crash1 column=crash_month value=JULE failure=UNRESOLVED_VALUE"]
    assert out["learned"]["embedded"] is True and len(memory.incidents["f1"]["embedding"]) == 768


def test_an_unreachable_embedding_model_still_stores_the_incident(memory, monkeypatch):
    monkeypatch.setattr(config, "EMBED_MODEL", "nomic-embed-text")
    monkeypatch.setattr(config, "EMBED_BASE_URL", "http://127.0.0.1:9")
    out = graph(**AUTO).invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    assert out["learned"]["status"] == "STORED" and out["learned"]["embedded"] is False
    assert out["learned"]["embed_error"] and memory.incidents["f1"]["embedding"] is None


# --- Neo4j down: Learn alone runs again -------------------------------------------------
def test_learn_that_kept_failing_runs_again_alone_when_the_failure_is_delivered_again(memory):
    ran = []

    def counting_diagnose(state):
        ran.append("diagnose")
        return diagnose_july(state)

    g = build_graph(InMemorySaver(), nodes={"diagnose": counting_diagnose, "policy_gate": gate_passes},
                    max_attempts=2, retry_seconds=0)
    item = item_in_batch("f1", 7)
    memory.down = True
    (first,) = process_lookup(g, lookup_message(item))
    assert first["state"]["status"] == "FAILED" and first["state"]["error"]["node"] == "learn"
    assert memory.incidents == {}

    memory.down = False
    (again,) = process_lookup(g, lookup_message(item))
    assert again["delivery"] == "relearn"
    assert again["state"]["status"] == "RESOLVED" and again["state"]["learned"]["status"] == "STORED"
    assert "error" not in again["state"] or not again["state"]["error"]
    assert ran == ["diagnose"]                          # nothing before Learn ran again
    assert memory.incidents["f1"]["outcome"] == "SUCCESS"
    assert g.get_state(thread("f1")).values["learned"]["status"] == "STORED"
    (third,) = process_lookup(g, lookup_message(item))
    assert third["delivery"] == "duplicate"


def test_a_case_closed_while_learn_was_a_stub_is_learned_without_asking_again(memory):
    """Before Task 107 a decided case ended on the Learn stub. Delivered again, only
    Learn runs: the engineer is not asked a second time."""
    saver = InMemorySaver()
    before = build_graph(saver, nodes={"diagnose": diagnose_july, "policy_gate": gate_rejects,
                                       "learn": stub("learn")}, retry_seconds=0)
    item = item_in_batch("f1", 7)
    before.invoke(initial_state("L1", item), thread("f1"))
    decide(before, "f1", "DISMISS", note="not a month")
    assert before.get_state(thread("f1")).values["stubbed"] == ["learn"]

    after = build_graph(saver, nodes={"diagnose": diagnose_july, "policy_gate": gate_rejects}, retry_seconds=0)
    summary = run_workflow(after, "L2", item)
    assert summary["delivery"] == "relearn" and summary["state"]["status"] == "DISMISSED"
    assert memory.incidents["f1"]["outcome"] == "DISMISSED"
    assert path(summary["state"]).count("escalate") == 1


# --- the review command ------------------------------------------------------------------
def test_no_promote_travels_with_the_decision():
    from agent import review

    g = graph(diagnose=diagnose_july, policy_gate=gate_rejects)
    g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    sent = []
    review.main(["fix", "f1", "july", "--by", "thay", "--no-promote"], graph=g, send=sent.append)
    review.main(["approve", "f1", "--by", "thay"], graph=g, send=sent.append)
    assert [m["promote"] for m in sent] == [False, True]
    state = g.get_state(thread("f1")).values
    assert esc.check_decision(state, {**sent[0], "promote": "no"})[1] == ["promote must be true or false"]


# --- review of PR #32 -------------------------------------------------------------------
def test_an_engineers_approval_overrides_an_earlier_failure_of_the_same_fix():
    """EL2: one earlier FAILED_VALIDATION of JULY (a record with a wrong crash_date) must
    not block forever a JULY an engineer approved and that passed Validate."""
    incident = {"incident_id": "now", "action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                "outcome": "SUCCESS", "verified": True, "column_name": "crash_month", "raw_key": "JULE",
                "canonical_value": "JULY"}
    failed = {"incident_id": "old", "action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
              "outcome": "FAILED_VALIDATION", "verified": False}
    approved = promotion.decide(incident=incident, history=[failed], existing=None, engineer_approved=True)
    assert approved["status"] == "PROMOTED"
    assert approved["overridden"] == ["the same fix failed validation before (incident old)"]
    automatic = promotion.decide(incident=incident, history=[failed], existing=None, engineer_approved=False)
    assert automatic["status"] == "BLOCKED"

    # a conflicting value still blocks, approval or not
    other = {**failed, "incident_id": "june", "action_params": {"canonical_value": "JUNE"}, "outcome": "SUCCESS",
             "verified": True}
    assert promotion.decide(incident=incident, history=[other], existing=None,
                            engineer_approved=True)["status"] == "BLOCKED"


def test_a_learn_that_failed_is_retried_without_waiting_for_a_redelivery(memory):
    from agent.consumer import LearnRetry

    now = [0.0]
    g = build_graph(InMemorySaver(), nodes=AUTO, max_attempts=1, retry_seconds=0)
    memory.down = True
    g.invoke(initial_state("L1", item_in_batch("f1", 7)), thread("f1"))
    g.invoke(initial_state("L1", item_in_batch("f2", 8)), thread("f2"))
    assert g.get_state(thread("f1")).values["status"] == "FAILED"

    retry = LearnRetry(g, interval=60, clock=lambda: now[0])
    assert retry.scan() == 2 and retry.pending == {"f1", "f2"}
    assert [s["delivery"] for s in retry.due()] == ["relearn", "relearn"]   # still down
    assert retry.pending == {"f1", "f2"} and memory.incidents == {}

    memory.down = False
    now[0] = 30
    assert retry.due() == []                                    # not yet: once a minute
    now[0] = 61
    done = retry.due()
    assert [s["state"]["status"] for s in done] == ["RESOLVED", "RESOLVED"]
    assert retry.pending == set() and set(memory.incidents) == {"f1", "f2"}
    assert g.get_state(thread("f1")).values["learned"]["status"] == "STORED"


def test_the_poll_loop_retries_learn_on_its_own(memory):
    import json as json_

    from agent.consumer import LearnRetry, run
    from agent.tests.test_consumer import FakeConsumer, FakeMessage

    g = build_graph(InMemorySaver(), nodes=AUTO, max_attempts=1, retry_seconds=0)
    retry = LearnRetry(g, interval=60, clock=lambda: 0.0)
    consumer = FakeConsumer([json_.dumps(lookup_message(item_in_batch("f1", 7))).encode()])
    memory.down = True
    run(consumer, g, max_messages=1, retry=retry)
    assert retry.pending == {"f1"} and consumer.committed == [0] and memory.incidents == {}

    memory.down = False
    consumer.pending = [FakeMessage(b"not json", 1)]   # any later turn of the loop
    run(consumer, g, max_messages=1, retry=retry)
    assert retry.pending == set() and memory.incidents["f1"]["outcome"] == "SUCCESS"
    assert consumer.paused is False
