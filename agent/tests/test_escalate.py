"""Escalate and the engineer's decision: python -m pytest agent/tests   (no Kafka, no Neo4j, no model)"""
import json

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from agent import audit, config, escalation as esc, reference, review
from agent.consumer import apply_decision, process_lookup, run
from agent.graph import build_graph
from agent.nodes.escalate import escalate
from agent.state import initial_state
from agent.tests.helpers import (FakeModel, JULY_REPLY, PAST_FIX, canonical_values, diagnose_july, jule_quarantine,
                                 july_samples, learn_ok, lookup_message, path, remediate_ok, stack_item, trace,
                                 validate_fails, validate_ok)


@pytest.fixture(autouse=True)
def vocabulary(monkeypatch):
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)


def thread(failure_id="f1"):
    return {"configurable": {"thread_id": failure_id}}


def graph(**nodes):
    return build_graph(InMemorySaver(), nodes=nodes, retry_seconds=0)


def escalated(g, item=None):
    """Run one failure until it waits for the engineer; return its saved state."""
    item = item or stack_item("f1")
    g.invoke(initial_state("L1", item), thread(item["failure"]["failure_id"]))
    saved = g.get_state(thread(item["failure"]["failure_id"]))
    assert saved.next == ("engineer_review",) and saved.interrupts
    return saved.values


def decide(state, kind, engineer="thay", note=None, action=None, params=None, **changes):
    message = esc.make_decision(state["failure_id"], state["escalation"]["escalation_id"], kind, engineer,
                                note, action, params)
    return {**message, **changes}


def gate_rejects(state):
    proposed = state["diagnosis"]["proposed_action"]
    return {"policy": {"passed": False, "outcome": "REJECTED", "reasons": ["confidence 0.60 is below 0.85"],
                       "checks": [{"check": "confidence", "passed": False, "detail": "0.60 < 0.85"},
                                  {"check": "safe_action", "passed": True}]},
            "trace": trace("policy_gate")}


REJECTED = {"diagnose": diagnose_july, "policy_gate": gate_rejects}


# --- the case the engineer gets (spec section 8) -------------------------------------
def test_escalation_opens_a_case_with_the_full_context_not_a_bare_alert():
    item = stack_item("f1")
    item["incident_context"]["candidates"] = [
        {"incident_id": "i7", "matched_by": "STRUCTURED", "score": 0.8, "raw_value": "Julee",
         "column_name": "crash_month", "table_name": "silver2_crash2", "action": "ADD_VARIANT",
         "action_params": {"canonical_value": "JULY"}, "transferable": False, "reason": "value differs"}] * 5
    item["lineage_context"]["downstream"] = [{"name": "gold", "kind": "delta", "depth": 2}]
    state = escalated(graph(**REJECTED), item)

    escalation = state["escalation"]
    assert escalation["status"] == "WAITING" and escalation["round"] == 1
    assert escalation["escalation_id"] == "f1:1" and escalation["decision"] is None
    assert escalation["reason"] == "POLICY_GATE_REJECTED"
    assert escalation["details"] == ["confidence 0.60 is below 0.85"]
    assert state["status"] == "ESCALATED"

    case = escalation["case"]
    assert case["failure"]["raw_value"] == "Jule" and case["failure"]["column_name"] == "crash_month"
    assert case["failure"]["rows_affected"] == 10 and case["table"] == "silver2_crash1"
    assert case["blast_radius"] == {"score": 42.0, "tier": "P2"} and case["lineage"]["downstream"] == ["gold"]
    assert len(case["past_incidents"]) == 3 and case["past_incidents"][0]["incident_id"] == "i7"
    assert case["diagnosis"]["hypothesis"] == "typo of July" and case["diagnosis"]["confidence"] == 0.93
    assert case["policy"]["failed_checks"] == [{"check": "confidence", "passed": False, "detail": "0.60 < 0.85"}]
    assert case["proposed_fix"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                                    "source": "DIAGNOSIS", "incident_id": None, "confidence": 0.93}
    assert "JUNE" in case["canonical_values"]
    assert case["execution"] is None and case["validation"] is None


def test_case_proposes_the_fix_that_failed_and_says_why():
    state = escalated(graph(remediate=remediate_ok, validate=validate_fails),
                      stack_item("f1", route="remediate", match=PAST_FIX))
    case = state["escalation"]["case"]
    assert case["reason"] == "VALIDATION_FAILED"
    assert case["validation"]["reasons"] == ["new failure in weather_condition"]
    assert case["proposed_fix"]["source"] == "INCIDENT" and case["proposed_fix"]["incident_id"] == "i1"


def test_a_model_that_asked_for_an_engineer_has_no_fix_to_approve():
    reply = {"hypothesis": "June or July", "action": "ESCALATE", "canonical_value": None, "confidence": 0.4}
    from agent.nodes.diagnose import make_diagnose
    from agent.nodes.policy_gate import make_policy_gate
    state = escalated(graph(diagnose=make_diagnose(chat=FakeModel(reply), canonical_values=canonical_values,
                                                   sample_records=july_samples),
                            policy_gate=make_policy_gate(canonical_values=canonical_values,
                                                         sample_records=july_samples)))
    case = state["escalation"]["case"]
    assert case["reason"] == "ENGINEER_REQUESTED" and case["proposed_fix"] is None
    assert case["sample_records"] and case["sample_records"][0]["crash_month"] == "Jule"


def test_escalate_writes_only_its_own_keys_and_can_run_twice():
    state = initial_state("L1", stack_item())
    state["policy"] = {"passed": False, "outcome": "NO_DIAGNOSIS", "reasons": ["no usable diagnosis"]}
    first, second = escalate(state), escalate(state)
    assert set(first) == {"escalation", "status", "trace"}
    assert first["escalation"]["escalation_id"] == second["escalation"]["escalation_id"] == "f1:1"


# --- the workflow waits, without blocking anything -----------------------------------
def test_waiting_workflow_is_saved_and_a_redelivered_lookup_leaves_it_alone():
    g = graph()
    message = lookup_message(stack_item("f1"), stack_item("f2", rank=2))
    first = process_lookup(g, message)
    assert [s["delivery"] for s in first] == ["new", "new"]       # f1 waiting did not hold f2 back
    again = process_lookup(g, message)
    assert [s["delivery"] for s in again] == ["waiting", "waiting"]
    assert len(g.get_state(thread("f1")).values["trace"]) == 4    # nothing ran again


# --- APPLY_FIX ------------------------------------------------------------------------
def test_approved_fix_runs_through_real_remediate_and_validate_then_learn():
    jule_quarantine(config.QUARANTINE_PATH, rows=10)
    g = graph(**REJECTED, learn=learn_ok)
    state = escalated(g)
    out = g.invoke(Command(resume=decide(state, "APPLY_FIX", action="ADD_VARIANT",
                                         params={"canonical_value": "JULY"}, note="checked the dates")),
                   thread())
    assert path(out)[-5:] == ["escalate", "engineer_review", "remediate", "validate", "learn"]
    assert out["status"] == "RESOLVED_BY_ENGINEER"
    assert out["action"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                             "source": "ENGINEER", "incident_id": None, "approved_by": "thay"}
    assert out["execution"]["status"] == "SUCCESS" and out["execution"]["rows_affected"] == 10
    assert out["validation"]["status"] == "PASSED" and out["validation"]["rows_released"] == 10
    decision = out["escalation"]["decision"]
    assert out["escalation"]["status"] == "DECIDED"
    assert decision["decision"] == "APPLY_FIX" and decision["engineer"] == "thay"
    assert decision["note"] == "checked the dates"
    assert audit.find(out["execution"]["execution_id"])["action_source"] == "ENGINEER"   # TA2


def test_engineer_can_apply_another_value_than_the_proposal():
    jule_quarantine(config.QUARANTINE_PATH, rows=10)
    g = graph(**REJECTED, learn=learn_ok)
    state = escalated(g)
    out = g.invoke(Command(resume=decide(state, "APPLY_FIX", action="ADD_VARIANT",
                                         params={"canonical_value": "JUNE"})), thread())
    assert out["execution"]["overlay"] == {"crash_month": {"JULE": "JUNE"}}
    assert out["status"] == "RESOLVED_BY_ENGINEER"


def test_engineer_fix_that_fails_validation_comes_back_as_a_new_round():
    g = graph(**REJECTED, remediate=remediate_ok, validate=validate_fails)
    state = escalated(g)
    g.invoke(Command(resume=decide(state, "APPLY_FIX", action="ADD_VARIANT",
                                   params={"canonical_value": "JUNE"})), thread())
    saved = g.get_state(thread())
    assert saved.next == ("engineer_review",)
    escalation = saved.values["escalation"]
    assert escalation["round"] == 2 and escalation["escalation_id"] == "f1:2"
    assert escalation["reason"] == "VALIDATION_FAILED" and saved.values["status"] == "ESCALATED"
    assert escalation["case"]["proposed_fix"]["source"] == "ENGINEER"
    assert [h["round"] for h in escalation["history"]] == [1]
    assert escalation["history"][0]["decision"]["decision"] == "APPLY_FIX"


def test_after_a_step_failed_an_engineer_fix_is_not_sent_back_by_the_old_error():
    def validate_broken(state):
        raise RuntimeError("Spark is down")

    g = graph(remediate=remediate_ok, validate=validate_broken, learn=learn_ok)
    state = escalated(g, stack_item("f1", route="remediate", match=PAST_FIX))
    assert state["escalation"]["reason"] == "STEP_FAILED"
    fixed = build_graph(g.checkpointer, nodes={"remediate": remediate_ok, "validate": validate_ok,
                                               "learn": learn_ok}, retry_seconds=0)
    out = fixed.invoke(Command(resume=decide(state, "APPLY_FIX", action="ADD_VARIANT",
                                             params={"canonical_value": "JULY"})), thread())
    assert out["error"] is None and path(out)[-3:] == ["remediate", "validate", "learn"]
    assert out["status"] == "RESOLVED_BY_ENGINEER"
    assert out["action"]["incident_id"] == "i1"     # the approved fix is the reused one


# --- RETRY, RESOLVED_MANUALLY, DISMISS ---------------------------------------------------
def test_retry_runs_the_automated_path_again_once_the_model_is_back():
    from agent.nodes.diagnose import make_diagnose
    from agent.nodes.policy_gate import make_policy_gate
    item = stack_item("f1")
    item["failure"]["sample_record_ids"] = ["r1", "r2"]
    gate = make_policy_gate(canonical_values=canonical_values, sample_records=july_samples)
    down = graph(policy_gate=gate)
    state = escalated(down, item)                  # no model in tests: NO_DIAGNOSIS
    assert state["escalation"]["reason"] == "NO_DIAGNOSIS"

    up = build_graph(down.checkpointer, retry_seconds=0, nodes={
        "diagnose": make_diagnose(chat=FakeModel(JULY_REPLY), canonical_values=canonical_values,
                                  sample_records=july_samples),
        "policy_gate": gate, "remediate": remediate_ok, "validate": validate_ok, "learn": learn_ok})
    out = up.invoke(Command(resume=decide(state, "RETRY")), thread())
    assert path(out)[-7:] == ["engineer_review", "deterministic_lookup", "diagnose", "policy_gate",
                              "remediate", "validate", "learn"]
    assert out["status"] == "RESOLVED" and out["policy"]["passed"] is True
    assert out["action"]["source"] == "DIAGNOSIS"


@pytest.mark.parametrize("kind, status", [("RESOLVED_MANUALLY", "RESOLVED_BY_ENGINEER"),
                                          ("DISMISS", "DISMISSED")])
def test_closing_decisions_go_to_learn_without_executing_anything(kind, status):
    g = graph(**REJECTED, learn=learn_ok)
    state = escalated(g)
    out = g.invoke(Command(resume=decide(state, kind, note="weather OTHER has no canonical value yet")), thread())
    assert path(out)[-3:] == ["escalate", "engineer_review", "learn"]
    assert out["status"] == status and "execution" not in out
    assert out["escalation"]["decision"]["note"] == "weather OTHER has no canonical value yet"


# --- what is refused -------------------------------------------------------------------
@pytest.mark.parametrize("changes, problem", [
    ({"decision": "MERGE"}, "is not one of"),
    ({"engineer": " "}, "must name the engineer"),
    ({"escalation_id": "f1:0"}, "the open one is 'f1:1'"),
    ({"decision": "DISMISS", "note": None}, "DISMISS needs a note"),
    ({"action_params": {"canonical_value": "JULYY"}}, "'JULYY' is not a canonical value"),
    ({"action": "RENAME_COLUMN", "action_params": {"renames": {"a": "b"}}}, "cannot execute RENAME_COLUMN"),
    ({"action": "ESCALATE", "action_params": {}}, "APPLY_FIX needs a fix"),
])
def test_decisions_that_do_not_hold_are_refused(changes, problem):
    state = escalated(graph(**REJECTED))
    message = {**decide(state, "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JULY"}), **changes}
    clean, problems = esc.check_decision(state, message)
    assert clean is None and any(problem in p for p in problems), problems


def test_a_decision_that_slips_past_the_checks_is_asked_again_not_acted_on():
    g = graph(**REJECTED, remediate=remediate_ok)
    state = escalated(g)
    out = g.invoke(Command(resume=decide(state, "APPLY_FIX", action="ADD_VARIANT",
                                         params={"canonical_value": "SMARCH"})), thread())
    (asked,) = out["__interrupt__"]
    assert "'SMARCH' is not a canonical value of crash_month" in asked.value["refused"]["problems"][0]
    assert g.get_state(thread()).next == ("engineer_review",) and "execution" not in out


# --- the consumer: recovery.decisions ------------------------------------------------------
def test_consumer_applies_a_decision_once_and_refuses_the_rest():
    g = graph(**REJECTED, remediate=remediate_ok, validate=validate_ok, learn=learn_ok)
    state = escalated(g)
    message = decide(state, "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JULY"})

    assert apply_decision(g, {**message, "engineer": ""})["delivery"] == "refused"
    assert g.get_state(thread()).next == ("engineer_review",)          # still waiting
    done = apply_decision(g, message)
    assert done["delivery"] == "decided" and done["state"]["status"] == "RESOLVED_BY_ENGINEER"
    assert apply_decision(g, message)["delivery"] == "duplicate"        # Kafka redelivers
    late = apply_decision(g, decide(state, "DISMISS", note="too late"))
    assert late["delivery"] == "refused" and "not waiting" in late["problems"][0]
    assert apply_decision(g, {**message, "failure_id": "nobody"})["delivery"] == "unknown"


def test_decision_redelivered_after_the_agent_died_continues_the_workflow(tmp_path):
    from agent.graph import open_checkpointer
    from agent.tests.helpers import Killed

    def validate_killed(state):
        raise Killed()

    db = str(tmp_path / "checkpoints.sqlite")
    before = open_checkpointer(db)
    dying = build_graph(before, nodes={**REJECTED, "remediate": remediate_ok, "validate": validate_killed},
                        retry_seconds=0)
    state = escalated(dying)
    message = decide(state, "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JULY"})
    with pytest.raises(Killed):
        apply_decision(dying, message)
    before.conn.close()

    after = open_checkpointer(db)
    restarted = build_graph(after, nodes={**REJECTED, "remediate": remediate_ok, "validate": validate_ok,
                                          "learn": learn_ok})
    summary = apply_decision(restarted, message)
    after.conn.close()
    assert summary["delivery"] == "resumed" and summary["state"]["status"] == "RESOLVED_BY_ENGINEER"


class Message:
    def __init__(self, topic, value, offset):
        self._topic, self._value, self._offset = topic, value, offset

    def value(self):
        return self._value

    def error(self):
        return None

    def topic(self):
        return self._topic

    def offset(self):
        return self._offset


class TwoTopics:
    def __init__(self, messages):
        self.pending, self.committed = list(messages), []

    def poll(self, timeout):
        return self.pending.pop(0) if self.pending else None

    def assignment(self):
        return []

    def pause(self, partitions):
        pass

    def resume(self, partitions):
        pass

    def commit(self, message, asynchronous):
        self.committed.append((message.topic(), message.offset()))


def test_poll_loop_reads_lookups_and_decisions(capsys):
    g = graph(**REJECTED, learn=learn_ok)
    consumer = TwoTopics([Message("recovery.lookup", json.dumps(lookup_message(stack_item("f1"))).encode(), 0)])
    run(consumer, g, max_messages=1)
    state = g.get_state(thread()).values
    decision = decide(state, "DISMISS", note="not a crash month")
    consumer.pending.append(Message(config.DECISIONS_TOPIC, json.dumps(decision).encode(), 0))
    run(consumer, g, max_messages=1)
    assert consumer.committed == [("recovery.lookup", 0), (config.DECISIONS_TOPIC, 0)]
    assert g.get_state(thread()).values["status"] == "DISMISSED"
    log = capsys.readouterr().out
    assert "waiting for an engineer: python -m agent.review show f1" in log
    assert "decision DISMISS by thay on f1:1: decided" in log


# --- the review CLI -------------------------------------------------------------------
def test_review_lists_shows_and_sends_a_checked_decision(capsys):
    g = graph(**REJECTED)
    escalated(g, stack_item("f1"))
    escalated(g, stack_item("f2", value="Ju"))
    sent = []

    review.main([], graph=g)
    listing = capsys.readouterr().out
    assert listing.startswith("2 failure(s) waiting for an engineer")
    assert "POLICY_GATE_REJECTED" in listing and "proposed=ADD_VARIANT" in listing

    review.main(["show", "f1"], graph=g)
    shown = capsys.readouterr().out
    assert "Reason: POLICY_GATE_REJECTED" in shown and "typo of July" in shown
    assert "python -m agent.review approve f1 --by NAME" in shown

    review.main(["approve", "f1", "--by", "thay"], graph=g, send=sent.append)
    assert sent[-1]["decision"] == "APPLY_FIX" and sent[-1]["escalation_id"] == "f1:1"
    assert sent[-1]["action_params"] == {"canonical_value": "JULY"} and sent[-1]["engineer"] == "thay"

    review.main(["fix", "f2", "june", "--by", "thay"], graph=g, send=sent.append)
    assert sent[-1]["action_params"] == {"canonical_value": "JUNE"}

    with pytest.raises(SystemExit, match="'SMARCH' is not a canonical value"):
        review.main(["fix", "f2", "smarch", "--by", "thay"], graph=g, send=sent.append)
    with pytest.raises(SystemExit, match="DISMISS needs a note"):
        review.main(["dismiss", "f2", "--by", "thay"], graph=g, send=sent.append)
    with pytest.raises(SystemExit, match="--by NAME"):
        review.main(["retry", "f2"], graph=g, send=sent.append)
    with pytest.raises(SystemExit, match="No workflow"):
        review.main(["show", "nobody"], graph=g)
    assert len(sent) == 2


def test_review_lists_cases_from_the_sqlite_checkpoints(tmp_path):
    from agent.graph import open_checkpointer
    checkpointer = open_checkpointer(str(tmp_path / "checkpoints.sqlite"))
    g = build_graph(checkpointer, nodes=REJECTED)
    escalated(g, stack_item("f1"))
    process_lookup(g, lookup_message(stack_item("f2", route="remediate", match=PAST_FIX)))
    finished = build_graph(checkpointer, nodes={"remediate": remediate_ok, "validate": validate_ok})
    finished.invoke(initial_state("L1", stack_item("f3", route="remediate", match=PAST_FIX)), thread("f3"))
    assert sorted(review.paused_thread_ids(checkpointer)) == ["f1", "f2"]   # the finished f3 is not loaded
    assert [s["failure_id"] for s in review.open_cases(g)] == ["f1", "f2"]
    checkpointer.conn.close()


# --- a borrowed past fix on a P1 failure waits for approval (Thay, 2026-11-14) ---------
BORROWED = {**PAST_FIX, "matched_by": "STRUCTURED", "score": 0.9}


def p1(item):
    item["priority"]["tier"] = "P1"
    return item


@pytest.mark.parametrize("matched_by", ["STRUCTURED", "EMBEDDING"])
def test_borrowed_fix_on_a_p1_failure_waits_for_approval_then_runs(matched_by):
    jule_quarantine(config.QUARANTINE_PATH, rows=10)
    g = graph(learn=learn_ok)
    state = escalated(g, p1(stack_item("f1", route="remediate", match={**BORROWED, "matched_by": matched_by})))
    assert path(state) == ["deterministic_lookup", "escalate"] and "execution" not in state
    escalation = state["escalation"]
    assert escalation["reason"] == "APPROVAL_REQUIRED"
    assert f"borrowed from a {matched_by.lower()} match (incident i1" in escalation["details"][0]
    assert escalation["case"]["proposed_fix"] == {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                                                  "source": "INCIDENT", "incident_id": "i1"}

    sent = []
    review.main(["approve", "f1", "--by", "thay"], graph=g, send=sent.append)
    out = g.invoke(Command(resume=sent[0]), thread())
    assert path(out)[-4:] == ["engineer_review", "remediate", "validate", "learn"]
    assert out["status"] == "RESOLVED_BY_ENGINEER" and out["validation"]["rows_released"] == 10
    assert out["action"]["source"] == "ENGINEER" and out["action"]["incident_id"] == "i1"


@pytest.mark.parametrize("tier, matched_by", [("P1", "EXACT"), ("P2", "STRUCTURED"), ("P3", "EMBEDDING")])
def test_exact_matches_and_lower_tiers_still_reuse_the_fix_directly(tier, matched_by):
    item = stack_item("f1", route="remediate", match={**BORROWED, "matched_by": matched_by})
    item["priority"]["tier"] = tier
    state = graph(remediate=remediate_ok, validate=validate_ok, learn=learn_ok).invoke(
        initial_state("L1", item), thread())
    assert path(state) == ["deterministic_lookup", "remediate", "validate", "learn"]
    assert state["status"] == "RESOLVED"


def test_the_approval_tiers_can_be_changed(monkeypatch):
    monkeypatch.setattr(config, "APPROVE_BORROWED_FIX_TIERS", frozenset())
    state = graph(remediate=remediate_ok, validate=validate_ok, learn=learn_ok).invoke(
        initial_state("L1", p1(stack_item("f1", route="remediate", match=BORROWED))), thread())
    assert state["status"] == "RESOLVED" and "escalation" not in state


def test_a_borrowed_fix_that_no_longer_fits_is_diagnosed_not_sent_for_approval():
    item = p1(stack_item("f1", route="remediate",
                         match={**BORROWED, "action_params": {"canonical_value": "SMARCH"}}))
    state = graph().invoke(initial_state("L1", item), thread())
    assert state["route"] == "diagnose" and path(state)[:2] == ["deterministic_lookup", "diagnose"]


# --- review of #31: a decided workflow is never thrown away -------------------------------
@pytest.mark.parametrize("kind, params", [("DISMISS", None), ("APPLY_FIX", {"canonical_value": "JULY"})])
def test_redelivered_lookup_never_reruns_a_workflow_an_engineer_decided(tmp_path, kind, params):
    """Learn is still a stub, so the decided workflow ends with stubbed = ['learn']."""
    from agent.graph import open_checkpointer
    jule_quarantine(config.QUARANTINE_PATH, rows=10)
    checkpointer = open_checkpointer(str(tmp_path / "checkpoints.sqlite"))
    g = build_graph(checkpointer, retry_seconds=0)
    message = lookup_message(stack_item("f1"))
    (first,) = process_lookup(g, message)
    state = first["state"]
    done = apply_decision(g, decide(state, kind, note="not a month", action="ADD_VARIANT" if params else None,
                                    params=params))
    assert done["delivery"] == "decided" and done["state"]["stubbed"] == ["learn"]

    (again,) = process_lookup(g, message)
    checkpointer.conn.close()
    assert again["delivery"] == "duplicate"
    assert again["state"]["escalation"]["decision"]["decision"] == kind      # the decision is kept
    assert again["state"]["status"] == done["state"]["status"]


# --- review of #31: approving the fix that just failed Validate -----------------------------
def test_approve_is_held_back_after_the_proposed_fix_failed_validation(capsys):
    g = graph(remediate=remediate_ok, validate=validate_fails)
    escalated(g, stack_item("f1", route="remediate", match=PAST_FIX))
    review.main(["show", "f1"], graph=g)
    shown = capsys.readouterr().out
    assert "approve f1 --by NAME" not in shown and "approve is hidden" in shown
    sent = []
    with pytest.raises(SystemExit, match="just failed Validate"):
        review.main(["approve", "f1", "--by", "thay"], graph=g, send=sent.append)
    review.main(["approve", "f1", "--by", "thay", "--force"], graph=g, send=sent.append)
    assert sent and sent[0]["action_params"] == {"canonical_value": "JULY"}


# --- review of #31: a refusal by engineer_review itself is visible ---------------------------
def test_a_decision_refused_on_resume_is_shown_not_silently_dropped(monkeypatch, capsys):
    g = graph(**REJECTED, remediate=remediate_ok, validate=validate_ok, learn=learn_ok)
    state = escalated(g)
    calls = []

    def flaky(column, path=None):   # readable when the consumer checks, not a moment later
        calls.append(column)
        if len(calls) > 1:
            raise OSError("ref_canonical_mappings is being rewritten")
        return canonical_values(column)

    monkeypatch.setattr(reference, "read_canonical_values", flaky)
    summary = apply_decision(g, decide(state, "APPLY_FIX", action="ADD_VARIANT", params={"canonical_value": "JULY"}))
    assert summary["delivery"] == "refused" and "cannot read the canonical values" in summary["problems"][0]
    assert g.get_state(thread()).next == ("engineer_review",)
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)

    review.main([], graph=g)
    assert "[last decision REFUSED, see show]" in capsys.readouterr().out
    review.main(["show", "f1"], graph=g)
    shown = capsys.readouterr().out
    assert "LAST DECISION REFUSED: APPLY_FIX by thay" in shown and "cannot read the canonical values" in shown

    sent = []
    review.main(["approve", "f1", "--by", "thay"], graph=g, send=sent.append)   # deciding again works
    out = apply_decision(g, sent[0])
    assert out["delivery"] == "decided" and out["state"]["status"] == "RESOLVED_BY_ENGINEER"
