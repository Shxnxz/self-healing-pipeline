"""Later batches of a problem join its open workflow (Task 109), and finished workflows
are pruned to their last checkpoint: python -m pytest agent/tests   (no Kafka, no Neo4j)"""
import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from agent import audit, config, escalation as esc, reference
from agent.attach import OpenWorkflows, merge, signature
from agent.consumer import apply_decision, process_lookup, thread_ids
from agent.graph import build_graph, open_checkpointer
from agent.tests.helpers import (PAST_FIX, REUSE_PATH_BUILT, canonical_values, diagnose_july, learn_ok,
                                 lookup_message, path, quarantine_row, stack_item, trace, write_quarantine)
from agent.upkeep import Upkeep, checkpoint_count, prune_all


@pytest.fixture(autouse=True)
def vocabulary(monkeypatch):
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)


def thread(failure_id):
    return {"configurable": {"thread_id": failure_id}}


def gate_rejects(state):
    return {"policy": {"passed": False, "outcome": "REJECTED", "reasons": ["confidence 0.60 is below 0.85"],
                       "checks": []}, "trace": trace("policy_gate")}


def batch_item(failure_id, batch, rows, value="Jule", **kwargs):
    """The failure the trigger publishes for one micro-batch of the same problem."""
    item = stack_item(failure_id, value=value, rows=rows, **kwargs)
    item["failure"].update(batch_id=batch, evidence={"quarantine_batches": [batch]},
                           sample_record_ids=[f"{batch}-r{n}" for n in range(min(rows, 5))])
    return item


def rows_of(batch, count, value="Jule"):
    return [quarantine_row(f"{batch}-r{n}", {"crash_month": value}, batch=batch) for n in range(count)]


def escalating_graph(checkpointer=None, **nodes):
    return build_graph(checkpointer or InMemorySaver(),
                       nodes={"diagnose": diagnose_july, "policy_gate": gate_rejects, "learn": learn_ok, **nodes},
                       retry_seconds=0)


# --- signature and merge ------------------------------------------------------------
def test_batches_of_one_problem_share_a_signature_and_other_problems_do_not():
    a, b = batch_item("f1", "7", 3)["failure"], batch_item("f2", "8", 4)["failure"]
    assert signature(a) == signature(b)
    assert signature(a) != signature(batch_item("f3", "8", 4, value="Ju")["failure"])
    other_folder = {**b, "folder": "Crash3"}
    assert signature(a) != signature(other_folder)
    assert signature({**b, "folder": "crash1"}) == signature(a)          # folder case does not matter


def test_merge_adds_batches_rows_and_samples():
    merged = merge(batch_item("f1", "7", 3)["failure"], batch_item("f2", "8", 4)["failure"])
    assert merged["failure_id"] == "f1"
    assert merged["batch_id"] == "7,8" and merged["evidence"]["quarantine_batches"] == ["7", "8"]
    assert merged["rows_affected"] == 7
    assert merged["sample_record_ids"] == ["7-r0", "7-r1", "7-r2", "8-r0", "8-r1"]
    assert merged["attached_failure_ids"] == ["f2"]
    # The same batch published again (after a crash) is not counted twice.
    again = merge(merged, batch_item("f3", "8", 4)["failure"])
    assert again["rows_affected"] == 7 and again["batch_id"] == "7,8"


# --- the consumer ---------------------------------------------------------------------
def test_later_batch_joins_the_workflow_waiting_for_an_engineer():
    """Thay's review item: one recurring problem must not open a case per batch."""
    g = escalating_graph()
    index = OpenWorkflows(g)
    (first,) = process_lookup(g, lookup_message(batch_item("f1", "7", 3)), open_workflows=index)
    assert first["delivery"] == "new" and g.get_state(thread("f1")).next == ("engineer_review",)

    (later,) = process_lookup(g, lookup_message(batch_item("f2", "8", 4), lookup_id="L2"), open_workflows=index)
    assert later["delivery"] == "attached" and later["attached_to"] == "f1"
    saved = g.get_state(thread("f1"))
    assert saved.next == ("engineer_review",) and saved.interrupts          # still waiting for the decision
    failure = saved.values["failure"]
    assert failure["batch_id"] == "7,8" and failure["rows_affected"] == 7
    assert failure["attached_failure_ids"] == ["f2"]
    case = saved.values["escalation"]["case"]["failure"]
    assert case["rows_affected"] == 7 and case["batch_id"] == "7,8"          # the engineer sees every row
    # nothing ran for the later batch: no diagnosis, no second case
    assert [e["node"] for e in saved.values["trace"]].count("diagnose") == 1
    own = g.get_state(thread("f2")).values
    assert own["status"] == "ATTACHED" and own["attached_to"] == "f1" and not g.get_state(thread("f2")).next

    # Delivered again: a duplicate, the rows are not added twice.
    (again,) = process_lookup(g, lookup_message(batch_item("f2", "8", 4), lookup_id="L2"), open_workflows=index)
    assert again["delivery"] == "duplicate"
    assert g.get_state(thread("f1")).values["failure"]["rows_affected"] == 7


def test_approved_fix_stages_every_attached_batch(monkeypatch):
    write_quarantine(config.QUARANTINE_PATH, rows_of("7", 3) + rows_of("8", 4))
    g = escalating_graph()
    index = OpenWorkflows(g)
    process_lookup(g, lookup_message(batch_item("f1", "7", 3)), open_workflows=index)
    process_lookup(g, lookup_message(batch_item("f2", "8", 4)), open_workflows=index)
    state = g.get_state(thread("f1")).values
    decision = esc.make_decision("f1", state["escalation"]["escalation_id"], "APPLY_FIX", "thay", None,
                                 "ADD_VARIANT", {"canonical_value": "JULY"})
    summary = apply_decision(g, decision)
    out = summary["state"]
    assert summary["delivery"] == "decided" and out["status"] == "RESOLVED_BY_ENGINEER"
    assert out["execution"]["rows_affected"] == 7                            # both batches staged
    staged = audit.find(out["execution"]["execution_id"])["record_ids"]
    assert sorted(staged) == sorted([f"7-r{n}" for n in range(3)] + [f"8-r{n}" for n in range(4)])
    assert out["validation"]["rows_released"] == 7
    # A new batch after the workflow ended starts its own workflow (it reuses the fix
    # from memory then, and counts towards promotion by recurrence).
    index.note("f1")
    (late,) = process_lookup(g, lookup_message(batch_item("f3", "9", 2)), open_workflows=index)
    assert late["delivery"] == "new"


def test_attached_batches_change_the_execution_id_so_the_bigger_group_is_staged_again():
    one = audit.execution_id("f1", "ADD_VARIANT", {"canonical_value": "JULY"})
    assert audit.execution_id("f1", "ADD_VARIANT", {"canonical_value": "JULY"}, None) == one   # unchanged without
    two = audit.execution_id("f1", "ADD_VARIANT", {"canonical_value": "JULY"}, ["7", "8"])
    assert two != one


def test_a_workflow_that_reached_validate_or_ended_is_not_joined():
    g = build_graph(InMemorySaver(), nodes=REUSE_PATH_BUILT)
    index = OpenWorkflows(g)
    process_lookup(g, lookup_message(batch_item("f1", "7", 3, route="remediate", match=PAST_FIX)), open_workflows=index)
    assert g.get_state(thread("f1")).values["status"] == "RESOLVED"
    (later,) = process_lookup(g, lookup_message(batch_item("f2", "8", 4, route="remediate", match=PAST_FIX)),
                              open_workflows=index)
    assert later["delivery"] == "new" and later["state"]["failure"]["rows_affected"] == 4


def test_different_problems_never_attach():
    g = escalating_graph()
    index = OpenWorkflows(g)
    summaries = process_lookup(g, lookup_message(batch_item("f1", "7", 3), batch_item("f2", "7", 2, value="Ju")),
                               open_workflows=index)
    assert [s["delivery"] for s in summaries] == ["new", "new"]


def test_index_is_rebuilt_from_saved_workflows_after_a_restart(tmp_path):
    db = str(tmp_path / "checkpoints.sqlite")
    before = open_checkpointer(db)
    process_lookup(escalating_graph(before), lookup_message(batch_item("f1", "7", 3)))
    before.conn.close()

    after = open_checkpointer(db)
    g = escalating_graph(after)
    index = OpenWorkflows(g)
    assert index.scan(thread_ids(after)) == 1
    (later,) = process_lookup(g, lookup_message(batch_item("f2", "8", 4)), open_workflows=index)
    after.conn.close()
    assert later["delivery"] == "attached" and later["attached_to"] == "f1"


def test_dismissed_workflow_closes_and_the_next_batch_opens_a_new_case():
    g = escalating_graph()
    index = OpenWorkflows(g)
    process_lookup(g, lookup_message(batch_item("f1", "7", 3)), open_workflows=index)
    state = g.get_state(thread("f1")).values
    g.invoke(Command(resume=esc.make_decision("f1", state["escalation"]["escalation_id"], "DISMISS", "thay",
                                              "no canonical value yet", None, None)), thread("f1"))
    index.note("f1")
    (later,) = process_lookup(g, lookup_message(batch_item("f2", "8", 4)), open_workflows=index)
    assert later["delivery"] == "new"


# --- pruning (agent/upkeep.py) -----------------------------------------------------------
def test_finished_workflow_keeps_only_its_last_checkpoint_and_its_whole_state(tmp_path):
    saver = open_checkpointer(str(tmp_path / "checkpoints.sqlite"))
    g = build_graph(saver, nodes=REUSE_PATH_BUILT)
    process_lookup(g, lookup_message(batch_item("f1", "7", 3, route="remediate", match=PAST_FIX)))
    before = g.get_state(thread("f1")).values
    assert checkpoint_count(saver, "f1") > 1
    upkeep = Upkeep(g, prune=True, interval=0)
    assert upkeep.finished("f1") > 0
    assert checkpoint_count(saver, "f1") == 1
    assert g.get_state(thread("f1")).values == before
    assert path(before) == ["deterministic_lookup", "remediate", "validate", "learn"]
    # A redelivery is still a duplicate.
    (again,) = process_lookup(g, lookup_message(batch_item("f1", "7", 3, route="remediate", match=PAST_FIX)))
    saver.conn.close()
    assert again["delivery"] == "duplicate"


def test_waiting_and_relearn_workflows_are_not_pruned(tmp_path):
    saver = open_checkpointer(str(tmp_path / "checkpoints.sqlite"))

    def learn_down(state):
        raise ConnectionError("neo4j down")

    g = escalating_graph(saver)
    process_lookup(g, lookup_message(batch_item("f1", "7", 3)))                  # waits for an engineer
    broken = build_graph(saver, nodes={**REUSE_PATH_BUILT, "learn": learn_down}, retry_seconds=0)
    process_lookup(broken, lookup_message(batch_item("f2", "7", 3, value="Ju", route="remediate", match=PAST_FIX)))
    assert broken.get_state(thread("f2")).values["error"]["node"] == "learn"
    counts = {t: checkpoint_count(saver, t) for t in ("f1", "f2")}
    result = prune_all(g, thread_ids(saver))
    assert result["checkpoints_removed"] == 0
    assert {t: checkpoint_count(saver, t) for t in ("f1", "f2")} == counts
    saver.conn.close()


def test_start_up_loads_each_saved_workflow_once(tmp_path, monkeypatch):
    """Learn retry, open-workflow index and pruning share one load per saved workflow
    at start (review of #37), with the same result as their own scans."""
    from agent.consumer import LearnRetry, start_up

    saver = open_checkpointer(str(tmp_path / "checkpoints.sqlite"))

    def learn_down(state):
        raise ConnectionError("neo4j down")

    process_lookup(escalating_graph(saver), lookup_message(batch_item("f1", "7", 3)))   # waits
    broken = build_graph(saver, nodes={**REUSE_PATH_BUILT, "learn": learn_down}, retry_seconds=0)
    process_lookup(broken, lookup_message(batch_item("f2", "7", 3, value="Ju", route="remediate", match=PAST_FIX)))
    done = build_graph(saver, nodes=REUSE_PATH_BUILT)
    process_lookup(done, lookup_message(batch_item("f3", "7", 3, value="JUNEE", route="remediate", match=PAST_FIX)))
    assert checkpoint_count(saver, "f3") > 1

    g = escalating_graph(saver)
    loads = []
    real_get_state = g.get_state
    monkeypatch.setattr(g, "get_state", lambda config, *a, **k: loads.append(config) or real_get_state(config, *a, **k))
    retry, index, upkeep = LearnRetry(g), OpenWorkflows(g), Upkeep(g, prune=True, interval=0)
    result = start_up(g, retry, index, upkeep)
    assert len(loads) == 3                                       # one per saved workflow
    assert retry.pending == {"f2"}
    assert list(index.by_signature.values()) == ["f1"]
    assert result["threads"] == 1 and checkpoint_count(saver, "f3") == 1
    assert checkpoint_count(saver, "f1") > 1 and checkpoint_count(saver, "f2") > 1
    saver.conn.close()


def test_upkeep_off_prunes_nothing(tmp_path):
    saver = open_checkpointer(str(tmp_path / "checkpoints.sqlite"))
    g = build_graph(saver, nodes=REUSE_PATH_BUILT)
    process_lookup(g, lookup_message(batch_item("f1", "7", 3, route="remediate", match=PAST_FIX)))
    count = checkpoint_count(saver, "f1")
    assert Upkeep(g, prune=False, interval=0).finished("f1") == 0
    assert checkpoint_count(saver, "f1") == count
    saver.conn.close()


def test_audit_table_is_compacted_without_changing_what_is_read(tmp_path):
    from deltalake import DeltaTable

    from agent.upkeep import compact_audit

    path_ = str(tmp_path / "audit")
    for n in range(6):
        audit.write({"execution_id": f"e{n}", "failure_id": "f1", "status": "SUCCESS", "action": "ADD_VARIANT",
                     "action_params": {"canonical_value": "JULY"}, "record_ids": [f"r{n}"],
                     "executed_at": None, "started_at": None}, path_)
    before = [audit.find(f"e{n}", path_) for n in range(6)]
    assert len(DeltaTable(path_).file_uris()) == 6
    assert compact_audit(path_, min_files=10)["status"] == "SKIPPED"
    result = compact_audit(path_, min_files=3, retention_hours=0)
    assert result["status"] == "COMPACTED" and result["files_after"] == 1
    assert [audit.find(f"e{n}", path_) for n in range(6)] == before
    assert result["vacuumed"] == 6                                           # replaced files are gone
    assert compact_audit(str(tmp_path / "none"))["status"] == "NO_TABLE"


def test_upkeep_compacts_on_its_timer_only():
    now = [0.0]
    upkeep = Upkeep(None, prune=False, interval=60, clock=lambda: now[0])
    assert upkeep.due() is None
    now[0] = 61
    assert upkeep.due()["status"] in ("NO_TABLE", "SKIPPED")
    assert upkeep.due() is None


def test_poll_loop_attaches_and_prunes_as_the_agent_runs_it(tmp_path):
    import json

    from agent.consumer import run
    from agent.tests.test_consumer import FakeConsumer

    saver = open_checkpointer(str(tmp_path / "checkpoints.sqlite"))
    g = build_graph(saver, nodes={**REUSE_PATH_BUILT, "diagnose": diagnose_july, "policy_gate": gate_rejects})
    consumer = FakeConsumer([
        json.dumps(lookup_message(batch_item("f1", "7", 3), batch_item("d1", "7", 2, value="Ju", route="remediate",
                                                                       match=PAST_FIX))).encode(),
        json.dumps(lookup_message(batch_item("f2", "8", 4), lookup_id="L2")).encode(),
    ])
    run(consumer, g, max_messages=2, open_workflows=OpenWorkflows(g), upkeep=Upkeep(g, prune=True, interval=0))
    assert consumer.committed == [0, 1]
    assert g.get_state(thread("f2")).values["attached_to"] == "f1"
    assert g.get_state(thread("f1")).values["failure"]["rows_affected"] == 7
    assert checkpoint_count(saver, "d1") == 1                 # finished: pruned at once
    assert checkpoint_count(saver, "f1") > 1                  # waiting: kept
    saver.conn.close()


def test_quarantine_claimed_by_the_trigger_is_still_readable():
    """The trigger's UPDATE writes Arrow string_view files; Remediate's filtered read of a
    table that mixes them with Spark's files must still work."""
    from deltalake import DeltaTable

    write_quarantine(config.QUARANTINE_PATH, rows_of("7", 3))
    write_quarantine(config.QUARANTINE_PATH, rows_of("8", 4))
    DeltaTable(config.QUARANTINE_PATH).update(predicate="silver2_batch_id = '7'",
                                              updates={"healing_status": "'IN_PROGRESS'"})
    failure = merge(batch_item("f1", "7", 3)["failure"], batch_item("f2", "8", 4)["failure"])
    group = reference.failure_records(failure)
    assert len(group["rows"]) == 7 and group["not_waiting"] == 0


def test_fresh_agent_state_volume_starts(tmp_path):
    """A fresh volume has no checkpoint tables: start-up must not crash-loop on them."""
    import sqlite3

    from langgraph.checkpoint.sqlite import SqliteSaver

    from agent.consumer import LearnRetry, thread_ids
    from agent.graph import build_graph, open_checkpointer
    from agent.upkeep import checkpoint_count

    bare = SqliteSaver(sqlite3.connect(str(tmp_path / "bare.sqlite"), check_same_thread=False))
    assert thread_ids(bare) == [] and checkpoint_count(bare, "x") == 0     # no tables at all
    graph = build_graph(open_checkpointer(str(tmp_path / "state" / "checkpoints.sqlite")))
    assert thread_ids(graph.checkpointer) == []
    LearnRetry(graph).scan()
    assert OpenWorkflows(graph).scan(thread_ids(graph.checkpointer)) == 0
    assert Upkeep(graph).start(thread_ids(graph.checkpointer)) == {"threads": 0, "checkpoints_removed": 0}
