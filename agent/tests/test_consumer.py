"""The recovery.lookup consumer: python -m pytest agent/tests   (no Kafka, no Neo4j)"""
import json

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from agent.consumer import describe, process_lookup, run
from agent.graph import build_graph, open_checkpointer
from agent.show import saved_state
from agent.tests.helpers import PAST_FIX, lookup_message, path, remediate_ok, stack_item, trace


def thread(failure_id):
    return {"configurable": {"thread_id": failure_id}}


def test_each_stack_item_gets_its_own_saved_workflow_in_stack_order():
    graph = build_graph(InMemorySaver())
    message = lookup_message(stack_item("f1", route="remediate", match=PAST_FIX, rank=1),
                             stack_item("f2", route="diagnose", rank=2))
    summaries = process_lookup(graph, message)
    assert [(s["failure_id"], s["delivery"]) for s in summaries] == [("f1", "new"), ("f2", "new")]
    assert graph.get_state(thread("f1")).values["route"] == "remediate"
    assert graph.get_state(thread("f2")).values["route"] == "diagnose"
    assert graph.get_state(thread("f2")).values["lookup_id"] == "L1"


def test_redelivered_message_does_not_run_a_workflow_twice():
    calls = []

    def counting_remediate(state):
        calls.append(state["failure_id"])
        return remediate_ok(state)

    graph = build_graph(InMemorySaver(), nodes={"remediate": counting_remediate})
    message = lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))
    first = process_lookup(graph, message)
    again = process_lookup(graph, message)
    assert calls == ["f1"]
    assert again[0]["delivery"] == "duplicate" and again[0]["state"]["status"] == first[0]["state"]["status"]


def test_workflow_resumes_after_a_crash_from_the_node_that_failed(tmp_path):
    """Checkpoints are on disk: a new process continues, it does not start over."""
    db = str(tmp_path / "state" / "checkpoints.sqlite")
    ran = []

    def remediate_once(state):
        ran.append("remediate")
        return remediate_ok(state)

    def validate_crashes(state):
        raise RuntimeError("Spark is down")

    def validate_works(state):
        return {"validation": {"passed": True, "reasons": []}, "trace": trace("validate")}

    message = lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))
    before = open_checkpointer(db)
    crashing = build_graph(before, nodes={"remediate": remediate_once, "validate": validate_crashes})
    with pytest.raises(RuntimeError):
        process_lookup(crashing, message)
    before.conn.close()

    after = open_checkpointer(db)
    restarted = build_graph(after, nodes={"remediate": remediate_once, "validate": validate_works})
    (summary,) = process_lookup(restarted, message)
    after.conn.close()
    assert summary["delivery"] == "resumed"
    assert ran == ["remediate"]  # not executed a second time
    assert path(summary["state"]) == ["deterministic_lookup", "remediate", "validate", "learn"]
    assert summary["state"]["status"] == "RESOLVED"


def test_item_without_a_failure_id_is_skipped_and_the_rest_still_run():
    graph = build_graph(InMemorySaver())
    summaries = process_lookup(graph, lookup_message({"route": "diagnose"}, "junk", stack_item("f2")))
    assert [s["delivery"] for s in summaries] == ["malformed", "malformed", "new"]
    assert "Skipping" in describe(summaries[0])
    assert process_lookup(graph, {"lookup_id": "L2"}) == []


def test_describe_shows_the_failure_the_outcome_and_the_path():
    (summary,) = process_lookup(build_graph(InMemorySaver()), lookup_message(stack_item("f1")))
    line = describe(summary)
    assert "f1" in line and "Crash1:crash_month" in line and "'Jule'" in line
    assert "ESCALATED (deterministic_lookup > diagnose > policy_gate > escalate > learn)" in line


def test_saved_state_can_be_read_back_by_failure_id():
    graph = build_graph(InMemorySaver())
    process_lookup(graph, lookup_message(stack_item("f1")))
    state = saved_state(graph, "f1")
    assert state["status"] == "ESCALATED" and state["next"] == []
    assert state["failure"]["raw_value"] == "Jule"
    assert saved_state(graph, "never-seen") == {}


# --- poll loop, with a fake Kafka consumer ----------------------------------------
class FakeMessage:
    def __init__(self, value, offset):
        self._value, self._offset = value, offset

    def value(self):
        return self._value

    def error(self):
        return None

    def topic(self):
        return "recovery.lookup"

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, values):
        self.pending = [FakeMessage(v, i) for i, v in enumerate(values)]
        self.committed = []

    def poll(self, timeout):
        return self.pending.pop(0) if self.pending else None

    def commit(self, message, asynchronous):
        self.committed.append(message.offset())


def test_poll_loop_commits_each_message_after_its_stack_is_handled():
    graph = build_graph(InMemorySaver())
    consumer = FakeConsumer([
        b"not json",
        json.dumps(lookup_message(stack_item("f1"), stack_item("f2", rank=2))).encode(),
    ])
    run(consumer, graph, max_messages=2)
    assert consumer.committed == [0, 1]  # the unreadable message is skipped, not retried forever
    assert graph.get_state(thread("f2")).values["status"] == "ESCALATED"


def test_poll_loop_does_not_commit_a_message_whose_workflow_crashed():
    def remediate_crashes(state):
        raise RuntimeError("boom")

    graph = build_graph(InMemorySaver(), nodes={"remediate": remediate_crashes})
    consumer = FakeConsumer([json.dumps(lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))).encode()])
    with pytest.raises(RuntimeError):
        run(consumer, graph, max_messages=1)
    assert consumer.committed == []
