"""The recovery.lookup consumer: python -m pytest agent/tests   (no Kafka, no Neo4j)"""
import json

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from agent.consumer import describe, process_lookup, run
from agent.graph import build_graph, open_checkpointer
from agent.show import saved_state
from agent.tests.helpers import (PAST_FIX, REUSE_PATH_BUILT, Killed, lookup_message, path, remediate_ok,
                                 stack_item, trace)


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

    graph = build_graph(InMemorySaver(), nodes={**REUSE_PATH_BUILT, "remediate": counting_remediate})
    message = lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))
    first = process_lookup(graph, message)
    again = process_lookup(graph, message)
    assert calls == ["f1"]
    assert first[0]["state"]["status"] == "RESOLVED"
    assert again[0]["delivery"] == "duplicate" and again[0]["state"]["status"] == "RESOLVED"


def test_failure_handled_by_stubs_is_run_again_once_the_steps_are_built(tmp_path):
    """A workflow that ended on a stub did nothing, so it must not count as done."""
    db = str(tmp_path / "checkpoints.sqlite")
    message = lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))

    before = open_checkpointer(db)
    (stubbed,) = process_lookup(build_graph(before), message)
    before.conn.close()
    assert stubbed["delivery"] == "new" and stubbed["state"]["status"] == "ESCALATED"

    after = open_checkpointer(db)
    built = build_graph(after, nodes=REUSE_PATH_BUILT)
    (rerun,) = process_lookup(built, message)
    (again,) = process_lookup(built, message)
    after.conn.close()
    assert rerun["delivery"] == "rerun" and rerun["state"]["status"] == "RESOLVED"
    # a clean run, with nothing left over from the stub run
    assert path(rerun["state"]) == ["deterministic_lookup", "remediate", "validate", "learn"]
    assert "escalation" not in rerun["state"] and rerun["state"]["stubbed"] == []
    assert again["delivery"] == "duplicate"


def test_workflow_resumes_after_a_crash_from_the_node_that_failed(tmp_path):
    """Checkpoints are on disk: a new process continues, it does not start over."""
    db = str(tmp_path / "state" / "checkpoints.sqlite")
    ran = []

    def remediate_once(state):
        ran.append("remediate")
        return remediate_ok(state)

    def validate_killed(state):
        raise Killed()

    def validate_works(state):
        return {"validation": {"passed": True, "reasons": []}, "trace": trace("validate")}

    message = lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))
    before = open_checkpointer(db)
    crashing = build_graph(before, nodes={"remediate": remediate_once, "validate": validate_killed})
    with pytest.raises(Killed):
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
    assert line.endswith("[stubs: diagnose, policy_gate, escalate, learn]")


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
    """Like Kafka: a paused consumer gets nothing from poll(), but the poll still counts."""

    def __init__(self, values):
        self.pending = [FakeMessage(v, i) for i, v in enumerate(values)]
        self.committed, self.paused, self.polls_while_paused = [], False, 0

    def poll(self, timeout):
        if self.paused:
            self.polls_while_paused += 1
            return None
        return self.pending.pop(0) if self.pending else None

    def assignment(self):
        return ["recovery.lookup[0]"]

    def pause(self, partitions):
        self.paused = True

    def resume(self, partitions):
        self.paused = False

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


def test_poll_loop_keeps_polling_kafka_while_it_works_through_a_long_stack():
    """One poll per workflow keeps the agent in the group; paused, so no stack is taken early."""
    graph = build_graph(InMemorySaver())
    stack = [stack_item(f"f{i}", rank=i) for i in range(1, 6)]
    consumer = FakeConsumer([json.dumps(lookup_message(*stack)).encode(),
                             json.dumps(lookup_message(stack_item("g1"), lookup_id="L2")).encode()])
    run(consumer, graph, max_messages=1)
    assert consumer.polls_while_paused == 5 and consumer.paused is False
    assert consumer.committed == [0] and len(consumer.pending) == 1  # the second stack was left alone
    run(consumer, graph, max_messages=1)
    assert consumer.committed == [0, 1]


def test_step_that_always_raises_does_not_block_the_messages_behind_it():
    def remediate_broken(state):
        raise RuntimeError("boom")

    graph = build_graph(InMemorySaver(), nodes={"remediate": remediate_broken}, retry_seconds=0)
    consumer = FakeConsumer([
        json.dumps(lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))).encode(),
        json.dumps(lookup_message(stack_item("f2"), lookup_id="L2")).encode(),
    ])
    run(consumer, graph, max_messages=2)
    assert consumer.committed == [0, 1]
    assert graph.get_state(thread("f1")).values["escalation"]["reason"] == "STEP_FAILED"
    assert graph.get_state(thread("f2")).values["status"] == "ESCALATED"


def test_poll_loop_does_not_commit_a_message_when_the_agent_dies_mid_workflow():
    def remediate_killed(state):
        raise Killed()

    graph = build_graph(InMemorySaver(), nodes={"remediate": remediate_killed})
    consumer = FakeConsumer([json.dumps(lookup_message(stack_item("f1", route="remediate", match=PAST_FIX))).encode()])
    with pytest.raises(Killed):
        run(consumer, graph, max_messages=1)
    assert consumer.committed == []
