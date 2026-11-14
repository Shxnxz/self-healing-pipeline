"""Recovery agent: Kafka `recovery.lookup` in, one LangGraph workflow per failure.

  recovery.lookup (lookup/consumer.py) --> for each stack item, top first --> graph (agent/graph.py)
  recovery.decisions (agent/review.py)  --> resume the workflow waiting for that decision

Each workflow is checkpointed under thread_id = failure_id, which gives five
behaviours for a stack item:

  waiting    saved state paused in engineer_review: the failure is with an engineer,
             leave it alone (the decision resumes it, not a new publish)
  new        no saved state: start the workflow
  resumed    saved state that stopped part-way (the agent was killed): continue
             from the last finished node
  duplicate  saved state that ended with every step real: skip it (Kafka delivers
             at least once)
  rerun      saved state that ended on a stub step: nothing was really done, so
             forget it and run again. A failure handled before a step was built is
             therefore picked up when it is published again afterwards.
  relearn    saved state whose only missing piece is Learn: Learn kept raising (Neo4j
             or the reference table down) or was still the stub. Learn alone runs again
             from the checkpoint before it, so nothing else (no fix, no model call, no
             engineer's case) is repeated.

The offset is committed only after every item of the message was handled. A step
that raises does not stop the agent (agent/graph.py: guarded). If the agent itself
dies, the message is redelivered: finished items are skipped, the rest resume.
"""
import json
from typing import Callable, List, Optional

from langgraph.types import Command

from agent import config
from agent import escalation as esc
from agent.graph import build_graph, open_checkpointer
from agent.state import initial_state

NEW, RESUMED, DUPLICATE, RERUN, MALFORMED = "new", "resumed", "duplicate", "rerun", "malformed"
RELEARN = "relearn"
WAITING = "waiting"
# What happened to a decision from recovery.decisions.
DECIDED, REFUSED, UNKNOWN = "decided", "refused", "unknown"

REVIEW_NODE = "engineer_review"
LEARN_NODE = "learn"


def is_waiting(saved) -> bool:
    """The workflow is paused for an engineer's decision."""
    return bool(saved.values) and tuple(saved.next) == (REVIEW_NODE,) and bool(saved.interrupts)


def needs_learn(saved) -> bool:
    """The workflow ended, and Learn is the only step that did not really run: it kept
    raising, or it was the stub (workflows finished before Task 107)."""
    values = saved.values or {}
    if not values or saved.next:
        return False
    stubbed = set(values.get("stubbed") or [])
    return stubbed == {LEARN_NODE} or (not stubbed and (values.get("error") or {}).get("node") == LEARN_NODE)


def before_learn(graph, thread: dict):
    """The newest checkpoint of the thread that is about to run Learn, or None."""
    for snapshot in graph.get_state_history(thread):
        if tuple(snapshot.next) == (LEARN_NODE,):
            return snapshot
    return None


def run_workflow(graph, lookup_id: Optional[str], item: dict) -> dict:
    """Start, resume, skip or rerun the workflow of one stack item. Returns a summary for the log."""
    failure = item.get("failure") if isinstance(item, dict) else None
    failure_id = failure.get("failure_id") if isinstance(failure, dict) else None
    if not failure_id:
        return {"delivery": MALFORMED, "failure_id": None, "state": {}}

    thread = {"configurable": {"thread_id": failure_id}}
    saved = graph.get_state(thread)
    delivery = NEW
    if is_waiting(saved):
        return {"delivery": WAITING, "failure_id": failure_id, "state": saved.values}
    if saved.values and saved.next:
        return {"delivery": RESUMED, "failure_id": failure_id, "state": graph.invoke(None, thread)}
    if needs_learn(saved):
        snapshot = before_learn(graph, thread)
        if snapshot is not None and not set(snapshot.values.get("stubbed") or []):
            # Replays from that checkpoint: only Learn runs, on the state it had then.
            return {"delivery": RELEARN, "failure_id": failure_id, "state": graph.invoke(None, snapshot.config)}
    if saved.values and not saved.values.get("stubbed"):
        return {"delivery": DUPLICATE, "failure_id": failure_id, "state": saved.values}
    if saved.values:
        graph.checkpointer.delete_thread(failure_id)
        delivery = RERUN
    state = graph.invoke(initial_state(lookup_id, item), thread)
    return {"delivery": delivery, "failure_id": failure_id, "state": state}


def apply_decision(graph, message: dict) -> dict:
    """Resume the workflow an engineer's decision answers (agent/review.py sends it).

    The decision is checked against the saved state first: a decision for a workflow
    that is not waiting, or for an older round of its escalation, is refused and the
    workflow is left as it is. A decision delivered again after the agent died while
    acting on it continues the workflow from where it stopped.
    """
    failure_id = message.get("failure_id") if isinstance(message, dict) else None
    if not isinstance(failure_id, str) or not failure_id:
        return {"delivery": MALFORMED, "failure_id": None, "state": {}, "problems": ["no failure_id"]}
    thread = {"configurable": {"thread_id": failure_id}}
    saved = graph.get_state(thread)
    if not saved.values:
        return {"delivery": UNKNOWN, "failure_id": failure_id, "state": {},
                "problems": [f"no workflow is saved for failure {failure_id}"]}
    if not is_waiting(saved):
        decided = (saved.values.get("escalation") or {}).get("decision") or {}
        same = decided.get("escalation_id") == message.get("escalation_id") and \
            decided.get("decided_at") == message.get("decided_at")
        if same and saved.next:   # redelivered after the agent died while acting on it
            return {"delivery": RESUMED, "failure_id": failure_id, "state": graph.invoke(None, thread)}
        if same:
            return {"delivery": DUPLICATE, "failure_id": failure_id, "state": saved.values}
        return {"delivery": REFUSED, "failure_id": failure_id, "state": saved.values,
                "problems": [f"failure {failure_id} is not waiting for an engineer"]}
    _, problems = esc.check_decision(saved.values, message)
    if problems:
        return {"delivery": REFUSED, "failure_id": failure_id, "state": saved.values, "problems": problems}
    state = graph.invoke(Command(resume=message), thread)
    return {"delivery": DECIDED, "failure_id": failure_id, "state": state}


def process_lookup(graph, message: dict, between: Optional[Callable[[], None]] = None) -> List[dict]:
    """Work through one recovery.lookup message from the top of its stack.
    `between` is called after each workflow (the poll loop uses it to stay in the group)."""
    stack = message.get("stack")
    if not isinstance(stack, list):
        return []
    summaries = []
    for item in stack:
        summaries.append(run_workflow(graph, message.get("lookup_id"), item))
        if between is not None:
            between()
    return summaries


def describe(summary: dict) -> str:
    state = summary["state"]
    if summary["delivery"] == MALFORMED:
        return "[agent] Skipping stack item or decision without a failure_id"
    if not state:
        return f"[agent] decision for {summary['failure_id']} {summary['delivery']}: {'; '.join(summary.get('problems') or [])}"
    failure = state.get("failure") or {}
    what = failure.get("column_name") or failure.get("failure_type")
    path = " > ".join(entry["node"] for entry in state.get("trace", []))
    stubs = f" [stubs: {', '.join(state['stubbed'])}]" if state.get("stubbed") else ""
    escalation = state.get("escalation") or {}
    outcome = state.get("status")
    if escalation.get("status") == esc.WAITING and (summary["delivery"] == WAITING or "__interrupt__" in state):
        outcome = (f"{outcome} {escalation.get('reason')}, waiting for an engineer: "
                   f"python -m agent.review show {summary['failure_id']}")
    problems = f" [{'; '.join(summary['problems'])}]" if summary.get("problems") else ""
    return (f"[agent] {summary['failure_id']} {summary['delivery']:<9} "
            f"{(state.get('priority') or {}).get('tier') or '-'} "
            f"{failure.get('folder') or '-'}:{what} value={failure.get('signature_value')!r} "
            f"-> {outcome} ({path}){stubs}{problems}")


def parse(msg) -> Optional[dict]:
    try:
        message = json.loads(msg.value())
    except (TypeError, ValueError):
        message = None
    if not isinstance(message, dict):
        print(f"[agent] Skipping unparseable message {msg.topic()}@{msg.offset()}")
        return None
    return message


def run(consumer, graph, max_messages: Optional[int] = None) -> None:
    """Poll loop. `max_messages` stops it after that many messages (tests).

    A stack can hold many failures, and a workflow can be slow once Diagnose calls
    an LLM. Kafka drops a consumer that does not poll for max.poll.interval.ms, so
    the partitions are paused while a stack is worked and polled after every
    workflow: the agent stays in the group without receiving the next stack early.
    """
    from confluent_kafka import KafkaError

    early = []  # messages a keep-alive poll returned anyway (partition assigned mid-stack)

    def keep_alive() -> None:
        extra = consumer.poll(0)
        if extra is not None and not extra.error():
            early.append(extra)

    handled = 0
    while max_messages is None or handled < max_messages:
        msg = early.pop(0) if early else consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            # The topic does not exist until the lookup service publishes its first stack.
            if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"[agent] Kafka error: {msg.error()}")
            continue
        message = parse(msg)
        if message is not None:
            assigned = consumer.assignment()
            consumer.pause(assigned)
            if msg.topic() == config.DECISIONS_TOPIC:
                summary = apply_decision(graph, message)
                consumer.resume(assigned)
                print(f"[agent] decision {message.get('decision')} by {message.get('engineer')} "
                      f"on {message.get('escalation_id')}: {summary['delivery']}")
                print(describe(summary), flush=True)
            else:
                summaries = process_lookup(graph, message, between=keep_alive)
                consumer.resume(assigned)
                print(f"[agent] lookup {message.get('lookup_id')}: {len(summaries)} failure(s)")
                for summary in summaries:
                    print(describe(summary), flush=True)
        consumer.commit(message=msg, asynchronous=False)
        handled += 1


def ensure_topics(topics: List[str]) -> None:
    """Create the topics if they do not exist yet. A consumer subscribed to a missing
    topic only sees it at its next metadata refresh (minutes), so the first decision an
    engineer sends would wait that long."""
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics if t not in existing]
    for topic, future in (admin.create_topics(missing) if missing else {}).items():
        try:
            future.result()
            print(f"[agent] Created topic {topic}", flush=True)
        except Exception as exc:  # created meanwhile by someone else, or auto-created: fine
            print(f"[agent] Topic {topic}: {exc}", flush=True)


def main() -> None:
    from confluent_kafka import Consumer

    graph = build_graph(open_checkpointer(config.CHECKPOINT_PATH))
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": config.MAX_POLL_INTERVAL_MS,
    })
    ensure_topics([config.LOOKUP_TOPIC, config.DECISIONS_TOPIC])
    consumer.subscribe([config.LOOKUP_TOPIC, config.DECISIONS_TOPIC])
    print(f"[agent] Consuming {config.LOOKUP_TOPIC} and {config.DECISIONS_TOPIC} from {config.KAFKA_BOOTSTRAP} "
          f"(checkpoints {config.CHECKPOINT_PATH})", flush=True)
    try:
        run(consumer, graph)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
