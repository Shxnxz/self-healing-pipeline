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
             therefore picked up when it is published again afterwards. Never a
             workflow an engineer decided: that one is a duplicate (or relearned).
  relearn    saved state whose only missing piece is Learn: Learn kept raising (Neo4j
             or the reference table down) or was still the stub. Learn alone runs again
             from the checkpoint before it, so nothing else (no fix, no model call, no
             engineer's case) is repeated.

The scanner does not publish a failure twice, so a Learn that failed is not left
waiting for a redelivery: `LearnRetry` finds such workflows when the agent starts and
after every workflow, and relearns them every AGENT_RELEARN_INTERVAL_SECONDS.

The offset is committed only after every item of the message was handled. A step
that raises does not stop the agent (agent/graph.py: guarded). If the agent itself
dies, the message is redelivered: finished items are skipped, the rest resume.
"""
import json
import time
from typing import Callable, List, Optional

from langgraph.types import Command

from agent import config
from agent import escalation as esc
from agent.graph import build_graph, open_checkpointer
from agent.state import STATUS_DISMISSED, STATUS_RESOLVED_BY_ENGINEER, initial_state

NEW, RESUMED, DUPLICATE, RERUN, MALFORMED = "new", "resumed", "duplicate", "rerun", "malformed"
RELEARN = "relearn"
WAITING = "waiting"
# What happened to a decision from recovery.decisions.
DECIDED, REFUSED, UNKNOWN = "decided", "refused", "unknown"

REVIEW_NODE = "engineer_review"
LEARN_NODE = "learn"


def engineer_decided(state: dict) -> bool:
    """An engineer answered this workflow's escalation. Its outcome is theirs: it is never
    dropped and run again, even when it ended on a stub, or the engineer would be asked
    again for a failure they closed (and a fix already released would be redone)."""
    return bool((state.get("escalation") or {}).get("decision")) or \
        state.get("status") in (STATUS_RESOLVED_BY_ENGINEER, STATUS_DISMISSED)


def pending_refusal(saved) -> Optional[dict]:
    """The decision engineer_review refused when it re-checked it on resume, if any:
    it lives only in the pending interrupt's value, so the review CLI reads it there."""
    for pending in saved.interrupts or ():
        value = getattr(pending, "value", None)
        if isinstance(value, dict) and value.get("refused"):
            return value["refused"]
    return None


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


def relearn(graph, failure_id: str) -> Optional[dict]:
    """Run Learn alone again for a workflow that needs it. None when it does not."""
    thread = {"configurable": {"thread_id": failure_id}}
    if not needs_learn(graph.get_state(thread)):
        return None
    snapshot = before_learn(graph, thread)
    if snapshot is None or set(snapshot.values.get("stubbed") or []):
        return None
    # Replays from that checkpoint: only Learn runs, on the state it had then.
    return {"delivery": RELEARN, "failure_id": failure_id, "state": graph.invoke(None, snapshot.config)}


def thread_ids(checkpointer) -> List[str]:
    """Every workflow the checkpoints hold."""
    conn = getattr(checkpointer, "conn", None)
    if conn is not None:   # SqliteSaver: one query instead of loading every checkpoint
        return [row[0] for row in conn.execute("SELECT DISTINCT thread_id FROM checkpoints")]
    return sorted({c.config["configurable"]["thread_id"] for c in checkpointer.list(None)})


class LearnRetry:
    """Workflows whose Learn has to run again, retried on a timer.

    Filled at start from every saved workflow (this also picks up the ones that ended
    on the Learn stub before Task 107) and after every workflow the agent runs or
    resumes. `due()` relearns them all when the interval has passed; a Learn that fails
    again stays in the list for the next round.
    """

    def __init__(self, graph, interval: Optional[float] = None, clock: Callable[[], float] = time.monotonic):
        self.graph, self.clock = graph, clock
        self.interval = config.RELEARN_INTERVAL_SECONDS if interval is None else interval
        self.pending: set = set()
        self.next_at = 0.0

    def scan(self) -> int:
        for failure_id in thread_ids(self.graph.checkpointer):
            self.note(failure_id)
        return len(self.pending)

    def note(self, failure_id: Optional[str]) -> None:
        if failure_id and needs_learn(self.graph.get_state({"configurable": {"thread_id": failure_id}})):
            self.pending.add(failure_id)

    def due(self, between: Optional[Callable[[], None]] = None) -> List[dict]:
        if not self.pending or self.interval <= 0 or self.clock() < self.next_at:
            return []
        self.next_at = self.clock() + self.interval
        summaries = []
        for failure_id in sorted(self.pending):
            summary = relearn(self.graph, failure_id)
            if summary is not None:
                summaries.append(summary)
            self.pending.discard(failure_id)
            self.note(failure_id)      # still failing: keep it for the next round
            if between is not None:
                between()
        return summaries


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
    if needs_learn(saved):   # before the duplicate check: a decided workflow is relearned too
        summary = relearn(graph, failure_id)
        if summary is not None:
            return summary
    if saved.values and (not saved.values.get("stubbed") or engineer_decided(saved.values)):
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
    refused = pending_refusal(graph.get_state(thread))
    if refused:   # accepted above, refused by engineer_review's own check a moment later
        return {"delivery": REFUSED, "failure_id": failure_id, "state": state, "problems": refused["problems"]}
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


def run(consumer, graph, max_messages: Optional[int] = None, retry: Optional[LearnRetry] = None) -> None:
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

    retry = retry or LearnRetry(graph)
    handled = 0
    while max_messages is None or handled < max_messages:
        if retry.pending and not early:
            assigned = consumer.assignment()
            consumer.pause(assigned)
            summaries = retry.due(between=keep_alive)
            consumer.resume(assigned)
            for summary in summaries:
                print(describe(summary), flush=True)
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
                retry.note(summary.get("failure_id"))
                print(f"[agent] decision {message.get('decision')} by {message.get('engineer')} "
                      f"on {message.get('escalation_id')}: {summary['delivery']}")
                print(describe(summary), flush=True)
            else:
                summaries = process_lookup(graph, message, between=keep_alive)
                consumer.resume(assigned)
                for summary in summaries:
                    retry.note(summary.get("failure_id"))
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
    retry = LearnRetry(graph)
    if retry.scan():
        print(f"[agent] {len(retry.pending)} workflow(s) still need Learn; retrying every "
              f"{retry.interval:g} s", flush=True)
    try:
        run(consumer, graph, retry=retry)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
