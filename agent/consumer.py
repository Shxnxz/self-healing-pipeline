"""Recovery agent: Kafka `recovery.lookup` in, one LangGraph workflow per failure.

  recovery.lookup (lookup/consumer.py) --> for each stack item, top first --> graph (agent/graph.py)

Each workflow is checkpointed under thread_id = failure_id, which gives four
behaviours for a stack item:

  new        no saved state: start the workflow
  resumed    saved state that stopped part-way (the agent was killed): continue
             from the last finished node
  duplicate  saved state that ended with every step real: skip it (Kafka delivers
             at least once)
  rerun      saved state that ended on a stub step: nothing was really done, so
             forget it and run again. A failure handled before a step was built is
             therefore picked up when it is published again afterwards.

The offset is committed only after every item of the message was handled. A step
that raises does not stop the agent (agent/graph.py: guarded). If the agent itself
dies, the message is redelivered: finished items are skipped, the rest resume.
"""
import json
from typing import Callable, List, Optional

from agent import config
from agent.graph import build_graph, open_checkpointer
from agent.state import initial_state

NEW, RESUMED, DUPLICATE, RERUN, MALFORMED = "new", "resumed", "duplicate", "rerun", "malformed"


def run_workflow(graph, lookup_id: Optional[str], item: dict) -> dict:
    """Start, resume, skip or rerun the workflow of one stack item. Returns a summary for the log."""
    failure = item.get("failure") if isinstance(item, dict) else None
    failure_id = failure.get("failure_id") if isinstance(failure, dict) else None
    if not failure_id:
        return {"delivery": MALFORMED, "failure_id": None, "state": {}}

    thread = {"configurable": {"thread_id": failure_id}}
    saved = graph.get_state(thread)
    delivery = NEW
    if saved.values and saved.next:
        return {"delivery": RESUMED, "failure_id": failure_id, "state": graph.invoke(None, thread)}
    if saved.values and not saved.values.get("stubbed"):
        return {"delivery": DUPLICATE, "failure_id": failure_id, "state": saved.values}
    if saved.values:
        graph.checkpointer.delete_thread(failure_id)
        delivery = RERUN
    state = graph.invoke(initial_state(lookup_id, item), thread)
    return {"delivery": delivery, "failure_id": failure_id, "state": state}


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
        return "[agent] Skipping stack item without failure.failure_id"
    failure = state.get("failure") or {}
    what = failure.get("column_name") or failure.get("failure_type")
    path = " > ".join(entry["node"] for entry in state.get("trace", []))
    stubs = f" [stubs: {', '.join(state['stubbed'])}]" if state.get("stubbed") else ""
    return (f"[agent] {summary['failure_id']} {summary['delivery']:<9} "
            f"{(state.get('priority') or {}).get('tier') or '-'} "
            f"{failure.get('folder') or '-'}:{what} value={failure.get('signature_value')!r} "
            f"-> {state.get('status')} ({path}){stubs}")


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
            summaries = process_lookup(graph, message, between=keep_alive)
            consumer.resume(assigned)
            print(f"[agent] lookup {message.get('lookup_id')}: {len(summaries)} failure(s)")
            for summary in summaries:
                print(describe(summary), flush=True)
        consumer.commit(message=msg, asynchronous=False)
        handled += 1


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
    consumer.subscribe([config.LOOKUP_TOPIC])
    print(f"[agent] Consuming {config.LOOKUP_TOPIC} from {config.KAFKA_BOOTSTRAP} "
          f"(checkpoints {config.CHECKPOINT_PATH})", flush=True)
    try:
        run(consumer, graph)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
