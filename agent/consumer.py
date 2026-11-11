"""Recovery agent: Kafka `recovery.lookup` in, one LangGraph workflow per failure.

  recovery.lookup (lookup/consumer.py) --> for each stack item, top first --> graph (agent/graph.py)

Each workflow is checkpointed under thread_id = failure_id, which gives three
behaviours for a stack item:

  new        no saved state: start the workflow
  resumed    saved state that stopped part-way (the agent crashed): continue from
             the last finished node
  duplicate  saved state that already ended: skip it (Kafka delivers at least once)

The offset is committed only after every item of the message was handled. If a
node raises, the agent exits without committing; Compose restarts it, the message
is redelivered, finished items are skipped and the failed one resumes.
"""
import json
from typing import List, Optional

from agent import config
from agent.graph import build_graph, open_checkpointer
from agent.state import initial_state

NEW, RESUMED, DUPLICATE, MALFORMED = "new", "resumed", "duplicate", "malformed"


def run_workflow(graph, lookup_id: Optional[str], item: dict) -> dict:
    """Start, resume or skip the workflow of one stack item. Returns a summary for the log."""
    failure = item.get("failure") if isinstance(item, dict) else None
    failure_id = failure.get("failure_id") if isinstance(failure, dict) else None
    if not failure_id:
        return {"delivery": MALFORMED, "failure_id": None, "state": {}}

    thread = {"configurable": {"thread_id": failure_id}}
    saved = graph.get_state(thread)
    if saved.values and not saved.next:
        return {"delivery": DUPLICATE, "failure_id": failure_id, "state": saved.values}
    if saved.values:
        return {"delivery": RESUMED, "failure_id": failure_id, "state": graph.invoke(None, thread)}
    state = graph.invoke(initial_state(lookup_id, item), thread)
    return {"delivery": NEW, "failure_id": failure_id, "state": state}


def process_lookup(graph, message: dict) -> List[dict]:
    """Work through one recovery.lookup message from the top of its stack."""
    stack = message.get("stack")
    if not isinstance(stack, list):
        return []
    return [run_workflow(graph, message.get("lookup_id"), item) for item in stack]


def describe(summary: dict) -> str:
    state = summary["state"]
    if summary["delivery"] == MALFORMED:
        return "[agent] Skipping stack item without failure.failure_id"
    failure = state.get("failure") or {}
    what = failure.get("column_name") or failure.get("failure_type")
    path = " > ".join(entry["node"] for entry in state.get("trace", []))
    return (f"[agent] {summary['failure_id']} {summary['delivery']:<9} "
            f"{(state.get('priority') or {}).get('tier') or '-'} "
            f"{failure.get('folder') or '-'}:{what} value={failure.get('signature_value')!r} "
            f"-> {state.get('status')} ({path})")


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
    """Poll loop. `max_messages` stops it after that many messages (tests)."""
    from confluent_kafka import KafkaError

    handled = 0
    while max_messages is None or handled < max_messages:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            # The topic does not exist until the lookup service publishes its first stack.
            if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"[agent] Kafka error: {msg.error()}")
            continue
        message = parse(msg)
        if message is not None:
            summaries = process_lookup(graph, message)
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
