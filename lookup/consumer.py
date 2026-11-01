"""Deterministic Lookup service: Kafka in, ranked stack out.

  recovery.incidents  (Qwen handoff, qwen_classifier/handoff.py)  --+
                                                                     +--> window --> lookup --> recovery.lookup
  recovery.failures   (quarantine groups, scanner.py)            --+

Failures that arrive within LOOKUP_WINDOW_SECONDS are ranked together and
published as one message (see stack.py for its shape). Offsets are committed only
after the output message is acknowledged, so a crash replays the window
(at-least-once); downstream consumers deduplicate on failure.failure_id.
"""
import json
import time

from confluent_kafka import Consumer, KafkaError, Producer
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, SessionExpired, TransientError

import config
from embedding import make_embedder
from failures import StructuredFailure, from_qwen_incident
from stack import run_lookup

MAX_BACKOFF_SECONDS = 30


def parse(msg) -> StructuredFailure | None:
    try:
        data = json.loads(msg.value())
    except (TypeError, ValueError):
        print(f"[lookup] Skipping unparseable message {msg.topic()}@{msg.offset()}")
        return None
    try:
        if msg.topic() == config.QWEN_TOPIC:
            return from_qwen_incident(data)
        return StructuredFailure.from_dict(data)
    except (TypeError, ValueError, KeyError) as exc:
        print(f"[lookup] Skipping malformed failure {msg.topic()}@{msg.offset()}: {exc}")
        return None


def lookup_with_retry(driver, failures, embed):
    delay = 1
    while True:
        try:
            return run_lookup(driver, failures, embed)
        except (ServiceUnavailable, SessionExpired, TransientError) as exc:
            print(f"[lookup] Neo4j unavailable ({type(exc).__name__}); retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, MAX_BACKOFF_SECONDS)


def publish(producer, result: dict) -> None:
    delivered = []
    producer.produce(
        config.OUTPUT_TOPIC,
        key=result["lookup_id"].encode(),
        value=json.dumps(result, ensure_ascii=False, default=str).encode(),
        on_delivery=lambda err, _msg: delivered.append(err),
    )
    producer.flush(30)
    if not delivered or delivered[0] is not None:
        raise RuntimeError(f"[lookup] Output delivery failed: {delivered}")


def describe(result: dict) -> str:
    lines = [f"[lookup] {result['summary']}"]
    for it in result["stack"][:10]:
        f = it["failure"]
        what = f["column_name"] or f["failure_type"]
        lines.append(
            f"  #{it['rank']} {it['priority']['tier']} {it['priority']['score']:5.1f} "
            f"{it['route']:<9} {f['source']:<10} {f['folder'] or '-'}:{what} "
            f"value={f['signature_value']!r} rows={f['rows_affected']} -- {it['route_reason']}"
        )
    return "\n".join(lines)


def main() -> None:
    # Missing properties (e.g. no incident has `confidence` yet) are expected: no warnings.
    driver = GraphDatabase.driver(config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD),
                                  notifications_min_severity="OFF")
    embed = make_embedder()
    consumer = Consumer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "group.id": config.GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers": config.KAFKA_BOOTSTRAP,
        "enable.idempotence": True, "acks": "all",
    })
    topics = [config.QWEN_TOPIC, config.FAILURE_TOPIC]
    consumer.subscribe(topics)
    print(f"[lookup] Consuming {topics} -> {config.OUTPUT_TOPIC} "
          f"(window {config.WINDOW_SECONDS}s, threshold {config.SIMILARITY_THRESHOLD}, "
          f"embeddings {'on' if embed else 'off'})")

    window, opened = [], None
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error():
                # Topics do not exist until their first producer writes.
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"[lookup] Kafka error: {msg.error()}")
                msg = None
            if msg is not None:
                opened = opened or time.monotonic()
                failure = parse(msg)
                if failure is not None:
                    window.append(failure)

            full = len(window) >= config.MAX_WINDOW_SIZE
            due = opened is not None and time.monotonic() - opened >= config.WINDOW_SECONDS
            if not (full or due):
                continue
            if window:
                result = lookup_with_retry(driver, window, embed)
                publish(producer, result)
                print(describe(result), flush=True)
            consumer.commit(asynchronous=False)
            window, opened = [], None
    finally:
        consumer.close()
        driver.close()


if __name__ == "__main__":
    main()
