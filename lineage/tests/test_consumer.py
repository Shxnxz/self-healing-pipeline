"""lineage/consumer.py when Neo4j is down, refuses us, or rejects one event.

Run from the lineage/ directory:  python -m pytest tests

The first tests use a fake Kafka consumer and a fake write, so they need neither
Kafka nor Neo4j. The tests at the end use the real neo4j driver: one against a port
where nothing listens (always runs), and, when NEO4J_TEST_URI points at a Neo4j you
started, one where Neo4j comes back while the consumer is waiting, and one with a
wrong password:

    docker run -d --name neo4j-test -p 7687:7687 -e NEO4J_AUTH=neo4j/selfhealing neo4j:5.26-community
    NEO4J_TEST_URI=bolt://localhost:7687 python -m pytest tests/test_consumer.py
"""
import json
import os
import socket
import sys
import threading
from pathlib import Path
from urllib.parse import urlparse

import pytest

pytest.importorskip("confluent_kafka")
neo4j = pytest.importorskip("neo4j")

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import consumer  # noqa: E402
from normalize import normalize  # noqa: E402
from neo4j.exceptions import (  # noqa: E402
    AuthError,
    ClientError,
    CypherSyntaxError,
    ServiceUnavailable,
    SessionExpired,
    TransientError,
)

FIXTURE = Path(__file__).parent / "fixtures" / "spark_events.jsonl"
NEO4J_TEST_URI = os.environ.get("NEO4J_TEST_URI")
NEO4J_TEST_USER = os.environ.get("NEO4J_TEST_USER", "neo4j")
NEO4J_TEST_PASSWORD = os.environ.get("NEO4J_TEST_PASSWORD", "selfhealing")


def raw_events():
    return [line for line in FIXTURE.read_text(encoding="utf-8").splitlines() if line]


def pipeline_events():
    """Raw events that normalize() keeps, i.e. the ones the consumer writes."""
    return [raw for raw in raw_events() if normalize(json.loads(raw)) is not None]


class CodedClientError(ClientError):
    """A ClientError with a server status code, as the driver raises them."""

    def __init__(self, code):
        super().__init__(f"{code}: refused")
        self._test_code = code

    @property
    def code(self):
        return self._test_code


def security_error(code):
    return CodedClientError(code)


class Stop(Exception):
    """Ends consumer.main()'s endless poll loop once the fake topic is read."""


class FakeKafkaError:
    def __init__(self, code):
        self._code = code

    def code(self):
        return self._code

    def __str__(self):
        return f"KafkaError({self._code})"


class FakeMessage:
    def __init__(self, offset, value=None, error=None):
        self._offset, self._value, self._error = offset, value, error

    def offset(self):
        return self._offset

    def value(self):
        return self._value

    def error(self):
        return self._error


class FakeConsumer:
    """Plays a list of messages, then raises Stop. Records what was committed."""

    def __init__(self, messages, log):
        self.messages, self.log, self.closed = list(messages), log, False
        self.config = None

    def __call__(self, config):  # used as the Consumer class
        self.config = config
        return self

    def subscribe(self, topics):
        self.topics = topics

    def poll(self, timeout):
        if not self.messages:
            raise Stop
        return self.messages.pop(0)

    def commit(self, message, asynchronous):
        assert asynchronous is False
        self.log.append(("commit", message.offset()))

    def close(self):
        self.closed = True


class FakeDriver:
    closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def no_sleep(monkeypatch):
    delays = []
    monkeypatch.setattr(consumer.time, "sleep", delays.append)
    return delays


def flaky_write(log, failures):
    """write_update that raises the given exceptions first, then succeeds."""
    failures = list(failures)

    def write(driver, update):
        if failures:
            exc = failures.pop(0)
            log.append(("failed", type(exc).__name__))
            raise exc
        log.append(("written", update.job_name))

    return write


def run_main(monkeypatch, messages, write):
    log = []
    fake = FakeConsumer(messages, log)
    driver = FakeDriver()
    monkeypatch.setattr(consumer, "Consumer", fake)
    monkeypatch.setattr(consumer.GraphDatabase, "driver", lambda *a, **k: driver)
    monkeypatch.setattr(consumer, "write_update", write(log) if callable(write) else write)
    with pytest.raises(Stop):
        consumer.main()
    return log, fake, driver


# --- what counts as "Neo4j is not there" ------------------------------------------


@pytest.mark.parametrize("exc", [
    ServiceUnavailable("down"),
    SessionExpired("connection lost mid-transaction"),
    TransientError("deadlock"),
    security_error("Neo.ClientError.Security.Unauthorized"),
    security_error("Neo.ClientError.Security.AuthenticationRateLimit"),
])
def test_down_busy_or_refusing_neo4j_is_waited_for(exc):
    assert consumer._neo4j_unavailable(exc)


@pytest.mark.parametrize("exc", [
    security_error("Neo.ClientError.Statement.SyntaxError"),
    CypherSyntaxError("bad query"),
    ValueError("not a neo4j error"),
])
def test_a_problem_with_the_event_is_not_waited_for(exc):
    assert not consumer._neo4j_unavailable(exc)


# --- the retry loop -------------------------------------------------------------


def test_write_is_retried_until_neo4j_is_back(monkeypatch, no_sleep):
    log = []
    monkeypatch.setattr(consumer, "write_update", flaky_write(log, [ServiceUnavailable("down")] * 3))
    consumer._write_with_retry(FakeDriver(), normalize(json.loads(pipeline_events()[0])))
    assert [entry[0] for entry in log] == ["failed", "failed", "failed", "written"]
    assert no_sleep == [1, 2, 4]


def test_backoff_doubles_and_stops_growing_at_the_cap(monkeypatch, no_sleep):
    log = []
    monkeypatch.setattr(consumer, "write_update", flaky_write(log, [TransientError("busy")] * 8))
    consumer._write_with_retry(FakeDriver(), normalize(json.loads(pipeline_events()[0])))
    assert no_sleep == [1, 2, 4, 8, 16, 30, 30, 30]
    assert max(no_sleep) == consumer.MAX_BACKOFF_SECONDS


def test_an_error_in_the_event_is_raised_at_once(monkeypatch, no_sleep):
    log = []
    monkeypatch.setattr(consumer, "write_update", flaky_write(log, [CypherSyntaxError("bad")]))
    with pytest.raises(CypherSyntaxError):
        consumer._write_with_retry(FakeDriver(), normalize(json.loads(pipeline_events()[0])))
    assert no_sleep == []


# --- the poll loop: offsets are committed only after Neo4j took the event ---------


def test_offset_is_committed_only_after_neo4j_accepted_the_event(monkeypatch, no_sleep):
    events = pipeline_events()[:2]
    messages = [FakeMessage(i, value) for i, value in enumerate(events)]
    log, fake, driver = run_main(
        monkeypatch, messages,
        lambda log: flaky_write(log, [ServiceUnavailable("down"), ServiceUnavailable("down")]))

    # While Neo4j is down nothing is committed: a restart would read the event again.
    assert log[:4] == [("failed", "ServiceUnavailable"), ("failed", "ServiceUnavailable"),
                       ("written", log[2][1]), ("commit", 0)]
    assert log[4:] == [("written", log[4][1]), ("commit", 1)]
    assert no_sleep == [1, 2]
    assert fake.config["enable.auto.commit"] is False
    assert fake.closed and driver.closed


def test_wrong_password_waits_instead_of_skipping_every_event(monkeypatch, no_sleep):
    """Before Task 110 an AuthError counted as a bad event: all lineage was dropped."""
    messages = [FakeMessage(0, pipeline_events()[0])]
    log, _, _ = run_main(
        monkeypatch, messages,
        lambda log: flaky_write(log, [security_error("Neo.ClientError.Security.Unauthorized")] * 2))
    assert [entry[0] for entry in log] == ["failed", "failed", "written", "commit"]


def test_an_event_neo4j_rejects_is_skipped_and_the_stream_goes_on(monkeypatch, no_sleep):
    events = pipeline_events()[:2]
    messages = [FakeMessage(i, value) for i, value in enumerate(events)]
    log, _, _ = run_main(monkeypatch, messages,
                         lambda log: flaky_write(log, [CypherSyntaxError("bad event")]))
    assert log == [("failed", "CypherSyntaxError"), ("commit", 0),
                   ("written", log[2][1]), ("commit", 1)]
    assert no_sleep == []


def test_unparseable_and_irrelevant_events_are_committed_without_a_write(monkeypatch, no_sleep):
    irrelevant = next(raw for raw in raw_events() if normalize(json.loads(raw)) is None)
    messages = [FakeMessage(0, b"{not json"), FakeMessage(1, irrelevant)]
    log, _, _ = run_main(monkeypatch, messages, lambda log: flaky_write(log, []))
    assert log == [("commit", 0), ("commit", 1)]


def test_missing_topic_is_not_an_error(monkeypatch, no_sleep):
    from confluent_kafka import KafkaError

    messages = [None, FakeMessage(0, error=FakeKafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART)),
                FakeMessage(1, pipeline_events()[0])]
    log, _, _ = run_main(monkeypatch, messages, lambda log: flaky_write(log, []))
    assert log == [("written", log[0][1]), ("commit", 1)]


# --- with the real neo4j driver ----------------------------------------------------


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def real_driver(uri, password=NEO4J_TEST_PASSWORD):
    # The driver retries a failed transaction itself for 30 s by default; the
    # consumer's own loop is what is tested here, so that is turned off.
    return neo4j.GraphDatabase.driver(uri, auth=(NEO4J_TEST_USER, password),
                                      max_transaction_retry_time=0)


def test_real_driver_with_neo4j_down_raises_what_the_consumer_waits_for():
    from graph import write_update

    driver = real_driver(f"bolt://127.0.0.1:{free_port()}")
    try:
        with pytest.raises(Exception) as caught:
            write_update(driver, normalize(json.loads(pipeline_events()[0])))
    finally:
        driver.close()
    assert isinstance(caught.value, ServiceUnavailable)
    assert consumer._neo4j_unavailable(caught.value)


class Forwarder:
    """A TCP port that is closed until start(), then forwards to Neo4j's bolt port.

    Stands for "Neo4j is down, then comes back" without stopping a container.
    """

    def __init__(self, target_host, target_port):
        self.port = free_port()
        self.target = (target_host, target_port)
        self.server = None

    def start(self):
        self.server = socket.create_server(("127.0.0.1", self.port))
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                client, _ = self.server.accept()
            except OSError:
                return
            upstream = socket.create_connection(self.target)
            for a, b in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    @staticmethod
    def _pipe(src, dst):
        try:
            while data := src.recv(65536):
                dst.sendall(data)
        except OSError:
            pass
        finally:
            for s in (src, dst):
                try:
                    s.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass

    def stop(self):
        if self.server:
            self.server.close()


needs_neo4j = pytest.mark.skipif(not NEO4J_TEST_URI, reason="NEO4J_TEST_URI not set")


@needs_neo4j
def test_consumer_waits_for_neo4j_to_come_back_and_then_writes(monkeypatch):
    target = urlparse(NEO4J_TEST_URI)
    forwarder = Forwarder(target.hostname, target.port or 7687)
    delays = []

    def sleep(seconds):  # Neo4j "comes back" after the consumer waited twice
        delays.append(seconds)
        if len(delays) == 2:
            forwarder.start()

    monkeypatch.setattr(consumer.time, "sleep", sleep)
    update = normalize(json.loads(pipeline_events()[0]))
    driver = real_driver(f"bolt://127.0.0.1:{forwarder.port}")
    try:
        consumer._write_with_retry(driver, update)
    finally:
        driver.close()
        forwarder.stop()
    assert delays == [1, 2]

    with real_driver(NEO4J_TEST_URI) as check:
        records, _, _ = check.execute_query(
            "MATCH (r:Run {run_id: $run_id})-[:RUN_OF]->(j:Job) RETURN j.name AS job",
            run_id=update.run_id)
    assert [r["job"] for r in records] == [update.job_name]


@needs_neo4j
def test_real_wrong_password_is_waited_for():
    from graph import write_update

    driver = real_driver(NEO4J_TEST_URI, password="not-the-password")
    try:
        with pytest.raises(Exception) as caught:
            write_update(driver, normalize(json.loads(pipeline_events()[0])))
    finally:
        driver.close()
    assert isinstance(caught.value, AuthError)
    assert consumer._neo4j_unavailable(caught.value)
