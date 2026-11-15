"""No test may reach a real model or the pipeline's Delta tables by accident."""
import pytest

from agent import config, llm, promotion, reference, replay
from agent.memory import incidents
from agent.tests import helpers


@pytest.fixture(autouse=True)
def temp_tables(monkeypatch, tmp_path):
    """Remediate writes its audit table: keep it, and the quarantine it reads, in the test's own folder."""
    monkeypatch.setattr(config, "REMEDIATION_AUDIT_PATH", str(tmp_path / "remediation_audit"))
    monkeypatch.setattr(config, "QUARANTINE_PATH", str(tmp_path / "silver2_quarantine"))
    monkeypatch.setattr(config, "REF_MAPPINGS_PATH", str(tmp_path / "ref_canonical_mappings"))
    monkeypatch.setattr(config, "DELTA_ROOT", str(tmp_path))   # Validate's Silver 2 tables


@pytest.fixture(autouse=True)
def no_spark(monkeypatch, request):
    """Validate replays through Spark. Unless a test asks for the real thing, the replay
    finds every staged record fixed (helpers.fixed_replay), and no JVM is started."""
    if "spark" not in request.keywords:
        monkeypatch.setattr(replay, "replay_group", helpers.fixed_replay)


@pytest.fixture(autouse=True)
def offline(monkeypatch, request):
    if "real_io" in request.keywords:  # tests of llm.py / reference.py themselves
        return

    def no_model(system, user, schema):
        raise llm.LLMError(llm.UNAVAILABLE, "no model in tests")

    monkeypatch.setattr(llm, "chat_json", no_model)
    monkeypatch.setattr(reference, "canonical_values", helpers.canonical_values)
    monkeypatch.setattr(reference, "read_canonical_values", helpers.canonical_values)
    monkeypatch.setattr(reference, "sample_records", lambda failure, limit=None, path=None: [])


@pytest.fixture(autouse=True)
def memory(monkeypatch, request):
    """Learn writes incidents to Neo4j and promotes into ref_canonical_mappings. Unless a
    test asks for the real thing (`neo4j`, `real_io`), both are one in-memory FakeMemory,
    which a test can take as the `memory` fixture to look at what was stored."""
    fake = helpers.FakeMemory()
    if "neo4j" not in request.keywords:
        for name in ("shared_driver", "write_incident", "signature_history", "mark_promotion"):
            monkeypatch.setattr(incidents, name, getattr(fake, name))
    if "real_io" not in request.keywords:
        monkeypatch.setattr(promotion, "current_mapping", fake.current_mapping)
        monkeypatch.setattr(promotion, "promote", fake.promote)
    return fake


def pytest_configure(config):
    config.addinivalue_line("markers", "real_io: the test exercises llm.py or reference.py itself")
    config.addinivalue_line("markers", "spark: the test replays through real Spark (agent/replay.py)")
    config.addinivalue_line("markers", "neo4j: the test writes to a real Neo4j (NEO4J_TEST_URI)")
