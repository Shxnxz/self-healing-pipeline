"""No test may reach a real model or the pipeline's Delta tables by accident."""
import pytest

from agent import config, llm, reference
from agent.tests import helpers


@pytest.fixture(autouse=True)
def temp_tables(monkeypatch, tmp_path):
    """Remediate writes its audit table: keep it, and the quarantine it reads, in the test's own folder."""
    monkeypatch.setattr(config, "REMEDIATION_AUDIT_PATH", str(tmp_path / "remediation_audit"))
    monkeypatch.setattr(config, "QUARANTINE_PATH", str(tmp_path / "silver2_quarantine"))
    monkeypatch.setattr(config, "REF_MAPPINGS_PATH", str(tmp_path / "ref_canonical_mappings"))


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


def pytest_configure(config):
    config.addinivalue_line("markers", "real_io: the test exercises llm.py or reference.py itself")
