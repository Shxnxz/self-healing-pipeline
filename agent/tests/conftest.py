"""No test may reach a real model or the pipeline's Delta tables by accident."""
import pytest

from agent import llm, reference


@pytest.fixture(autouse=True)
def offline(monkeypatch, request):
    if "real_io" in request.keywords:  # tests of llm.py / reference.py themselves
        return

    def no_model(system, user, schema):
        raise llm.LLMError(llm.UNAVAILABLE, "no model in tests")

    monkeypatch.setattr(llm, "chat_json", no_model)
    monkeypatch.setattr(reference, "canonical_values", lambda column, path=None: [])
    monkeypatch.setattr(reference, "sample_records", lambda failure, limit=None, path=None: [])


def pytest_configure(config):
    config.addinivalue_line("markers", "real_io: the test exercises llm.py or reference.py itself")
