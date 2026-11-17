"""The fault injector only sends faults into the evaluation stack (no Spark needed)."""
import pytest

from tools import fault_injector
from tools.faults import FaultSpecError


class NoSpark:
    def __getattr__(self, name):
        raise AssertionError("the guard must refuse before touching Spark")


@pytest.mark.parametrize("stack", [None, "main", "production", ""])
def test_inject_refuses_outside_the_evaluation_stack(monkeypatch, stack):
    if stack is None:
        monkeypatch.delenv("PIPELINE_STACK", raising=False)
    else:
        monkeypatch.setenv("PIPELINE_STACK", stack)
    with pytest.raises(FaultSpecError, match="evaluation stack"):
        fault_injector.inject(NoSpark(), {"name": "jule", "faults": []}, run_id="x1")


def test_cli_refuses_before_starting_spark(monkeypatch, capsys):
    monkeypatch.setenv("PIPELINE_STACK", "main")
    monkeypatch.setattr(fault_injector.session, "spark",
                        lambda *a, **k: (_ for _ in ()).throw(AssertionError("Spark started")))
    assert fault_injector.main(["inject", "jule"]) == 2
    assert "docker-compose.eval.yml" in capsys.readouterr().err


def test_evaluation_stack_passes_the_guard(monkeypatch):
    monkeypatch.setenv("PIPELINE_STACK", "Evaluation")
    fault_injector.require_evaluation_stack()
