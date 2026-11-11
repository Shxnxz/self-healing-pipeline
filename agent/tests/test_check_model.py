"""The one-shot model check: python -m pytest agent/tests   (the model is a fake)"""
from agent import check_model, llm
from agent.tests.helpers import FakeModel


def test_reports_ok_when_the_model_answers_in_the_asked_form(monkeypatch, capsys):
    model = FakeModel({"hypothesis": "All three records are dated in June.", "action": "ADD_VARIANT",
                       "canonical_value": "JUNE", "confidence": 0.95})
    monkeypatch.setattr(llm, "chat_json", model)
    assert check_model.main() == 0
    out = capsys.readouterr().out
    assert "policy gate: PASSED" in out and "OK in" in out and out.rstrip().endswith("asked form.")
    assert '"canonical_value": "JUNE"' in out
    assert "2024-06-22" in model.calls[0]["user"]     # the canned records reached the model


def test_a_wrong_but_well_formed_answer_is_still_a_working_model(monkeypatch, capsys):
    monkeypatch.setattr(llm, "chat_json", FakeModel({"hypothesis": "Looks like July.", "action": "ADD_VARIANT",
                                                     "canonical_value": "JULY", "confidence": 0.97}))
    assert check_model.main() == 0
    out = capsys.readouterr().out
    assert "policy gate: REJECTED" in out and "rule out JULY" in out and "did not choose JUNE" in out


def test_fails_when_there_is_no_usable_answer(capsys):
    assert check_model.main() == 1                     # conftest: no model in tests
    out = capsys.readouterr().out
    assert "FAILED" in out and "UNAVAILABLE" in out and "policy gate: NO_DIAGNOSIS" in out
