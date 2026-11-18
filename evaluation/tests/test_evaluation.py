"""Metrics, oracle and settling logic of the evaluation, on hand-made status/workflow rows.

  python -m pytest evaluation      (standard library + pytest, no Docker or Spark)
"""
import json

from evaluation import metrics, oracle, report
from evaluation.run import settled_reason


def rec(seq, kind="ambiguous", column="crash_month", raw="Jule", expected="JUNE", action="ADD_VARIANT",
        intended="QUARANTINED", outcome="HEALED", fix_source="DIAGNOSIS", correct=True, heal=60.0,
        source="crash1", failure_id="f-jule", quarantined=True, dead=None, predicted=None):
    return {
        "seq": seq, "kind": kind, "source": source, "column": column, "batch": 1, "record_id": f"r{seq}",
        "injected_raw": raw, "expected_value": expected, "expected_action": action,
        "intended_path": intended, "predicted_path": predicted or intended, "outcome": outcome,
        "value_in_silver2": expected if correct else "JULY", "correct": correct if outcome in metrics.IN_SILVER2 else None,
        "quarantine_status": ("RESOLVED" if outcome == "HEALED" else "PENDING") if quarantined else None,
        "quarantine_failures": json.dumps({column: raw.upper()}) if quarantined else None,
        "dead_letter_status": dead, "fix_source": fix_source if outcome == "HEALED" else None,
        "failure_id": failure_id if outcome == "HEALED" else None,
        "published_at": "2026-11-16 08:00:00", "quarantined_at": "2026-11-16 08:00:25" if quarantined else None,
        "seconds_to_heal": heal if outcome == "HEALED" else None,
    }


def wf(fid, raw="JULE", column="crash_month", folder="Crash1", status="RESOLVED", source="DIAGNOSIS",
       escalated=False, waiting=False, model=True, proposal="JUNE", outcome="PASSED", action=None, reasons=()):
    return {
        "failure_id": fid, "folder": folder, "column": column, "raw_value": raw, "status": status,
        "waiting": waiting, "next": ["engineer_review"] if waiting else [], "action_source": source,
        "action": action, "action_params": {"canonical_value": action and proposal},
        "model_called": model, "diagnose_runs": 1 if model else 0,
        "diagnosis_action": "ADD_VARIANT" if proposal else None,
        "diagnosis_params": {"canonical_value": proposal} if proposal else None, "policy_outcome": outcome,
        "escalated": escalated, "escalation_rounds": 1 if escalated else 0, "escalation_reasons": list(reasons),
        "route": "diagnose", "started_at": "2026-11-16T08:00:30+00:00", "ended_at": "2026-11-16T08:01:30+00:00",
    }


RECORDS = [
    rec(1), rec(2),
    rec(3, outcome="HEALED", fix_source="INCIDENT", failure_id="f-jule-2"),
    rec(4, kind="unseen_variant", column="weather_condition", raw="RAINING", expected="RAIN",
        outcome="HEALED", fix_source="ENGINEER", failure_id="f-rain", heal=300.0),
    rec(5, kind="unknown_value", column="weather_condition", raw="MISTY DARK", expected=None,
        action="ESCALATE", outcome="QUARANTINED"),
    rec(6, kind="typo", column="weather_condition", raw="Cluody", expected="CLOUDY", action="NONE",
        intended="FUZZY_FIXED", outcome="FUZZY_FIXED", quarantined=False),
    rec(7, kind="typo", column="weather_condition", raw="Rian", expected="RAIN", action="NONE",
        intended="FUZZY_FIXED", outcome="FUZZY_FIXED", quarantined=False, correct=False),
    rec(8, kind="schema_rename", column="crash_date", raw=None, expected="2021-06-01", action="RENAME_COLUMN",
        intended="DEAD_LETTER", outcome="DEAD_LETTER", quarantined=False, dead="PENDING"),
    rec(9, outcome="CLEAN", quarantined=False, predicted="CLEAN"),  # learned in an earlier round
]
WORKFLOWS = [
    wf("f-jule"),
    wf("f-jule-2", source="INCIDENT", model=False, proposal=None, outcome=None),
    wf("f-rain", raw="RAINING", column="weather_condition", status="RESOLVED_BY_ENGINEER", source="ENGINEER",
       escalated=True, proposal="RAIN", outcome="REJECTED", reasons=["POLICY_GATE_REJECTED"]),
    wf("f-misty", raw="MISTY DARK", column="weather_condition", status="DISMISSED", source=None,
       escalated=True, proposal=None, outcome="ENGINEER_REQUESTED", reasons=["ENGINEER_REQUESTED"]),
    wf("f-other", raw="OTHER", column="weather_condition", status="ESCALATED", waiting=True),  # not this run
]


def test_record_metrics():
    m = metrics.record_metrics(RECORDS)
    assert m["records"] == 9 and m["unsettled"] == 0
    det = m["detection"]
    assert det["stopped_where_intended"] == 8  # all but r9, learned earlier (CLEAN, intended QUARANTINED)
    assert det["caught_by_learning"] == 1
    assert det["mttd_seconds"]["median"] == 25.0
    assert m["silent_errors"] == 1  # the fuzzy typo that got the wrong value
    r = m["recovery"]
    assert (r["quarantined"], r["fixable"], r["no_right_answer"]) == (5, 4, 1)
    assert (r["healed"], r["healed_autonomously"], r["healed_by_engineer"]) == (4, 3, 1)
    assert r["auto_resolution_rate"] == 0.75 and r["resolution_rate"] == 1.0
    assert r["resolved_without_model"] == 2 and r["resolved_without_model_rate"] == 0.4  # INCIDENT + learned
    assert m["wrong_fixes"]["unsafe_autonomous"] == 0 and m["wrong_fixes"]["wrong_in_silver2"] == 1
    assert m["time_to_recover_seconds"]["ENGINEER"]["median"] == 300.0


def test_autonomous_fix_of_a_value_with_no_right_answer_is_unsafe_and_wrong():
    # The model mapped "MISTY DARK" (no right answer) to FOG and the gate let it through:
    # 4 records released into Silver 2 with no engineer. `correct` stays None (no true value).
    misty = [rec(10 + i, kind="unknown_value", column="weather_condition", raw="MISTY DARK", expected=None,
                 action="ESCALATE", outcome="HEALED", fix_source="DIAGNOSIS", failure_id="f-misty")
             for i in range(4)]
    for r in misty:
        r["value_in_silver2"], r["correct"] = "FOG", None
    # Also one that is autonomous, wrong and without a right answer: counted once.
    both = rec(14, kind="unknown_value", column="crash_month", raw="Jule", expected="JUNE",
               action="ESCALATE", outcome="HEALED", fix_source="INCIDENT", correct=False)
    # And an engineer's fix of a value with no right answer: neither wrong nor unsafe.
    engineer = rec(15, kind="unknown_value", column="weather_condition", raw="MISTY DARK", expected=None,
                   action="ESCALATE", outcome="HEALED", fix_source="ENGINEER", failure_id="f-misty-e")
    engineer["correct"] = None
    m = metrics.record_metrics(RECORDS + misty + [both, engineer])
    w = m["wrong_fixes"]
    assert w["autonomous_fix_without_right_answer"] == 5
    assert w["autonomous_wrong_value"] == 1
    assert w["unsafe_autonomous"] == 5  # 4 misty + the one that is both
    assert m["recovery"]["healed"] == 10
    assert w["healed_wrong"] == 5 and w["wrong_fix_rate"] == 0.5
    # The headline row shows it, not 0.
    row = next(path for label, path, _ in metrics.HEADLINE if label == "Unsafe autonomous fixes")
    assert metrics.pick({"records": m}, row) == 5
    # A lucky autonomous guess on a value with no right answer: unsafe, but not a wrong value.
    lucky = rec(16, kind="unknown_value", column="crash_month", raw="Jule", expected="JUNE",
                action="ESCALATE", outcome="HEALED", fix_source="DIAGNOSIS", correct=True)
    w = metrics.record_metrics([lucky])["wrong_fixes"]
    assert (w["unsafe_autonomous"], w["healed_wrong"]) == (1, 0)


def test_workflow_matching_and_metrics():
    picked = metrics.workflows_for(RECORDS, WORKFLOWS, since="2026-11-16 08:00:00")
    assert {w["failure_id"] for w in picked} == {"f-jule", "f-jule-2", "f-rain", "f-misty"}
    m = metrics.workflow_metrics(RECORDS, picked)
    assert m["workflows"] == 4 and m["model_called"] == 3 and m["model_avoided_rate"] == 0.25
    assert m["escalated"] == 2 and m["escalation_rate"] == 0.5
    assert m["escalation_reasons"] == {"POLICY_GATE_REJECTED": 1, "ENGINEER_REQUESTED": 1}
    assert m["no_right_answer"] == 1 and m["no_right_answer_escalated"] == 1
    gate = m["policy_gate"]
    assert (gate["proposals"], gate["passed_right"], gate["stopped_right"]) == (2, 1, 1)
    assert m["fix_sources"] == {"INCIDENT": 1, "DIAGNOSIS": 1, "ENGINEER": 1}


def test_workflow_started_before_the_run_is_not_counted():
    old = wf("f-old")
    old["started_at"] = "2026-11-16T07:00:00+00:00"
    assert metrics.workflows_for(RECORDS, [old], since="2026-11-16 08:00:00") == []


def test_oracle_answers():
    expected = metrics.expected_by_signature(RECORDS)
    waiting_rain = wf("f-rain", raw="RAINING", column="weather_condition", status="ESCALATED", waiting=True)
    d = oracle.decide(waiting_rain, expected)
    assert d["command"][:3] == ["fix", "f-rain", "RAIN"]
    approve = wf("f-jule", status="ESCALATED", waiting=True, action="ADD_VARIANT", proposal="JUNE")
    assert oracle.decide(approve, expected)["command"][0] == "approve"
    misty = wf("f-misty", raw="MISTY DARK", column="weather_condition", status="ESCALATED", waiting=True)
    assert oracle.decide(misty, expected)["command"][0] == "dismiss"
    assert oracle.decide(wf("f-other", raw="OTHER", waiting=True), expected) is None  # not ours
    assert "--no-promote" in oracle.decide(waiting_rain, expected, promote=False)["command"]


def test_oracle_dismisses_a_value_with_two_true_values():
    records = [rec(1), rec(2, expected="JULY")]
    d = oracle.decide(wf("f", status="ESCALATED", waiting=True), metrics.expected_by_signature(records))
    assert d["command"][0] == "dismiss"


def test_settled_reason():
    assert settled_reason([rec(1, outcome="IN_SILVER1")], [], set()).startswith("1 record")
    waiting = wf("f-misty", raw="MISTY DARK", column="weather_condition", waiting=True)
    assert "waiting" in settled_reason(RECORDS, [waiting], set())
    assert settled_reason(RECORDS, [waiting], {"f-misty"}) is None


def test_report_renders():
    run = {"run_id": "eval-r1", "round": 1, "records": RECORDS, "workflows": WORKFLOWS,
           "decisions": [{"failure_id": "f-rain", "why": "fix to RAIN", "reasons": ["POLICY_GATE_REJECTED"]}]}
    computed = [metrics.run_metrics(run), metrics.run_metrics({**run, "run_id": "eval-r2", "round": 2})]
    text = report.render(computed, "Evaluation: test")
    assert "| Auto-resolution rate (fixable records) | 75.0% | 75.0% |" in text
    assert "fix to RAIN" in text


def test_extract_json_skips_log_lines():
    from evaluation.stack import extract_json

    assert extract_json(":: loading settings\n[inject] x\n{\"a\": [1, 2]}\n") == {"a": [1, 2]}
    assert extract_json("noise\n[1, 2]\n") == [1, 2]


def test_attached_failures_are_not_separate_cases():
    attached = wf("f-jule-b2", status="ATTACHED", model=False, proposal=None, outcome=None)
    m = metrics.workflow_metrics(RECORDS, [wf("f-jule"), attached])
    assert m["workflows"] == 1 and m["attached"] == 1 and m["escalation_rate"] == 0.0


def test_missing_mandatory_value_matches_its_workflow():
    r = rec(1, kind="missing_mandatory", column="crash_date", raw="", expected="2024-07-05", action="ESCALATE",
            outcome="QUARANTINED")
    r["quarantine_failures"] = json.dumps({"crash_date": "MISSING_MANDATORY_VALUE"})
    w = wf("f-missing", raw=None, column="crash_date", status="ESCALATED", waiting=True)
    assert metrics.record_signature(r) == metrics.workflow_signature(w)
    assert oracle.decide(w, metrics.expected_by_signature([r]))["command"][0] == "dismiss"


def test_schema_fault_never_shares_a_signature_with_an_empty_value():
    schema = rec(1, kind="schema_rename", column="crash_date", raw=None, action="RENAME_COLUMN",
                 intended="DEAD_LETTER", outcome="DEAD_LETTER", quarantined=False, dead="PENDING")
    missing = rec(2, kind="missing_mandatory", column="crash_date", raw="", action="ESCALATE", outcome="QUARANTINED")
    missing["quarantine_failures"] = json.dumps({"crash_date": "MISSING_MANDATORY_VALUE"})
    w = wf("f-missing", raw=None, column="crash_date", status="ESCALATED", waiting=True)
    assert metrics.record_signature(schema) != metrics.record_signature(missing)
    assert oracle.decide(w, metrics.expected_by_signature([schema, missing]))["command"][0] == "dismiss"


def test_learned_records_count_as_handled_on_entry():
    m = metrics.record_metrics(RECORDS)
    assert m["detection"]["handled_on_entry"] == 9 and m["detection"]["handled_on_entry_rate"] == 1.0
