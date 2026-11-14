"""Validate without Spark: the checks of agent/validation.py and the node, on a fake replay.

The same steps on real Spark-written tables are in test_validate_spark.py.
"""
import pytest

from agent import audit, config, validation
from agent.nodes.validate import make_validate, validate
from agent.replay import ReplayError, replay_group
from agent.state import initial_state
from agent.tests.helpers import FakeReplay, fixed_row, stack_item

EID = "e1"
IDS = ["r1", "r2", "r3"]


def record(**changes):
    """An audit record as Remediate writes it for the Jule group."""
    base = {"execution_id": EID, "failure_id": "f1", "action": "ADD_VARIANT",
            "action_params": {"canonical_value": "JULY"}, "action_source": "DIAGNOSIS", "status": "SUCCESS",
            "reasons": [], "target_table": "silver2_crash1", "column_name": "crash_month", "raw_value": "Jule",
            "overlay": {"crash_month": {"JULE": "JULY"}}, "rows_expected": 3, "rows_affected": 3,
            "rows_with_other_failures": 0, "record_ids": IDS, "quarantine_version": 2}
    return {**base, **changes}


def state():
    s = initial_state("L1", stack_item(rows=3))
    s["execution"] = {"status": "SUCCESS", "execution_id": EID, "target_table": "silver2_crash1"}
    return s


def evaluate(rows, staged=IDS, **report):
    rep = {"rows": rows, "canonical_values": ["JUNE", "JULY"], "existing_mapping": None, "in_silver2": [],
           "released_before": False}
    rep.update(report)
    return validation.evaluate(execution_id=EID, column="crash_month", raw_value="Jule", canonical="JULY",
                               overlay={"crash_month": {"JULE": "JULY"}}, staged_ids=list(staged), replay=rep)


def failed(result):
    return [c["name"] for c in result["checks"] if not c["passed"]]


# --- the checks (VD3) ------------------------------------------------------------------
def test_every_record_fixed_passes_and_all_of_them_are_released():
    result = evaluate([fixed_row(r, crash_date="2019-07-04") for r in IDS])
    assert result["passed"] is True and result["reasons"] == []
    assert [c["name"] for c in result["checks"]] == ["resolution", "no_conflicting_mapping", "rows_conserved",
                                                    "targeted_resolved", "no_new_failures",
                                                    "agrees_with_crash_date"]
    assert result["to_release"] == IDS and (result["rows_valid"], result["rows_remaining"]) == (3, 0)
    assert result["crash_date"]["verdict"] == "AGREES" and result["crash_date"]["confirming"] == 3


def test_a_record_that_still_fails_another_column_stays_quarantined():
    rows = [fixed_row("r1"), fixed_row("r2"),
            fixed_row("r3", before=["crash_month", "damage"], after={"damage": "$1489"})]
    result = evaluate(rows)
    assert result["passed"] is True
    assert result["to_release"] == ["r1", "r2"] and result["rows_remaining"] == 1
    assert "3 staged = 2 valid + 1 still quarantined" in result["checks"][2]["detail"]


def test_one_record_whose_crash_date_rules_the_value_out_fails_the_whole_group():
    """Agreement across every row, not a sample: two July records do not outvote a June one."""
    rows = [fixed_row("r1", crash_date="2019-07-04"), fixed_row("r2", crash_date="2019-07-21"),
            fixed_row("r3", crash_date="2019-06-12")]
    result = evaluate(rows)
    assert result["passed"] is False and failed(result) == ["agrees_with_crash_date"]
    assert result["reasons"] == ["agrees_with_crash_date: 1 of 3 records have a crash_date that rules out JULY: "
                                 "r3 (crash_date 2019-06-12 gives JUNE)"]
    assert result["to_release"] == []


def test_day_of_week_is_checked_against_the_weekday_of_crash_date():
    rows = [fixed_row(r, column="crash_day_of_week", value="THURSDAY", crash_date="2019-07-04") for r in IDS]
    result = validation.evaluate(
        execution_id=EID, column="crash_day_of_week", raw_value="Thurs", canonical="THURSDAY",
        overlay={"crash_day_of_week": {"THURS": "THURSDAY"}}, staged_ids=IDS,
        replay={"rows": rows, "canonical_values": ["THURSDAY"], "existing_mapping": None})
    assert result["passed"] is True and result["crash_date"]["verdict"] == "AGREES"
    rows[0]["crash_date"] = "2019-07-05"   # a Friday
    assert failed(validation.evaluate(
        execution_id=EID, column="crash_day_of_week", raw_value="Thurs", canonical="THURSDAY",
        overlay={"crash_day_of_week": {"THURS": "THURSDAY"}}, staged_ids=IDS,
        replay={"rows": rows, "canonical_values": ["THURSDAY"], "existing_mapping": None})) \
        == ["agrees_with_crash_date"]


def test_records_without_a_readable_crash_date_decide_nothing():
    result = evaluate([fixed_row(r, crash_date=None) for r in IDS])
    assert result["passed"] is True and result["crash_date"]["verdict"] == "NONE"


def test_a_column_that_no_date_decides_has_no_crash_date_check():
    rows = [fixed_row(r, column="weather_condition", value="CLOUDY") for r in IDS]
    result = validation.evaluate(execution_id=EID, column="weather_condition", raw_value="Misty dark",
                                 canonical="CLOUDY", overlay={"weather_condition": {"MISTY DARK": "CLOUDY"}},
                                 staged_ids=IDS, replay={"rows": rows, "canonical_values": ["CLOUDY"]})
    assert result["passed"] is True
    assert "agrees_with_crash_date" not in [c["name"] for c in result["checks"]]


def test_targeted_column_still_failing_in_one_record_fails():
    rows = [fixed_row("r1"), fixed_row("r2"), fixed_row("r3", value=None, after={"crash_month": "Jule"})]
    result = evaluate(rows)
    assert failed(result) == ["targeted_resolved"]
    assert "crash_month still fails in 1 of 3 records: r3" in result["reasons"][0]


def test_targeted_column_resolving_to_another_value_fails():
    """The overlay should win; if the table or the fuzzy match gave something else, it did not."""
    result = evaluate([fixed_row("r1"), fixed_row("r2"), fixed_row("r3", value="JUNE")])
    assert failed(result) == ["targeted_resolved"]
    assert "other than JULY in 1 records (e.g. r3: 'JUNE')" in result["reasons"][0]


def test_a_new_failure_in_another_column_fails():
    rows = [fixed_row("r1"), fixed_row("r2"), fixed_row("r3", after={"crash_date": "31/02/2019"})]
    result = evaluate(rows)
    assert failed(result) == ["no_new_failures"]
    assert "(e.g. r3: crash_date)" in result["reasons"][0]


def test_a_value_outside_the_canonical_set_fails():
    result = evaluate([fixed_row(r) for r in IDS], canonical_values=["JUNE"])
    assert failed(result) == ["resolution"]


def test_an_overlay_that_is_not_the_approved_fix_fails():
    result = validation.evaluate(execution_id=EID, column="crash_month", raw_value="Jule", canonical="JULY",
                                 overlay={"crash_month": {"JULE": "JULY", "JU": "JULY"}}, staged_ids=IDS,
                                 replay={"rows": [fixed_row(r) for r in IDS], "canonical_values": ["JULY"]})
    assert failed(result) == ["resolution"]


def test_an_existing_mapping_to_another_value_is_a_conflict():
    result = evaluate([fixed_row(r) for r in IDS], existing_mapping="JUNE")
    assert failed(result) == ["no_conflicting_mapping"]
    assert evaluate([fixed_row(r) for r in IDS], existing_mapping="JULY")["passed"] is True


@pytest.mark.parametrize("rows, message", [
    ([fixed_row("r1"), fixed_row("r2")], "1 staged record(s) are not in the quarantine: r3"),
    ([fixed_row("r1"), fixed_row("r2"), fixed_row("r3"), fixed_row("r3")],
     "1 record(s) are in the quarantine more than once: r3"),
    ([fixed_row("r1"), fixed_row("r2"), fixed_row("r3"), fixed_row("x9")],
     "the replay returned 1 record(s) that were not staged: x9"),
    ([fixed_row("r1"), fixed_row("r2"), fixed_row("r3", status="RESOLVED", resolved_by="other")],
     "1 staged record(s) were resolved by something else in the meantime: r3"),
])
def test_rows_must_be_conserved(rows, message):
    result = evaluate(rows)
    assert "rows_conserved" in failed(result)
    assert message in result["reasons"][0]


def test_a_staged_record_already_in_silver2_would_be_duplicated():
    result = evaluate([fixed_row(r) for r in IDS], in_silver2=["r2"])
    assert failed(result) == ["rows_conserved"]
    assert "1 staged record(s) are already in Silver 2: r2" in result["reasons"][0]


def test_records_this_execution_released_before_are_counted_not_written_again():
    rows = [fixed_row("r1", status="RESOLVED", resolved_by=EID), fixed_row("r2", status="RESOLVED", resolved_by=EID),
            fixed_row("r3")]   # stopped between the Silver 2 append and the quarantine update for r3
    result = evaluate(rows, in_silver2=IDS, released_before=True)
    assert result["passed"] is True
    assert result["already_released"] == ["r1", "r2"] and result["to_release"] == ["r3"]


# --- the node --------------------------------------------------------------------------
@pytest.fixture
def audited():
    audit.write(record(), config.REMEDIATION_AUDIT_PATH)


def node_with(**fake):
    made = []

    def fake_replay(target_table, record_ids, column, overlay):
        made.append(FakeReplay(target_table, record_ids, column, overlay, **fake))
        return made[-1]

    return make_validate(replay=fake_replay), made


def test_passed_validation_releases_the_fixed_records(audited):
    node, made = node_with()
    out = node(state())
    result = out["validation"]
    assert result["status"] == "PASSED" and result["passed"] is True
    assert (result["rows_staged"], result["rows_released"], result["target_table"]) == (3, 3, "silver2_crash1")
    assert made[0].released == [(EID, IDS)] and made[0].closed is True
    assert out["trace"][0]["note"] == "passed: crash_month resolves to JULY in all 3 record(s); 3 released to silver2_crash1"
    assert set(out) == {"validation", "trace"}     # no `stubbed`: the step is real


def test_failed_validation_releases_nothing(audited):
    node, made = node_with(rows=[fixed_row("r1"), fixed_row("r2"), fixed_row("r3", value="JUNE")])
    out = node(state())
    assert out["validation"]["status"] == "FAILED" and out["validation"]["passed"] is False
    assert made[0].released == [] and made[0].closed is True
    assert out["trace"][0]["note"].startswith("failed: targeted_resolved:")


def test_the_whole_group_comes_from_the_audit_record_not_the_state_sample(audited):
    node, made = node_with()
    s = state()
    s["execution"]["sample_record_ids"] = ["r1"]
    node(s)
    assert made[0].rows and [r["record_id"] for r in made[0].rows] == IDS


def test_no_audit_record_means_nothing_to_validate():
    out = validate(state())["validation"]
    assert out["passed"] is False and "no successful execution e1 in the audit table" in out["reasons"][0]


def test_an_action_without_a_validation_goes_to_an_engineer():
    audit.write(record(action="RENAME_COLUMN", action_params={"renames": {"A": "b"}}), config.REMEDIATION_AUDIT_PATH)
    out = validate(state())["validation"]
    assert out["passed"] is False and "Validate has no check for 'RENAME_COLUMN'" in out["reasons"][0]


def test_a_request_the_replay_refuses_is_a_failed_validation():
    audit.write(record(target_table="silver2_quarantine; DROP"), config.REMEDIATION_AUDIT_PATH)

    def refuse(*args):
        raise ReplayError("'silver2_quarantine; DROP' is not a per-source Silver 2 table (silver2_<source>)")

    out = make_validate(replay=refuse)(state())["validation"]
    assert out["passed"] is False and "is not a per-source Silver 2 table" in out["reasons"][0]


def test_spark_failing_raises_so_the_graph_retries_the_step(audited):
    def broken(*args):
        raise RuntimeError("Java gateway exited")

    with pytest.raises(RuntimeError):
        make_validate(replay=broken)(state())


@pytest.mark.parametrize("table, column, overlay, message", [
    ("silver2_quarantine", "crash_month", {"crash_month": {"JULE": "JULY"}}, "not a per-source Silver 2 table"),
    ("../gold", "crash_month", {"crash_month": {"JULE": "JULY"}}, "not a per-source Silver 2 table"),
    ("silver2_crash1", "crash_month`; DROP", {}, "not a column Silver 2 standardizes"),
    ("silver2_crash1", "crash_month", {"damage": {"X": "Y"}}, "is not one value of crash_month"),
])
def test_the_replay_only_runs_on_a_silver2_table_and_a_standardized_column(table, column, overlay, message):
    with pytest.raises(ReplayError, match=message):
        replay_group(table, ["r1"], column, overlay)   # the real one: it refuses before starting Spark
