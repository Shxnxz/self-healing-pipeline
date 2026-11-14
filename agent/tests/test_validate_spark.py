"""Validate on real tables written by the pipeline's Spark jobs (spark_tables.py).

Real Remediate stages the group, real Validate replays it through Silver 2's own
standardize_silver2 and releases it. Skipped where Spark, Java or data/ is missing.

  python -m pytest agent/tests/test_validate_spark.py
"""
import shutil
from datetime import datetime, timezone

import pytest

from agent import config, reference
from agent.nodes.remediate import remediate
from agent.nodes.validate import validate
from agent.replay import release_app_id
from agent.state import initial_state
from agent.tests import spark_tables
from agent.tests.helpers import canonical_values, stack_item

pytestmark = [pytest.mark.spark,
              pytest.mark.skipif(bool(spark_tables.available()), reason=spark_tables.available() or "ok")]


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """The pipeline's tables, built once; every test gets its own copy."""
    root = tmp_path_factory.mktemp("pipeline")
    session = spark_tables.build(str(root))
    yield root / "delta", session


@pytest.fixture
def tables(built, tmp_path, monkeypatch):
    template, session = built
    delta = tmp_path / "delta"
    shutil.copytree(template, delta)
    monkeypatch.setattr(config, "DELTA_ROOT", str(delta))
    monkeypatch.setattr(config, "QUARANTINE_PATH", str(delta / "silver2_quarantine"))
    monkeypatch.setattr(config, "REF_MAPPINGS_PATH", str(delta / "ref_canonical_mappings"))
    monkeypatch.setattr(config, "REMEDIATION_AUDIT_PATH", str(delta / "remediation_audit"))
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)
    return delta, session


def staged(folder="Crash1", rows=4, canonical="JULY", failure_id="f-crash1"):
    """Run the real Remediate for the Jule group of `folder` and return the state for Validate."""
    item = stack_item(failure_id=failure_id, rows=rows)
    item["failure"].update(folder=folder, batch_id="0", evidence={"quarantine_batches": ["0"]})
    state = initial_state("L1", item)
    state["action"] = {"action": "ADD_VARIANT", "action_params": {"canonical_value": canonical},
                       "source": "DIAGNOSIS", "incident_id": None}
    state.update(remediate(state))
    assert state["execution"]["status"] == "SUCCESS", state["execution"]["reasons"]
    return state


def read(session, path):
    return {row["record_id"]: row.asDict() for row in session.read.format("delta").load(str(path)).collect()}


def test_the_fix_holds_and_the_fixed_records_are_released(tables):
    delta, session = tables
    columns_before = session.read.format("delta").load(str(delta / "silver2_crash1")).columns
    state = staged()

    result = validate(state)["validation"]

    assert result["status"] == "PASSED" and result["passed"] is True, result["reasons"]
    assert {c["name"]: c["passed"] for c in result["checks"]} == {
        "resolution": True, "no_conflicting_mapping": True, "rows_conserved": True,
        "targeted_resolved": True, "no_new_failures": True, "agrees_with_crash_date": True}
    assert (result["rows_staged"], result["rows_valid"], result["rows_remaining"], result["rows_released"]) \
        == (4, 3, 1, 3)
    assert result["crash_date"]["verdict"] == "AGREES" and result["crash_date"]["confirming"] == 4

    silver2 = read(session, delta / "silver2_crash1")
    assert set(silver2) == {"r1-clean", "r1-ok-1", "r1-ok-2", "r1-ok-3"}
    assert silver2["r1-ok-1"]["crash_month"] == "JULY" and silver2["r1-ok-1"]["crash_date"] == "2019-07-04"
    # exactly the columns the Silver 2 job writes, so Gold reads the healed rows like any other
    assert session.read.format("delta").load(str(delta / "silver2_crash1")).columns == columns_before

    quarantine = read(session, delta / "silver2_quarantine")
    eid = state["execution"]["execution_id"]
    for rid in ("r1-ok-1", "r1-ok-2", "r1-ok-3"):
        assert quarantine[rid]["healing_status"] == "RESOLVED" and quarantine[rid]["resolved_by"] == eid
        assert quarantine[rid]["resolved_at"] is not None
    # still fails damage: it stays for that failure
    assert quarantine["r1-other"]["healing_status"] == "PENDING" and quarantine["r1-other"]["resolved_by"] is None
    assert quarantine["r3-ok-1"]["healing_status"] == "PENDING"     # another folder, untouched


def test_running_the_step_again_releases_nothing_twice(tables):
    delta, session = tables
    state = staged()
    validate(state)

    again = validate(state)["validation"]
    assert again["passed"] is True, again["reasons"]
    assert again["rows_released"] == 0 and again["rows_already_released"] == 3
    assert len(read(session, delta / "silver2_crash1")) == 4

    # The agent stopped after the Silver 2 append but before the quarantine update:
    session.sql(f"UPDATE delta.`{delta / 'silver2_quarantine'}` SET healing_status = 'PENDING', "
                "resolved_by = NULL, resolved_at = NULL")
    resumed = validate(state)["validation"]
    assert resumed["passed"] is True, resumed["reasons"]
    assert len(read(session, delta / "silver2_crash1")) == 4          # Delta skipped the second append
    quarantine = read(session, delta / "silver2_quarantine")
    assert [quarantine[r]["healing_status"] for r in ("r1-ok-1", "r1-ok-2", "r1-ok-3")] == ["RESOLVED"] * 3


def test_a_value_the_crash_dates_rule_out_is_not_released(tables):
    delta, session = tables
    state = staged(canonical="JUNE")

    result = validate(state)["validation"]

    assert result["passed"] is False and result["status"] == "FAILED"
    assert [c["name"] for c in result["checks"] if not c["passed"]] == ["agrees_with_crash_date"]
    assert "4 of 4 records have a crash_date that rules out JUNE" in result["reasons"][0]
    assert set(read(session, delta / "silver2_crash1")) == {"r1-clean"}
    assert {r["healing_status"] for r in read(session, delta / "silver2_quarantine").values()} == {"PENDING"}


def test_a_day_first_source_is_replayed_day_first(tables):
    """04/07/2021 in Crash3 is 4 July. Read month first it would be April and rule JULY out."""
    delta, session = tables
    state = staged(folder="Crash3", rows=2, failure_id="f-crash3")

    result = validate(state)["validation"]

    assert result["passed"] is True, result["reasons"]
    assert result["target_table"] == "silver2_crash3" and result["rows_released"] == 2
    silver2 = read(session, delta / "silver2_crash3")
    assert silver2["r3-ok-1"]["crash_date"] == "2021-07-04" and silver2["r3-ok-1"]["crash_month"] == "JULY"
    assert silver2["r3-ok-2"]["crash_date"] == "2021-07-21"


def test_a_mapping_added_elsewhere_in_the_meantime_is_a_conflict(tables):
    delta, session = tables
    state = staged()
    # after staging, ref_canonical_mappings learned JULE -> JUNE from somewhere else
    ref = session.read.format("delta").load(str(delta / "ref_canonical_mappings"))
    session.createDataFrame([("crash_month", "JULE", "JUNE", "engineer", datetime.now(timezone.utc), 1.0, None)],
                            ref.schema).write.format("delta").mode("append").save(str(delta / "ref_canonical_mappings"))

    result = validate(state)["validation"]

    assert result["passed"] is False
    assert "ref_canonical_mappings already maps JULE to JUNE, not JULY" in result["reasons"][0]
    assert set(read(session, delta / "silver2_crash1")) == {"r1-clean"}


def test_a_staged_record_already_in_silver2_stops_the_release(tables):
    delta, session = tables
    state = staged()
    # one staged record reached Silver 2 by some other way: releasing it would duplicate it
    quarantined = session.read.format("delta").load(str(delta / "silver2_quarantine"))
    import silver2_job
    from ref_mappings import load_mappings_dict, overlay_mappings
    row = silver2_job.standardize_silver2(
        quarantined.filter("record_id = 'r1-ok-1'").select("raw_record.*"),
        overlay_mappings(load_mappings_dict(session, str(delta / "ref_canonical_mappings")),
                         {"crash_month": {"JULE": "JULY"}}), day_first=False)
    row.drop("columns_to_remediate", "_fuzzy_audit_entries", "raw_record") \
        .write.format("delta").mode("append").save(str(delta / "silver2_crash1"))

    result = validate(state)["validation"]

    assert result["passed"] is False
    assert "1 staged record(s) are already in Silver 2: r1-ok-1" in result["reasons"][0]
    assert len(read(session, delta / "silver2_crash1")) == 2


def test_release_is_tagged_with_the_execution(tables):
    from deltalake import DeltaTable

    delta, _ = tables
    state = staged()
    validate(state)
    app_id = release_app_id(state["execution"]["execution_id"])
    assert DeltaTable(str(delta / "silver2_crash1")).transaction_version(app_id) == 0


# --- Task 107: Learn promotes into the Spark-written reference table -------------------
@pytest.mark.real_io
def test_an_engineers_fix_is_promoted_into_the_table_silver2_reads(tables, memory):
    """deltalake MERGEs into the table Spark created; the Silver 2 job's own loader sees it."""
    import importlib

    from ref_mappings import load_mappings_dict

    learn = importlib.import_module("agent.nodes.learn").learn
    delta, session = tables
    state = staged()
    state["action"].update(source="ENGINEER", approved_by="thay")
    state["escalation"] = {"status": "DECIDED", "escalation_id": "f-crash1:1", "round": 1, "reason": "POLICY_GATE_REJECTED",
                           "decision": {"decision": "APPLY_FIX", "engineer": "thay", "promote": True}, "history": []}
    state["status"] = "RESOLVED_BY_ENGINEER"
    state.update(validate(state))
    assert state["validation"]["passed"] is True, state["validation"]["reasons"]

    learned = learn(state)["learned"]
    again = learn(state)["learned"]

    assert learned["promotion"]["status"] == "PROMOTED" and learned["promotion"]["inserted"] is True
    assert again["promotion"]["status"] == "ALREADY_IN_TABLE"
    ref = str(delta / "ref_canonical_mappings")
    assert load_mappings_dict(session, ref)["crash_month"]["JULE"] == "JULY"
    rows = session.read.format("delta").load(ref).where("raw_synonym = 'JULE'").collect()
    assert len(rows) == 1
    assert rows[0]["added_by"] == "recovery_agent:engineer:thay" and rows[0]["source_batch_id"] == "recovery_agent:f-crash1"
    assert rows[0]["field_name"] == "crash_month" and rows[0]["confidence"] == 1.0 and rows[0]["added_at"] is not None
