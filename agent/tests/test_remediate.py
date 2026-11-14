"""Remediate: python -m pytest agent/tests   (real Delta tables in a temp folder, no Spark)"""
import sys
from pathlib import Path

import pytest
from deltalake import DeltaTable
from langgraph.checkpoint.memory import InMemorySaver

from agent import audit, config, executors, reference
from agent.consumer import run_workflow
from agent.graph import build_graph
from agent.nodes.remediate import make_remediate, remediate
from agent.state import initial_state
from agent.tests.helpers import (PAST_FIX, Killed, canonical_values, jule_quarantine, path, quarantine_row,
                                 schema_item, stack_item, write_quarantine)

JULY = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"}, "source": "DIAGNOSIS",
        "incident_id": None}


@pytest.fixture(autouse=True)
def vocabulary(monkeypatch):
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)


def state_for(item=None, action=JULY, confidence=0.93):
    state = initial_state("L1", item or stack_item())
    state["action"] = action
    state["diagnosis"] = {"status": "OK", "confidence": confidence}
    return state


def audit_rows():
    try:
        table = DeltaTable(config.REMEDIATION_AUDIT_PATH)
    except Exception:
        return []
    return table.to_pyarrow_dataset().to_table(use_threads=False).to_pylist()


# --- execute: stage the records of the failure with the overlay ---------------------------
def test_approved_fix_is_staged_for_every_record_of_the_failure():
    jule_quarantine(config.QUARANTINE_PATH, rows=10)
    out = remediate(state_for())
    execution = out["execution"]
    assert execution["status"] == "SUCCESS" and execution["reasons"] == []
    assert execution["action"] == "ADD_VARIANT" and execution["action_params"] == {"canonical_value": "JULY"}
    assert execution["target_table"] == "silver2_crash1"
    assert execution["rows_expected"] == 10 and execution["rows_affected"] == 10
    # the shape ref_mappings.overlay_mappings() takes: Validate applies it over the reference table
    assert execution["overlay"] == {"crash_month": {"JULE": "JULY"}}
    assert execution["sample_record_ids"] == ["r00", "r01", "r02", "r03", "r04"]
    assert execution["idempotent_replay"] is False and execution["audit"]["written"] is True
    assert out["trace"][0]["note"] == "executed: ADD_VARIANT staged 10 of 10 record(s) of silver2_crash1"
    assert set(out) == {"execution", "trace"}      # the node writes only its own keys


def test_only_the_waiting_records_of_this_failure_are_staged():
    write_quarantine(config.QUARANTINE_PATH, [
        quarantine_row("mine1", {"crash_month": "Jule"}),
        quarantine_row("mine2", {"crash_month": " jule "}),                         # same value after trim + upper
        quarantine_row("mine3", {"crash_month": "Jule", "damage": "$1489"}),        # also fails another column
        quarantine_row("claimed", {"crash_month": "Jule"}, status="IN_PROGRESS"),   # marked by the trigger
        quarantine_row("done", {"crash_month": "Jule"}, status="RESOLVED"),         # already fixed
        quarantine_row("other_value", {"crash_month": "Ju"}),
        quarantine_row("other_column", {"weather_condition": "Jule"}),
        quarantine_row("other_folder", {"crash_month": "Jule"}, table="silver2_crash3"),
        quarantine_row("other_batch", {"crash_month": "Jule"}, batch="8"),          # belongs to a later failure
    ])
    # the failure reported 5 rows: 4 are still waiting, 1 was fixed in the meantime
    execution = remediate(state_for(stack_item(rows=5)))["execution"]
    assert execution["status"] == "SUCCESS", execution["reasons"]
    assert audit.find(execution["execution_id"])["record_ids"] == ["claimed", "mine1", "mine2", "mine3"]
    assert execution["rows_affected"] == 4 and execution["rows_expected"] == 5
    assert execution["rows_with_other_failures"] == 1


def test_a_failure_without_batch_information_covers_the_whole_group():
    write_quarantine(config.QUARANTINE_PATH, [quarantine_row("a", {"crash_month": "Jule"}, batch="1"),
                                              quarantine_row("b", {"crash_month": "Jule"}, batch="2")])

    def item(failure_id, rows, **failure):
        made = stack_item(failure_id=failure_id, rows=rows)
        made["failure"].update(failure)
        return made

    assert remediate(state_for(item("f1", 2, batch_id=None, evidence={})))["execution"]["rows_affected"] == 2
    # the comma-separated batch_id is used when the evidence is missing
    assert remediate(state_for(item("f2", 1, batch_id="2", evidence={})))["execution"]["rows_affected"] == 1
    assert remediate(state_for(item("f3", 2, batch_id="1,2", evidence={})))["execution"]["rows_affected"] == 2


def test_a_failure_merged_by_the_lookup_stages_the_rows_of_every_batch():
    """Two scanner publishes of one group in one lookup window are merged into one failure. The merge
    joins batch_id but may keep the evidence of the first publish only, so both are read."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lookup"))
    from failures import group_quarantine_rows
    from stack import merge_failures

    first = [quarantine_row("r1", {"crash_month": "Jule"}, batch="7"),
             quarantine_row("r2", {"crash_month": "Jule"}, batch="7")]
    second = [quarantine_row(f"r{n}", {"crash_month": "Jule"}, batch="8") for n in (3, 4, 5)]
    write_quarantine(config.QUARANTINE_PATH, first + second)

    def publish(rows):
        return group_quarantine_rows([{**row, "columns_to_remediate": dict(row["columns_to_remediate"])}
                                      for row in rows])

    (merged,) = merge_failures(publish(first) + publish(second))
    assert merged.rows_affected == 5 and merged.batch_id == "7,8"
    item = stack_item()
    item["failure"] = merged.to_dict()
    # whatever the lookup does with the evidence, the first publish's alone must not narrow the group
    item["failure"]["evidence"]["quarantine_batches"] = ["7"]
    execution = remediate(state_for(item))["execution"]
    assert execution["status"] == "SUCCESS", execution["reasons"]
    assert execution["rows_affected"] == 5
    assert audit.find(execution["execution_id"])["record_ids"] == ["r1", "r2", "r3", "r4", "r5"]


def test_a_group_with_missing_rows_is_not_staged_in_part():
    jule_quarantine(config.QUARANTINE_PATH, rows=2)
    execution = failed(state_for(stack_item(rows=5)),
                       "only 2 of the 5 records the failure reported were found in the quarantine")
    assert execution["rows_expected"] == 5 and execution["sample_record_ids"] == []
    # more rows than reported (duplicates) is for Validate's row count check, not a reason to stop here
    assert remediate(state_for(stack_item(failure_id="f2", rows=1)))["execution"]["status"] == "SUCCESS"
    # a failure that reports no count (a Qwen incident) cannot be compared
    unknown = stack_item(failure_id="f3")
    unknown["failure"]["rows_affected"] = None
    assert remediate(state_for(unknown))["execution"]["status"] == "SUCCESS"


def test_nothing_but_the_audit_table_is_written(tmp_path):
    jule_quarantine(config.QUARANTINE_PATH)
    before = sorted(str(p) for p in Path(config.QUARANTINE_PATH).rglob("*"))
    execution = remediate(state_for())["execution"]
    assert sorted(str(p) for p in Path(config.QUARANTINE_PATH).rglob("*")) == before
    assert DeltaTable(config.QUARANTINE_PATH).version() == execution["quarantine_version"] == 0
    assert sorted(p.name for p in tmp_path.iterdir()) == ["remediation_audit", "silver2_quarantine"]


# --- audit record (RD2, TA2) -------------------------------------------------------------
def test_audit_record_has_action_parameters_affected_records_and_timestamp():
    jule_quarantine(config.QUARANTINE_PATH, rows=3)
    execution = remediate(state_for(stack_item(rows=3)))["execution"]
    record = audit.find(execution["execution_id"])
    assert record["failure_id"] == "f1" and record["lookup_id"] == "L1"
    assert record["action"] == "ADD_VARIANT" and record["action_params"] == {"canonical_value": "JULY"}
    assert record["action_source"] == "DIAGNOSIS" and record["confidence"] == 0.93 and record["incident_id"] is None
    assert record["status"] == "SUCCESS" and record["reasons"] == []
    assert record["column_name"] == "crash_month" and record["raw_value"] == "Jule"
    assert record["overlay"] == {"crash_month": {"JULE": "JULY"}}
    assert record["rows_affected"] == 3 and record["record_ids"] == ["r00", "r01", "r02"]
    assert record["started_at"] <= record["executed_at"]
    assert record["executed_at"].isoformat() == execution["executed_at"]
    assert record["executed_at"].tzinfo is not None


def test_a_reused_fix_is_audited_with_the_incident_it_came_from():
    jule_quarantine(config.QUARANTINE_PATH)
    action = {"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"}, "source": "INCIDENT",
              "incident_id": "i1"}
    execution = remediate(state_for(stack_item(route="remediate", match=PAST_FIX), action))["execution"]
    record = audit.find(execution["execution_id"])
    assert execution["source"] == record["action_source"] == "INCIDENT"
    assert record["incident_id"] == "i1" and record["confidence"] is None   # no model was asked


# --- idempotency check (RD1) -------------------------------------------------------------
def test_second_run_of_the_same_fix_executes_nothing_and_returns_the_first_result(monkeypatch):
    jule_quarantine(config.QUARANTINE_PATH)
    first = remediate(state_for())["execution"]

    def must_not_run(failure):
        raise AssertionError("executed twice")

    monkeypatch.setattr(reference, "failure_records", must_not_run)
    out = remediate(state_for())
    second = out["execution"]
    assert second["idempotent_replay"] is True
    assert {**second, "idempotent_replay": False} == first
    assert len(audit_rows()) == 1 and DeltaTable(config.REMEDIATION_AUDIT_PATH).version() == 0
    assert out["trace"][0]["note"].startswith(f"already executed at {first['executed_at']}, not run again")


def test_workflow_that_stopped_after_executing_resumes_without_executing_again():
    """The case the check exists for: the agent dies after Remediate executed but before
    LangGraph saved the node. On restart the workflow resumes at Remediate."""
    jule_quarantine(config.QUARANTINE_PATH)
    real, runs = make_remediate(), []

    def dies_after_the_first_execution(state):
        out = real(state)
        runs.append(out["execution"])
        if len(runs) == 1:
            raise Killed()                             # the process dies: no retry, nothing saved
        return out

    graph = build_graph(InMemorySaver(), nodes={"remediate": dies_after_the_first_execution})
    item = stack_item(route="remediate", match=PAST_FIX)
    with pytest.raises(Killed):
        run_workflow(graph, "L1", item)
    assert len(audit_rows()) == 1                      # the execution happened and is recorded

    summary = run_workflow(graph, "L1", item)          # Kafka redelivers, the workflow resumes
    assert summary["delivery"] == "resumed" and len(runs) == 2
    assert runs[1]["idempotent_replay"] is True and runs[1]["executed_at"] == runs[0]["executed_at"]
    assert len(audit_rows()) == 1                      # not executed, and not recorded, twice
    assert summary["state"]["execution"]["status"] == "SUCCESS"
    assert path(summary["state"])[:3] == ["deterministic_lookup", "remediate", "validate"]


def test_a_step_retry_after_the_fix_was_executed_does_not_execute_it_again():
    """The graph retries a step that raises (agent/graph.py guarded), so Remediate can be
    called twice inside one workflow. The second call finds the audit record."""
    jule_quarantine(config.QUARANTINE_PATH)
    real, runs = make_remediate(), []

    def raises_after_the_first_execution(state):
        out = real(state)
        runs.append(out["execution"])
        if len(runs) == 1:
            raise RuntimeError("lost the connection after the commit")
        return out

    graph = build_graph(InMemorySaver(), nodes={"remediate": raises_after_the_first_execution}, retry_seconds=0)
    summary = run_workflow(graph, "L1", stack_item(route="remediate", match=PAST_FIX))
    assert len(runs) == 2 and runs[1]["idempotent_replay"] is True
    assert len(audit_rows()) == 1 and summary["state"]["execution"]["status"] == "SUCCESS"
    assert "error" not in summary["state"]


def test_a_failure_rerun_because_a_later_step_is_a_stub_is_not_executed_again():
    """Learn is still a stub, so the consumer runs the whole workflow again when the
    failure is published again. Remediate is real and has already staged the fix."""
    jule_quarantine(config.QUARANTINE_PATH)
    graph = build_graph(InMemorySaver())
    item = stack_item(route="remediate", match=PAST_FIX)
    first = run_workflow(graph, "L1", item)
    assert first["delivery"] == "new" and first["state"]["execution"]["idempotent_replay"] is False
    assert "remediate" not in first["state"]["stubbed"]

    again = run_workflow(graph, "L2", item)
    assert again["delivery"] == "rerun" and again["state"]["execution"]["idempotent_replay"] is True
    assert again["state"]["execution"]["executed_at"] == first["state"]["execution"]["executed_at"]
    assert len(audit_rows()) == 1


def test_a_failed_attempt_does_not_block_the_fix_once_the_cause_is_gone():
    """A failed attempt staged nothing, so running the fix again is not a duplicate."""
    first = remediate(state_for())["execution"]        # no quarantine table yet: FAILED
    assert first["status"] == "FAILED" and "could not be read" in first["reasons"][0]
    jule_quarantine(config.QUARANTINE_PATH)            # the cause is gone
    second = remediate(state_for())["execution"]
    assert second["status"] == "SUCCESS" and second["idempotent_replay"] is False
    assert second["execution_id"] == first["execution_id"] and second["rows_affected"] == 10
    # both attempts are in the audit trail, and only the SUCCESS is replayed from now on
    assert [row["status"] for row in audit.attempts(first["execution_id"])] == ["FAILED", "SUCCESS"]
    third = remediate(state_for())["execution"]
    assert third["idempotent_replay"] is True and third["executed_at"] == second["executed_at"]
    assert len(audit_rows()) == 2


def test_an_unreadable_reference_table_is_reported_as_that_and_retried(monkeypatch):
    jule_quarantine(config.QUARANTINE_PATH)

    def unreadable(column, path=None):
        raise OSError("mount is gone")

    monkeypatch.setattr(reference, "read_canonical_values", unreadable)
    first = failed(state_for(), "the reference table could not be read: mount is gone")
    assert "canonical value" not in first["reasons"][0]      # not blamed on the vocabulary
    monkeypatch.setattr(reference, "read_canonical_values", canonical_values)
    assert remediate(state_for())["execution"]["status"] == "SUCCESS"


def test_another_fix_for_the_same_failure_is_another_execution():
    jule_quarantine(config.QUARANTINE_PATH)
    july = remediate(state_for())["execution"]
    june = remediate(state_for(action={**JULY, "action_params": {"canonical_value": "JUNE"}}))["execution"]
    other = remediate(state_for(stack_item(failure_id="f2")))["execution"]
    assert len({july["execution_id"], june["execution_id"], other["execution_id"]}) == 3
    assert june["idempotent_replay"] is False and june["overlay"] == {"crash_month": {"JULE": "JUNE"}}
    assert len(audit_rows()) == 3


def test_without_a_readable_audit_table_nothing_is_executed(monkeypatch):
    jule_quarantine(config.QUARANTINE_PATH)

    def unreadable(execution, path=None):
        raise OSError("disk gone")

    monkeypatch.setattr(audit, "find", unreadable)
    monkeypatch.setattr(reference, "failure_records", lambda failure: pytest.fail("executed without the check"))
    execution = remediate(state_for())["execution"]
    assert execution["status"] == "FAILED" and execution["audit"]["written"] is False
    assert execution["reasons"] == ["idempotency check failed, the audit table could not be read: disk gone"]
    assert audit_rows() == []


# --- failed executions go to an engineer (RD3) -------------------------------------------
def failed(state, reason):
    out = remediate(state)
    execution = out["execution"]
    assert execution["status"] == "FAILED" and execution["rows_affected"] == 0 and execution["overlay"] is None
    assert reason in execution["reasons"][0], execution["reasons"]
    assert out["trace"][0]["note"].startswith("failed: ")
    assert audit.find(execution["execution_id"]) is None        # not a result that is ever replayed
    record = audit.attempts(execution["execution_id"])[-1]      # but failed attempts are audited too
    assert record["status"] == "FAILED" and record["reasons"] == execution["reasons"] and record["record_ids"] == []
    return execution


def test_no_waiting_record_fails():
    write_quarantine(config.QUARANTINE_PATH, [quarantine_row("done", {"crash_month": "Jule"}, status="RESOLVED")])
    failed(state_for(), "no quarantined record of this failure is waiting")


def test_unreadable_quarantine_fails():
    failed(state_for(), "the quarantine table could not be read")


def test_a_value_that_is_not_canonical_at_execution_time_fails(monkeypatch):
    jule_quarantine(config.QUARANTINE_PATH)
    failed(state_for(action={**JULY, "action_params": {"canonical_value": "JULLY"}}),
           "'JULLY' is not a canonical value of crash_month")
    # the column has no vocabulary at all (it is parsed, not looked up)
    monkeypatch.setattr(reference, "read_canonical_values", lambda column, path=None: [])
    failed(state_for(stack_item(failure_id="f2")), "no canonical values are known")


def test_an_action_without_an_executor_fails_even_as_a_verified_past_fix():
    rename = {"action": "RENAME_COLUMN", "action_params": {"renames": {"CrashDate": "crash_date"}},
              "source": "INCIDENT", "incident_id": "i7"}
    execution = failed(state_for(schema_item(), rename), "no automatic execution exists for 'RENAME_COLUMN'")
    assert execution["action"] == "RENAME_COLUMN" and execution["source"] == "INCIDENT"
    failed(state_for(stack_item(failure_id="f2"), {"action": "DROP_TABLE", "action_params": {}}),
           "no automatic execution exists for 'DROP_TABLE'")
    failed(state_for(stack_item(failure_id="f3"), None), "no automatic execution exists for None")


def test_rows_from_before_the_per_folder_split_are_matched_by_the_producer_table():
    write_quarantine(config.QUARANTINE_PATH, [quarantine_row("a", {"crash_month": "Jule"}, table=None),
                                              quarantine_row("b", {"crash_month": "Jule"}, table=None)])
    # the failure's folder is "Crash1", the rows' table_name
    execution = remediate(state_for(stack_item(rows=2)))["execution"]
    assert execution["status"] == "SUCCESS" and execution["rows_affected"] == 2
    assert execution["target_table"] is None


def test_records_spread_over_several_tables_fail(monkeypatch):
    rows = [{"record_id": "a", "target_table": "silver2_crash1", "other_failures": False},
            {"record_id": "b", "target_table": "silver2_crash2", "other_failures": False}]
    monkeypatch.setattr(reference, "failure_records", lambda failure: {"version": 0, "rows": rows})
    failed(state_for(stack_item(rows=2)),
           "the records of one failure belong to several tables: silver2_crash1, silver2_crash2")


def test_success_is_not_claimed_when_the_audit_record_cannot_be_written(monkeypatch):
    jule_quarantine(config.QUARANTINE_PATH)

    def full_disk(record, path=None):
        raise OSError("no space left on device")

    monkeypatch.setattr(audit, "write", full_disk)
    execution = remediate(state_for())["execution"]
    assert execution["status"] == "FAILED" and execution["audit"]["written"] is False
    assert "the audit record could not be written, so nothing is staged: no space left" in execution["reasons"][0]
    assert audit_rows() == []


def test_a_write_that_landed_despite_an_error_counts_as_written(monkeypatch):
    jule_quarantine(config.QUARANTINE_PATH)
    write = audit.write

    def lands_then_fails(record, path=None):
        write(record, path)
        raise OSError("connection lost after the commit")

    monkeypatch.setattr(audit, "write", lands_then_fails)
    execution = remediate(state_for())["execution"]
    assert execution["status"] == "SUCCESS" and execution["audit"]["written"] is True
    assert len(audit_rows()) == 1


def test_a_bug_in_an_executor_does_not_stop_the_agent(monkeypatch):
    def broken(failure, params):
        raise KeyError("oops")

    monkeypatch.setitem(executors.EXECUTORS, "ADD_VARIANT", broken)
    failed(state_for(), "ADD_VARIANT raised KeyError")


def test_remediate_never_raises_on_a_broken_state():
    for state in ({}, {"failure": None, "action": None}, {"failure": {}, "action": {"action_params": "x"}}):
        execution = make_remediate()(state)["execution"]
        assert execution["status"] == "FAILED" and execution["reasons"]


def test_audit_path_can_be_given_to_the_node(tmp_path):
    jule_quarantine(config.QUARANTINE_PATH)
    execution = make_remediate(audit_path=str(tmp_path / "elsewhere"))(state_for())["execution"]
    assert execution["audit"] == {"path": str(tmp_path / "elsewhere"), "written": True}
    assert audit.find(execution["execution_id"], str(tmp_path / "elsewhere")) and audit_rows() == []


# --- contract with the lookup service ----------------------------------------------------
def test_stages_exactly_the_rows_the_scanner_grouped_into_the_failure():
    """Group quarantine rows with lookup/ itself: each failure must stage its own rows, all of them."""
    sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "lookup"))
    from failures import group_quarantine_rows

    rows = [quarantine_row("a1", {"crash_month": "Jule"}, batch="3"),
            quarantine_row("a2", {"crash_month": "JULE "}, batch="4"),
            quarantine_row("a3", {"crash_month": "Jule", "weather_condition": "misty dark"}, batch="4"),
            quarantine_row("b1", {"crash_month": "Jule"}, batch="3", table="silver2_crash3"),
            quarantine_row("c1", {"weather_condition": "MISTY DARK"}, batch="5")]
    write_quarantine(config.QUARANTINE_PATH, rows)
    failures = group_quarantine_rows([{**row, "columns_to_remediate": dict(row["columns_to_remediate"])}
                                      for row in rows])
    staged = {}
    for failure in failures:
        target = "JULY" if failure.column_name == "crash_month" else "FOG"
        item = stack_item()
        item["failure"] = failure.to_dict()
        action = {**JULY, "action_params": {"canonical_value": target}}
        execution = remediate(state_for(item, action))["execution"]
        assert execution["status"] == "SUCCESS", execution["reasons"]
        assert execution["rows_affected"] == execution["rows_expected"] == failure.rows_affected
        staged[(failure.folder, failure.column_name)] = audit.find(execution["execution_id"])["record_ids"]
    assert staged == {("crash1", "crash_month"): ["a1", "a2", "a3"], ("crash3", "crash_month"): ["b1"],
                      ("crash1", "weather_condition"): ["a3", "c1"]}
