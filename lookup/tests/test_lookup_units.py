"""Unit tests for the Deterministic Lookup (no Kafka, no Neo4j)."""
from blast_radius import LineageView, layer_of, resolve_table, score
from failures import (StructuredFailure, from_qwen_incident, group_quarantine_rows,
                      schema_value)
from patterns import evaluate, osa_distance, parse_schema_value, similarity, string_similarity
from stack import build_stack, merge_failures


# --- fixtures -----------------------------------------------------------------
def qwen_schema_msg(**over):
    msg = {
        "incident_id": "abc", "failure_type": "SCHEMA_MISMATCH", "table": "Crash4",
        "column": None, "job_name": "Silver1SchemaValidation", "batch_id": 3,
        "classification_status": "OK", "needs_review": False, "handoff_status": "READY",
        "affected_dataset": "bronze", "affected_record_count": 50,
        "affected_columns": {"expected_only": ["crash_date"], "source_only": ["CRASH_DATE"],
                             "reported_column": None},
        "dead_letter_ref": {"path": "/data/delta/silver1_dead_letter"},
    }
    msg.update(over)
    return msg


def quarantine_failure(folder="Crash1", column="crash_month", value="Jule", rows=10):
    return StructuredFailure(source="QUARANTINE", failure_type="UNRESOLVED_VALUE",
                             origin_layer="silver1", detected_layer="silver2", folder=folder,
                             column_name=column, raw_value=value, rows_affected=rows)


def folder_lineage(folder="crash1", origin="silver1", gold_cols=("crash_month",)):
    """Lineage of the picture: <layer>_crashN -> ... -> gold (UNION ALL)."""
    chain = {"bronze": ["silver1", "silver2"], "silver1": ["silver2"], "silver2": []}[origin]
    downstream = [{"name": f"{l}_{folder}", "kind": "delta", "depth": d, "columns": ["crash_month"]}
                  for d, l in enumerate(chain, start=1)]
    downstream.append({"name": "gold", "kind": "delta", "depth": len(chain) + 1, "columns": list(gold_cols)})
    downstream.append({"name": "silver2_quarantine", "kind": "delta", "depth": len(chain) + 1, "columns": []})
    return LineageView(f"{origin}_{folder}", f"silver2_{folder}", True, downstream)


def incident(**over):
    inc = {"incident_id": "i1", "failure_type": "UNRESOLVED_VALUE", "table_name": "silver2_crash1",
           "column_name": "crash_month", "raw_value": "JULE", "action": "ADD_VARIANT",
           "action_params": '{"canonical_value": "JULY"}', "outcome": "SUCCESS"}
    inc.update(over)
    return inc


# --- failures ---------------------------------------------------------------------
def test_qwen_schema_incident_is_mapped_to_origin_and_detected_layers():
    f = from_qwen_incident(qwen_schema_msg())
    assert (f.origin_layer, f.detected_layer, f.folder) == ("bronze", "silver1", "Crash4")
    assert f.family == "SCHEMA" and f.trusted
    assert f.signature_value == "EXPECTED_ONLY=crash_date;SOURCE_ONLY=CRASH_DATE"
    assert f.rows_affected == 50 and f.batch_id == "3"


def test_unreviewed_qwen_output_is_not_trusted_but_keeps_deterministic_class():
    f = from_qwen_incident(qwen_schema_msg(failure_type="UNKNOWN", classification_status="UNAVAILABLE",
                                           needs_review=True, detected_error_class="SCHEMA_MISMATCH"))
    assert not f.trusted
    assert f.failure_type == "SCHEMA_MISMATCH"


def test_pipeline_crash_marks_origin_itself_affected():
    f = from_qwen_incident(qwen_schema_msg(job_name="Pipeline-silver2-gold", affected_dataset=None))
    assert f.origin_layer == "silver2" and f.origin_affected


def test_quarantine_rows_group_by_folder_column_and_value():
    rows = [
        {"record_id": "r1", "table_name": "Crash1", "silver2_batch_id": "7",
         "columns_to_remediate": {"crash_month": "Jule", "damage": "$1489"}},
        {"record_id": "r2", "table_name": "Crash1", "silver2_batch_id": "8",
         "columns_to_remediate": [("crash_month", " jule ")]},  # pyarrow map -> list of pairs
        {"record_id": "r3", "table_name": "Crash2", "silver2_batch_id": "8",
         "columns_to_remediate": {"crash_date": "MISSING_MANDATORY_VALUE"}},
    ]
    groups = {(f.folder, f.column_name): f for f in group_quarantine_rows(rows)}
    month = groups[("Crash1", "crash_month")]
    assert month.rows_affected == 2 and month.batch_id == "7,8"
    assert month.sample_record_ids == ("r1", "r2")
    assert groups[("Crash2", "crash_date")].mandatory
    assert len(groups) == 3


def test_failure_round_trips_through_kafka_json():
    f = from_qwen_incident(qwen_schema_msg())
    assert StructuredFailure.from_dict(f.to_dict()) == f


# --- blast radius -------------------------------------------------------------
def test_table_resolution_prefers_folder_table_then_falls_back_to_layer():
    assert resolve_table("silver2", "Crash1", {"silver2_crash1", "silver2"}) == "silver2_crash1"
    assert resolve_table("silver2", "Crash1", {"silver2"}) == "silver2"
    assert resolve_table("silver2", "Crash1", set()) == "silver2"


def test_layer_of_uses_longest_prefix():
    assert layer_of("silver2_quarantine") == "silver2_quarantine"
    assert layer_of("silver2_crash3") == "silver2"
    assert layer_of("gold") == "gold"


def test_upstream_schema_failure_outranks_a_value_failure():
    schema = from_qwen_incident(qwen_schema_msg(table="Crash1"))
    value = quarantine_failure(rows=50)
    s_schema = score(schema, folder_lineage(origin="bronze"))
    s_value = score(value, folder_lineage(origin="silver1"))
    assert s_schema["score"] > s_value["score"]
    assert s_schema["affected_layers"] == ["silver1", "silver2", "gold"]
    assert "silver2_quarantine" not in s_value["affected_layers"]  # side tables weigh 0


def test_column_dropped_downstream_lowers_propagation():
    f = quarantine_failure()
    used = score(f, folder_lineage(gold_cols=("crash_month",)))
    dropped = score(f, folder_lineage(gold_cols=("other",)))
    assert used["breakdown"]["propagation"] == 1.0
    # silver2_crash1 still has the column in this fixture
    assert used["column_reaches"] == ["silver2_crash1", "gold"]
    assert dropped["column_reaches"] == ["silver2_crash1"]


def test_unknown_lineage_uses_static_medallion_chain():
    s = score(quarantine_failure(), LineageView("silver1", "silver2", False))
    assert s["affected_layers"] == ["silver2", "gold"] and not s["lineage_known"]


def test_more_rows_score_higher():
    lin = folder_lineage()
    assert score(quarantine_failure(rows=5000), lin)["score"] > score(quarantine_failure(rows=2), lin)["score"]


# --- incident pattern -------------------------------------------------------------
def test_osa_distance_counts_transposition_as_one_edit():
    assert osa_distance("JUEN", "JUNE") == 1
    assert string_similarity("Jule", "JULY") == 0.75


def test_schema_value_round_trip():
    assert parse_schema_value(schema_value(["b", "a"], ["C"])) == (("a", "b"), ("C",))


def test_exact_signature_routes_to_remediate():
    f = quarantine_failure()
    ctx = evaluate(f, "silver2_crash1", incident(), [], [])
    assert ctx["reusable"] and ctx["match"]["matched_by"] == "EXACT"
    assert ctx["match"]["action_params"] == {"canonical_value": "JULY"}


def test_similar_but_different_value_is_not_reused():
    """'Juen' is close to past 'JUNE' but the fix is a canonical value: diagnose."""
    f = quarantine_failure(value="Jule")
    ctx = evaluate(f, "silver2_crash1", None, [incident(raw_value="JUNE")], [], threshold=0.8)
    assert ctx["candidates"][0]["score"] >= 0.8
    assert not ctx["reusable"]
    assert "canonical target may differ" in ctx["reason"]


def test_value_differing_only_in_punctuation_is_reused():
    f = quarantine_failure(value="Jul.")
    ctx = evaluate(f, "silver2_crash1", None, [incident(raw_value="JUL")], [])
    assert ctx["reusable"]


def test_same_schema_rename_in_another_folder_is_reused():
    f = from_qwen_incident(qwen_schema_msg(table="Crash4"))
    past = incident(failure_type="SCHEMA_MISMATCH", table_name="silver1_crash2", column_name=None,
                    raw_value=schema_value(["crash_date"], ["CRASH_DATE"]),
                    action="RENAME_COLUMN", action_params='{"from": "CRASH_DATE", "to": "crash_date"}')
    s, comps = similarity(f, "silver1_crash4", past)
    assert comps["table"] == 0.7 and comps["value"] == 1.0
    ctx = evaluate(f, "silver1_crash4", None, [past], [])
    assert ctx["reusable"] and ctx["match"]["action"] == "RENAME_COLUMN"


def test_untrusted_failure_never_reuses_a_fix():
    f = from_qwen_incident(qwen_schema_msg(classification_status="INVALID_OUTPUT", needs_review=True))
    past = incident(failure_type="SCHEMA_MISMATCH", column_name=None,
                    raw_value=schema_value(["crash_date"], ["CRASH_DATE"]))
    ctx = evaluate(f, "silver1_crash4", past, [], [])
    assert not ctx["reusable"] and "needs review" in ctx["reason"]


def test_fix_that_already_failed_is_not_repeated():
    f = quarantine_failure()
    attempts = [{"action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"},
                 "outcome": "FAILED_VALIDATION"}]
    ctx = evaluate(f, "silver2_crash1", incident(), [], attempts)
    assert not ctx["reusable"]


def test_no_memory_routes_to_diagnose():
    ctx = evaluate(quarantine_failure(), "silver2_crash1", None, [], [])
    assert not ctx["reusable"] and ctx["reason"] == "no verified incident in memory"


# --- stack ----------------------------------------------------------------------
def test_merge_combines_same_signature_from_several_batches():
    a = from_qwen_incident(qwen_schema_msg(batch_id=1, incident_id="x", affected_record_count=10))
    b = from_qwen_incident(qwen_schema_msg(batch_id=2, incident_id="y", affected_record_count=15))
    (m,) = merge_failures([a, b])
    assert m.rows_affected == 25 and m.batch_id == "1,2"
    assert m.evidence["merged_incident_ids"] == ["x", "y"]


def test_stack_is_ranked_and_upstream_failure_blocks_downstream_one():
    schema = from_qwen_incident(qwen_schema_msg(table="Crash1"))
    value = quarantine_failure(rows=50)
    fails = [value, schema]
    lins = [folder_lineage(origin="silver1"), folder_lineage(origin="bronze")]
    ctxs = [evaluate(f, l.detected_table, None, [], []) for f, l in zip(fails, lins)]
    stack = build_stack(fails, lins, ctxs)
    assert [it["failure"]["source"] for it in stack] == ["QWEN", "QUARANTINE"]
    assert stack[0]["lineage_context"]["blast_radius"]["blocks_failures"] == 1
    assert stack[1]["lineage_context"]["blast_radius"]["blocks_failures"] == 0
    assert [it["rank"] for it in stack] == [1, 2]
    assert all(it["route"] == "diagnose" for it in stack)
    assert set(stack[0]) >= {"failure", "lineage_context", "incident_context", "priority", "route"}


def test_case_variant_rename_is_not_reused_even_with_same_signature_key():
    """signature_key upper-cases: CRASH_DATE and Crash_Date collide, but the rename differs."""
    f = from_qwen_incident(qwen_schema_msg(affected_columns={
        "expected_only": ["crash_date"], "source_only": ["Crash_Date"], "reported_column": None}))
    past = incident(failure_type="SCHEMA_MISMATCH", table_name="silver1_crash4", column_name=None,
                    raw_value=schema_value(["crash_date"], ["CRASH_DATE"]),
                    action="RENAME_COLUMN", action_params='{"from": "CRASH_DATE", "to": "crash_date"}')
    ctx = evaluate(f, "silver1_crash4", past, [], [])  # find_exact returned it
    assert not ctx["reusable"]
    assert ctx["candidates"][0]["matched_by"] == "STRUCTURED"


# --- per-folder layout (PR #25: bronze_crashN -> silver1_crashN -> silver2_crashN -> gold) ---
def test_split_table_handles_per_folder_names_only():
    from failures import split_table
    assert split_table("silver2_crash3") == ("silver2", "crash3")
    assert split_table("/data/delta/bronze_crash1") == ("bronze", "crash1")
    assert split_table("silver2_quarantine") == (None, None)
    assert split_table("silver1_dead_letter") == (None, None)
    assert split_table("gold") == (None, None)


def test_schema_guard_table_name_in_affected_dataset_is_split():
    f = from_qwen_incident(qwen_schema_msg(affected_dataset="bronze_crash3", table="Crash3"))
    assert (f.origin_layer, f.detected_layer, f.folder) == ("bronze", "silver1", "Crash3")


def test_silver2_crash_gets_folder_from_target_table():
    msg = qwen_schema_msg(job_name="Silver2", table="silver2_crash3", affected_dataset=None,
                          failure_type="MISSING_COLUMN")
    f = from_qwen_incident(msg)
    assert (f.origin_layer, f.detected_layer, f.folder) == ("silver1", "silver2", "crash3")
    assert resolve_table(f.origin_layer, f.folder, {"silver1_crash3"}) == "silver1_crash3"


def test_quarantine_folder_comes_from_target_table():
    rows = [{"record_id": "r1", "table_name": "Crash3", "target_table": "silver2_crash3",
             "columns_to_remediate": {"crash_month": "Jule"}},
            {"record_id": "r2", "table_name": None, "target_table": "silver2_crash3",
             "columns_to_remediate": {"crash_month": "JULE"}}]
    (g,) = group_quarantine_rows(rows)
    assert g.folder == "crash3" and g.rows_affected == 2


# --- what Learn stores (Task 107) ------------------------------------------------
def test_a_manual_resolution_is_context_but_never_reused():
    f = quarantine_failure()
    past = incident(action="MANUAL", action_params="{}", diagnosis="re-sent the file")
    ctx = evaluate(f, "silver2_crash1", past, [], [])
    assert not ctx["reusable"] and ctx["match"] is None
    assert ctx["candidates"][0]["diagnosis"] == "re-sent the file"
    assert "fixed by hand" in ctx["candidates"][0]["reason"]


def test_a_fix_learn_stored_as_borrowed_stays_borrowed_on_an_exact_match():
    f = quarantine_failure()
    ctx = evaluate(f, "silver2_crash1", incident(borrowed=True), [], [])
    assert ctx["reusable"] and ctx["match"]["matched_by"] == "EXACT" and ctx["match"]["borrowed"] is True
    plain = evaluate(f, "silver2_crash1", incident(), [], [])
    assert plain["match"]["borrowed"] is False


def test_learn_and_the_lookup_embed_the_same_text():
    from patterns import signature_text
    from agent.memory.incidents import signature_text as learned_text
    f = quarantine_failure(value=" jule ")
    assert signature_text(f, "silver2_crash1") == learned_text("silver2_crash1", "crash_month", " jule ",
                                                               "UNRESOLVED_VALUE")


def test_an_exact_value_with_disagreeing_past_fixes_is_not_reused():
    """MISTY fixed to FOG once and to CLOUDY later: both validated, neither may be reused."""
    f = quarantine_failure(column="weather_condition", value="Misty")
    exact = incident(column_name="weather_condition", raw_value="MISTY", action_params='{"canonical_value": "CLOUDY"}',
                     distinct_fixes=['ADD_VARIANT {"canonical_value": "CLOUDY"}', 'ADD_VARIANT {"canonical_value": "FOG"}'])
    ctx = evaluate(f, "silver2_crash1", exact, [], [])
    assert not ctx["reusable"] and ctx["match"] is None
    assert "past fixes of this exact value disagree" in ctx["reason"]
    one = evaluate(f, "silver2_crash1", {**exact, "distinct_fixes": exact["distinct_fixes"][:1]}, [], [])
    assert one["reusable"]
