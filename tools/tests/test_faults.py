"""The fault catalog on its own: no Spark, no Kafka."""
import json
import random

import pytest

from tools import faults

COLUMNS = {"crash_record_id", "crash_date", "crash_month", "crash_day_of_week", "weather_condition",
           "lighting_condition", "crash_hour", "posted_speed_limit", "lane_cnt", "damage",
           "date_police_notified", "street_direction", "hit_and_run_i", "num_units"}


def spec(**entry):
    return faults.normalize_spec(entry, 0, COLUMNS)


def test_every_kind_has_an_intended_path():
    for kind, info in faults.KINDS.items():
        assert info["intended"] in (faults.FUZZY_FIXED, faults.QUARANTINED, faults.DEAD_LETTER), kind


def test_defaults_and_checks():
    s = spec(kind="ambiguous", column="crash_month")
    assert (s["source"], s["count"], s["batches"]) == ("crash1", 5, 1)
    assert s["base_any"] == ["JULY", "JUNE", "MARCH", "MAY"]
    assert spec(kind="schema_rename", column="crash_date")["to"] == "CrashDate"
    assert spec(kind="missing_mandatory")["column"] == "crash_date"
    assert spec(kind="schema_extra")["column"] == "extra_field"
    # An explicit ambiguous value picks the base values it can stand for.
    assert spec(kind="ambiguous", column="crash_month", value="Ma")["base_any"] == ["MARCH", "MAY"]
    for bad in ({"kind": "nope"}, {"kind": "typo"}, {"kind": "typo", "column": "damage"},
                {"kind": "ambiguous", "column": "crash_month", "value": "Xyz"},
                {"kind": "schema_extra", "column": "crash_date"},
                {"kind": "schema_rename", "column": "crash_date", "to": "crash_month"},
                {"kind": "unseen_format", "column": "no_such"}, {"kind": "typo", "column": "crash_month", "count": 0}):
        with pytest.raises(faults.FaultSpecError):
            spec(**bad)


def test_value_corruptions():
    rng = random.Random(1)
    assert faults.corrupt(spec(kind="ambiguous", column="crash_month"), "JUNE", rng) in ("Jule", "Juny", "Ju")
    assert faults.corrupt(spec(kind="ambiguous", column="crash_month"), "APRIL", rng) is None  # no ambiguous form
    assert faults.corrupt(spec(kind="unseen_variant", column="weather_condition"), "RAIN", rng) in (
        "RAINING", "RAINFALL", "SHOWERS")
    assert faults.corrupt(spec(kind="unseen_format", column="crash_date"), "2021-06-22", rng) == "22nd of June 2021"
    assert faults.corrupt(spec(kind="unseen_format", column="crash_date"), "2019-07-11", rng) == "11th of July 2019"
    assert faults.corrupt(spec(kind="unseen_format", column="crash_hour"), "19.00", rng) == "7 o'clock in the evening"
    assert faults.corrupt(spec(kind="unseen_format", column="posted_speed_limit"), "30", rng) == "thirty"
    assert faults.corrupt(spec(kind="unseen_format", column="date_police_notified"),
                          "2021-06-22 19:05:00", rng) == "22nd of June 2021 at 19:05"
    assert faults.corrupt(spec(kind="missing_mandatory"), "2021-06-22", rng) == ""
    assert faults.corrupt(spec(kind="unknown_value", column="weather_condition"), "RAIN", rng) in \
        faults.UNKNOWN_VALUES["weather_condition"]
    assert faults.corrupt(spec(kind="ambiguous", column="crash_month", value="Jule", base="JUNE"), "JUNE", rng) == "Jule"


def test_typos_are_one_edit_and_never_the_value_itself():
    rng = random.Random(3)
    for truth in ("JUNE", "SEPTEMBER", "CLOUDY", "DARKNESS, LIGHTED ROAD", "WEDNESDAY"):
        for _ in range(20):
            out = faults._typo(truth, rng)
            assert out and out.upper() != truth
            assert out[0].upper() == truth[0]
            assert abs(len(out) - len(truth)) <= 1


def test_expected_fix_is_the_ground_truth():
    assert faults.expected_fix(spec(kind="ambiguous", column="crash_month"), "JUNE") == {
        "action": "ADD_VARIANT", "params": {"canonical_value": "JUNE"}}
    assert faults.expected_fix(spec(kind="unseen_variant", column="hit_and_run_i"), "TRUE")["action"] == "ADD_VARIANT"
    assert faults.expected_fix(spec(kind="typo", column="crash_month"), "JUNE")["action"] == "NONE"
    assert faults.expected_fix(spec(kind="unseen_format", column="crash_date"), "2021-06-22")["action"] == "ESCALATE"
    assert faults.expected_fix(spec(kind="unknown_value", column="weather_condition"), "RAIN")["action"] == "ESCALATE"
    assert faults.expected_fix(spec(kind="schema_rename", column="crash_date"), None) == {
        "action": "RENAME_COLUMN", "params": {"renames": {"CrashDate": "crash_date"}}}


def test_canonical_matches_the_reference_table_spelling():
    from decimal import Decimal

    assert faults.canonical("hit_and_run_i", True) == "TRUE"
    assert faults.canonical("num_units", Decimal("2.0")) == "2.0"
    assert faults.canonical("crash_month", "JUNE") == "JUNE"
    assert faults.canonical("crash_month", None) is None


def test_schema_faults_and_messages():
    payload = {"crash_date": "06/22/2021", "lane_cnt": "2", "crash_month": "6"}
    assert faults.apply_fault(spec(kind="schema_rename", column="crash_date"), payload, None) == {
        "CrashDate": "06/22/2021", "lane_cnt": "2", "crash_month": "6"}
    assert "lane_cnt" not in faults.apply_fault(spec(kind="schema_missing", column="lane_cnt"), payload, None)
    assert faults.apply_fault(spec(kind="schema_extra", column="vehicle_color", value="RED"), payload, None)[
        "vehicle_color"] == "RED"
    assert faults.apply_fault(spec(kind="ambiguous", column="crash_month"), payload, "Jule")["crash_month"] == "Jule"
    assert payload["crash_date"] == "06/22/2021"  # not changed in place

    envelope = {"record_id": "rid-123", "batch_id": 1, "table_name": "Crash1", "payload": payload}
    assert json.loads(faults.message(envelope, spec(kind="typo", column="crash_month"))) == envelope
    broken = faults.message(envelope, spec(kind="corrupt_record"))
    assert "rid-123" in broken
    with pytest.raises(ValueError):
        json.loads(broken)


def test_scenario_files_are_valid():
    import os

    from tools import fault_injector

    names = [f[:-5] for f in os.listdir(fault_injector.FAULTS_DIR) if f.endswith(".json")]
    assert {"jule", "all-kinds", "schema-rename"} <= set(names)
    kinds = set()
    for name in names:
        scenario = fault_injector.load_scenario(name)
        for i, entry in enumerate(scenario["faults"]):
            kinds.add(faults.normalize_spec(entry, i, COLUMNS)["kind"])
    assert kinds == set(faults.KINDS)  # all-kinds really covers the catalog
