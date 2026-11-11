"""What the records say about a proposed value: python -m pytest agent/tests"""
from datetime import date

import pytest

from agent import evidence


@pytest.mark.parametrize("text, expected", [
    ("2024-06-22", {date(2024, 6, 22)}),
    ("2024-06-22T13:05:00", {date(2024, 6, 22)}),
    ("2024/6/2 08:00", {date(2024, 6, 2)}),
    ("07/21/2019 08:00:00 AM", {date(2019, 7, 21)}),                 # only month-first is a real date
    ("22/06/2024", {date(2024, 6, 22)}),                             # only day-first is
    ("04/07/2019", {date(2019, 4, 7), date(2019, 7, 4)}),            # 7 April or 4 July
    (" 07-07-2019", {date(2019, 7, 7)}),
    ("2024-13-40", set()), ("31/31/2019", set()), ("yesterday", set()), ("", set()), (None, set()),
    (20240622, set()), ("2024-06-221", set()), ("12/25/20191", set()),
])
def test_date_readings(text, expected):
    assert evidence.date_readings(text) == expected


def month(value, *dates):
    return evidence.check("crash_month", value, [{"crash_date": d} for d in dates])


def test_a_record_that_decides_for_the_value_agrees():
    assert month("JUNE", "2024-06-22")["verdict"] == evidence.AGREES
    assert month("JUNE", "22/06/2024", "06/23/2024", "not a date")["verdict"] == evidence.AGREES


def test_a_record_that_rules_the_value_out_contradicts_whatever_the_others_say():
    found = month("JULY", "2024-07-01", "2024-07-02", "2024-06-30")
    assert found["verdict"] == evidence.CONTRADICTS
    assert found["detail"] == "1 of 3 sample records rule out JULY: crash_date '2024-06-30' gives JUNE"


def test_a_date_that_reads_both_ways_decides_nothing_alone():
    assert month("JULY", "04/07/2019")["verdict"] == evidence.NONE     # 7 April or 4 July
    assert month("APRIL", "04/07/2019")["verdict"] == evidence.NONE
    # ... but it still rules out every other month
    found = month("JUNE", "04/07/2019")
    assert found["verdict"] == evidence.CONTRADICTS and "gives APRIL or JULY" in found["detail"]
    # ... and decides when both readings are the same month
    assert month("JULY", "07/07/2019")["verdict"] == evidence.AGREES


def test_no_readable_date_is_no_evidence():
    assert month("JULY")["verdict"] == evidence.NONE
    assert month("JULY", None, "soon", 7)["verdict"] == evidence.NONE
    assert evidence.check("crash_month", "JULY", [{"crash_month": "Jule"}])["verdict"] == evidence.NONE


def test_weekday_of_the_date():
    saturday = [{"crash_date": "2024-06-22"}]
    assert evidence.check("crash_day_of_week", "SATURDAY", saturday)["verdict"] == evidence.AGREES
    assert evidence.check("crash_day_of_week", "SUNDAY", saturday)["verdict"] == evidence.CONTRADICTS


@pytest.mark.parametrize("column", ["weather_condition", "lighting_condition", "crash_date", None])
def test_a_column_no_other_field_determines_has_no_rule(column):
    found = evidence.check(column, "FOG", [{"crash_date": "2024-06-22"}])
    assert found["verdict"] == evidence.NONE and "no other field" in found["detail"]


def test_the_values_are_the_ones_the_pipeline_standardizes_to():
    """A rename in spark-jobs/ref_mappings.py must not turn every check into CONTRADICTS.
    The seed file needs Spark to import, so its dictionary is read from the source."""
    import ast
    from pathlib import Path

    source = (Path(__file__).resolve().parents[2] / "spark-jobs" / "ref_mappings.py").read_text(encoding="utf-8")
    seeds = []
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Dict):
            try:
                value = ast.literal_eval(node)
            except ValueError:
                continue
            if isinstance(value.get("crash_day_of_week"), dict) and isinstance(value.get("crash_month"), dict):
                seeds.append(value)
    assert seeds, "seed dictionary not found in spark-jobs/ref_mappings.py"
    assert set(seeds[0]["crash_day_of_week"]) == set(evidence.WEEKDAYS)
    assert set(seeds[0]["crash_month"]) == set(evidence.MONTHS)
