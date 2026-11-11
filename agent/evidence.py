"""Does the data itself confirm a proposed canonical value? (policy gate, no model)

The model's confidence says how sure it is, not whether it is right. For some
columns the record carries the answer in another field, and then the gate checks
the proposal against it instead of trusting the confidence:

  crash_month        must be the month of crash_date
  crash_day_of_week  must be the weekday of crash_date

`check()` answers for a set of quarantined records of one failure:

  AGREES       at least one record decides the value, and every one that does
               decides for the proposal
  CONTRADICTS  at least one record rules the proposal out
  NONE         the column has no such rule, or no record decides anything

A slash or dash date is read both ways (04/07/2019 is 7 April or 4 July) because
the order depends on the source. It therefore decides a month only when both
readings give the same one, or when only one reading is a real date (22/06/2024).
The gate sees a handful of sample records, so AGREES is necessary, not sufficient:
agreement across every record of the failure is Validate's check (Task 105).
"""
import re
from datetime import date
from typing import Dict, List, Optional, Set

AGREES, CONTRADICTS, NONE = "AGREES", "CONTRADICTS", "NONE"

MONTHS = ["JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY", "AUGUST", "SEPTEMBER",
          "OCTOBER", "NOVEMBER", "DECEMBER"]
WEEKDAYS = ["MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"]

# column -> (field of the same record that decides it, canonical value for a date)
RULES: Dict[str, tuple] = {
    "crash_month": ("crash_date", lambda day: MONTHS[day.month - 1]),
    "crash_day_of_week": ("crash_date", lambda day: WEEKDAYS[day.weekday()]),
}

_YEAR_FIRST = re.compile(r"^\s*(\d{4})[-/](\d{1,2})[-/](\d{1,2})(?!\d)")
_YEAR_LAST = re.compile(r"^\s*(\d{1,2})[-/](\d{1,2})[-/](\d{4})(?!\d)")


def _date(year: int, month: int, day: int) -> Optional[date]:
    try:
        return date(year, month, day)
    except ValueError:
        return None


def date_readings(text) -> Set[date]:
    """Every calendar date the text can mean; empty when it is not a numeric date."""
    if not isinstance(text, str):
        return set()
    match = _YEAR_FIRST.match(text)
    if match:
        year, month, day = map(int, match.groups())
        return {d for d in [_date(year, month, day)] if d}
    match = _YEAR_LAST.match(text)
    if match:
        first, second, year = map(int, match.groups())
        return {d for d in (_date(year, first, second), _date(year, second, first)) if d}
    return set()


def check(column: Optional[str], canonical_value: str, records: List[dict]) -> dict:
    """Compare a proposed canonical value of `column` with the records that carry the failure."""
    rule = RULES.get(column)
    if rule is None:
        return {"verdict": NONE, "detail": f"no other field of a record determines {column}"}
    source, derive = rule

    confirming, against = 0, []
    for record in records:
        values = {derive(day) for day in date_readings(record.get(source))}
        if not values:
            continue  # unreadable date: says nothing
        if canonical_value not in values:
            against.append(f"{source} {record.get(source)!r} gives {' or '.join(sorted(values))}")
        elif len(values) == 1:
            confirming += 1
    if against:
        return {"verdict": CONTRADICTS, "detail": f"{len(against)} of {len(records)} sample records "
                f"rule out {canonical_value}: {against[0]}"}
    if confirming:
        return {"verdict": AGREES, "detail": f"{source} of {confirming} sample record(s) gives {canonical_value}, "
                                             "none says otherwise"}
    return {"verdict": NONE, "detail": f"no sample record has a {source} that decides {column}"}
