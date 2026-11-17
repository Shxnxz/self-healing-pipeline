"""The checks of Validate (spec VD3), on the result of a replay. Pure Python, no Spark.

agent/replay.py re-runs Silver 2's standardization on the staged group and reports,
for every quarantine row it found, what the row failed before and after. This module
decides from that report whether the fix holds. Every check is computed and listed,
also after one has failed, so the engineer sees the whole picture.

  resolution             the overlay is exactly the fix that was approved (one column,
                         the failure's raw value, one canonical value) and that value
                         is one of the column's canonical values in ref_canonical_mappings
  no_conflicting_mapping the raw value is not already mapped to a different canonical
                         value in ref_canonical_mappings
  rows_conserved         every staged record was found exactly once, nobody else has
                         resolved it, none is already in Silver 2, and the staged
                         records = the records released + the records that stay quarantined
  targeted_resolved      with the overlay, the targeted column no longer fails in ANY
                         staged record and its standardized value is the canonical value
  no_new_failures        no record fails a column after the replay that it did not fail
                         before
  agrees_with_crash_date crash_month / crash_day_of_week only: the canonical value is the
                         month / weekday of the standardized crash_date of EVERY record
                         that has one. The policy gate compared a handful of sample
                         records; this is the agreement across all rows of the group.
                         The date is the one Silver 2 parsed, in the source's own day
                         order, so 04/07/2019 is not ambiguous here.

A record that still fails another column after the replay is not an error: it stays
in quarantine for that other failure, and only the records with nothing left to fix
are released.
"""
from datetime import date
from typing import Dict, List, Optional

from agent.evidence import MONTHS, WEEKDAYS

WAITING = ("PENDING", "IN_PROGRESS")
RESOLVED = "RESOLVED"

# column -> the canonical value a standardized crash_date (yyyy-MM-dd) gives it
DATE_RULES = {
    "crash_month": lambda day: MONTHS[day.month - 1],
    "crash_day_of_week": lambda day: WEEKDAYS[day.weekday()],
}

AGREES, CONTRADICTS, NONE = "AGREES", "CONTRADICTS", "NONE"
EXAMPLES = 3


def _check(name: str, problems: List[str], ok: str) -> dict:
    return {"name": name, "passed": not problems, "detail": "; ".join(problems) if problems else ok}


def _ids(ids, limit: int = EXAMPLES) -> str:
    ids = sorted(ids)
    more = f" and {len(ids) - limit} more" if len(ids) > limit else ""
    return ", ".join(ids[:limit]) + more


def _same(value, canonical: str) -> bool:
    """A standardized value against a canonical value. Silver 2 casts some columns
    (hit_and_run_i to boolean, num_units to decimal), so compare their text."""
    return value is not None and str(value).strip().upper() == str(canonical).strip().upper()


def _parse(text) -> Optional[date]:
    try:
        return date.fromisoformat(str(text)[:10]) if text else None
    except ValueError:
        return None


def crash_date_agreement(column: str, canonical: str, rows: List[dict]) -> dict:
    """Does the crash_date of every record give `canonical` for `column`?"""
    derive = DATE_RULES.get(column)
    if derive is None:
        return {"verdict": NONE, "confirming": 0, "contradicting": 0,
                "detail": f"no other field of a record determines {column}"}
    confirming, against = 0, []
    for row in rows:
        day = _parse(row.get("crash_date"))
        if day is None:
            continue  # crash_date did not parse: the record stays quarantined anyway
        if derive(day) == canonical:
            confirming += 1
        else:
            against.append(f"{row['record_id']} (crash_date {day.isoformat()} gives {derive(day)})")
    if against:
        return {"verdict": CONTRADICTS, "confirming": confirming, "contradicting": len(against),
                "detail": f"{len(against)} of {len(rows)} records have a crash_date that rules out "
                          f"{canonical}: {', '.join(against[:EXAMPLES])}"}
    if confirming:
        return {"verdict": AGREES, "confirming": confirming, "contradicting": 0,
                "detail": f"the crash_date of all {confirming} dated record(s) gives {canonical}"}
    return {"verdict": NONE, "confirming": 0, "contradicting": 0,
            "detail": f"no record has a crash_date that decides {column}"}


def evaluate(*, execution_id: str, column: str, raw_value: str, canonical: str, overlay: dict,
             staged_ids: List[str], replay: dict) -> dict:
    """Run every check on a replay report (agent/replay.py) and say what can be released.

    `replay` = {"rows": [{"record_id", "healing_status", "resolved_by", "before": [columns],
    "after": {column: raw}, "value", "crash_date"}], "canonical_values": [...],
    "existing_mapping": canonical value the raw value already maps to or None,
    "in_silver2": [record ids already in the Silver 2 table], "released_before": bool}.
    """
    rows: List[dict] = replay["rows"]
    raw_key = (raw_value or "").strip().upper()
    checks = []

    # 1. The fix is the approved one, and its value is valid for the field.
    problems = []
    if overlay != {column: {raw_key: canonical}}:
        problems.append(f"the staged overlay {overlay} is not the approved fix {{{column}: {{{raw_key}: {canonical}}}}}")
    if canonical not in (replay.get("canonical_values") or []):
        problems.append(f"{canonical!r} is not a canonical value of {column} in ref_canonical_mappings")
    checks.append(_check("resolution", problems, f"{raw_key} -> {canonical}, a canonical value of {column}"))

    # 2. No existing entry maps the raw value elsewhere.
    existing = replay.get("existing_mapping")
    problems = [] if existing in (None, canonical) else [
        f"ref_canonical_mappings already maps {raw_key} to {existing}, not {canonical}"]
    checks.append(_check("no_conflicting_mapping", problems,
                         f"{raw_key} is already mapped to {canonical}" if existing else
                         f"{raw_key} has no entry in ref_canonical_mappings"))

    # 3. Row counts are conserved.
    by_id: Dict[str, List[dict]] = {}
    for row in rows:
        by_id.setdefault(row["record_id"], []).append(row)
    staged = set(staged_ids)
    missing = staged - set(by_id)
    duplicated = {rid for rid, found in by_id.items() if len(found) > 1}
    unexpected = set(by_id) - staged
    mine = {rid for rid, found in by_id.items()
            if all(r.get("healing_status") == RESOLVED and r.get("resolved_by") == execution_id for r in found)}
    taken = {rid for rid, found in by_id.items() if rid not in mine and any(
        (r.get("healing_status") or "PENDING") not in WAITING for r in found)}
    in_silver2 = set(replay.get("in_silver2") or [])
    # When this execution's append already landed (the step is running again, maybe
    # after stopping between the append and the quarantine update), its records are in
    # Silver 2 by design. Otherwise a staged record found there would be written twice.
    clash = set() if replay.get("released_before") else in_silver2 & staged

    single = {rid: found[0] for rid, found in by_id.items() if rid in staged and len(found) == 1}
    valid = {rid for rid, row in single.items() if not row.get("after")}
    remaining = {rid for rid, row in single.items() if row.get("after")}
    problems = []
    if missing:
        problems.append(f"{len(missing)} staged record(s) are not in the quarantine: {_ids(missing)}")
    if duplicated:
        problems.append(f"{len(duplicated)} record(s) are in the quarantine more than once: {_ids(duplicated)}")
    if unexpected:
        problems.append(f"the replay returned {len(unexpected)} record(s) that were not staged: {_ids(unexpected)}")
    if taken:
        problems.append(f"{len(taken)} staged record(s) were resolved by something else in the meantime: {_ids(taken)}")
    if clash:
        problems.append(f"{len(clash)} staged record(s) are already in Silver 2: {_ids(clash)}")
    if not problems and len(staged) != len(valid) + len(remaining):
        problems.append(f"{len(staged)} staged != {len(valid)} valid + {len(remaining)} still quarantined")
    checks.append(_check("rows_conserved", problems,
                         f"{len(staged)} staged = {len(valid)} valid + {len(remaining)} still quarantined"))

    # 4. The targeted column is fixed in every record, to the canonical value.
    still = {rid for rid, row in single.items() if column in (row.get("after") or {})}
    wrong = {rid for rid, row in single.items() if rid not in still and not _same(row.get("value"), canonical)}
    problems = []
    if still:
        problems.append(f"{column} still fails in {len(still)} of {len(staged)} records: {_ids(still)}")
    if wrong:
        example = single[sorted(wrong)[0]]
        problems.append(f"{column} became something other than {canonical} in {len(wrong)} records "
                        f"(e.g. {example['record_id']}: {example.get('value')!r})")
    checks.append(_check("targeted_resolved", problems,
                         f"{column} resolves to {canonical} in all {len(single)} records"))

    # 5. No new failure in another column.
    new = {}
    for rid, row in single.items():
        # The targeted column still failing is targeted_resolved's finding, not a new one.
        added = set(row.get("after") or {}) - set(row.get("before") or []) - {column}
        if added:
            new[rid] = sorted(added)
    problems = []
    if new:
        rid = sorted(new)[0]
        problems.append(f"{len(new)} record(s) fail a column they did not fail before "
                        f"(e.g. {rid}: {', '.join(new[rid])})")
    checks.append(_check("no_new_failures", problems, "no record fails a column it did not fail before"))

    # 6. Agreement with crash_date across every record of the group.
    agreement = crash_date_agreement(column, canonical, list(single.values()))
    if column in DATE_RULES:
        checks.append({"name": "agrees_with_crash_date", "passed": agreement["verdict"] != CONTRADICTS,
                       "detail": agreement["detail"]})

    passed = all(check["passed"] for check in checks)
    return {
        "passed": passed,
        "reasons": [f"{c['name']}: {c['detail']}" for c in checks if not c["passed"]],
        "checks": checks,
        "crash_date": agreement,
        "rows_staged": len(staged),
        "rows_valid": len(valid),
        "rows_remaining": len(remaining),
        # Valid records still waiting: what a release writes. Records this execution
        # released before are counted, not written again.
        "to_release": sorted(valid - mine) if passed else [],
        "already_released": sorted(valid & mine),
    }
