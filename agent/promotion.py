"""Promotion into ref_canonical_mappings (spec EL2): Learn's second job.

A fix that held in Validate healed one group of quarantined records through an
in-memory overlay. Promotion makes it permanent: the raw value becomes a verified
variant in `ref_canonical_mappings`, so Silver 2 maps it by exact lookup and the next
occurrence never reaches quarantine, the lookup or the model.

The spec keeps the table from growing with every misspelling: a variant is promoted
only

  at once                  when an engineer approved the fix (APPLY_FIX that passed
                           Validate; `--no-promote` on the review command declines it)
  when it recurs           when the same fix of the same value, made by the agent, has
                           healed at least PROMOTE_MIN_OCCURRENCES rows in at least
                           PROMOTE_MIN_BATCHES distinct micro-batches, each validated

and never when memory says the value is ambiguous:

  - another verified fix maps the same signature to a different value ("Ju" fixed to
    JUNE in one batch and to JULY in another: each group was right, the variant is not)
  - the table already maps the raw value to something else

An automatic promotion also waits while there is doubt, which an engineer approving
this very fix overrides (EL2):

  - the same fix failed Validate for the same signature before (some records disagreed)
  - an engineer declined promotion of this fix before

A fix borrowed from a similar incident that nobody approved (`borrowed`, see
agent/nodes/deterministic_lookup.py) never counts towards recurrence: promoting it
would turn a borrowed fix into a rule for every tier without the approval Thay asked
for on P1 (decision of 2026-11-14).

Only ADD_VARIANT is promoted; it is the only action with a mapping to add.

`decide()` is pure (tests drive it directly); `current_mapping()` and `promote()`
read and write the Delta table with `deltalake`, without Spark.
"""
import json
from datetime import datetime, timezone
from typing import Iterable, List, Optional, Tuple

from agent import actions, config
from agent.memory import incidents

# learned["promotion"]["status"]
PROMOTED = "PROMOTED"                  # the variant was added to ref_canonical_mappings now
ALREADY_IN_TABLE = "ALREADY_IN_TABLE"  # the table maps the raw value to this value already
BELOW_THRESHOLD = "BELOW_THRESHOLD"    # an automatic fix that has not recurred enough yet
BLOCKED = "BLOCKED"                    # memory or the table says the value is ambiguous
DECLINED = "DECLINED"                  # the engineer approved the fix but not the variant
NOT_APPLICABLE = "NOT_APPLICABLE"      # nothing to promote (no validated ADD_VARIANT)

ENGINEER_APPROVED = "ENGINEER_APPROVED"
RECURRENCE = "RECURRENCE"


def batch_key(table: Optional[str], batch: str) -> str:
    """Micro-batch ids restart at 0 in every source's Silver 2 stream, so a batch is
    only distinct together with its table: silver2_crash1:7 is not silver2_crash3:7."""
    return f"{table or '?'}:{batch}"


def _canonical(params) -> Optional[str]:
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except ValueError:
            return None
    return (params or {}).get("canonical_value") if isinstance(params, dict) else None


def _is_success(incident: dict) -> bool:
    return incident.get("outcome") == incidents.SUCCESS and incident.get("verified") is True


def counts(history: Iterable[dict], canonical: str) -> dict:
    """Rows and distinct batches healed by the same automatic or approved fix, from
    every incident that may count (not borrowed without approval)."""
    rows, batches, used = 0, set(), []
    for inc in history:
        if not _is_success(inc) or inc.get("action") != actions.ADD_VARIANT:
            continue
        if _canonical(inc.get("action_params")) != canonical or inc.get("borrowed"):
            continue
        rows += int(inc.get("rows_affected") or 0)
        batches.update(inc.get("batch_ids") or [])
        used.append(inc.get("incident_id"))
    return {"occurrences": rows, "batches": len(batches), "incidents": used}


def _ambiguity(history: Iterable[dict], canonical: str, incident_id: str) -> Tuple[List[str], List[str]]:
    """(conflicts, doubts). A conflict blocks every promotion: the value has been fixed
    to something else. A doubt blocks only the automatic one; an engineer who approves
    this very fix after it failed once, or was declined once, overrides it (EL2)."""
    conflicts, doubts = [], []
    for inc in history:
        if inc.get("incident_id") == incident_id or inc.get("action") != actions.ADD_VARIANT:
            continue
        other = _canonical(inc.get("action_params"))
        if _is_success(inc) and other and other != canonical:
            conflicts.append(f"incident {inc.get('incident_id')} fixed the same value to {other}")
        elif inc.get("outcome") == incidents.FAILED_VALIDATION and other == canonical:
            doubts.append(f"the same fix failed validation before (incident {inc.get('incident_id')})")
        if inc.get("promote_declined") and other == canonical:
            doubts.append(f"an engineer declined promoting this fix (incident {inc.get('incident_id')})")
    return conflicts, doubts


def decide(*, incident: dict, history: List[dict], existing: Optional[str], engineer_approved: bool,
           promote_requested: bool = True, min_occurrences: Optional[int] = None,
           min_batches: Optional[int] = None) -> dict:
    """What to do with the variant of the incident Learn just wrote.

    incident  {incident_id, action, action_params, outcome, verified, borrowed, column_name,
               raw_key, canonical_value}
    history   incidents.signature_history() for the same signature (includes this one)
    existing  the value ref_canonical_mappings maps raw_key to today, or None
    """
    min_occurrences = config.PROMOTE_MIN_OCCURRENCES if min_occurrences is None else min_occurrences
    min_batches = config.PROMOTE_MIN_BATCHES if min_batches is None else min_batches
    canonical = incident.get("canonical_value")
    result = {"status": NOT_APPLICABLE, "route": None, "reasons": [], "column": incident.get("column_name"),
              "raw_value": incident.get("raw_key"), "canonical_value": canonical,
              "needed": {"occurrences": min_occurrences or "NEVER", "batches": min_batches or "NEVER"}}
    if incident.get("action") != actions.ADD_VARIANT or not canonical or not incident.get("raw_key"):
        result["reasons"] = ["only a validated ADD_VARIANT adds a variant"]
        return result
    if not _is_success(incident):
        result["reasons"] = ["the fix did not pass validation"]
        return result
    result.update(counts(history, canonical))

    if existing == canonical:
        result.update(status=ALREADY_IN_TABLE, reasons=[f"ref_canonical_mappings already maps it to {canonical}"])
        return result
    conflicts, doubts = _ambiguity(history, canonical, incident.get("incident_id"))
    if existing is not None:
        conflicts.insert(0, f"ref_canonical_mappings maps it to {existing}, not {canonical}")
    blocked = conflicts if engineer_approved and promote_requested else conflicts + doubts
    if blocked:
        result.update(status=BLOCKED, reasons=blocked)
        return result
    if doubts:   # overridden by this engineer's approval
        result["overridden"] = doubts

    if engineer_approved:
        if not promote_requested:
            result.update(status=DECLINED, route=ENGINEER_APPROVED,
                          reasons=["the engineer approved the fix for these records only (--no-promote)"])
            return result
        result.update(status=PROMOTED, route=ENGINEER_APPROVED, reasons=["approved by an engineer (EL2)"])
        return result

    if incident.get("borrowed"):
        note = "borrowed from a similar incident without approval, so it does not count; "
    else:
        note = ""
    if not min_occurrences or not min_batches:
        result.update(status=BELOW_THRESHOLD, route=RECURRENCE,
                      reasons=[note + "automatic promotion is off (NEVER): only an engineer's fix is promoted"])
        return result
    if result["occurrences"] >= min_occurrences and result["batches"] >= min_batches:
        result.update(status=PROMOTED, route=RECURRENCE,
                      reasons=[f"healed {result['occurrences']} rows in {result['batches']} batches "
                               f"(needs {min_occurrences} in {min_batches})"])
        return result
    result.update(status=BELOW_THRESHOLD, route=RECURRENCE,
                  reasons=[note + f"healed {result['occurrences']} rows in {result['batches']} batches so far, "
                           f"needs {min_occurrences} rows in {min_batches} batches"])
    return result


# ----------------------------------------------------------------------------
# The Delta table
# ----------------------------------------------------------------------------
def _schema():
    import pyarrow as pa

    # Same columns and types as ref_mappings.REF_MAPPINGS_SCHEMA (Spark TimestampType = UTC).
    return pa.schema([
        pa.field("field_name", pa.string(), nullable=False),
        pa.field("raw_synonym", pa.string(), nullable=False),
        pa.field("canonical_value", pa.string(), nullable=False),
        pa.field("added_by", pa.string(), nullable=False),
        pa.field("added_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("confidence", pa.float64(), nullable=False),
        pa.field("source_batch_id", pa.string(), nullable=True),
    ])


def raw_key(value: Optional[str]) -> str:
    """How Silver 2 looks a value up (ref_mappings.load_mappings_dict): trimmed, upper case."""
    return (value or "").strip().upper()


def current_mapping(column: str, raw: str, path: Optional[str] = None) -> Optional[str]:
    """The canonical value the table maps (column, raw) to, the latest row winning as in
    load_mappings_dict. None when it has no row. Raises when the table cannot be read."""
    import pyarrow.dataset as ds
    from deltalake import DeltaTable

    table = DeltaTable(path or config.REF_MAPPINGS_PATH)
    rows = table.to_pyarrow_dataset().to_table(
        columns=["raw_synonym", "canonical_value", "added_at"],
        filter=ds.field("field_name") == column, use_threads=False).to_pylist()
    hits = [r for r in rows if raw_key(r["raw_synonym"]) == raw_key(raw)]
    if not hits:
        return None
    return max(hits, key=lambda r: r["added_at"] or datetime.min.replace(tzinfo=timezone.utc))["canonical_value"]


def promote(column: str, raw: str, canonical: str, added_by: str, source: str,
            path: Optional[str] = None) -> bool:
    """Add (column, RAW) -> canonical, keyed like the Silver 2 job's own promotion
    (ref_mappings.promote_fuzzy_variants): a MERGE on (field_name, raw_synonym) that
    inserts only when the pair is new, so running it twice adds one row. True when a
    row was inserted."""
    import pyarrow as pa
    from deltalake import DeltaTable

    row = {"field_name": column, "raw_synonym": raw_key(raw), "canonical_value": canonical,
           "added_by": added_by[:200], "added_at": datetime.now(timezone.utc), "confidence": 1.0,
           "source_batch_id": source[:200]}
    metrics = (DeltaTable(path or config.REF_MAPPINGS_PATH)
               .merge(source=pa.Table.from_pylist([row], schema=_schema()),
                      predicate="t.field_name = s.field_name AND t.raw_synonym = s.raw_synonym",
                      source_alias="s", target_alias="t")
               .when_not_matched_insert_all()
               .execute())
    return int((metrics or {}).get("num_target_rows_inserted") or 0) > 0
