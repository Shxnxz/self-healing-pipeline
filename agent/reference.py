"""Read-only evidence from the pipeline's Delta tables (no Spark, `deltalake` only).

  canonical_values(column)   the values a column may take, from ref_canonical_mappings.
                             The table is the list Silver 2 standardizes against, so
                             it is what "a valid value for the field" means at run time.
  sample_records(failure)    a few quarantined records that carry the failure, as
                             they looked in Silver 1 (quarantine keeps them in raw_record)

Both answer "nothing" when a table is missing or unreadable. Without canonical
values the policy gate cannot approve ADD_VARIANT, so an unreadable table sends the
failure to an engineer instead of stopping the agent.

Remediate uses two readers that raise instead, because "nothing there" and "could
not look" must not be confused when something is about to be executed:

  read_canonical_values(column)   canonical_values() without the safety net
  failure_records(failure)        every quarantined record that belongs to the failure
                                  and is still waiting: the group Remediate stages
"""
from typing import List, Optional

from agent import config

# The columns Silver 2 standardizes: a failing value is usually explained by one of
# the others in the same record (crash_month by crash_date, lighting by crash_hour).
EVIDENCE_COLUMNS = (
    "crash_date", "crash_hour", "crash_day_of_week", "crash_month", "date_police_notified",
    "weather_condition", "lighting_condition", "street_direction", "damage",
    "hit_and_run_i", "num_units", "posted_speed_limit",
)


# path -> (Delta table version, {column: [canonical values]}). The table is tiny and
# both Diagnose and the gate ask for every failure of a stack, so its files are
# scanned again only when a commit changed it.
_canonical_cache: dict = {}


def _scan(table, columns: List[str], **kwargs) -> List[dict]:
    # Single-threaded on purpose: these reads are tiny, and a threaded scan of a
    # table with several files can keep the Python process from exiting.
    return table.to_pyarrow_dataset().to_table(columns=columns, use_threads=False, **kwargs).to_pylist()


def read_canonical_values(column: Optional[str], path: Optional[str] = None) -> List[str]:
    """Canonical values of a closed-vocabulary column, sorted. [] when it has none.
    Raises when the reference table cannot be read."""
    from deltalake import DeltaTable

    if not column:
        return []
    path = path or config.REF_MAPPINGS_PATH
    table = DeltaTable(path)  # reads the transaction log only
    cached = _canonical_cache.get(path)
    if cached is None or cached[0] != table.version():
        by_column: dict = {}
        for row in _scan(table, ["field_name", "canonical_value"]):
            if row.get("field_name") and row.get("canonical_value"):
                by_column.setdefault(row["field_name"], set()).add(row["canonical_value"])
        cached = (table.version(), {name: sorted(values) for name, values in by_column.items()})
        _canonical_cache[path] = cached
    return list(cached[1].get(column, []))


def canonical_values(column: Optional[str], path: Optional[str] = None) -> List[str]:
    """read_canonical_values() that answers [] instead of raising (Diagnose, policy gate)."""
    try:
        return read_canonical_values(column, path)
    except Exception as exc:  # table not created yet, volume not mounted, ...
        print(f"[agent] canonical values of {column} not readable: {exc}", flush=True)
        return []


def sample_records(failure: dict, limit: Optional[int] = None, path: Optional[str] = None) -> List[dict]:
    """Up to `limit` quarantined records of this failure, reduced to EVIDENCE_COLUMNS."""
    limit = config.DIAGNOSE_SAMPLE_RECORDS if limit is None else limit
    ids = list(failure.get("sample_record_ids") or [])[:limit]
    column = failure.get("column_name")
    if not ids or limit <= 0:
        return []
    try:
        import pyarrow.dataset as ds
        from deltalake import DeltaTable

        rows = _scan(DeltaTable(path or config.QUARANTINE_PATH),
                     ["record_id", "raw_record", "columns_to_remediate"], filter=ds.field("record_id").isin(ids))
    except Exception as exc:
        print(f"[agent] sample records not readable: {exc}", flush=True)
        return []

    samples, seen = [], set()
    for row in rows:
        failing = row.get("columns_to_remediate") or {}
        failing = failing if isinstance(failing, dict) else dict(failing)
        # The same record_id can sit in quarantine for another column or folder.
        if row["record_id"] in seen or (column and column not in failing):
            continue
        seen.add(row["record_id"])
        raw = row.get("raw_record") or {}
        samples.append({name: raw[name] for name in EVIDENCE_COLUMNS if raw.get(name) is not None})
    return samples[:limit]


# healing_status values of a quarantined row that is still waiting for a fix. The
# trigger may mark a group IN_PROGRESS when it picks it up (spec section 3); a
# RESOLVED row is finished and is never staged again.
WAITING = ("PENDING", "IN_PROGRESS")
_PER_SOURCE_LAYERS = ("bronze", "silver1", "silver2")


def _folder_of(row: dict) -> Optional[str]:
    """The folder a quarantine row belongs to, as lookup/failures.py groups it:
    "silver2_crash3" -> "crash3", else the producer's table_name."""
    layer, sep, rest = (row.get("target_table") or "").partition("_")
    if sep and layer in _PER_SOURCE_LAYERS and rest and "_" not in rest and rest != "quarantine":
        return rest
    return row.get("table_name")


def _key(value) -> str:
    return (value or "").strip().upper()


def failure_batches(failure: dict) -> List[str]:
    """The Silver 2 micro-batches the failure was found in (lookup/failures.py).

    The union of `batch_id` and `evidence.quarantine_batches`: when the lookup merges
    two publishes of the same group (lookup/stack.py merge_failures) it joins the
    batch ids but keeps the evidence of the first one, so neither alone is complete.
    """
    listed = list((failure.get("evidence") or {}).get("quarantine_batches") or [])
    if failure.get("batch_id"):
        listed += str(failure["batch_id"]).split(",")
    return sorted({str(b).strip() for b in listed if str(b).strip()})


def failure_records(failure: dict, path: Optional[str] = None) -> dict:
    """The waiting quarantine rows of one failure: same folder, column, raw value
    (trimmed, upper-cased) and micro-batches as the group the scanner published.

    Returns {"version": Delta version read, "rows": [{"record_id", "target_table",
    "other_failures": the row also fails another column}], "not_waiting": rows of the
    group that are no longer waiting (RESOLVED)}. Raises if the table cannot be read.
    """
    import pyarrow.dataset as ds
    from deltalake import DeltaTable

    column, value = failure.get("column_name"), _key(failure.get("raw_value"))
    folder, batches = _key(failure.get("folder")), failure_batches(failure)
    table = DeltaTable(path or config.QUARANTINE_PATH)
    available = {field.name for field in table.schema().fields}
    wanted = ["record_id", "table_name", "target_table", "silver2_batch_id", "columns_to_remediate",
              "healing_status"]
    # raw_record is left out on purpose: it is the wide part of the table.
    scanned = table.to_pyarrow_dataset().to_table(
        columns=[c for c in wanted if c in available],
        filter=ds.field("silver2_batch_id").isin(batches) if batches and "silver2_batch_id" in available else None,
        use_threads=False,
    ).to_pylist()

    rows, not_waiting = [], 0
    for row in scanned:
        failing = row.get("columns_to_remediate") or {}
        failing = failing if isinstance(failing, dict) else dict(failing)
        if _key(_folder_of(row)) != folder or not column or column not in failing \
                or _key(failing[column]) != value:
            continue
        if (row.get("healing_status") or "PENDING") not in WAITING:
            not_waiting += 1
            continue
        rows.append({"record_id": row.get("record_id"), "target_table": row.get("target_table"),
                     "other_failures": len(failing) > 1})
    return {"version": table.version(), "rows": rows, "not_waiting": not_waiting}
