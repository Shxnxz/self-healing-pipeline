"""Read-only evidence from the pipeline's Delta tables (no Spark, `deltalake` only).

  canonical_values(column)   the values a column may take, from ref_canonical_mappings.
                             The table is the list Silver 2 standardizes against, so
                             it is what "a valid value for the field" means at run time.
  sample_records(failure)    a few quarantined records that carry the failure, as
                             they looked in Silver 1 (quarantine keeps them in raw_record)

Both answer "nothing" when a table is missing or unreadable. Without canonical
values the policy gate cannot approve ADD_VARIANT, so an unreadable table sends the
failure to an engineer instead of stopping the agent.
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


def canonical_values(column: Optional[str], path: Optional[str] = None) -> List[str]:
    """Canonical values of a closed-vocabulary column, sorted. [] when it has none."""
    from deltalake import DeltaTable

    if not column:
        return []
    path = path or config.REF_MAPPINGS_PATH
    try:
        table = DeltaTable(path)  # reads the transaction log only
        cached = _canonical_cache.get(path)
        if cached is None or cached[0] != table.version():
            by_column: dict = {}
            for row in _scan(table, ["field_name", "canonical_value"]):
                if row.get("field_name") and row.get("canonical_value"):
                    by_column.setdefault(row["field_name"], set()).add(row["canonical_value"])
            cached = (table.version(), {name: sorted(values) for name, values in by_column.items()})
            _canonical_cache[path] = cached
    except Exception as exc:  # table not created yet, volume not mounted, ...
        print(f"[agent] canonical values of {column} not readable: {exc}", flush=True)
        return []
    return list(cached[1].get(column, []))


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
