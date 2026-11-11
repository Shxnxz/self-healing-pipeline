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


def _read(path: str, columns: List[str], column: str, values: list) -> List[dict]:
    """Rows of a Delta table where `column` is one of `values`."""
    import pyarrow.dataset as ds
    from deltalake import DeltaTable

    table = DeltaTable(path)
    available = {field.name for field in table.schema().fields}
    if column not in available:
        return []
    # Single-threaded on purpose: these reads are tiny, and a threaded scan of a
    # table with several files can keep the Python process from exiting.
    return table.to_pyarrow_dataset().to_table(
        columns=[c for c in columns if c in available],
        filter=ds.field(column).isin(values), use_threads=False,
    ).to_pylist()


def canonical_values(column: Optional[str], path: Optional[str] = None) -> List[str]:
    """Canonical values of a closed-vocabulary column, sorted. [] when it has none."""
    if not column:
        return []
    try:
        rows = _read(path or config.REF_MAPPINGS_PATH, ["canonical_value"], "field_name", [column])
    except Exception as exc:  # table not created yet, volume not mounted, ...
        print(f"[agent] canonical values of {column} not readable: {exc}", flush=True)
        return []
    return sorted({row["canonical_value"] for row in rows if row.get("canonical_value")})


def sample_records(failure: dict, limit: Optional[int] = None, path: Optional[str] = None) -> List[dict]:
    """Up to `limit` quarantined records of this failure, reduced to EVIDENCE_COLUMNS."""
    limit = config.DIAGNOSE_SAMPLE_RECORDS if limit is None else limit
    ids = list(failure.get("sample_record_ids") or [])[:limit]
    column = failure.get("column_name")
    if not ids or limit <= 0:
        return []
    try:
        rows = _read(path or config.QUARANTINE_PATH, ["record_id", "raw_record", "columns_to_remediate"],
                     "record_id", ids)
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
