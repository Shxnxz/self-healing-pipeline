"""
Pipeline Data Flow Report (HTML)
================================

Writes one self-contained, printable HTML page that shows how data moves through the
medallion layers, for putting in a report:

  1. Row counts per source: Bronze -> Silver 1 -> Silver 2 / quarantine -> Gold
  2. Record journeys: a few real records followed layer by layer, one per kind of path
     (standardized, fuzzy-fixed, quarantined then healed, still quarantined, rejected
     by the Silver 1 schema check). Values that change are highlighted.
  3. Sample rows of every layer, with the same highlighting
  4. Healing activity: fuzzy fixes, recovery agent fixes, dictionary rules learned,
     Silver 1 schema mismatches

It reads the tables exactly like read_parquet_v3.py (it imports it), so it needs the
same packages: duckdb, pandas, deltalake. pyarrow is optional and only used when DuckDB
cannot read a table. The page itself has no external links, scripts or fonts.

USAGE (from the repo root, PowerShell):
  python "Read Data/read_parquet_report.py"
  python "Read Data/read_parquet_report.py" --source crash4
  python "Read Data/read_parquet_report.py" --rows 8 --per-story 2 --seed 7
  python "Read Data/read_parquet_report.py" --id <record_id> --id <record_id>
  python "Read Data/read_parquet_report.py" --cols weather_condition,lighting_condition,damage
  python "Read Data/read_parquet_report.py" --out report\\pipeline_flow.html --no-open

OPTIONS:
  --source NAME     only one data folder (crash1, crash2, ...)
  --rows N          sample rows per layer table (default 6)
  --per-story N     records per journey kind (default 1)
  --id RECORD_ID    also follow this record (repeatable); prefixes work
  --cols a,b,c      columns for the layer sample tables (default: a curated set)
  --all-fields      journey tables list every field, not only the ones that changed
  --seed N          which sample records get picked (same seed = same page)
  --out PATH        output file (default: Read Data/pipeline_flow_report.html)
  --no-open         do not open the page in the browser

Print to PDF from the browser (Ctrl+P). The page prints in landscape with the colors kept.
"""

import argparse
import glob
import html
import json
import math
import os
import sys
import webbrowser
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if SCRIPT_DIR not in sys.path:
    sys.path.insert(0, SCRIPT_DIR)

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
from deltalake import DeltaTable  # noqa: E402

import read_parquet_v3 as v3  # noqa: E402  (same table discovery and Delta snapshot reading)

# ----------------------------------------------------------------------------------------
# Reading tables (through read_parquet_v3, with a pyarrow fallback)
# ----------------------------------------------------------------------------------------

WARNINGS: List[str] = []
_FALLBACK: Dict[Tuple[str, Optional[str]], str] = {}
_MISSING = object()  # a column the layer does not have (not the same as a null value)

META_FIELDS = {
    "record_id", "batch_id", "table_name", "ingestion_ts", "topic", "kafka_timestamp", "partition",
    "offset", "ingested_at", "source_table", "target_table", "kafka_key", "raw_value",
    "columns_to_remediate", "raw_record", "quarantined_at", "silver2_batch_id", "healing_status",
    "resolved_by", "resolved_at", "crash_record_id",
}
GOLD_DERIVED = ["crash_year", "is_weekend", "time_of_day", "is_injury_crash", "is_fatal_crash", "is_injected"]
DEFAULT_COLS = ["crash_date", "crash_hour", "crash_month", "crash_day_of_week", "weather_condition",
                "lighting_condition", "num_units", "damage"]
LAYER_EXTRA_COLS = {
    "quarantine": ["healing_status"],
    "gold": ["crash_year", "time_of_day"],
}
LAYER_TITLES = {
    "bronze": ("Bronze", "Raw Kafka messages, payload as sent by the producer"),
    "silver1": ("Silver 1", "Flattened, schema-checked, one column per field"),
    "quarantine": ("Quarantine", "Silver 2 rows held back because a value could not be standardized"),
    "silver2": ("Silver 2", "Standardized with the reference dictionary and fuzzy matching"),
    "gold": ("Gold", "Business-ready union of every Silver 2 table, with derived columns"),
}


def sq(value: Any) -> str:
    return v3._sql_str(str(value))


def _delta_files(path: str) -> List[str]:
    dt = DeltaTable(path)
    try:
        return [os.path.join(path, f) for f in dt.files()]
    except Exception:
        from urllib.parse import unquote, urlparse
        from urllib.request import url2pathname
        out = []
        for uri in dt.file_uris():
            out.append(url2pathname(unquote(urlparse(uri).path)) if uri.startswith("file:") else uri)
        return out


def _pyarrow_view(key: str, source: Optional[str]) -> str:
    """Reads a table file by file with pyarrow and registers it in DuckDB as a view."""
    import pyarrow.parquet as pq  # optional dependency, only needed here

    layer = v3.layer_of(key)
    tables = v3.LAYERS[key]
    if source and layer in v3.PER_SOURCE_LAYERS and key == layer:
        tables = v3.LAYERS[f"{layer}_{source}"]
    frames = []
    for name, path, tag in tables:
        if os.path.isdir(os.path.join(path, "_delta_log")):
            files = _delta_files(path)
        else:
            files = sorted(glob.glob(os.path.join(path, "**", "*.parquet"), recursive=True))
        for f in files:
            df = pq.read_table(f).to_pandas()
            if tag:
                df["source_table"] = name
            frames.append(df)
    if not frames:
        raise FileNotFoundError(f"No data yet for '{key}'")
    df = pd.concat(frames, ignore_index=True, sort=False)
    col = v3.SHARED_SOURCE_COLUMN.get(layer)
    if source and col and col in df.columns:
        df = df[df[col] == f"silver2_{source}"]
    view = f"fallback_{layer}_{source or 'all'}"
    duckdb.register(view, df)
    return view


def query(key: str, sql: str, source: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Runs `sql` with {t} replaced by the table's FROM source. Returns None when the table
    has no data yet. If DuckDB cannot read the files, retries once through pyarrow.
    """
    fk = (key, source)
    if _FALLBACK.get(fk) == "__unreadable__":
        return None
    try:
        src = _FALLBACK.get(fk) or (v3.layer_source(key, source) if key in v3.LAYERS else v3.table_source(key))
    except FileNotFoundError:
        return None
    try:
        return duckdb.sql(sql.format(t=src)).df()
    except Exception as exc:
        if fk in _FALLBACK or key not in v3.LAYERS:
            WARNINGS.append(f"Could not query {key}: {str(exc).splitlines()[0][:200]}")
            return None
        try:
            _FALLBACK[fk] = _pyarrow_view(key, source)
        except ImportError:
            WARNINGS.append(f"DuckDB could not read {key} ({str(exc).splitlines()[0][:160]}) and pyarrow is "
                            f"not installed for the fallback (pip install pyarrow). {key} is left out.")
            _FALLBACK[fk] = "__unreadable__"
            return None
        except Exception as exc2:
            WARNINGS.append(f"Could not read {key}: {str(exc2).splitlines()[0][:200]}")
            _FALLBACK[fk] = "__unreadable__"
            return None
        note = (f"DuckDB could not read {key} ({str(exc).splitlines()[0][:160]}), so it was read with pyarrow instead.")
        if note not in WARNINGS:
            WARNINGS.append(note)
        try:
            return duckdb.sql(sql.format(t=_FALLBACK[fk])).df()
        except Exception as exc3:
            WARNINGS.append(f"Could not query {key}: {str(exc3).splitlines()[0][:200]}")
            return None


def scalar(key: str, sql: str, source: Optional[str] = None) -> Optional[int]:
    df = query(key, sql, source)
    if df is None or len(df) == 0:
        return None
    val = df.iloc[0, 0]
    return None if val is None or (isinstance(val, float) and math.isnan(val)) else int(val)


def check_folders() -> None:
    for key in ("gold", "quarantine"):
        for _name, path, _tag in v3.LAYERS[key]:
            if os.path.isdir(path) and not os.path.isdir(os.path.join(path, "_delta_log")) \
                    and glob.glob(os.path.join(path, "*.parquet")):
                WARNINGS.append(
                    f"{os.path.relpath(path, v3.PROJECT_ROOT)} has Parquet files but no _delta_log folder, so every "
                    f"file in it was read, including files Delta would have removed. Rows are counted once per record_id.")


# ----------------------------------------------------------------------------------------
# Value helpers
# ----------------------------------------------------------------------------------------

def is_null(v: Any) -> bool:
    if v is None or v is _MISSING:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    try:
        if not isinstance(v, (list, dict, tuple)) and pd.isna(v):
            return True
    except (TypeError, ValueError):
        pass
    return str(v).strip() in ("", "None", "nan", "NaN", "<NA>", "NaT")


def norm(v: Any):
    """Comparable form of a value: '30' and 30 are the same, 'Clear' and 'CLEAR' are not."""
    if is_null(v):
        return None
    s = str(v).strip()
    try:
        return ("n", float(s))
    except ValueError:
        return ("s", s)


def as_dict(m: Any) -> Dict[str, Any]:
    """columns_to_remediate comes back as a dict, a list of pairs or a key/value struct."""
    if m is None or (isinstance(m, float) and math.isnan(m)):
        return {}
    if isinstance(m, dict):
        if set(m.keys()) == {"key", "value"} and isinstance(m["key"], (list, tuple)):
            return dict(zip(m["key"], m["value"]))
        return dict(m)
    if isinstance(m, str):
        try:
            return dict(json.loads(m))
        except Exception:
            return {"?": m}
    try:
        return {k: v for k, v in list(m)}
    except Exception:
        return {"?": str(m)}


def as_list(v: Any) -> List[Any]:
    if v is None:
        return []
    if isinstance(v, str):
        try:
            return list(json.loads(v))
        except Exception:
            return [v]
    try:
        return list(v)
    except TypeError:
        return [v]


def esc(v: Any) -> str:
    return html.escape(str(v), quote=True)


def show(v: Any, limit: int = 60) -> str:
    """HTML for one cell value."""
    if v is _MISSING:
        return '<span class="na">not in layer</span>'
    if is_null(v):
        return '<span class="null">null</span>'
    if isinstance(v, dict):
        s = ", ".join(f"{k}: {val}" for k, val in v.items())
    elif isinstance(v, (pd.Timestamp, datetime)):
        ts = pd.Timestamp(v)
        s = ts.strftime("%Y-%m-%d") if (ts.hour, ts.minute, ts.second, ts.microsecond) == (0, 0, 0, 0) \
            else ts.strftime("%Y-%m-%d %H:%M")
    else:
        s = str(v).strip()
    if len(s) > limit:
        return f'<span title="{esc(s)}">{esc(s[:limit - 1])}&hellip;</span>'
    return esc(s)


def short_id(v: Any) -> str:
    s = "" if is_null(v) else str(v)
    return f'<span class="mono" title="{esc(s)}">{esc(s[:8])}</span>' if s else '<span class="null">null</span>'


def row_dict(df: Optional[pd.DataFrame]) -> Optional[Dict[str, Any]]:
    if df is None or len(df) == 0:
        return None
    return df.iloc[0].to_dict()


def branch_of(table_value: Any) -> Optional[str]:
    """silver1_crash3 / silver2_crash3 / Crash3 -> crash3"""
    if is_null(table_value):
        return None
    s = str(table_value).lower()
    return s.split("_", 1)[1] if "_" in s else s


# ----------------------------------------------------------------------------------------
# Overview counts
# ----------------------------------------------------------------------------------------

def overview(sources: List[str]) -> Tuple[List[Dict[str, Any]], Dict[str, Optional[int]]]:
    q_status = query("quarantine", "SELECT target_table, healing_status, COUNT(*) AS n FROM {t} GROUP BY ALL")
    dl = query("s1_dead_letter", "SELECT lower(CAST(source_table AS VARCHAR)) AS s, COUNT(*) AS n FROM {t} GROUP BY ALL")
    gold = query("gold", "SELECT source_table, COUNT(DISTINCT record_id) AS n FROM {t} GROUP BY ALL")
    rows = []
    for s in sources:
        branch = f"silver2_{s}"
        q_open = q_res = None
        if q_status is not None:
            sub = q_status[q_status["target_table"] == branch]
            q_res = int(sub[sub["healing_status"] == "RESOLVED"]["n"].sum())
            q_open = int(sub["n"].sum()) - q_res
        r = {
            "source": s,
            "bronze": scalar(f"bronze_{s}", "SELECT COUNT(*) FROM {t}"),
            "silver1": scalar(f"silver1_{s}", "SELECT COUNT(*) FROM {t}"),
            "dead": int(dl[dl["s"] == s]["n"].sum()) if dl is not None else None,
            "silver2": scalar(f"silver2_{s}", "SELECT COUNT(*) FROM {t}"),
            "q_open": q_open,
            "q_res": q_res,
            "gold": int(gold[gold["source_table"] == branch]["n"].sum()) if gold is not None else None,
        }
        if r["silver1"] is not None:
            r["waiting"] = max(0, r["silver1"] - (r["silver2"] or 0) - (q_open or 0))
        else:
            r["waiting"] = None
        rows.append(r)
    totals = {}
    for k in ("bronze", "silver1", "dead", "silver2", "q_open", "q_res", "gold", "waiting"):
        vals = [r[k] for r in rows if r[k] is not None]
        totals[k] = sum(vals) if vals else None
    return rows, totals


def fmt_n(n: Optional[int]) -> str:
    return "&ndash;" if n is None else f"{n:,}"


def render_overview(rows: List[Dict[str, Any]], totals: Dict[str, Optional[int]]) -> str:
    def stage(title, n, sub, cls=""):
        return (f'<div class="stage {cls}"><div class="st-title">{title}</div>'
                f'<div class="st-n">{fmt_n(n)}</div><div class="st-sub">{sub}</div></div>')

    arrow = '<div class="arrow" aria-hidden="true">&rarr;</div>'
    strip = (
        '<div class="flow">'
        + stage("Bronze", totals["bronze"], "raw messages", "b")
        + arrow
        + stage("Silver 1", totals["silver1"],
                f'schema-checked<br><span class="bad">{fmt_n(totals["dead"])} rejected (schema mismatch)</span>', "s1")
        + arrow
        + stage("Silver 2", totals["silver2"],
                f'standardized<br><span class="bad">{fmt_n(totals["q_open"])} in quarantine</span><br>'
                f'<span class="good">{fmt_n(totals["q_res"])} healed and released</span>', "s2")
        + arrow
        + stage("Gold", totals["gold"], "business-ready", "g")
        + '</div>'
    )
    head = ("<tr><th>Source</th><th>Bronze</th><th>Silver 1</th><th>Rejected at Silver 1</th><th>Silver 2</th>"
            "<th>In quarantine</th><th>Healed from quarantine</th><th>Not past Silver 2 yet</th><th>Gold</th></tr>")
    body = ""
    for r in rows + [dict(totals, source="All sources")]:
        cls = ' class="total"' if r["source"] == "All sources" else ""
        body += (f"<tr{cls}><td>{esc(r['source'])}</td><td class='num'>{fmt_n(r['bronze'])}</td>"
                 f"<td class='num'>{fmt_n(r['silver1'])}</td><td class='num'>{fmt_n(r['dead'])}</td>"
                 f"<td class='num'>{fmt_n(r['silver2'])}</td><td class='num'>{fmt_n(r['q_open'])}</td>"
                 f"<td class='num'>{fmt_n(r['q_res'])}</td><td class='num'>{fmt_n(r['waiting'])}</td>"
                 f"<td class='num'>{fmt_n(r['gold'])}</td></tr>")
    note = ('<p class="note">Bronze = Silver 1 + rejected at Silver 1 once the stream has caught up. '
            '"Healed from quarantine" rows are counted in Silver 2 too. "Not past Silver 2 yet" is Silver 1 rows '
            'found neither in Silver 2 nor in open quarantine when this page was made (still streaming, or not '
            'processed). Gold counts Silver 2 rows by the branch they came from.</p>')
    return strip + f'<table class="grid counts"><thead>{head}</thead><tbody>{body}</tbody></table>' + note


# ----------------------------------------------------------------------------------------
# Record journeys
# ----------------------------------------------------------------------------------------

TITLES_SCHEMA = "Rejected at Silver 1 (schema mismatch)"
STORY_KINDS = [
    ("standardized", "Clean path, standardized on the way"),
    ("fuzzy", "Typo fixed on entry by fuzzy matching"),
    ("healed", "Quarantined, then healed by the recovery agent"),
    ("open", "Still in quarantine"),
    ("schema", "Rejected at Silver 1 (schema mismatch)"),
]


def pick_records(per_story: int, seed: int, source: Optional[str]) -> List[Tuple[str, str]]:
    """(kind, record_id) pairs, picked the same way for the same seed."""
    order = f"ORDER BY hash(CAST(record_id AS VARCHAR) || {sq(seed)})"
    picks: List[Tuple[str, str]] = []

    def take(kind, df):
        if df is not None:
            for rid in df["record_id"].dropna().astype(str).tolist()[:per_story]:
                picks.append((kind, rid))

    fuzzy_ids = query("audit", "SELECT DISTINCT record_id FROM {t}")
    fuzzy_list = ", ".join(sq(x) for x in fuzzy_ids["record_id"].dropna()) if fuzzy_ids is not None and len(fuzzy_ids) else ""
    q_ids = query("quarantine", "SELECT DISTINCT record_id FROM {t}", source)
    q_list = ", ".join(sq(x) for x in q_ids["record_id"].dropna()) if q_ids is not None and len(q_ids) else ""

    excl = []
    if fuzzy_list:
        excl.append(f"record_id NOT IN ({fuzzy_list})")
    if q_list:
        excl.append(f"record_id NOT IN ({q_list})")
    where = ("WHERE " + " AND ".join(excl)) if excl else ""
    take("standardized", query("silver2", f"SELECT record_id FROM {{t}} {where} {order} LIMIT {per_story}", source))

    if fuzzy_list:
        df = query("silver1", f"SELECT record_id FROM {{t}} WHERE record_id IN ({fuzzy_list}) {order} LIMIT {per_story * 4}", source)
        if df is not None and len(df):
            s2 = query("silver2", "SELECT record_id FROM {t} WHERE record_id IN ("
                       + ", ".join(sq(x) for x in df["record_id"]) + ")", source)
            in_s2 = set(s2["record_id"]) if s2 is not None else set()
            df = df.assign(_in=df["record_id"].isin(in_s2)).sort_values("_in", ascending=False, kind="stable")
        take("fuzzy", df)

    if q_list:
        healed = query("quarantine", f"SELECT record_id FROM {{t}} WHERE healing_status = 'RESOLVED' {order} LIMIT {per_story}", source)
        take("healed", healed)
        take("open", query("quarantine", f"SELECT record_id FROM {{t}} WHERE healing_status IS DISTINCT FROM 'RESOLVED' "
                                         f"AND record_id NOT IN (SELECT record_id FROM {{t}} WHERE healing_status = 'RESOLVED') "
                                         f"{order} LIMIT {per_story}", source))

    where = f"WHERE lower(CAST(source_table AS VARCHAR)) = {sq(source)}" if source else ""
    take("schema", query("s1_dead_letter", f"SELECT record_id FROM {{t}} {where} {order} LIMIT {per_story}"))
    return picks


def fetch_journey(rec_id: str, source: Optional[str]) -> Dict[str, Any]:
    """Every layer's row for one record, found within the record's own branch."""
    like = f"(record_id = {sq(rec_id)} OR record_id LIKE {sq(rec_id + '%')})"
    out: Dict[str, Any] = {}
    s1 = query("silver1", f"SELECT * FROM {{t}} WHERE {like} LIMIT 1", source)
    out["silver1"] = row_dict(s1)
    dead = query("s1_dead_letter", f"SELECT * FROM {{t}} WHERE {like} LIMIT 1")
    out["dead"] = row_dict(dead)
    rid = (out["silver1"] or out["dead"] or {}).get("record_id") or rec_id
    branch = source or branch_of((out["silver1"] or {}).get("source_table")) or branch_of((out["dead"] or {}).get("source_table"))
    eq = f"record_id = {sq(rid)}"

    b = query("bronze", f"SELECT * FROM {{t}} WHERE kafka_key = {sq(rid)} LIMIT 1", branch)
    if (b is None or len(b) == 0):
        b = query("bronze", f"SELECT * FROM {{t}} WHERE raw_value LIKE {sq('%' + str(rid) + '%')} LIMIT 1", branch)
    out["bronze"] = row_dict(v3.unpack_bronze_record(b)) if b is not None and len(b) else None
    if out["bronze"] is None and out["dead"] is not None:
        out["bronze"] = row_dict(v3.unpack_bronze_record(pd.DataFrame([out["dead"]])))
    out["quarantine"] = row_dict(query("quarantine", f"SELECT * FROM {{t}} WHERE {eq} ORDER BY quarantined_at DESC LIMIT 1", branch))
    out["silver2"] = row_dict(query("silver2", f"SELECT * FROM {{t}} WHERE {eq} LIMIT 1", branch))
    out["gold"] = row_dict(query("gold", f"SELECT * FROM {{t}} WHERE {eq} LIMIT 1", branch))
    a = query("audit", f"SELECT * FROM {{t}} WHERE {eq}")
    out["audit"] = a.to_dict("records") if a is not None else []
    ra_path = os.path.join(v3.DELTA_ROOT, "remediation_audit")
    rem = None
    if os.path.isdir(ra_path):
        rem = query(ra_path, f"SELECT * FROM {{t}} WHERE list_contains(record_ids, {sq(rid)}) ORDER BY executed_at DESC")
    out["fixes"] = rem.to_dict("records") if rem is not None else []
    out["record_id"] = str(rid)
    out["branch"] = branch
    return out


JOURNEY_LAYERS = ["bronze", "silver1", "quarantine", "silver2", "gold"]


def journey_story(kind: str, j: Dict[str, Any], changes: Dict[str, Dict[str, str]]) -> str:
    q = j.get("quarantine") or {}
    flagged = as_dict(q.get("columns_to_remediate"))
    if j.get("dead"):
        d = j["dead"]
        missing = ", ".join(map(str, as_list(d.get("_expected_only")))) or "none"
        extra = ", ".join(map(str, as_list(d.get("_source_only")))) or "none"
        return (f"Silver 1 rejected the whole record ({esc(d.get('error_class') or 'schema mismatch')}). "
                f"Expected columns missing: <b>{esc(missing)}</b>. Columns the source sent instead: <b>{esc(extra)}</b>. "
                f"It waits in the Silver 1 dead letter table (status {esc(d.get('healing_status'))}).")
    parts = []
    std = [f for f, layers in changes.items() if layers.get("silver2") == "std"]
    if j.get("audit"):
        for a in j["audit"]:
            parts.append(f"Silver 2 fixed a typo by fuzzy matching: <b>{esc(a.get('field'))}</b> "
                         f"<code>{esc(a.get('raw'))}</code> &rarr; <code>{esc(a.get('matched'))}</code> "
                         f"(edit distance {esc(a.get('distance'))}).")
    if flagged:
        cols = "; ".join(f"<b>{esc(k)}</b> = <code>{esc(v)}</code>" for k, v in flagged.items())
        parts.append(f"Silver 2 could not standardize {cols}, so the row went to quarantine.")
        for f in j.get("fixes", [])[:4]:
            params = f.get("action_params")
            try:
                canon = json.loads(params).get("canonical_value") if params else None
            except Exception:
                canon = None
            who = {"ENGINEER": "approved by an engineer", "DIAGNOSIS": "diagnosed by the model",
                   "INCIDENT": "reused from a past incident"}.get(str(f.get("action_source")), str(f.get("action_source")))
            parts.append(f"Recovery agent fix: {esc(f.get('action'))} <b>{esc(f.get('column_name'))}</b> "
                         f"<code>{esc(f.get('raw_value'))}</code>"
                         + (f" &rarr; <code>{esc(canon)}</code>" if canon else "")
                         + f" ({esc(who)}, {esc(str(f.get('status')).lower())}).")
        if q.get("healing_status") == "RESOLVED":
            parts.append("The healed row was released into Silver 2.")
        else:
            parts.append(f"It is still waiting (status {esc(q.get('healing_status'))}).")
    if std:
        parts.append(f"Silver 2 standardized {len(std)} value{'s' if len(std) != 1 else ''} with the reference dictionary "
                     f"({esc(', '.join(std[:6]))}{', ...' if len(std) > 6 else ''}).")
    if j.get("gold"):
        parts.append("Gold has the row, with derived columns added.")
    elif j.get("silver2"):
        parts.append("Not in Gold yet.")
    return " ".join(parts) or "Passed every layer unchanged."


def diff_journey(j: Dict[str, Any]) -> Tuple[List[str], Dict[str, Dict[str, str]]]:
    """Field order and, per field, a mark per layer: std / fuzzy / flag / healed."""
    fields: List[str] = []
    for layer in ("silver1", "bronze", "quarantine", "silver2", "gold"):
        for k in (j.get(layer) or {}):
            if k not in fields and k not in META_FIELDS and not k.startswith("_"):
                fields.append(k)
    flagged = set(as_dict((j.get("quarantine") or {}).get("columns_to_remediate")).keys())
    fuzzy = {str(a.get("field")) for a in j.get("audit") or []}
    marks: Dict[str, Dict[str, str]] = {}
    for f in fields:
        prev = _MISSING
        m: Dict[str, str] = {}
        for layer in JOURNEY_LAYERS:
            row = j.get(layer)
            if not row or f not in row:
                continue
            val = row[f]
            if layer == "quarantine" and f in flagged:
                m[layer] = "flag"
            elif layer == "silver2" and f in flagged:
                m[layer] = "healed"
            elif prev is not _MISSING and norm(prev) != norm(val):
                m[layer] = "fuzzy" if (layer in ("silver2", "quarantine") and f in fuzzy) else "std"
            prev = val
        if m:
            marks[f] = m
    return fields, marks


def render_schema_journey(title: str, j: Dict[str, Any]) -> str:
    d, b = j["dead"], j.get("bronze") or {}
    sent = [str(c) for c in as_list(d.get("_source_only"))]
    missing = [str(c) for c in as_list(d.get("_expected_only"))]
    body = "".join(f"<tr><th class='field'>{esc(c)}</th><td class='flag'>{show(b.get(c, _MISSING))}</td>"
                   + (f"<td class='na' rowspan='{len(sent)}'>whole record rejected, waits in the Silver 1 dead letter "
                      f"table (status {esc(d.get('healing_status'))})</td>" if i == 0 else "") + "</tr>"
                   for i, c in enumerate(sent))
    meta = (f"record <span class='mono'>{esc(j['record_id'])}</span> &middot; source <b>{esc(j.get('branch') or '?')}</b>"
            f" &middot; batch {esc(b.get('batch_id', '?'))}")
    story = (f"Silver 1 checks every message against the expected schema. This one came with renamed columns "
             f"({esc(d.get('error_class') or 'schema mismatch')}): expected <b>{esc(', '.join(missing) or 'none')}</b> "
             f"were missing, so the record was not loaded.")
    return (f'<article class="journey k-schema"><h3>{esc(title)}</h3><p class="meta">{meta}</p>'
            f'<p class="story">{story}</p><table class="grid journey-t"><thead><tr><th>Column the source sent</th>'
            f'<th>Bronze value</th><th>Silver 1</th></tr></thead><tbody>{body}</tbody></table></article>')


def render_journey(kind: str, title: str, j: Dict[str, Any], all_fields: bool) -> str:
    if j.get("dead"):
        return render_schema_journey(TITLES_SCHEMA, j)
    fields, marks = diff_journey(j)
    layers = [l for l in JOURNEY_LAYERS if j.get(l)]
    absent = []
    if j.get("silver1") and not j.get("silver2"):
        absent.append(("Silver 2", "still in quarantine" if j.get("quarantine") else "not in Silver 2 yet"))
    if j.get("silver1") and not j.get("gold"):
        absent.append(("Gold", "not in Gold yet"))
    flagged_raw = as_dict((j.get("quarantine") or {}).get("columns_to_remediate"))
    shown = fields if all_fields else [f for f in fields if f in marks]
    gold_extra = [c for c in GOLD_DERIVED if j.get("gold") and c in j["gold"]]
    if not all_fields:
        shown = [f for f in shown if f not in gold_extra]
    head = "<th>Field</th>" + "".join(f"<th>{LAYER_TITLES[l][0]}</th>" for l in layers)
    head += "".join(f"<th>{t}</th>" for t, _ in absent)
    body = ""
    for i, f in enumerate(shown):
        tds = ""
        for l in layers:
            row = j[l]
            cls = marks.get(f, {}).get(l, "")
            val = row.get(f, _MISSING)
            if cls == "flag" and is_null(val) and not is_null(flagged_raw.get(f)):
                val = flagged_raw[f]  # quarantine keeps the value it could not standardize here
            tds += f'<td class="{cls}">{show(val)}</td>'
        if i == 0:
            tds += "".join(f"<td class='na gap' rowspan='{len(shown)}'>{why}</td>" for _, why in absent)
        body += f"<tr><th class='field'>{esc(f)}</th>{tds}</tr>"
    hidden = len([f for f in fields if f not in shown and f not in gold_extra])
    if not shown:
        body = f"<tr><td colspan='{len(layers) + len(absent) + 1}' class='na'>No value changed between layers.</td></tr>"
    extra = ""
    if gold_extra and not all_fields:
        extra = ("<p class='note'>Gold adds: " + ", ".join(f"<b>{esc(c)}</b> = {show(j['gold'][c])}" for c in gold_extra)
                 + "</p>")
    rest = f"<p class='note'>{hidden} other field{'s' if hidden != 1 else ''} passed through unchanged.</p>" if hidden and not all_fields else ""
    meta_row = j.get("silver1") or j.get("bronze") or {}
    meta = (f"record <span class='mono'>{esc(j['record_id'])}</span> &middot; source <b>{esc(j.get('branch') or '?')}</b>"
            f" &middot; batch {esc(meta_row.get('batch_id', '?'))}")
    story = journey_story(kind, j, marks)
    return (f'<article class="journey k-{kind}"><h3>{esc(title)}</h3><p class="meta">{meta}</p>'
            f'<p class="story">{story}</p><table class="grid journey-t"><thead><tr>{head}</tr></thead>'
            f'<tbody>{body}</tbody></table>{extra}{rest}</article>')


# ----------------------------------------------------------------------------------------
# Layer sample tables
# ----------------------------------------------------------------------------------------

def layer_samples(layer: str, ids: List[str], n: int, seed: int, source: Optional[str]) -> Optional[pd.DataFrame]:
    id_list = ", ".join(sq(i) for i in ids)
    id_col = "kafka_key" if layer == "bronze" else "record_id"
    order = f"ORDER BY hash(CAST({id_col} AS VARCHAR) || {sq(seed)})"
    frames = []
    if id_list:
        df = query(layer, f"SELECT DISTINCT ON ({id_col}) * FROM {{t}} WHERE {id_col} IN ({id_list}) LIMIT {n}", source)
        if df is not None:
            frames.append(df)
    have = sum(len(f) for f in frames)
    if have < n:
        excl = f"WHERE {id_col} NOT IN ({id_list})" if id_list else ""
        df = query(layer, f"SELECT * FROM (SELECT DISTINCT ON ({id_col}) * FROM {{t}} {excl}) {order} LIMIT {n - have}", source)
        if df is not None:
            frames.append(df)
    frames = [f for f in frames if len(f)]
    if not frames:
        return None
    df = pd.concat(frames, ignore_index=True, sort=False)
    if layer == "bronze":
        df = v3.unpack_bronze_record(df)
    return df


def render_layer_table(layer: str, df: Optional[pd.DataFrame], cols: List[str],
                       ref_rows: Dict[str, Dict[str, Any]], prev_rows: Dict[str, Dict[str, Any]],
                       fuzzy: Dict[str, set], count: Optional[int]) -> str:
    title, desc = LAYER_TITLES[layer]
    head = f'<h3>{title} <span class="count">{fmt_n(count)} rows</span></h3><p class="note">{desc}</p>'
    if df is None or len(df) == 0:
        return head + '<p class="na">No rows yet.</p>'
    show_cols = ["record_id", "source_table"] + [c for c in cols if c in df.columns]
    show_cols += [c for c in LAYER_EXTRA_COLS.get(layer, []) if c in df.columns and c not in show_cols]
    if layer == "quarantine" and "target_table" in df.columns:
        show_cols[1] = "target_table"
    label = {"source_table": "source", "target_table": "source", "columns_to_remediate": "failed columns"}
    ths = "".join(f"<th>{esc(label.get(c, c))}</th>" for c in show_cols)
    body = ""
    for _, r in df.iterrows():
        rid = str(r.get("record_id"))
        flagged = set(as_dict(r.get("columns_to_remediate")).keys()) if layer == "quarantine" else set()
        base = None if layer == "bronze" else prev_rows.get(rid) if layer == "silver1" else ref_rows.get(rid)
        raw = as_dict(r.get("columns_to_remediate")) if layer == "quarantine" else {}
        healed = ref_rows.get(rid, {}).get("__flagged__", set()) if layer == "silver2" else set()
        tds = ""
        for c in show_cols:
            val = r.get(c, _MISSING)
            cls = ""
            if c == "record_id":
                tds += f"<td>{short_id(val)}</td>"
                continue
            if c in ("source_table", "target_table"):
                tds += f"<td>{esc(branch_of(val) or '')}</td>"
                continue
            if c in flagged:
                cls = "flag"
                if is_null(val) and not is_null(raw.get(c)):
                    val = raw[c]
            elif c in healed:
                cls = "healed"
            elif base is not None and c in base and c not in META_FIELDS and norm(base[c]) != norm(val):
                cls = "fuzzy" if (layer != "silver1" and c in fuzzy.get(rid, set())) else "std"
            tds += f'<td class="{cls}">{show(val, 40)}</td>'
        body += f"<tr>{tds}</tr>"
    return head + f'<div class="wrap"><table class="grid sample"><thead><tr>{ths}</tr></thead><tbody>{body}</tbody></table></div>'


# ----------------------------------------------------------------------------------------
# Healing activity
# ----------------------------------------------------------------------------------------

def simple_table(df: Optional[pd.DataFrame], cols: List[Tuple[str, str]], empty: str, limit: int = 60) -> str:
    if df is None or len(df) == 0:
        return f'<p class="na">{empty}</p>'
    ths = "".join(f"<th>{esc(label)}</th>" for _, label in cols)
    body = ""
    for _, r in df.iterrows():
        tds = ""
        for c, _ in cols:
            v = r.get(c, _MISSING)
            tds += f"<td>{short_id(v)}</td>" if c == "record_id" else f"<td>{show(v, limit)}</td>"
        body += f"<tr>{tds}</tr>"
    return f'<div class="wrap"><table class="grid"><thead><tr>{ths}</tr></thead><tbody>{body}</tbody></table></div>'


def render_healing(source: Optional[str], rows: int) -> str:
    out = []
    lim = max(rows * 2, 10)
    a = query("audit", f"SELECT field, raw, matched, distance, COUNT(*)::BIGINT AS rows_fixed, MAX(corrected_at) AS last_seen "
                       f"FROM {{t}} GROUP BY ALL ORDER BY rows_fixed DESC, last_seen DESC LIMIT {lim}")
    out.append("<h3>Typos fixed on entry by fuzzy matching <span class='count'>silver2_fuzzy_audit</span></h3>")
    out.append(simple_table(a, [("field", "Field"), ("raw", "Raw value"), ("matched", "Fixed to"),
                                ("distance", "Edit distance"), ("rows_fixed", "Rows"), ("last_seen", "Last seen")],
                            "No fuzzy fixes recorded yet."))

    ra_path = os.path.join(v3.DELTA_ROOT, "remediation_audit")
    out.append("<h3>Fixes made by the recovery agent <span class='count'>remediation_audit</span></h3>")
    if os.path.isdir(ra_path):
        where = f"WHERE target_table = {sq('silver2_' + source)}" if source else ""
        ra = query(ra_path, f"SELECT column_name, raw_value, "
                            f"json_extract_string(action_params, '$.canonical_value') AS canonical, action, action_source, "
                            f"status, SUM(rows_affected)::BIGINT AS rows_fixed, COUNT(*) AS runs, MAX(executed_at) AS last_run "
                            f"FROM {{t}} {where} GROUP BY ALL ORDER BY last_run DESC LIMIT {lim}")
        tot = query(ra_path, f"SELECT action_source, status, COUNT(*) AS runs, SUM(rows_affected) AS rows_fixed "
                             f"FROM {{t}} {where} GROUP BY ALL ORDER BY runs DESC")
        if tot is not None and len(tot):
            out.append("<p class='note'>" + "; ".join(
                f"{esc(r['action_source'])} {esc(r['status'])}: {int(r['runs'])} runs, {int(r['rows_fixed'] or 0)} rows"
                for _, r in tot.iterrows()) + f". Newest {lim} shown.</p>")
        out.append(simple_table(ra, [("column_name", "Column"), ("raw_value", "Raw value"), ("canonical", "Fixed to"),
                                     ("action", "Action"), ("action_source", "Decided by"), ("status", "Status"),
                                     ("rows_fixed", "Rows"), ("last_run", "When")],
                                "The recovery agent has not fixed anything yet."))
    else:
        out.append('<p class="na">No remediation_audit table yet.</p>')

    r = query("ref", f"SELECT field_name, raw_synonym, canonical_value, added_by, added_at FROM {{t}} "
                     f"WHERE added_by NOT LIKE 'seed%' ORDER BY added_at DESC LIMIT {lim}")
    n_seed = scalar("ref", "SELECT COUNT(*) FROM {t} WHERE added_by LIKE 'seed%'")
    n_new = scalar("ref", "SELECT COUNT(*) FROM {t} WHERE added_by NOT LIKE 'seed%'")
    out.append("<h3>Dictionary rules learned <span class='count'>ref_canonical_mappings</span></h3>")
    out.append(f"<p class='note'>{fmt_n(n_seed)} seed rules, {fmt_n(n_new)} added by the pipeline (newest shown). "
               f"New rules fix the same value automatically next time.</p>")
    out.append(simple_table(r, [("field_name", "Field"), ("raw_synonym", "Raw value"), ("canonical_value", "Canonical"),
                                ("added_by", "Added by"), ("added_at", "When")], "No learned rules yet."))

    where = f"WHERE lower(CAST(source_table AS VARCHAR)) = {sq(source)}" if source else ""
    d = query("s1_dead_letter", f"SELECT source_table, error_class, healing_status, "
                                f"array_to_string(_expected_only, ', ') AS missing, "
                                f"array_to_string(_source_only, ', ') AS unexpected, COUNT(*) AS rows "
                                f"FROM {{t}} {where} GROUP BY ALL ORDER BY rows DESC LIMIT {lim}")
    out.append("<h3>Records rejected at Silver 1 <span class='count'>silver1_dead_letter</span></h3>")
    out.append(simple_table(d, [("source_table", "Source"), ("error_class", "Error"), ("missing", "Expected columns missing"),
                                ("unexpected", "Columns sent instead"), ("healing_status", "Status"), ("rows", "Rows")],
                            "No schema mismatches.", limit=400))
    return "\n".join(out)


# ----------------------------------------------------------------------------------------
# Page
# ----------------------------------------------------------------------------------------

CSS = """
:root{--ink:#1d2733;--muted:#5d6b7a;--line:#d5dce4;--soft:#f3f6f9;--head:#e9eef3;
--std-bg:#fff1c9;--std-ink:#6b4b00;--fz-bg:#e6e0ff;--fz-ink:#3d2a8c;--flag-bg:#ffd9d6;--flag-ink:#8a1c12;
--heal-bg:#d3f2dc;--heal-ink:#0f5a2a;--b:#a0702a;--s1:#7b8794;--s2:#3c6e9e;--g:#b8901a}
*{box-sizing:border-box}
html{-webkit-print-color-adjust:exact;print-color-adjust:exact}
body{margin:0;background:#fff;color:var(--ink);font:13px/1.45 "Segoe UI",system-ui,-apple-system,Roboto,Arial,sans-serif}
main{max-width:1280px;margin:0 auto;padding:28px 24px 48px}
h1{font-size:24px;margin:0 0 4px}
h2{font-size:17px;margin:34px 0 10px;padding-bottom:6px;border-bottom:2px solid var(--ink)}
h3{font-size:14px;margin:22px 0 4px}
.meta,.note{color:var(--muted);font-size:12px;margin:2px 0 8px}
.count{font-weight:400;color:var(--muted);font-size:12px;margin-left:6px}
.mono,code{font-family:Consolas,"Cascadia Mono",Menlo,monospace;font-size:11.5px}
code{background:var(--soft);padding:0 3px;border-radius:3px}
.warn{border:1px solid #e0b74a;background:#fff8e6;padding:8px 12px;border-radius:6px;margin:12px 0}
.warn li{margin:2px 0}
.legend{display:flex;flex-wrap:wrap;gap:8px 16px;margin:12px 0 0;font-size:12px}
.legend span.sw{display:inline-block;padding:1px 8px;border-radius:3px;margin-right:4px}
.flow{display:flex;align-items:stretch;gap:6px;margin:10px 0 14px}
.stage{flex:1;border:1px solid var(--line);border-top:5px solid var(--s1);border-radius:6px;padding:10px 12px;background:#fff}
.stage.b{border-top-color:var(--b)}.stage.s2{border-top-color:var(--s2)}.stage.g{border-top-color:var(--g)}
.st-title{font-weight:600}.st-n{font-size:22px;font-weight:600;margin:2px 0}.st-sub{font-size:11.5px;color:var(--muted)}
.bad{color:var(--flag-ink)}.good{color:var(--heal-ink)}
.arrow{align-self:center;font-size:20px;color:var(--muted)}
table.grid{border-collapse:collapse;width:100%;font-size:12px;margin:4px 0}
table.grid th,table.grid td{border:1px solid var(--line);padding:4px 7px;text-align:left;vertical-align:top;overflow-wrap:anywhere}
table.grid thead th{background:var(--head);font-weight:600}
table.grid th.field{background:var(--soft);font-weight:600;white-space:nowrap}
td.num{text-align:right;font-variant-numeric:tabular-nums}
tr.total td{font-weight:600;background:var(--soft)}
.wrap{overflow-x:auto}
td.std{background:var(--std-bg);color:var(--std-ink);font-weight:600}
td.fuzzy{background:var(--fz-bg);color:var(--fz-ink);font-weight:600}
td.flag{background:var(--flag-bg);color:var(--flag-ink);font-weight:600}
td.healed{background:var(--heal-bg);color:var(--heal-ink);font-weight:600}
.std-sw{background:var(--std-bg);color:var(--std-ink)}.fz-sw{background:var(--fz-bg);color:var(--fz-ink)}
.flag-sw{background:var(--flag-bg);color:var(--flag-ink)}.heal-sw{background:var(--heal-bg);color:var(--heal-ink)}
.null,.na{color:#9aa5b1;font-style:italic}
.journeys{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.journey{border:1px solid var(--line);border-left:5px solid var(--s2);border-radius:6px;padding:10px 12px;break-inside:avoid}
.journey h3{margin:0 0 2px}
.k-fuzzy{border-left-color:#6a55c9}.k-healed{border-left-color:#2f9a55}.k-open{border-left-color:#d0473a}
.k-schema{border-left-color:#8a96a3}.k-picked{border-left-color:var(--g)}
.story{margin:4px 0 8px}
.journey-t th:first-child{width:24%}
td.gap{vertical-align:middle;text-align:center;background:var(--soft);min-width:70px}
table.sample th{white-space:nowrap}
footer{margin-top:36px;color:var(--muted);font-size:11.5px;border-top:1px solid var(--line);padding-top:8px}
@media (max-width:900px){.journeys{grid-template-columns:1fr}.flow{flex-direction:column}.arrow{transform:rotate(90deg)}}
@media print{
 @page{size:A4 landscape;margin:10mm}
 main{max-width:none;padding:0}
 body{font-size:10.5px}
 h2{break-after:avoid;margin-top:18px}
 h3{break-after:avoid}
 section.page{break-before:page}
 table.grid{font-size:9.5px}
 table.grid th,table.grid td{padding:2px 4px}
 .wrap{overflow:visible}
 thead{display:table-header-group}
 tr{break-inside:avoid}
 .journeys{grid-template-columns:1fr 1fr}
}
"""


def build_page(args) -> str:
    source = args.source
    sources = [source] if source else v3.SOURCES
    check_folders()

    rows, totals = overview(sources)

    picks = pick_records(args.per_story, args.seed, source)
    for rid in args.id or []:
        picks.append(("picked", rid))
    titles = dict(STORY_KINDS, picked="Record you asked for")
    journeys = []
    seen = set()
    for kind, rid in picks:
        j = fetch_journey(rid, source)
        if j["record_id"] in seen or not any(j.get(l) for l in JOURNEY_LAYERS + ["dead"]):
            continue
        seen.add(j["record_id"])
        journeys.append((kind, j))
    missing_kinds = [t for k, t in STORY_KINDS if k not in {k2 for k2, _ in journeys}]

    journey_html = "".join(render_journey(k, titles[k], j, args.all_fields) for k, j in journeys)
    if missing_kinds:
        journey_html += ("<p class='note'>No record in the data yet for: " + esc(", ".join(missing_kinds)) + ".</p>")

    # Layer samples: the journey records first, so the tables line up with the stories
    cols = [c.strip() for c in args.cols.split(",")] if args.cols else DEFAULT_COLS
    ids = [j["record_id"] for _, j in journeys if not j.get("dead")]
    fuzzy_map: Dict[str, set] = {}
    a = query("audit", "SELECT record_id, field FROM {t}")
    if a is not None:
        for _, r in a.iterrows():
            fuzzy_map.setdefault(str(r["record_id"]), set()).add(str(r["field"]))
    samples = {}
    for layer in ("bronze", "silver1", "quarantine", "silver2", "gold"):
        samples[layer] = layer_samples(layer, ids, args.rows, args.seed, source)
    # Silver 1 rows of every sampled record, to highlight what later layers changed
    all_ids = set()
    for layer, df in samples.items():
        if df is not None and "record_id" in df.columns:
            all_ids |= set(df["record_id"].dropna().astype(str))
    s1_rows: Dict[str, Dict[str, Any]] = {}
    if all_ids:
        s1 = query("silver1", "SELECT * FROM {t} WHERE record_id IN (" + ", ".join(sq(i) for i in all_ids) + ")", source)
        if s1 is not None:
            s1_rows = {str(r["record_id"]): r.to_dict() for _, r in s1.iterrows()}
    b_rows: Dict[str, Dict[str, Any]] = {}
    if all_ids:
        b = query("bronze", "SELECT * FROM {t} WHERE kafka_key IN (" + ", ".join(sq(i) for i in all_ids) + ")", source)
        if b is not None and len(b):
            b_rows = {str(r["record_id"]): r.to_dict() for _, r in v3.unpack_bronze_record(b).iterrows()}
    q_all = query("quarantine", "SELECT record_id, columns_to_remediate FROM {t} WHERE record_id IN ("
                  + ", ".join(sq(i) for i in all_ids or {"-"}) + ")", source)
    if q_all is not None:
        for _, r in q_all.iterrows():
            rid = str(r["record_id"])
            s1_rows.setdefault(rid, {}).setdefault("__flagged__", set()).update(as_dict(r["columns_to_remediate"]).keys())

    counts = {
        "bronze": totals["bronze"], "silver1": totals["silver1"], "silver2": totals["silver2"], "gold": totals["gold"],
        "quarantine": (totals["q_open"] or 0) + (totals["q_res"] or 0) if totals["q_open"] is not None else None,
    }
    layer_html = "".join(
        render_layer_table(l, samples[l], cols, s1_rows, b_rows, fuzzy_map, counts[l])
        for l in ("bronze", "silver1", "silver2", "quarantine", "gold"))

    healing_html = render_healing(source, args.rows)

    stamp = datetime.now().astimezone().strftime("%Y-%m-%d %H:%M %Z")
    scope = f"source <b>{esc(source)}</b>" if source else f"sources <b>{esc(', '.join(sources))}</b>"
    warn = ""
    if WARNINGS:
        warn = "<div class='warn'><b>Notes about the data</b><ul>" + "".join(f"<li>{esc(w)}</li>" for w in dict.fromkeys(WARNINGS)) + "</ul></div>"
    legend = ("<div class='legend'>"
              "<span><span class='sw std-sw'>value</span>changed by standardization (dictionary or format)</span>"
              "<span><span class='sw fz-sw'>value</span>typo fixed by fuzzy matching</span>"
              "<span><span class='sw flag-sw'>value</span>could not be standardized, sent to quarantine</span>"
              "<span><span class='sw heal-sw'>value</span>healed by the recovery agent</span></div>")
    cmd = "python \"Read Data/read_parquet_report.py\" " + " ".join(sys.argv[1:])
    return f"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Pipeline Data Flow</title><style>{CSS}</style></head>
<body><main>
<header><h1>Self-healing pipeline: how data flows through the layers</h1>
<p class="meta">Generated {esc(stamp)} &middot; {scope} &middot; data folder <span class="mono">{esc(v3.DELTA_ROOT)}</span></p>
{warn}{legend}</header>

<section><h2>1. Rows per layer</h2>{render_overview(rows, totals)}</section>

<section class="page"><h2>2. Record journeys</h2>
<p class="note">Real records followed from Bronze to Gold. Only the fields whose value changed somewhere are listed;
the color says which step changed it.</p>
<div class="journeys">{journey_html}</div></section>

<section class="page"><h2>3. Sample rows per layer</h2>
<p class="note">The journey records come first so the tables line up with section 2. Highlighting compares each row with
the same record in the layer before it (Silver 1 with Bronze, later layers with Silver 1).</p>
{layer_html}</section>

<section class="page"><h2>4. Healing activity</h2>{healing_html}</section>

<footer>Made with <span class="mono">{esc(cmd.strip())}</span>. Reads the current Delta snapshot of each table, like
read_parquet_v3.py.</footer>
</main></body></html>"""


def main() -> None:
    p = argparse.ArgumentParser(description="Write a printable HTML page showing how data flows through the layers.")
    p.add_argument("--source", "-S", help="only this data folder, e.g. crash4")
    p.add_argument("--rows", type=int, default=6, help="sample rows per layer table (default 6)")
    p.add_argument("--per-story", type=int, default=1, help="records per journey kind (default 1)")
    p.add_argument("--id", action="append", help="also follow this record id (repeatable)")
    p.add_argument("--cols", help="comma-separated columns for the layer tables")
    p.add_argument("--all-fields", action="store_true", help="journey tables list every field")
    p.add_argument("--seed", type=int, default=1, help="changes which sample records are picked")
    p.add_argument("--out", default=os.path.join(SCRIPT_DIR, "pipeline_flow_report.html"), help="output HTML file")
    p.add_argument("--no-open", action="store_true", help="do not open the page in the browser")
    args = p.parse_args()

    if args.source:
        args.source = args.source.lower()
        if args.source not in v3.SOURCES:
            p.error(f"unknown source {args.source!r}; found: {', '.join(v3.SOURCES)}")
    if not os.path.isdir(v3.DELTA_ROOT):
        p.error(f"no delta folder at {v3.DELTA_ROOT}; run the pipeline first")

    print(f"Reading tables under {v3.DELTA_ROOT} ...")
    page = build_page(args)
    out = os.path.abspath(args.out)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8") as f:
        f.write(page)
    print(f"Report written to {out}")
    for w in dict.fromkeys(WARNINGS):
        print(f"  note: {w}")
    if not args.no_open:
        try:
            webbrowser.open("file:///" + out.replace(os.sep, "/").lstrip("/"))
        except Exception:
            pass


if __name__ == "__main__":
    main()
