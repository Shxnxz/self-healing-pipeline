"""Compute blast radius (spec DL1): what a failure damages, and how badly.

`downstream_tables()` in lineage/graph.py answers *which* tables are reached. To
decide *which failure to fix first* the lookup needs a number, so this module
turns the lineage into an impact score (0..100) made of five explainable factors:

  severity     how much of a batch the failure type stops (schema > value)
  reach        downstream tables weighted by layer (gold counts most)
  volume       rows affected, log-scaled
  propagation  is the failing column still present in downstream schemas
  blocks       other failures in the same window that sit downstream of this one,
               so fixing this one first may clear them too

Every factor is returned in `breakdown`, so the ranking can be explained and the
weights in config.py tuned.

Fetching (Neo4j) and scoring (pure) are separate so scoring is unit-testable.
"""
import math
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Set

import config
from failures import StructuredFailure


# ----------------------------------------------------------------------------
# Lineage view (fetched from Neo4j)
# ----------------------------------------------------------------------------
@dataclass
class LineageView:
    origin_table: Optional[str]
    detected_table: Optional[str]
    in_graph: bool                                   # origin table exists in the lineage graph
    downstream: List[dict] = field(default_factory=list)   # {name, kind, depth, columns}
    upstream: List[str] = field(default_factory=list)
    recent_runs: List[dict] = field(default_factory=list)  # runs that wrote detected_table

    def to_dict(self) -> dict:
        return {
            "origin_table": self.origin_table,
            "detected_table": self.detected_table,
            "in_graph": self.in_graph,
            "downstream": [{k: v for k, v in d.items() if k != "columns"} for d in self.downstream],
            "upstream": self.upstream,
            "recent_runs": self.recent_runs,
        }


def table_candidates(layer: Optional[str], folder: Optional[str]) -> List[str]:
    """Per-folder name first (picture: silver2_crash1), then the single-table name (main)."""
    if not layer:
        return []
    names = []
    if folder:
        names.append(f"{layer}_{folder.strip().lower()}")
    names.append(layer)
    return names


def resolve_table(layer: Optional[str], folder: Optional[str], known: Set[str]) -> Optional[str]:
    candidates = table_candidates(layer, folder)
    for name in candidates:
        if name in known:
            return name
    # Not in the graph yet: use the layer name, matching the static fallback chain.
    return candidates[-1] if candidates else None


def existing_tables(driver, names: Iterable[str]) -> Set[str]:
    records, _, _ = driver.execute_query(
        "MATCH (t:Table) WHERE t.name IN $names RETURN t.name AS name",
        names=sorted(set(names)),
    )
    return {r["name"] for r in records}


_DOWNSTREAM = """
MATCH p = (:Table {name: $origin})-[:FEEDS*1..]->(d:Table)
WITH d, min(length(p)) AS depth
OPTIONAL MATCH (d)-[:HAS_SCHEMA]->(v:SchemaVersion)
WHERE v.hash = d.schema_hash
RETURN d.name AS name, d.kind AS kind, depth, v.fields AS fields
ORDER BY depth, name
"""

_UPSTREAM = """
MATCH (u:Table)-[:FEEDS*1..]->(:Table {name: $origin})
RETURN DISTINCT u.name AS name ORDER BY name
"""

_RECENT_RUNS = """
MATCH (r:Run)-[:WRITES]->(:Table {name: $table})
RETURN r.status AS status, r.error_message AS error_message,
       toString(r.ended_at) AS ended_at
ORDER BY r.started_at DESC LIMIT $limit
"""


def _columns(fields) -> List[str]:
    # SchemaVersion.fields are "name:type" strings (lineage/normalize.py)
    return [f.split(":", 1)[0] for f in (fields or [])]


def fetch_lineage(driver, failure: StructuredFailure, known: Set[str], recent_runs: int = 3) -> LineageView:
    origin = resolve_table(failure.origin_layer, failure.folder, known)
    detected = resolve_table(failure.detected_layer, failure.folder, known)
    if origin not in known:
        return LineageView(origin, detected, in_graph=False)

    records, _, _ = driver.execute_query(_DOWNSTREAM, origin=origin)
    downstream = [
        {"name": r["name"], "kind": r["kind"], "depth": r["depth"], "columns": _columns(r["fields"])}
        for r in records
    ]
    records, _, _ = driver.execute_query(_UPSTREAM, origin=origin)
    upstream = [r["name"] for r in records]
    runs = []
    if detected in known:
        records, _, _ = driver.execute_query(_RECENT_RUNS, table=detected, limit=recent_runs)
        runs = [r.data() for r in records]
    return LineageView(origin, detected, True, downstream, upstream, runs)


# ----------------------------------------------------------------------------
# Scoring (pure)
# ----------------------------------------------------------------------------
def layer_of(table: str) -> str:
    for layer in sorted(config.LAYER_WEIGHTS, key=len, reverse=True):
        if table == layer or table.startswith(layer + "_") or table.startswith(layer + "."):
            return layer
    return "other"


def layer_weight(table: str, kind: Optional[str] = None) -> float:
    if kind == "kafka_topic":
        return 0.0
    layer = layer_of(table)
    return config.LAYER_WEIGHTS.get(layer, config.DEFAULT_LAYER_WEIGHT)


def _static_downstream(failure: StructuredFailure) -> List[dict]:
    """Fallback before the lineage graph knows the table (fresh stack, lineage off):
    assume the medallion chain bronze -> silver1 -> silver2 -> gold."""
    if failure.origin_layer not in config.LAYER_ORDER:
        return []
    start = config.LAYER_ORDER.index(failure.origin_layer)
    return [
        {"name": layer, "kind": "delta", "depth": depth, "columns": []}
        for depth, layer in enumerate(config.LAYER_ORDER[start + 1:], start=1)
    ]


def affected_tables(failure: StructuredFailure, lineage: LineageView) -> List[dict]:
    tables = []
    if failure.origin_affected and lineage.origin_table:
        tables.append({"name": lineage.origin_table, "kind": "delta", "depth": 0, "columns": []})
    tables.extend(lineage.downstream if lineage.in_graph else _static_downstream(failure))
    out = []
    for t in tables:
        out.append({
            "name": t["name"], "depth": t["depth"], "layer": layer_of(t["name"]),
            "weight": layer_weight(t["name"], t.get("kind")), "columns": t.get("columns") or [],
        })
    return out


def _severity(failure: StructuredFailure) -> float:
    base = config.SEVERITY.get(failure.failure_type, config.SEVERITY.get("UNKNOWN", 0.6))
    if failure.mandatory:
        base += config.MANDATORY_SEVERITY_BONUS
    if failure.origin_affected:  # the whole job stopped
        base = 1.0
    return min(1.0, base)


def _volume(rows: Optional[int]) -> float:
    if rows is None:
        return 0.5  # unknown: neither reward nor punish
    if rows <= 0:
        return 0.0
    return min(1.0, math.log10(1 + rows) / math.log10(1 + config.ROWS_CAP))


def _propagation(failure: StructuredFailure, tables: List[dict]):
    """1.0 if the failing column is still used downstream, 0.0 if every downstream
    schema dropped it, 0.5 if unknown. Whole-record failures always propagate."""
    if failure.family in ("SCHEMA", "RECORD") or failure.origin_affected or not failure.column_name:
        return 1.0, []
    consumers = [t for t in tables if t["weight"] > 0]
    with_schema = [t for t in consumers if t["columns"]]
    reaches = [t["name"] for t in with_schema if failure.column_name in t["columns"]]
    if not with_schema:
        return 0.5, []
    return (1.0 if reaches else 0.0), reaches


def tier_of(score: float) -> str:
    for name, minimum in config.TIERS:
        if score >= minimum:
            return name
    return config.TIERS[-1][0]


def score(failure: StructuredFailure, lineage: LineageView, blocked: int = 0) -> Dict:
    tables = affected_tables(failure, lineage)
    reach = sum(t["weight"] for t in tables)
    propagation, column_reaches = _propagation(failure, tables)
    factors = {
        "severity": _severity(failure),
        "reach": reach / (reach + config.REACH_HALF) if reach > 0 else 0.0,
        "volume": _volume(failure.rows_affected),
        "propagation": propagation,
        "blocks": blocked / (blocked + 1.0),
    }
    total = 100.0 * sum(config.SCORE_WEIGHTS[k] * v for k, v in factors.items())
    layers = sorted({t["layer"] for t in tables if t["weight"] > 0},
                    key=lambda l: config.LAYER_ORDER.index(l) if l in config.LAYER_ORDER else 99)
    return {
        "score": round(total, 1),
        "tier": tier_of(total),
        "breakdown": {k: round(v, 3) for k, v in factors.items()},
        "reach_weight": reach,
        "affected_tables": [{k: v for k, v in t.items() if k != "columns"} for t in tables],
        "affected_layers": layers,
        "column_reaches": column_reaches,
        "blocks_failures": blocked,
        "lineage_known": lineage.in_graph,
    }
