"""Deterministic Lookup node in LangGraph (spec DL1, DL2, DL3).

Answers two questions from Neo4j without an LLM:
  1. What does this failure damage? (Blast Radius via OpenLineage graph)
  2. Have we fixed it before? (Exact match or vector similarity over Incident corpus)

Decision:
  - Match Found (score >= threshold): routes directly to 'remediate', reusing the
    verified action and parameters without invoking the Reasoning LLM.
  - No Precedent: routes to 'diagnose', assembling blast radius, upstream lineage,
    recent run errors, schema drift info, and past column fixes for the Reasoning LLM.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

# Ensure lineage and agent root directories are resolvable
_REPO_ROOT = Path(__file__).resolve().parents[2]
_LINEAGE_PATH = _REPO_ROOT / "lineage"
if str(_LINEAGE_PATH) not in sys.path:
    sys.path.insert(0, str(_LINEAGE_PATH))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

try:
    from agent.memory import incidents
except ImportError:
    from memory import incidents

try:
    from graph import downstream_tables, upstream_tables  # from lineage/
except ImportError:
    from lineage.graph import downstream_tables, upstream_tables

logger = logging.getLogger(__name__)

# Neo4j cosine score is normalised to [0, 1] ((1 + cos) / 2).
# 0.5 means orthogonal/unrelated. 0.9 is the recommended reuse threshold.
DEFAULT_SIMILARITY_THRESHOLD = float(os.environ.get("LOOKUP_SIMILARITY_THRESHOLD", "0.9"))


def normalize_table_name(table_name: str) -> str:
    """Ensure table name matches lineage convention (e.g. 'silver2', not '/data/delta/silver2')."""
    if not table_name:
        return ""
    cleaned = table_name.strip().rstrip("/")
    if "/" in cleaned:
        cleaned = cleaned.split("/")[-1]
    return cleaned


def format_signature_text(
    table_name: str,
    column_name: Optional[str],
    raw_value: Optional[str],
    failure_type: str = "UNRESOLVED_VALUE",
) -> str:
    """Format signature text embedded for vector search (must match Learn node)."""
    t = normalize_table_name(table_name)
    c = (column_name or "").strip()
    v = (raw_value or "").strip().upper()
    f = (failure_type or "UNRESOLVED_VALUE").strip()
    return f"table={t} column={c} value={v} failure={f}"


def _get_recent_writes(driver, table_name: str, limit: int = 5) -> List[Dict[str, Any]]:
    """Fetch recent Spark runs that wrote to this table, including error messages."""
    records, _, _ = driver.execute_query(
        """
        MATCH (r:Run)-[:WRITES]->(:Table {name: $t})
        RETURN r.status AS status,
               r.error_message AS error_message,
               toString(r.ended_at) AS ended_at
        ORDER BY r.started_at DESC
        LIMIT $limit
        """,
        t=table_name,
        limit=limit,
    )
    return [r.data() for r in records]


def _get_recent_schema_versions(driver, table_name: str, limit: int = 3) -> List[Dict[str, Any]]:
    """Fetch recent schema versions for table to detect potential schema drift."""
    records, _, _ = driver.execute_query(
        """
        MATCH (:Table {name: $t})-[:HAS_SCHEMA]->(v:SchemaVersion)
        RETURN v.hash AS hash,
               v.field_count AS field_count,
               toString(v.first_seen) AS first_seen
        ORDER BY v.first_seen DESC
        LIMIT $limit
        """,
        t=table_name,
        limit=limit,
    )
    return [r.data() for r in records]


def _get_other_column_fixes(driver, table_name: str, column_name: Optional[str], limit: int = 10) -> List[Dict[str, Any]]:
    """Fetch previous verified fixes for the same column as reference context."""
    if not column_name:
        return []
    records, _, _ = driver.execute_query(
        """
        MATCH (i:Incident {table_name: $t, column_name: $c})
        WHERE i.verified = true AND i.outcome = 'SUCCESS'
        RETURN i.raw_value AS raw_value,
               i.action AS action,
               i.action_params AS action_params
        ORDER BY i.resolved_at DESC
        LIMIT $limit
        """,
        t=table_name,
        c=column_name,
        limit=limit,
    )
    results = []
    for r in records:
        data = r.data()
        params = data.get("action_params")
        if isinstance(params, str):
            try:
                data["action_params"] = json.loads(params)
            except Exception:
                pass
        results.append(data)
    return results


def lookup(
    driver,
    group: Dict[str, Any],
    embed: Optional[Callable[[str], List[float]]] = None,
    threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> Dict[str, Any]:
    """Execute deterministic lookup without invoking an LLM.

    Args:
        driver: Neo4j driver instance.
        group: Dictionary containing structured failure details:
               - table_name: affected table (e.g. 'silver2')
               - column_name: failing column (e.g. 'crash_month')
               - raw_value: quarantine raw value (e.g. 'Jule')
               - failure_type: optional, default 'UNRESOLVED_VALUE'
               - rows_affected: optional count of rows
        embed: Optional embedding function `embed(text) -> list[float]`.
        threshold: Minimum similarity score [0.0 - 1.0] to reuse past resolution.

    Returns:
        Dict routing either to 'remediate' (if known resolution found) or
        to 'diagnose' (with full context assembled from Neo4j).
    """
    table_name = normalize_table_name(group.get("table_name", ""))
    column_name = group.get("column_name")
    raw_value = group.get("raw_value")
    failure_type = group.get("failure_type", "UNRESOLVED_VALUE")

    # Step 1: Compute Blast Radius (DL1)
    # Traverses (:Table)-[:FEEDS*1..]->(:Table)
    blast_radius = downstream_tables(driver, table_name)

    # Step 2: Check Incident Patterns (DL2)
    # Tier A: Exact Match on signature_key (O(1) indexed query, zero embedding cost)
    hit = incidents.find_exact(driver, table_name, column_name, raw_value)
    matched_by = "EXACT" if hit is not None else None

    # Tier B: Vector similarity search (if no exact hit and embedder is available)
    sig_text = format_signature_text(table_name, column_name, raw_value, failure_type)
    embedding: Optional[List[float]] = None

    if hit is None and embed is not None:
        try:
            embedding = embed(sig_text)
            similar_hits = incidents.find_similar(driver, embedding, k=5, min_score=threshold)
            if similar_hits:
                hit = similar_hits[0]
                matched_by = "SIMILAR"
        except Exception as exc:
            logger.warning(f"Vector search failed in lookup: {exc}")

    # Decision: Known pattern found with score >= threshold
    if hit:
        raw_params = hit.get("action_params", {})
        if isinstance(raw_params, str):
            try:
                params = json.loads(raw_params)
            except Exception:
                params = {"raw": raw_params}
        else:
            params = raw_params or {}

        return {
            "route": "remediate",
            "blast_radius": blast_radius,
            "source_incident": hit.get("incident_id"),
            "action": hit.get("action"),
            "params": params,
            "score": hit.get("score", 1.0),
            "matched_by": matched_by,
        }

    # Step 3: No precedent found -> Fallback to Diagnosis (DL3)
    # Assemble rich context so the Reasoning LLM diagnoses from facts
    upstream = upstream_tables(driver, table_name)
    recent_writes = _get_recent_writes(driver, table_name, limit=5)
    schema_versions = _get_recent_schema_versions(driver, table_name, limit=3)
    column_fixes = _get_other_column_fixes(driver, table_name, column_name, limit=10)

    # Gather nearest incidents (min_score=0.0) as few-shot prompt examples for LLM
    nearest_incidents = []
    if embed is not None:
        try:
            if embedding is None:
                embedding = embed(sig_text)
            nearest_incidents = incidents.find_similar(driver, embedding, k=5, min_score=0.0)
            for inc in nearest_incidents:
                if isinstance(inc.get("action_params"), str):
                    try:
                        inc["action_params"] = json.loads(inc["action_params"])
                    except Exception:
                        pass
        except Exception as exc:
            logger.warning(f"Could not retrieve nearest incidents for diagnosis: {exc}")

    return {
        "route": "diagnose",
        "blast_radius": blast_radius,
        "upstream_tables": upstream,
        "recent_writes": recent_writes,
        "recent_schema_versions": schema_versions,
        "column_fixes": column_fixes,
        "nearest_incidents": nearest_incidents,
        "signature_text": sig_text,
    }


def lookup_node(
    state: Dict[str, Any],
    driver=None,
    embed: Optional[Callable[[str], List[float]]] = None,
    threshold: float = DEFAULT_SIMILARITY_THRESHOLD,
) -> Dict[str, Any]:
    """LangGraph node wrapper for Deterministic Lookups.

    Reads failure details from state (`failure_group`, `quarantine_group`, or `failure`),
    runs `lookup()`, and updates the pipeline state.
    """
    if driver is None:
        driver = incidents.get_driver()

    group = (
        state.get("failure_group")
        or state.get("quarantine_group")
        or state.get("failure")
        or {}
    )

    result = lookup(driver, group, embed=embed, threshold=threshold)

    updates: Dict[str, Any] = {
        "lookup_result": result,
        "route": result["route"],
        "blast_radius": result.get("blast_radius", []),
    }

    if result["route"] == "remediate":
        updates["action"] = result.get("action")
        updates["action_params"] = result.get("params")
        updates["source_incident"] = result.get("source_incident")
        updates["confidence"] = result.get("score")
    else:
        updates["diagnosis_context"] = result

    return updates


def route_after_lookup(state: Dict[str, Any]) -> str:
    """Conditional edge router for LangGraph after the lookup node."""
    return state.get("route", "diagnose")
