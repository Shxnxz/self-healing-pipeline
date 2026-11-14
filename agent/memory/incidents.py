"""Incident memory in Neo4j (spec KG2, DL2, EL3, TA3).

The graph schema lives in lineage/schema.cypher. This module is the only Python
code that reads or writes (:Incident) nodes, so the LangGraph nodes (lookup,
diagnose, learn) share one contract.

Embeddings are passed in, not computed here: the embedding model is chosen in
task 3.5, and its output size must match `vector.dimensions` in schema.cypher. An
incident written without one (no model configured, Ollama down) is still found by
the exact and structured lookups; only the optional vector recall misses it.

Writers: the Learn node (agent/nodes/learn.py) is the only one. Readers: the
Deterministic Lookup (lookup/patterns.py) and Learn's promotion
(agent/promotion.py: signature_history).
"""
import json
import os
import re
import uuid
from pathlib import Path
from typing import Optional

from neo4j import GraphDatabase

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "selfhealing")

VECTOR_INDEX = "incident_embedding"
SCHEMA_PATH = Path(__file__).resolve().parents[2] / "lineage" / "schema.cypher"


# Outcomes. SUCCESS + verified is what the lookup may reuse; everything else is history
# (lookup/patterns.py lists it as previous attempts and never repeats it).
SUCCESS = "SUCCESS"                       # validated fix, or fixed by hand by an engineer
FAILED_VALIDATION = "FAILED_VALIDATION"   # a fix that was staged and did not hold in Validate
REJECTED = "REJECTED"
DISMISSED = "DISMISSED"                   # the engineer decided not to fix it

# `action` of an incident that has no executable fix. MANUAL is SUCCESS (an engineer
# fixed it outside the agent) and is past-incident context for Diagnose, never reused.
MANUAL = "MANUAL"
DISMISS = "DISMISS"

# resolved_by
BY_LOOKUP, BY_LLM, BY_ENGINEER = "LOOKUP", "LLM", "ENGINEER"


def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


_shared = None


def shared_driver():
    """One driver per process (it pools connections). Created on first use."""
    global _shared
    if _shared is None:
        _shared = get_driver()
    return _shared


# Per-source tables are "<layer>_<source>" (spark-jobs/sources.py): silver2_crash3.
# Sources are letters and digits only, so the suffix never contains "_".
_PER_SOURCE_TABLE = re.compile(r"^(bronze|silver1|silver2)_([a-z][a-z0-9]*)$")
# Shared tables whose names look like "<layer>_<word>" but are not a source branch.
_SHARED_TABLES = {"silver2_quarantine", "silver1_dead_letter", "silver2_dead_letter", "silver2_fuzzy_audit"}


def logical_table(table_name: str) -> str:
    """The layer a table belongs to: "silver2_crash3" -> "silver2"; other names unchanged.

    The four source branches share one vocabulary, so a fix learned on Crash1 must be
    found again when the same value shows up in Crash3. Incidents are therefore keyed
    on the layer, while (:Incident)-[:AFFECTS]->(:Table) keeps the exact table.
    """
    if table_name in _SHARED_TABLES:
        return table_name
    match = _PER_SOURCE_TABLE.match(table_name or "")
    return match.group(1) if match else table_name


def signature_key(table_name: str, column_name: Optional[str], raw_value: Optional[str]) -> str:
    """Exact-match key for a failure: same layer, column and normalised raw value."""
    value = (raw_value or "").strip().upper()
    return f"{logical_table(table_name)}|{column_name or ''}|{value}"


def signature_text(table_name: Optional[str], column_name: Optional[str], raw_value: Optional[str],
                   failure_type: Optional[str]) -> str:
    """Text to embed. The lookup embeds a new failure and Learn an incident with this one
    function, so the two vectors are comparable (lookup/patterns.py)."""
    return (f"table={table_name or ''} column={(column_name or '').strip()} "
            f"value={(raw_value or '').strip().upper()} failure={failure_type}")


def apply_schema(driver) -> None:
    """Apply lineage/schema.cypher. Normally done by the neo4j-init compose service."""
    text = SCHEMA_PATH.read_text(encoding="utf-8")
    lines = (ln for ln in text.splitlines() if not ln.lstrip().startswith("//"))
    for statement in "\n".join(lines).split(";"):
        if statement.strip():
            driver.execute_query(statement)


def write_incident(
    driver,
    *,
    table_name: str,
    column_name: Optional[str],
    raw_value: Optional[str],
    failure_type: str,
    diagnosis: str,
    action: str,
    action_params: dict,
    outcome: str,
    resolved_by: str,
    confidence: Optional[float],
    verified: bool,
    rows_affected: int,
    embedding: Optional[list],
    signature_text: str,
    incident_id: Optional[str] = None,
    extra: Optional[dict] = None,
) -> str:
    """Create or update an incident (Learn node). Returns its incident_id.

    Idempotent on `incident_id`: writing the same incident again updates it. `extra`
    holds the provenance Learn records (failure_id, batch_ids, fix_source, matched_by,
    borrowed, approved_by, ...); values must be Neo4j properties (no maps), and a None
    removes the property. `embedding` None leaves the vector out.
    """
    incident_id = incident_id or str(uuid.uuid4())
    vector = """
        WITH i
        CALL db.create.setNodeVectorProperty(i, 'embedding', $embedding)""" if embedding else ""
    driver.execute_query(
        """
        MERGE (i:Incident {incident_id: $incident_id})
          ON CREATE SET i.created_at = datetime()
        SET i.signature_key  = $signature_key,
            i.signature_text = $signature_text,
            i.failure_type   = $failure_type,
            i.table_name     = $table_name,
            i.column_name    = $column_name,
            i.raw_value      = $raw_value,
            i.diagnosis      = $diagnosis,
            i.action         = $action,
            i.action_params  = $action_params,
            i.outcome        = $outcome,
            i.resolved_by    = $resolved_by,
            i.confidence     = $confidence,
            i.verified       = $verified,
            i.rows_affected  = $rows_affected,
            i.resolved_at    = datetime()
        SET i += $extra""" + vector + """
        WITH i
        MERGE (t:Table {name: $table_name})
        MERGE (i)-[:AFFECTS]->(t)
        """,
        incident_id=incident_id,
        signature_key=signature_key(table_name, column_name, raw_value),
        signature_text=signature_text,
        failure_type=failure_type,
        table_name=table_name,
        column_name=column_name,
        raw_value=raw_value,
        diagnosis=diagnosis,
        action=action,
        action_params=json.dumps(action_params, sort_keys=True),
        outcome=outcome,
        resolved_by=resolved_by,
        confidence=confidence,
        verified=verified,
        rows_affected=rows_affected,
        embedding=embedding,
        extra={k: v for k, v in (extra or {}).items()},
    )
    return incident_id


def mark_promotion(driver, incident_id: str, promotion: str, reason: str) -> None:
    """Record what Learn's promotion decided for an incident (agent/promotion.py)."""
    driver.execute_query(
        """
        MATCH (i:Incident {incident_id: $incident_id})
        SET i.promotion = $promotion, i.promotion_reason = $reason, i.promotion_at = datetime(),
            i.promoted = ($promotion = 'PROMOTED' OR coalesce(i.promoted, false))
        """,
        incident_id=incident_id, promotion=promotion, reason=reason)


def signature_history(driver, key: str) -> list:
    """Every incident with this signature_key, whatever its outcome (Learn's promotion
    weighs the successes against the failed and conflicting ones)."""
    records, _, _ = driver.execute_query(
        """
        MATCH (i:Incident {signature_key: $key})
        RETURN i.incident_id AS incident_id, i.outcome AS outcome, i.verified AS verified,
               i.action AS action, i.action_params AS action_params,
               i.rows_affected AS rows_affected, i.batch_ids AS batch_ids,
               i.borrowed AS borrowed, i.approved_by AS approved_by,
               i.promote_declined AS promote_declined, i.resolved_by AS resolved_by,
               i.failure_id AS failure_id, toString(i.resolved_at) AS resolved_at
        ORDER BY i.resolved_at
        """,
        key=key)
    return [{**r.data(), "action_params": _params(r["action_params"])} for r in records]


def _params(raw):
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return {"raw": raw}
    return raw or {}


_RETURN_FIELDS = """
    i.incident_id AS incident_id, i.signature_key AS signature_key,
    i.failure_type AS failure_type, i.table_name AS table_name,
    i.column_name AS column_name, i.raw_value AS raw_value,
    i.diagnosis AS diagnosis, i.action AS action, i.action_params AS action_params,
    i.outcome AS outcome, i.resolved_by AS resolved_by, i.confidence AS confidence,
    coalesce(i.borrowed, false) AS borrowed, i.approved_by AS approved_by
"""


def find_exact(driver, table_name: str, column_name: Optional[str], raw_value: Optional[str]) -> Optional[dict]:
    """Latest verified fix with the same signature, or None. Needs no embedding.

    A fix nobody approved that was itself borrowed from a similar incident (`borrowed`)
    is returned only when there is no other, and carries `borrowed` so the agent still
    asks for approval on P1 (agent/nodes/deterministic_lookup.py). An engineer's manual
    resolution has no fix to repeat and is left to the candidates.

    `distinct_fixes` lists every different fix verified for this signature ("ACTION
    {params}"). More than one means the value is ambiguous (MISTY fixed to FOG once and
    to CLOUDY later): the lookup must not reuse either (lookup/patterns.py: transferable),
    the same rule Learn's promotion applies (agent/promotion.py).
    """
    records, _, _ = driver.execute_query(
        f"""
        MATCH (i:Incident {{signature_key: $key}})
        WHERE i.verified = true AND i.outcome = 'SUCCESS' AND coalesce(i.action, '') <> '{MANUAL}'
        WITH i ORDER BY coalesce(i.borrowed, false) ASC, i.resolved_at DESC
        WITH collect(i) AS hits,
             collect(DISTINCT coalesce(i.action, '') + ' ' + coalesce(i.action_params, '{{}}')) AS fixes
        WHERE size(hits) > 0
        WITH hits[0] AS i, fixes
        RETURN {_RETURN_FIELDS}, 1.0 AS score, fixes AS distinct_fixes
        """,
        key=signature_key(table_name, column_name, raw_value),
    )
    return records[0].data() if records else None


def find_similar(driver, embedding: list, *, k: int = 5, min_score: float = 0.0) -> list:
    """Verified past incidents ranked by similarity (DL2 / AP1).

    `score` is Neo4j's cosine score on a [0, 1] scale ((1 + cos) / 2), so compare it
    with the Deterministic Lookup threshold on that scale. Unverified or failed
    incidents are excluded, so only proven resolutions can be reused.
    """
    records, _, _ = driver.execute_query(
        f"""
        CALL db.index.vector.queryNodes('{VECTOR_INDEX}', $k, $embedding)
        YIELD node AS i, score
        WHERE i.verified = true AND i.outcome = 'SUCCESS' AND score >= $min_score
        RETURN {_RETURN_FIELDS}, score
        ORDER BY score DESC
        """,
        k=k,
        embedding=embedding,
        min_score=min_score,
    )
    return [r.data() for r in records]
