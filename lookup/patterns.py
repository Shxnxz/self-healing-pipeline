"""Check incident pattern (spec DL2): have we fixed this before, and can the fix be reused?

How the similarity score is computed
------------------------------------
A failure is compared with past verified incidents field by field, not as one
blob of text. Each component is 0..1:

  failure_type  1 same type, 0.5 same family (schema / value / record), else 0
  column        1 same column (or both table-level), else 0
  table         1 same table, 0.7 same layer in another folder
                (silver2_crash1 vs silver2_crash3), else 0
  value         value failures:  1 - DamerauLevenshtein / max length on the
                                 normalised raw value ("Jule" vs "JULY" = 0.75)
                schema failures: Jaccard of the expected_only and source_only
                                 column sets

  score = sum(weight * component), weights in config.SIMILARITY_WEIGHTS.

Embeddings are optional and only widen *recall* (find more candidates). Every
candidate is then scored with the same structured formula, so one threshold has
one meaning.

Why "similar" is not enough to remediate
-----------------------------------------
"Jule" and "June" are one edit apart and score ~0.9, but their fixes differ
(JULY vs JUNE). A past fix is reused only when its parameters still fit:

  exact signature                     -> reusable
  value failure, same column and the
    same value after normalisation    -> reusable (case / spaces / punctuation only)
  schema failure, identical column
    sets (any folder)                 -> reusable (a rename is a rename)
  anything else                       -> diagnose, with the match as a hint

Previous attempts on the same signature that FAILED_VALIDATION or were REJECTED
are returned too, and a match that repeats one of them is not reused.
"""
import json
import re
import sys
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import config

try:
    from agent.memory import incidents  # container: /app/agent
except ImportError:  # repo checkout: lookup/../agent
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
    from agent.memory import incidents
from blast_radius import layer_of
from failures import SCHEMA_TYPES, RECORD_TYPES, VALUE_TYPES, StructuredFailure, family

FAMILY_TYPES = {"SCHEMA": SCHEMA_TYPES, "VALUE": VALUE_TYPES, "RECORD": RECORD_TYPES}


# ----------------------------------------------------------------------------
# Similarity primitives (pure)
# ----------------------------------------------------------------------------
def normalize_value(value: Optional[str]) -> str:
    """Upper case, punctuation and repeated spaces removed: ' jan. ' -> 'JAN'."""
    text = re.sub(r"[^\w$<>=≤%]+", " ", (value or "").upper())
    return re.sub(r"\s+", " ", text).strip()


def osa_distance(a: str, b: str) -> int:
    """Damerau-Levenshtein (optimal string alignment), same metric silver2's fuzzy match uses."""
    d = [[0] * (len(b) + 1) for _ in range(len(a) + 1)]
    for i in range(len(a) + 1):
        d[i][0] = i
    for j in range(len(b) + 1):
        d[0][j] = j
    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            cost = 0 if a[i - 1] == b[j - 1] else 1
            d[i][j] = min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + cost)
            if i > 1 and j > 1 and a[i - 1] == b[j - 2] and a[i - 2] == b[j - 1]:
                d[i][j] = min(d[i][j], d[i - 2][j - 2] + 1)
    return d[len(a)][len(b)]


def string_similarity(a: Optional[str], b: Optional[str]) -> float:
    a, b = normalize_value(a), normalize_value(b)
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return 1.0 - osa_distance(a, b) / max(len(a), len(b))


def jaccard(a, b) -> float:
    a, b = set(a or ()), set(b or ())
    if not a and not b:
        return 1.0
    return len(a & b) / len(a | b)


def parse_schema_value(value: Optional[str]) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    """Inverse of failures.schema_value. Case is kept: `Crash_Date` and `CRASH_DATE`
    are different source columns and need different renames. (Incident.raw_value
    keeps the case; only signature_key is upper-cased.)"""
    expected, source = (), ()
    for part in (value or "").split(";"):
        key, _, cols = part.partition("=")
        items = tuple(c.strip() for c in cols.split(",") if c.strip())
        if key.strip().upper() == "EXPECTED_ONLY":
            expected = items
        elif key.strip().upper() == "SOURCE_ONLY":
            source = items
    return expected, source


def _type_similarity(a: Optional[str], b: Optional[str]) -> float:
    if (a or "").upper() == (b or "").upper():
        return 1.0
    fa, fb = family(a), family(b)
    return 0.5 if fa == fb and fa != "UNKNOWN" else 0.0


def _table_similarity(a: Optional[str], b: Optional[str]) -> float:
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    return 0.7 if layer_of(a) == layer_of(b) != "other" else 0.0


def similarity(failure: StructuredFailure, table: Optional[str], incident: dict) -> Tuple[float, Dict]:
    if failure.family == "SCHEMA":
        exp, src = parse_schema_value(incident.get("raw_value"))
        value = (jaccard(failure.expected_only, exp) + jaccard(failure.source_only, src)) / 2
    else:
        value = string_similarity(failure.signature_value, incident.get("raw_value"))
    components = {
        "failure_type": _type_similarity(failure.failure_type, incident.get("failure_type")),
        "column": 1.0 if (failure.column_name or "") == (incident.get("column_name") or "") else 0.0,
        "table": _table_similarity(table, incident.get("table_name")),
        "value": value,
    }
    total = sum(config.SIMILARITY_WEIGHTS[k] * v for k, v in components.items())
    return round(total, 4), {k: round(v, 3) for k, v in components.items()}


def transferable(failure: StructuredFailure, incident: dict, components: Dict, exact: bool) -> Tuple[bool, str]:
    if exact:
        return True, "exact signature match"
    if failure.family == "VALUE":
        same_value = normalize_value(failure.signature_value) == normalize_value(incident.get("raw_value"))
        if components["column"] == 1.0 and same_value:
            return True, "same column and value after normalisation"
        return False, "similar value, but the canonical target may differ; needs diagnosis"
    if failure.family == "SCHEMA":
        if components["value"] == 1.0 and components["failure_type"] > 0:
            return True, "identical column-set difference"
        return False, "different column sets; the past fix does not cover all columns"
    return False, f"{failure.family.lower()} failures are only reused on an exact signature"


# ----------------------------------------------------------------------------
# Neo4j access
# ----------------------------------------------------------------------------
_CANDIDATES = f"""
MATCH (i:Incident)
WHERE i.verified = true AND i.outcome = 'SUCCESS'
  AND (i.column_name = $column OR i.failure_type IN $types)
RETURN {incidents._RETURN_FIELDS}
ORDER BY i.resolved_at DESC LIMIT $limit
"""

_ATTEMPTS = """
MATCH (i:Incident {signature_key: $key})
WHERE coalesce(i.outcome, '') <> 'SUCCESS'
RETURN i.incident_id AS incident_id, i.action AS action, i.action_params AS action_params,
       i.outcome AS outcome, toString(i.resolved_at) AS resolved_at
ORDER BY i.resolved_at DESC LIMIT 10
"""


def signature_text(failure: StructuredFailure, table: Optional[str]) -> str:
    """Text to embed. The Learn node must embed incidents with the same format and model."""
    return (f"table={table or ''} column={(failure.column_name or '').strip()} "
            f"value={(failure.signature_value or '').strip().upper()} failure={failure.failure_type}")


def _params(raw):
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except ValueError:
            return {"raw": raw}
    return raw or {}


def fetch_candidates(driver, failure: StructuredFailure, table: Optional[str],
                     embed: Optional[Callable[[str], List[float]]] = None):
    """Return (exact hit or None, candidate incidents, failed previous attempts)."""
    exact = incidents.find_exact(driver, table, failure.column_name, failure.signature_value)
    types = sorted(FAMILY_TYPES.get(failure.family, {failure.failure_type}))
    records, _, _ = driver.execute_query(
        _CANDIDATES, column=failure.column_name, types=types, limit=config.MAX_CANDIDATES)
    candidates = {r["incident_id"]: r.data() for r in records}
    if embed is not None:
        vector = embed(signature_text(failure, table))
        for hit in incidents.find_similar(driver, vector, k=config.TOP_K * 3):
            hit["embedding_score"] = hit.pop("score", None)
            candidates.setdefault(hit["incident_id"], {}).update(hit)
    key = incidents.signature_key(table, failure.column_name, failure.signature_value)
    records, _, _ = driver.execute_query(_ATTEMPTS, key=key)
    attempts = [{**r.data(), "action_params": _params(r["action_params"])} for r in records]
    return exact, list(candidates.values()), attempts


# ----------------------------------------------------------------------------
# Decision (pure)
# ----------------------------------------------------------------------------
def evaluate(failure: StructuredFailure, table: Optional[str], exact: Optional[dict],
             candidates: List[dict], attempts: List[dict],
             threshold: float = None) -> Dict:
    threshold = config.SIMILARITY_THRESHOLD if threshold is None else threshold
    scored = []
    if exact:
        s, comps = similarity(failure, table, exact)
        # signature_key is upper-cased, so for schema failures it cannot tell
        # `Crash_Date` from `CRASH_DATE`; trust the key only if the sets agree exactly.
        if failure.family == "SCHEMA" and comps["value"] < 1.0:
            candidates = [exact] + list(candidates)
            exact = None
        else:
            scored.append((1.0, comps, exact, True))
    for inc in candidates:
        if exact and inc.get("incident_id") == exact.get("incident_id"):
            continue
        s, comps = similarity(failure, table, inc)
        scored.append((s, comps, inc, False))
    scored.sort(key=lambda x: x[0], reverse=True)

    failed = {(a.get("action"), json.dumps(a.get("action_params"), sort_keys=True)) for a in attempts}
    ranked = []
    for s, comps, inc, is_exact in scored[: config.TOP_K]:
        ok, why = transferable(failure, inc, comps, is_exact)
        params = _params(inc.get("action_params"))
        if ok and (inc.get("action"), json.dumps(params, sort_keys=True)) in failed:
            ok, why = False, "this action already failed for the same signature"
        ranked.append({
            "incident_id": inc.get("incident_id"),
            "matched_by": "EXACT" if is_exact else ("EMBEDDING" if inc.get("embedding_score") else "STRUCTURED"),
            "score": s,
            "components": comps,
            "embedding_score": inc.get("embedding_score"),
            "transferable": ok,
            "reason": why,
            "failure_type": inc.get("failure_type"),
            "table_name": inc.get("table_name"),
            "column_name": inc.get("column_name"),
            "raw_value": inc.get("raw_value"),
            "diagnosis": inc.get("diagnosis"),
            "action": inc.get("action"),
            "action_params": params,
            "resolved_by": inc.get("resolved_by"),
        })

    best = ranked[0] if ranked else None
    reusable = bool(best and best["score"] >= threshold and best["transferable"])
    if not failure.trusted:
        reusable = False
        reason = "classification needs review; a past fix is not reused automatically"
    elif best is None:
        reason = "no verified incident in memory"
    elif best["score"] < threshold:
        reason = f"best similarity {best['score']:.2f} is below threshold {threshold:.2f}"
    else:
        reason = best["reason"]
    return {
        "signature_key": incidents.signature_key(table, failure.column_name, failure.signature_value),
        "threshold": threshold,
        "reusable": reusable,
        "reason": reason,
        "match": best if reusable else None,
        "candidates": ranked,
        "previous_attempts": attempts,
    }
