"""Assemble the Deterministic Lookup output: a ranked stack of failures.

  window of failures
     -> merge duplicates (same signature from several batches / redeliveries)
     -> resolve tables + fetch lineage           (blast_radius.fetch_lineage)
     -> score blast radius, incl. "blocks"       (blast_radius.score)
     -> check incident pattern                   (patterns.fetch_candidates / evaluate)
     -> route: remediate | diagnose
     -> sort by impact score, highest first

Each stack item carries the three contexts the next node needs:

  failure           the StructuredFailure (what broke)
  lineage_context   origin/detected tables, upstream, downstream, recent runs,
                    and the blast radius score with its breakdown (what it damages)
  incident_context  best match, top candidates, failed previous attempts (have we
                    fixed it before, and can that fix be reused)
"""
import uuid
from dataclasses import replace
from datetime import datetime, timezone
from typing import Callable, Dict, List, Optional

import blast_radius
import patterns
from failures import StructuredFailure

SCHEMA_VERSION = "1.0"


def merge_failures(failures: List[StructuredFailure]) -> List[StructuredFailure]:
    """One item per signature: the same failure from several batches is one problem."""
    merged: Dict[tuple, StructuredFailure] = {}
    seen = set()
    for f in failures:
        if f.failure_id in seen:  # Kafka redelivery of the same message: count it once
            continue
        seen.add(f.failure_id)
        key =(f.source, f.failure_type, (f.folder or "").lower(), f.detected_layer, f.column_name,
               (f.signature_value or "").strip().upper(), f.origin_layer, f.origin_affected)
        if key not in merged:
            merged[key] = f
            continue
        m = merged[key]
        rows = None if m.rows_affected is None and f.rows_affected is None else \
            (m.rows_affected or 0) + (f.rows_affected or 0)
        batches = sorted({b for x in (m.batch_id, f.batch_id) if x for b in x.split(",")})
        ids = tuple(dict.fromkeys(m.sample_record_ids + f.sample_record_ids))[:5]
        merged_ids = list(dict.fromkeys(
            m.evidence.get("merged_incident_ids", [m.evidence.get("incident_id")])
            + [f.evidence.get("incident_id")]))
        evidence = {**m.evidence, "merged_incident_ids": [i for i in merged_ids if i],
                    "merged_count": m.evidence.get("merged_count", 1) + 1}
        merged[key] = replace(m, rows_affected=rows, batch_id=",".join(batches) or None,
                              sample_record_ids=ids, trusted=m.trusted and f.trusted,
                              evidence=evidence)
    return list(merged.values())


def _route(failure: StructuredFailure, incident_ctx: dict) -> (str, str):
    if incident_ctx["reusable"]:
        m = incident_ctx["match"]
        return "remediate", f"{m['matched_by'].lower()} match {m['score']:.2f} -> reuse {m['action']}"
    return "diagnose", incident_ctx["reason"]


def build_stack(failures: List[StructuredFailure], lineages: List[blast_radius.LineageView],
                incident_ctxs: List[dict]) -> List[dict]:
    """Pure: rank already-fetched failures. The three lists are aligned by index."""
    # Affected table names per failure, to count which failures block which.
    pre = [blast_radius.affected_tables(f, l) for f, l in zip(failures, lineages)]
    affected = [{t["name"] for t in tables} for tables in pre]

    items = []
    for i, (failure, lineage, inc) in enumerate(zip(failures, lineages, incident_ctxs)):
        blocked = sum(
            1 for j, other in enumerate(lineages)
            if j != i and other.origin_table in affected[i]
        )
        radius = blast_radius.score(failure, lineage, blocked=blocked)
        route, why = _route(failure, inc)
        items.append({
            "route": route,
            "route_reason": why,
            "priority": {"score": radius["score"], "tier": radius["tier"]},
            "failure": failure.to_dict(),
            "lineage_context": {**lineage.to_dict(), "blast_radius": radius},
            "incident_context": inc,
        })
    items.sort(key=lambda it: (-it["priority"]["score"], -(it["failure"]["rows_affected"] or 0),
                               it["failure"]["failure_id"]))
    for rank, it in enumerate(items, start=1):
        it["rank"] = rank
    return items


def run_lookup(driver, failures: List[StructuredFailure],
               embed: Optional[Callable[[str], List[float]]] = None) -> dict:
    """Fetch everything from Neo4j for a window of failures and return the output message."""
    failures = merge_failures(failures)
    names = set()
    for f in failures:
        names.update(blast_radius.table_candidates(f.origin_layer, f.folder))
        names.update(blast_radius.table_candidates(f.detected_layer, f.folder))
    known = blast_radius.existing_tables(driver, names) if names else set()

    lineages, incident_ctxs = [], []
    for f in failures:
        lineage = blast_radius.fetch_lineage(driver, f, known)
        exact, candidates, attempts = patterns.fetch_candidates(driver, f, lineage.detected_table, embed)
        lineages.append(lineage)
        incident_ctxs.append(patterns.evaluate(f, lineage.detected_table, exact, candidates, attempts))

    stack = build_stack(failures, lineages, incident_ctxs)
    return envelope(stack)


def envelope(stack: List[dict]) -> dict:
    tiers, routes = {}, {}
    for it in stack:
        tiers[it["priority"]["tier"]] = tiers.get(it["priority"]["tier"], 0) + 1
        routes[it["route"]] = routes.get(it["route"], 0) + 1
    return {
        "schema_version": SCHEMA_VERSION,
        "lookup_id": str(uuid.uuid4()),
        "created_at": datetime.now(timezone.utc).isoformat(),
        "summary": {"failures": len(stack), "routes": routes, "tiers": tiers},
        "stack": stack,
    }
