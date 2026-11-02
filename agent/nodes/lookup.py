"""Deterministic Lookup node for the LangGraph agent (spec DL1, DL2, DL3).

The lookup logic lives in lookup/ (see lookup/README.md); this module only adapts
it to LangGraph state, so the Kafka service and the agent share one implementation:

  lookup/blast_radius.py  impact score 0..100 from the lineage graph
  lookup/patterns.py      exact + structured similarity against verified incidents,
                          with a check that the past fix still fits
  lookup/stack.py         ranks a batch of failures into a stack

State in:
  lookup_result   a message from Kafka `recovery.lookup` (already computed), or
  failures        a list of failures to look up now. Each item may be a
                  StructuredFailure dict, a Qwen handoff message, or the legacy
                  quarantine group {table_name, column_name, raw_value, ...}
  failure / failure_group / quarantine_group   a single failure (legacy keys)

State out: the whole stack plus the current (highest priority) item flattened
into `route`, `failure`, `lineage_context`, `incident_context`, `blast_radius`
and, for remediate, `action`, `action_params`, `source_incident`, `confidence`.
After remediate/diagnose finish, `next_failure` pops the stack.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

_LOOKUP_DIR = Path(__file__).resolve().parents[2] / "lookup"
if str(_LOOKUP_DIR) not in sys.path:
    sys.path.insert(0, str(_LOOKUP_DIR))

import config as lookup_config  # noqa: E402  (lookup/config.py)
from failures import StructuredFailure, from_qwen_incident  # noqa: E402
from stack import run_lookup  # noqa: E402

DEFAULT_SIMILARITY_THRESHOLD = lookup_config.SIMILARITY_THRESHOLD
DONE = "done"


def _legacy_group(group: Dict[str, Any]) -> StructuredFailure:
    """{table_name: 'silver2_crash1', column_name, raw_value, ...} -> StructuredFailure."""
    table = (group.get("table_name") or "silver2").strip().rstrip("/").split("/")[-1]
    layer, _, folder = table.partition("_")
    layers = lookup_config.LAYER_ORDER
    if layer not in layers:
        layer, folder = "silver2", ""
    origin = layers[max(layers.index(layer) - 1, 0)]
    return StructuredFailure(
        source="QUARANTINE",
        failure_type=group.get("failure_type") or "UNRESOLVED_VALUE",
        origin_layer=origin,
        detected_layer=layer,
        folder=folder or group.get("folder"),
        column_name=group.get("column_name"),
        raw_value=group.get("raw_value"),
        rows_affected=group.get("rows_affected"),
        batch_id=group.get("batch_id"),
    )


def to_failure(item: Any) -> Optional[StructuredFailure]:
    if isinstance(item, StructuredFailure):
        return item
    if not isinstance(item, dict):
        raise TypeError(f"Unsupported failure type: {type(item).__name__}")
    if "origin_layer" in item:
        return StructuredFailure.from_dict(item)
    if "classification_status" in item or "job_name" in item:
        return from_qwen_incident(item)
    return _legacy_group(item)


def lookup(driver, failures: List[Any],
           embed: Optional[Callable[[str], List[float]]] = None) -> Dict[str, Any]:
    """Run the Deterministic Lookup on failures; returns the recovery.lookup message."""
    parsed = [f for f in (to_failure(x) for x in failures) if f is not None]
    return run_lookup(driver, parsed, embed)


def _select(stack: List[dict], result: Dict[str, Any]) -> Dict[str, Any]:
    """State update that makes stack[0] the current item."""
    updates: Dict[str, Any] = {"lookup_result": result, "stack": stack}
    if not stack:
        return {**updates, "current": None, "route": DONE}
    item = stack[0]
    inc = item["incident_context"]
    updates.update({
        "current": item,
        "route": item["route"],
        "priority": item["priority"],
        "failure": item["failure"],
        "lineage_context": item["lineage_context"],
        "incident_context": inc,
        "blast_radius": item["lineage_context"]["blast_radius"],
    })
    match = inc.get("match")
    if item["route"] == "remediate" and match:
        updates.update({
            "action": match["action"],
            "action_params": match["action_params"],
            "source_incident": match["incident_id"],
            "confidence": match["score"],
            "diagnosis_context": None,
        })
    else:
        updates.update({"action": None, "action_params": None, "source_incident": None,
                        "confidence": None, "diagnosis_context": item})
    return updates


def lookup_node(state: Dict[str, Any], driver=None,
                embed: Optional[Callable[[str], List[float]]] = None) -> Dict[str, Any]:
    """LangGraph node: compute (or accept) the stack and select its top item."""
    result = state.get("lookup_result")
    if not (result and "stack" in result):
        failures = state.get("failures")
        if failures is None:
            single = state.get("failure_group") or state.get("quarantine_group") or state.get("failure")
            failures = [single] if single else []
        if driver is None:
            from agent.memory import incidents
            driver = incidents.get_driver()
        result = lookup(driver, failures, embed=embed)
    return _select(list(result["stack"]), result)


def next_failure(state: Dict[str, Any]) -> Dict[str, Any]:
    """LangGraph node: drop the handled item and select the next one."""
    remaining = list(state.get("stack") or [])[1:]
    return _select(remaining, state.get("lookup_result") or {})


def route_after_lookup(state: Dict[str, Any]) -> str:
    """Conditional edge: 'remediate', 'diagnose', or 'done' when the stack is empty."""
    return state.get("route") or DONE
