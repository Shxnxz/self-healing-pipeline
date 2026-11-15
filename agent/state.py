"""State of one recovery workflow (spec LG1).

One workflow handles one failure: one item of the stack that the Deterministic
Lookup publishes to Kafka `recovery.lookup` (lookup/stack.py). LangGraph saves this
state after every node under thread_id = failure_id, so a workflow can be inspected,
resumed after a crash, and is not started twice when Kafka redelivers a message.

Who writes what. Each node returns only its own keys:

  consumer              lookup_id, failure_id, failure, lineage_context,
                        incident_context, priority, lookup_route, route_reason
  deterministic_lookup  route, action (when a verified past fix is reused)
  diagnose              diagnosis   {status, hypothesis, confidence, proposed_action, ...}
  policy_gate           policy      {passed, outcome, reasons, checks, ...}, action (when it passes)
  remediate             execution   {status, execution_id, overlay, rows_affected, ...}
  validate              validation  {status, passed, reasons, checks, rows_released, ...}
  escalate              escalation  {status WAITING, escalation_id, round, reason, details,
                                     case, escalated_at, decision None, history}, status
  engineer_review       escalation  {..., status DECIDED, decision}, status, and for
                                    APPLY_FIX / RETRY: action and the keys the automated
                                    steps write again (cleared)
  learn                 learned     {status STORED|SKIPPED, written, incident_id, outcome,
                                     resolved_by, verified, borrowed, embedded, attempts,
                                     promotion, learned_at}, status
  a stub                stubbed     its own name, until the step is built
  the graph             error       {node, error, attempts} when a step kept raising

`action` is the only thing Remediate executes, and only three nodes may set it:
deterministic_lookup (a fix that already worked), policy_gate (a proposal that
passed both checks) and engineer_review (a fix an engineer decided). A diagnosis on
its own never reaches Remediate.
"""
import operator
from datetime import datetime, timezone
from typing import Annotated, Optional, TypedDict

# Routes published by the Deterministic Lookup (lookup/stack.py).
ROUTE_REMEDIATE = "remediate"
ROUTE_DIAGNOSE = "diagnose"
# deterministic_lookup only: a past fix that an engineer must approve before it runs.
ROUTE_ESCALATE = "escalate"

# execution["status"]
EXECUTION_SUCCESS = "SUCCESS"
EXECUTION_FAILED = "FAILED"

# Final status of a workflow.
STATUS_RESOLVED = "RESOLVED"    # fixed and validated without an engineer
STATUS_ESCALATED = "ESCALATED"  # handed to an engineer, waiting for the decision
STATUS_RESOLVED_BY_ENGINEER = "RESOLVED_BY_ENGINEER"  # an engineer's fix that validated, or fixed by hand
STATUS_DISMISSED = "DISMISSED"  # the engineer decided it is not to be fixed; records stay quarantined
STATUS_FAILED = "FAILED"        # escalate, engineer_review or learn kept raising: nobody has the case

# Marker a stub node leaves so nobody mistakes its output for a real result.
NOT_IMPLEMENTED = "NOT_IMPLEMENTED"


class RecoveryState(TypedDict, total=False):
    # --- from the recovery.lookup stack item ---
    lookup_id: str
    failure_id: str
    failure: dict            # StructuredFailure.to_dict() (lookup/failures.py)
    lineage_context: dict    # tables, upstream, downstream, recent runs, blast_radius
    incident_context: dict   # match, candidates, previous_attempts, reusable, reason
    priority: dict           # {"rank", "score", "tier"}
    lookup_route: str        # route as published: remediate | diagnose
    route_reason: str

    # --- written by the nodes ---
    route: str               # route the graph takes after deterministic_lookup
    action: Optional[dict]   # {"action", "action_params", "source", "incident_id"}
    diagnosis: dict
    policy: dict
    execution: dict
    validation: dict
    escalation: dict
    learned: dict
    status: str
    error: dict              # the step that kept raising (agent/graph.py: guarded)
    trace: Annotated[list, operator.add]  # one entry per node visited, in order
    # Stub steps this workflow went through. Not empty = nothing was really done, so
    # the consumer runs the failure again when it is delivered again.
    stubbed: Annotated[list, operator.add]


def initial_state(lookup_id: Optional[str], item: dict) -> RecoveryState:
    """Turn one stack item of a recovery.lookup message into the workflow input."""
    failure = item["failure"]
    priority = item.get("priority") or {}
    return {
        "lookup_id": lookup_id,
        "failure_id": failure["failure_id"],
        "failure": failure,
        "lineage_context": item.get("lineage_context") or {},
        "incident_context": item.get("incident_context") or {},
        "priority": {"rank": item.get("rank"), "score": priority.get("score"), "tier": priority.get("tier")},
        "lookup_route": item.get("route"),
        "route_reason": item.get("route_reason"),
    }


def step(node: str, note: str) -> dict:
    """A `trace` entry. Every node appends one, so the path of a workflow can be read back."""
    return {"node": node, "at": datetime.now(timezone.utc).isoformat(), "note": note}
