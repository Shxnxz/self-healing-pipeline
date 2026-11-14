"""The recovery graph (spec LG1): Deterministic Lookup through Learn.

  from                  to
  START                 deterministic_lookup
  deterministic_lookup  remediate when a verified past fix is reused, escalate when that
                        fix needs an engineer's approval first, else diagnose
  diagnose              policy_gate
  policy_gate           remediate when it passes, else escalate
  remediate             validate on SUCCESS, else escalate
  validate              learn when it passes, else escalate
  escalate              engineer_review (the workflow pauses there for the decision)
  engineer_review       remediate (APPLY_FIX), deterministic_lookup (RETRY),
                        learn (RESOLVED_MANUALLY, DISMISS: the engineer's outcome is stored too)
  learn                 END

The routing lives here and reads only the keys documented in agent/state.py, so a
step is filled in by rewriting its module in agent/nodes/ and nothing else.
Every route fails closed: a missing or unexpected value goes to the engineer, and
so does a step that keeps raising (`guarded`).
"""
import sqlite3
import time
from pathlib import Path
from typing import Callable, Dict, Optional

from langgraph.errors import GraphBubbleUp
from langgraph.graph import END, START, StateGraph

from agent import config
from agent import escalation as esc
from agent.nodes import NODES
from agent.state import (EXECUTION_SUCCESS, ROUTE_ESCALATE, ROUTE_REMEDIATE, STATUS_FAILED, RecoveryState,
                         step)

# Steps with nothing after them to catch a failure: if one keeps raising, the
# workflow ends FAILED instead of ESCALATED.
LAST_STEPS = ("escalate", "engineer_review", "learn")


def guarded(name: str, fn: Callable, max_attempts: int, retry_seconds: float) -> Callable:
    """Wrap a step so that an exception never stops the agent.

    The step is retried (a database that is restarting comes back), so it must be
    safe to run more than once. When it still raises, the error is written to the
    state and the graph routes the failure to Escalate. Without this, one failure
    whose step always raises would be redelivered and crash the agent forever, with
    every later message stuck behind it.
    """
    def run(state: RecoveryState) -> dict:
        delay = retry_seconds
        for attempt in range(1, max_attempts + 1):
            try:
                return fn(state)
            except GraphBubbleUp:  # LangGraph control flow (interrupt), not an error
                raise
            except Exception as exc:
                error = f"{type(exc).__name__}: {exc}"
                print(f"[agent] {state.get('failure_id')} {name} raised {error} "
                      f"(attempt {attempt}/{max_attempts})", flush=True)
                if attempt < max_attempts:
                    time.sleep(delay)
                    delay *= 2
        update = {"error": {"node": name, "error": error, "attempts": max_attempts},
                  "trace": [step(name, f"gave up after {max_attempts} attempts: {error}")]}
        if name in LAST_STEPS:
            update["status"] = STATUS_FAILED
        return update
    return run


def route_after_lookup(state: RecoveryState) -> str:
    if state.get("error"):
        return "escalate"
    has_action = bool((state.get("action") or {}).get("action"))
    if state.get("route") == ROUTE_REMEDIATE and has_action:
        return "remediate"
    if state.get("route") == ROUTE_ESCALATE and has_action:
        return "escalate"
    return "diagnose"


def route_after_diagnose(state: RecoveryState) -> str:
    return "escalate" if state.get("error") else "policy_gate"


def route_after_policy_gate(state: RecoveryState) -> str:
    if state.get("error"):
        return "escalate"
    passed = (state.get("policy") or {}).get("passed") is True
    return "remediate" if passed and (state.get("action") or {}).get("action") else "escalate"


def route_after_remediate(state: RecoveryState) -> str:
    if state.get("error"):
        return "escalate"
    return "validate" if (state.get("execution") or {}).get("status") == EXECUTION_SUCCESS else "escalate"


def route_after_validate(state: RecoveryState) -> str:
    if state.get("error"):
        return "escalate"
    return "learn" if (state.get("validation") or {}).get("passed") is True else "escalate"


def _failed_itself(state: RecoveryState, node: str) -> bool:
    return (state.get("error") or {}).get("node") == node


def route_after_escalate(state: RecoveryState) -> str:
    # Escalate that keeps raising has opened no case: end FAILED rather than wait for nobody.
    return "learn" if _failed_itself(state, "escalate") else "engineer_review"


def route_after_engineer_review(state: RecoveryState) -> str:
    if _failed_itself(state, "engineer_review"):
        return "learn"
    escalation = state.get("escalation") or {}
    decision = (escalation.get("decision") or {}).get("decision") if escalation.get("status") == esc.DECIDED else None
    if decision == esc.APPLY_FIX and (state.get("action") or {}).get("action"):
        return "remediate"
    if decision == esc.RETRY:
        return "deterministic_lookup"
    return "learn"


def build_graph(checkpointer=None, nodes: Optional[Dict[str, Callable]] = None,
                max_attempts: Optional[int] = None, retry_seconds: Optional[float] = None):
    """Compile the graph. `nodes` replaces steps by name (tests use it to drive each path)."""
    unknown = set(nodes or {}) - set(NODES)
    if unknown:
        raise ValueError(f"Unknown node(s): {sorted(unknown)}")
    steps = {**NODES, **(nodes or {})}
    max_attempts = max(1, config.NODE_MAX_ATTEMPTS if max_attempts is None else max_attempts)
    retry_seconds = config.NODE_RETRY_SECONDS if retry_seconds is None else retry_seconds

    builder = StateGraph(RecoveryState)
    for name, fn in steps.items():
        builder.add_node(name, guarded(name, fn, max_attempts, retry_seconds))

    builder.add_edge(START, "deterministic_lookup")
    builder.add_conditional_edges("deterministic_lookup", route_after_lookup, ["remediate", "diagnose", "escalate"])
    builder.add_conditional_edges("diagnose", route_after_diagnose, ["policy_gate", "escalate"])
    builder.add_conditional_edges("policy_gate", route_after_policy_gate, ["remediate", "escalate"])
    builder.add_conditional_edges("remediate", route_after_remediate, ["validate", "escalate"])
    builder.add_conditional_edges("validate", route_after_validate, ["learn", "escalate"])
    builder.add_conditional_edges("escalate", route_after_escalate, ["engineer_review", "learn"])
    builder.add_conditional_edges("engineer_review", route_after_engineer_review,
                                  ["remediate", "deterministic_lookup", "learn"])
    builder.add_edge("learn", END)
    return builder.compile(checkpointer=checkpointer)


def open_checkpointer(path: str):
    """SQLite checkpoints on disk, so workflow state survives a restart of the agent."""
    from langgraph.checkpoint.sqlite import SqliteSaver

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return SqliteSaver(sqlite3.connect(path, check_same_thread=False))


if __name__ == "__main__":  # python -m agent.graph  -> Mermaid diagram of the compiled graph
    print(build_graph().get_graph().draw_mermaid())
