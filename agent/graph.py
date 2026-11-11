"""The recovery graph (spec LG1): Deterministic Lookup through Learn.

  from                  to
  START                 deterministic_lookup
  deterministic_lookup  remediate when a verified past fix is reused, else diagnose
  diagnose              policy_gate
  policy_gate           remediate when it passes, else escalate
  remediate             validate on SUCCESS, else escalate
  validate              learn when it passes, else escalate
  escalate              learn (the engineer's outcome is stored too)
  learn                 END

The routing lives here and reads only the keys documented in agent/state.py, so a
step is filled in by rewriting its module in agent/nodes/ and nothing else.
Every route fails closed: a missing or unexpected value goes to the engineer.
"""
import sqlite3
from pathlib import Path
from typing import Callable, Dict, Optional

from langgraph.graph import END, START, StateGraph

from agent.nodes import NODES
from agent.state import EXECUTION_SUCCESS, ROUTE_REMEDIATE, RecoveryState


def route_after_lookup(state: RecoveryState) -> str:
    if state.get("route") == ROUTE_REMEDIATE and (state.get("action") or {}).get("action"):
        return "remediate"
    return "diagnose"


def route_after_policy_gate(state: RecoveryState) -> str:
    passed = (state.get("policy") or {}).get("passed") is True
    return "remediate" if passed and (state.get("action") or {}).get("action") else "escalate"


def route_after_remediate(state: RecoveryState) -> str:
    return "validate" if (state.get("execution") or {}).get("status") == EXECUTION_SUCCESS else "escalate"


def route_after_validate(state: RecoveryState) -> str:
    return "learn" if (state.get("validation") or {}).get("passed") is True else "escalate"


def build_graph(checkpointer=None, nodes: Optional[Dict[str, Callable]] = None):
    """Compile the graph. `nodes` replaces steps by name (tests use it to drive each path)."""
    unknown = set(nodes or {}) - set(NODES)
    if unknown:
        raise ValueError(f"Unknown node(s): {sorted(unknown)}")
    steps = {**NODES, **(nodes or {})}

    builder = StateGraph(RecoveryState)
    for name, fn in steps.items():
        builder.add_node(name, fn)

    builder.add_edge(START, "deterministic_lookup")
    builder.add_conditional_edges("deterministic_lookup", route_after_lookup, ["remediate", "diagnose"])
    builder.add_edge("diagnose", "policy_gate")
    builder.add_conditional_edges("policy_gate", route_after_policy_gate, ["remediate", "escalate"])
    builder.add_conditional_edges("remediate", route_after_remediate, ["validate", "escalate"])
    builder.add_conditional_edges("validate", route_after_validate, ["learn", "escalate"])
    builder.add_edge("escalate", "learn")
    builder.add_edge("learn", END)
    return builder.compile(checkpointer=checkpointer)


def open_checkpointer(path: str):
    """SQLite checkpoints on disk, so workflow state survives a restart of the agent."""
    from langgraph.checkpoint.sqlite import SqliteSaver

    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return SqliteSaver(sqlite3.connect(path, check_same_thread=False))


if __name__ == "__main__":  # python -m agent.graph  -> Mermaid diagram of the compiled graph
    print(build_graph().get_graph().draw_mermaid())
