"""Every recovery workflow, one summary line each: the agent's side of the evaluation.

  docker compose exec recovery-agent python -m agent.workflows            # table
  docker compose exec recovery-agent python -m agent.workflows --json     # for evaluation/

Each workflow is read from the SQLite checkpoints (one thread per failure_id) and
reduced to what the evaluation (evaluation/metrics.py) measures: which path it took
(a reused past fix, the model through the gate, an engineer), whether the model was
asked, why it was escalated and what the engineer decided, what Learn stored and
promoted, and when each of those happened. Read only: like agent.review, it never
runs or changes a workflow.
"""
import argparse
import json
import sqlite3
import sys
from typing import List, Optional

from agent import config

# diagnosis["status"] values that mean the model was not asked (nodes/diagnose.py).
NO_MODEL_CALL = {"SKIPPED"}


def _first(trace: list, node: str) -> Optional[str]:
    return next((t.get("at") for t in trace if t.get("node") == node), None)


def summarize(state: dict, next_nodes: Optional[list] = None) -> dict:
    """One workflow's state reduced to flat, JSON-friendly fields (pure, unit-tested)."""
    failure = state.get("failure") or {}
    match = (state.get("incident_context") or {}).get("match") or {}
    action = state.get("action") or {}
    diagnosis = state.get("diagnosis") or {}
    proposed = diagnosis.get("proposed_action") or {}
    policy = state.get("policy") or {}
    execution = state.get("execution") or {}
    validation = state.get("validation") or {}
    escalation = state.get("escalation") or {}
    decision = escalation.get("decision") or {}
    learned = state.get("learned") or {}
    promotion = learned.get("promotion") or {}
    trace = state.get("trace") or []
    nodes = [t.get("node") for t in trace]
    history = escalation.get("history") or []
    diagnose_runs = nodes.count("diagnose")
    rounds = [*history, escalation] if escalation else []
    return {
        "failure_id": state.get("failure_id"),
        "lookup_id": state.get("lookup_id"),
        "status": state.get("status"),
        "waiting": list(next_nodes or []) == ["engineer_review"],
        "next": list(next_nodes or []),
        "failure_type": failure.get("failure_type"),
        "source": failure.get("source"),
        "folder": failure.get("folder"),
        "column": failure.get("column_name"),
        "raw_value": failure.get("raw_value"),
        "signature_value": failure.get("signature_value"),
        "rows_affected": failure.get("rows_affected"),
        "batch_id": failure.get("batch_id"),
        "priority_tier": (state.get("priority") or {}).get("tier"),
        "priority_score": (state.get("priority") or {}).get("score"),
        "lookup_route": state.get("lookup_route"),
        "route": state.get("route"),
        "matched_by": match.get("matched_by"),
        "match_score": match.get("score"),
        "reused_incident_id": match.get("incident_id") if action.get("source") == "INCIDENT" else None,
        "action": action.get("action"),
        "action_params": action.get("action_params"),
        "action_source": action.get("source"),
        "approved_by": action.get("approved_by"),
        "diagnose_runs": diagnose_runs,
        "model_called": diagnose_runs > 0 and diagnosis.get("status") not in NO_MODEL_CALL,
        "diagnosis_status": diagnosis.get("status"),
        "diagnosis_model": diagnosis.get("model"),
        "diagnosis_confidence": diagnosis.get("confidence"),
        "diagnosis_action": proposed.get("action"),
        "diagnosis_params": proposed.get("action_params"),
        "policy_outcome": policy.get("outcome"),
        "policy_reasons": policy.get("reasons"),
        "execution_status": execution.get("status"),
        "execution_id": execution.get("execution_id"),
        "validation_status": validation.get("status"),
        "validation_reasons": validation.get("reasons"),
        "rows_released": validation.get("rows_released"),
        "escalated": bool(escalation),
        "escalation_rounds": len(rounds),
        "escalation_reasons": [r.get("reason") for r in rounds],
        "decision": decision.get("decision"),
        "engineer": decision.get("engineer"),
        "learned_status": learned.get("status"),
        "learned_outcome": learned.get("outcome"),
        "resolved_by": learned.get("resolved_by"),
        "borrowed": learned.get("borrowed"),
        "promotion_status": promotion.get("status"),
        "promotion_route": promotion.get("route"),
        "error_node": (state.get("error") or {}).get("node"),
        "path": nodes,
        "started_at": trace[0].get("at") if trace else None,
        "diagnosed_at": _first(trace, "diagnose"),
        "escalated_at": (rounds[0].get("escalated_at") if rounds else None),
        "decided_at": decision.get("decided_at"),
        "remediated_at": execution.get("executed_at"),
        "validated_at": validation.get("validated_at"),
        "ended_at": learned.get("learned_at") or (trace[-1].get("at") if trace and not next_nodes else None),
    }


def thread_ids(checkpointer) -> List[str]:
    conn = getattr(checkpointer, "conn", None)
    if isinstance(conn, sqlite3.Connection):
        try:
            return [r[0] for r in conn.execute("SELECT DISTINCT thread_id FROM checkpoints WHERE checkpoint_ns = ''")]
        except sqlite3.OperationalError:  # a fresh state volume: no workflow has been saved yet
            return []
    return sorted({c.config["configurable"]["thread_id"] for c in checkpointer.list(None)})


def all_workflows(graph) -> List[dict]:
    out = []
    for failure_id in thread_ids(graph.checkpointer):
        saved = graph.get_state({"configurable": {"thread_id": failure_id}})
        if saved.values:
            out.append(summarize(saved.values, list(saved.next)))
    out.sort(key=lambda w: w.get("started_at") or "")
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="python -m agent.workflows", description=__doc__.split("\n\n")[0])
    parser.add_argument("--json", action="store_true", help="one JSON list (the evaluation reads this)")
    parser.add_argument("--since", help="only workflows started at or after this ISO time")
    args = parser.parse_args(argv)

    from agent.graph import build_graph, open_checkpointer

    rows = all_workflows(build_graph(open_checkpointer(config.CHECKPOINT_PATH)))
    if args.since:
        rows = [w for w in rows if (w.get("started_at") or "") >= args.since]
    if args.json:
        print(json.dumps(rows, indent=1, ensure_ascii=False, default=str))
        return 0
    for w in rows:
        print(f"{w['failure_id']:<18} {w['status'] or '-':<22} {w['folder'] or '-'}:{w['column'] or w['failure_type']} "
              f"value={w['raw_value']!r} via={w['action_source'] or '-'} model={'yes' if w['model_called'] else 'no'} "
              f"escalations={w['escalation_rounds']} decision={w['decision'] or '-'} "
              f"promotion={w['promotion_status'] or '-'}")
    print(f"{len(rows)} workflow(s)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
