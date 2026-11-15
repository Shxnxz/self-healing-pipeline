"""The engineer's side of Escalate: list open cases, read one, send a decision.

  docker compose exec recovery-agent python -m agent.review                      # open cases
  docker compose exec recovery-agent python -m agent.review show <failure_id>    # one case
  docker compose exec recovery-agent python -m agent.review approve  <failure_id> --by NAME [--note ...]
  docker compose exec recovery-agent python -m agent.review fix      <failure_id> <CANONICAL_VALUE> --by NAME [--note ...]
  docker compose exec recovery-agent python -m agent.review retry    <failure_id> --by NAME [--note ...]
  docker compose exec recovery-agent python -m agent.review resolved <failure_id> --by NAME --note "what you did"
  docker compose exec recovery-agent python -m agent.review dismiss  <failure_id> --by NAME --note "why"

`--by` can be left out when ENGINEER_NAME is set. The decision is checked here
against the saved case, then published to Kafka `recovery.decisions`; the agent
checks it again and resumes the workflow (see its log). This tool only reads the
checkpoints, it never runs a workflow itself, so only the agent writes them.
"""
import argparse
import json
import os
import sqlite3
import sys
from typing import List, Optional

from agent import config
from agent import escalation as esc
from agent.consumer import is_waiting, pending_refusal


def thread(failure_id: str) -> dict:
    return {"configurable": {"thread_id": failure_id}}


def open_graph(path: Optional[str] = None):
    from agent.graph import build_graph, open_checkpointer

    return build_graph(open_checkpointer(path or config.CHECKPOINT_PATH))


# Threads whose latest checkpoint has a pending interrupt: the only ones that can be
# waiting. Checkpoint ids sort by time (SqliteSaver orders by them too), so the few
# open cases are found without loading every finished workflow.
_PAUSED = """
SELECT DISTINCT w.thread_id FROM writes w
WHERE w.checkpoint_ns = '' AND w.channel = '__interrupt__'
  AND w.checkpoint_id = (SELECT MAX(c.checkpoint_id) FROM checkpoints c
                         WHERE c.thread_id = w.thread_id AND c.checkpoint_ns = '')
"""


def paused_thread_ids(checkpointer) -> List[str]:
    """Candidates for open cases (is_waiting confirms each)."""
    if isinstance(getattr(checkpointer, "conn", None), sqlite3.Connection):
        return [row[0] for row in checkpointer.conn.execute(_PAUSED).fetchall()]
    return sorted({c.config["configurable"]["thread_id"] for c in checkpointer.list(None)})


def open_cases(graph) -> List[dict]:
    """The workflows waiting for an engineer, highest priority first, then oldest."""
    cases = []
    for failure_id in paused_thread_ids(graph.checkpointer):
        saved = graph.get_state(thread(failure_id))
        if is_waiting(saved):
            cases.append({**saved.values, "refused": pending_refusal(saved)})
    cases.sort(key=lambda s: (-((s.get("priority") or {}).get("score") or 0),
                              (s.get("escalation") or {}).get("escalated_at") or ""))
    return cases


def waiting_state(graph, failure_id: str) -> dict:
    saved = graph.get_state(thread(failure_id))
    if not saved.values:
        sys.exit(f"No workflow is saved for failure {failure_id}")
    if not is_waiting(saved):
        sys.exit(f"Failure {failure_id} is not waiting for an engineer (status {saved.values.get('status')}, "
                 f"next {list(saved.next) or 'nothing: it has ended'})")
    return {**saved.values, "refused": pending_refusal(saved)}


# ----------------------------------------------------------------------------
# Printing
# ----------------------------------------------------------------------------
def summary_line(state: dict) -> str:
    escalation = state.get("escalation") or {}
    failure = state.get("failure") or {}
    case = escalation.get("case") or {}
    what = failure.get("column_name") or failure.get("failure_type")
    return (f"{state.get('failure_id'):<18} {(state.get('priority') or {}).get('tier') or '-':<3} "
            f"{escalation.get('reason'):<22} {failure.get('folder') or '-'}:{what} "
            f"value={failure.get('raw_value')!r} rows={failure.get('rows_affected')} "
            f"proposed={esc.fix_text(case.get('proposed_fix'))} since {escalation.get('escalated_at')}"
            + (" [last decision REFUSED, see show]" if state.get("refused") else ""))


def _lines(title: str, items) -> List[str]:
    items = [i for i in items or [] if i not in (None, "", [], {})]
    return [f"{title}:"] + [f"  - {i}" for i in items] if items else []


def render_case(state: dict) -> str:
    escalation = state.get("escalation") or {}
    case = escalation.get("case") or {}
    failure = case.get("failure") or {}
    radius = case.get("blast_radius") or {}
    diagnosis = case.get("diagnosis") or {}
    out = [
        f"Escalation {case.get('escalation_id')} (round {case.get('round')}), opened {escalation.get('escalated_at')}",
        f"Reason: {case.get('reason')}",
        *_lines("Details", case.get("details")),
        "",
        f"Failure {failure.get('failure_id')}: {failure.get('failure_type')} in {failure.get('folder')} "
        f"({case.get('table')}), column {failure.get('column_name')}, raw value {failure.get('raw_value')!r}, "
        f"{failure.get('rows_affected')} row(s), batches {failure.get('batch_id')}",
        f"Priority {radius.get('tier')} (impact score {radius.get('score')}), downstream: "
        f"{', '.join(n for n in (case.get('lineage') or {}).get('downstream') or [] if n) or 'none known'}",
    ]
    if diagnosis:
        out += ["", f"Diagnosis ({diagnosis.get('status')}, model {diagnosis.get('model')}): "
                    f"{diagnosis.get('hypothesis') or 'no hypothesis'}"]
        if diagnosis.get("proposed_action"):
            out.append(f"  proposed {esc.fix_text(diagnosis.get('proposed_action'))} "
                       f"at confidence {diagnosis.get('confidence')}")
        out += [f"  {r}" for r in diagnosis.get("reasons") or []]
    for name in ("policy", "execution", "validation"):
        part = case.get(name)
        if part:
            out += ["", f"{name.capitalize()}: {part.get('outcome') or part.get('status')}"]
            out += [f"  - {r}" for r in part.get("reasons") or []]
    past = [f"{p.get('matched_by')} {p.get('score')}: {p.get('raw_value')!r} in {p.get('table_name')} -> "
            f"{esc.fix_text(p)} ({p.get('reason')})" for p in case.get("past_incidents") or []]
    attempts = [f"{esc.fix_text(a)} -> {a.get('outcome')}" for a in case.get("previous_attempts") or []]
    samples = [json.dumps(r, ensure_ascii=False, default=str) for r in case.get("sample_records") or []]
    out += [""] + _lines("Closest past incidents", past) + _lines("Earlier failed attempts", attempts) \
        + _lines("Sample records", samples)
    if case.get("canonical_values"):
        out += [f"Canonical values of {failure.get('column_name')}: {', '.join(case['canonical_values'])}"]
    history = [f"round {h.get('round')}: {h.get('reason')} -> "
               f"{(h.get('decision') or {}).get('decision')} by {(h.get('decision') or {}).get('engineer')}"
               for h in escalation.get("history") or []]
    out += _lines("Earlier rounds", history)
    refused = state.get("refused")
    if refused:
        decision = refused.get("decision") or {}
        out += ["", f"LAST DECISION REFUSED: {decision.get('decision')} by {decision.get('engineer')} "
                    f"({esc.fix_text(decision) if decision.get('action') else 'no fix'}) was not applied:"]
        out += [f"  - {p}" for p in refused.get("problems") or []] + ["  The case is still open: decide again."]
    fid = state.get("failure_id")
    out += ["", f"Proposed fix: {esc.fix_text(case.get('proposed_fix'))}", "Decide with one of:"]
    if case.get("proposed_fix") and not failed_validation(case):
        out.append(f"  python -m agent.review approve {fid} --by NAME")
    elif case.get("proposed_fix"):
        out.append(f"  (approve is hidden: this fix just failed Validate and would fail the same way; "
                   f"--force runs it again anyway)")
    if failure.get("column_name") and case.get("canonical_values"):
        out.append(f"  python -m agent.review fix {fid} <CANONICAL_VALUE> --by NAME")
    out += [f"  python -m agent.review retry {fid} --by NAME",
            f"  python -m agent.review resolved {fid} --by NAME --note \"what you did\"",
            f"  python -m agent.review dismiss {fid} --by NAME --note \"why\""]
    return "\n".join(out)


# ----------------------------------------------------------------------------
# Deciding
# ----------------------------------------------------------------------------
def failed_validation(case: dict) -> bool:
    """The proposed fix is the one Validate just failed: approving it repeats the same result."""
    return case.get("reason") == esc.VALIDATION_FAILED


def build_decision(state: dict, command: str, engineer: str, note: Optional[str],
                   value: Optional[str] = None, force: bool = False) -> dict:
    escalation = state.get("escalation") or {}
    case = escalation.get("case") or {}
    kind = {"approve": esc.APPLY_FIX, "fix": esc.APPLY_FIX, "retry": esc.RETRY,
            "resolved": esc.RESOLVED_MANUALLY, "dismiss": esc.DISMISS}[command]
    action, params = None, None
    if command == "approve":
        fix = case.get("proposed_fix")
        if not fix:
            sys.exit("This case has no proposed fix to approve; use `fix`, `resolved` or `dismiss`.")
        if failed_validation(case) and not force:
            sys.exit("The proposed fix is the one that just failed Validate, and approving it would fail the "
                     "same way. Use `fix VALUE` for another value, `dismiss`, or `approve --force` to run it again.")
        action, params = fix["action"], fix.get("action_params") or {}
    elif command == "fix":
        action, params = "ADD_VARIANT", {"canonical_value": (value or "").strip().upper()}
    return esc.make_decision(state["failure_id"], escalation.get("escalation_id"), kind, engineer, note,
                             action, params)


def publish(message: dict) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    errors = []
    producer.produce(config.DECISIONS_TOPIC, key=message["failure_id"],
                     value=json.dumps(message, ensure_ascii=False).encode(),
                     on_delivery=lambda err, msg: errors.append(err) if err else None)
    if producer.flush(30) or errors:
        sys.exit(f"Could not send the decision to {config.DECISIONS_TOPIC}: {errors or 'timed out'}")


def main(argv: Optional[List[str]] = None, graph=None, send=publish) -> None:
    parser = argparse.ArgumentParser(prog="python -m agent.review", description=__doc__.split("\n\n")[0])
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("list", help="open cases, highest priority first")
    show = sub.add_parser("show", help="one case in full")
    show.add_argument("failure_id")
    show.add_argument("--json", action="store_true", help="the saved case as JSON")
    for name, help_ in (("approve", "apply the proposed fix"), ("fix", "apply your own canonical value"),
                        ("retry", "run the automated path again"),
                        ("resolved", "you fixed it outside the agent"), ("dismiss", "not to be fixed")):
        cmd = sub.add_parser(name, help=help_)
        cmd.add_argument("failure_id")
        if name == "fix":
            cmd.add_argument("value", help="a canonical value of the column")
        cmd.add_argument("--by", default=os.environ.get("ENGINEER_NAME"), help="your name (or ENGINEER_NAME)")
        cmd.add_argument("--note", help="required for resolved and dismiss")
        if name == "approve":
            cmd.add_argument("--force", action="store_true",
                             help="approve a fix that just failed Validate (it will most likely fail again)")
    args = parser.parse_args(argv)
    graph = graph or open_graph()

    if args.command in (None, "list"):
        cases = open_cases(graph)
        print(f"{len(cases)} failure(s) waiting for an engineer")
        for state in cases:
            print(summary_line(state))
        return
    if args.command == "show":
        state = waiting_state(graph, args.failure_id)
        case = (state.get("escalation") or {}).get("case")
        print(json.dumps(case, indent=2, ensure_ascii=False, default=str) if args.json else render_case(state))
        return

    if not (args.by or "").strip():
        sys.exit("Say who decides: --by NAME (or set ENGINEER_NAME)")
    state = waiting_state(graph, args.failure_id)
    message = build_decision(state, args.command, args.by.strip(), args.note, getattr(args, "value", None),
                             getattr(args, "force", False))
    _, problems = esc.check_decision(state, message)
    if problems:
        sys.exit("Not sent:\n" + "\n".join(f"  - {p}" for p in problems))
    send(message)
    print(f"Sent {message['decision']} on {message['escalation_id']} by {message['engineer']}"
          + (f": {esc.fix_text(message)}" if message["decision"] == esc.APPLY_FIX else "")
          + ". The agent applies it; follow it with `docker compose logs -f recovery-agent`.")


if __name__ == "__main__":
    main()
