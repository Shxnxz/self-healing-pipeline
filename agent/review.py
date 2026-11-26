"""The engineer's side of Escalate: list open cases, read one, send a decision.

  docker compose exec recovery-agent python -m agent.review                      # open cases
  docker compose exec recovery-agent python -m agent.review show <failure_id>    # one case
  docker compose exec recovery-agent python -m agent.review approve  <failure_id> --by NAME [--note ...] [--no-promote]
  docker compose exec recovery-agent python -m agent.review fix      <failure_id> <CANONICAL_VALUE> --by NAME [--note ...] [--no-promote]
  docker compose exec recovery-agent python -m agent.review retry    <failure_id> --by NAME [--note ...]
  docker compose exec recovery-agent python -m agent.review resolved <failure_id> --by NAME --note "what you did"
  docker compose exec recovery-agent python -m agent.review dismiss  <failure_id> --by NAME --note "why"

  docker compose exec recovery-agent python -m agent.review promotions [--all]               # fixes awaiting promotion
  docker compose exec recovery-agent python -m agent.review promote <incident_id> --by NAME [--note ...]

`--by` can be left out when ENGINEER_NAME is set. The decision is checked here
against the saved case, then published to Kafka `recovery.decisions`; the agent
checks it again and resumes the workflow (see its log). This tool only reads the
checkpoints, it never runs a workflow itself, so only the agent writes them.

`promotions` and `promote` are about finished workflows, not open cases. A variant
joins ref_canonical_mappings only when an engineer approves it (spec EL2): an
engineer's own fix (`approve`, `fix`) is promoted by Learn at once, and a fix the
agent made on its own waits in Neo4j (promotion AWAITING_APPROVAL) until an engineer
promotes it here. `promote` writes the variant itself (the same insert-only Delta
MERGE as Learn) and records the approval on the incident.
"""
import argparse
import json
import os
import sqlite3
import sys
from typing import List, Optional

from agent import config
from agent import escalation as esc
from agent import promotion as promo
from agent.consumer import is_waiting, pending_refusal
from agent.memory import incidents as memory


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
                   value: Optional[str] = None, force: bool = False, promote: bool = True) -> dict:
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
                             action, params, promote=promote)


def publish(message: dict) -> None:
    from confluent_kafka import Producer

    producer = Producer({"bootstrap.servers": config.KAFKA_BOOTSTRAP})
    errors = []
    producer.produce(config.DECISIONS_TOPIC, key=message["failure_id"],
                     value=json.dumps(message, ensure_ascii=False).encode(),
                     on_delivery=lambda err, msg: errors.append(err) if err else None)
    if producer.flush(30) or errors:
        sys.exit(f"Could not send the decision to {config.DECISIONS_TOPIC}: {errors or 'timed out'}")


# ----------------------------------------------------------------------------
# Promotion of the agent's own fixes (EL2)
# ----------------------------------------------------------------------------
def _target(incident: dict) -> dict:
    """(column, RAW, canonical) of an incident's validated ADD_VARIANT: the variant
    Validate checked, as Learn recorded it with its decision (promotion_column/_raw/
    _canonical, from Remediate's overlay). Incidents Learn stored before it recorded
    that fall back to the incident's own column, raw value and fix, which is the same
    mapping for a value failure (the overlay key is the failure's raw value)."""
    params = incident.get("action_params") or {}
    return {"column_name": incident.get("promotion_column") or incident.get("column_name"),
            "raw_key": incident.get("promotion_raw") or promo.raw_key(incident.get("raw_value")),
            "canonical_value": incident.get("promotion_canonical")
            or (params.get("canonical_value") if isinstance(params, dict) else None)}


def _variant(target: dict) -> tuple:
    return target["column_name"], target["raw_key"], target["canonical_value"]


def _decide(driver, incident: dict, mappings: Optional[dict] = None) -> dict:
    """The promotion decision with an engineer's approval. `mappings` caches what the
    reference table maps each (column, RAW) to, so one command reads each key once."""
    target = _target(incident)
    history = memory.signature_history(driver, incident.get("signature_key")
                                       or memory.signature_key(incident.get("table_name"), incident.get("column_name"),
                                                               incident.get("raw_value")))
    key = (target["column_name"], target["raw_key"])
    mappings = {} if mappings is None else mappings
    if target["raw_key"] and key not in mappings:
        mappings[key] = promo.current_mapping(*key)
    existing = mappings.get(key)
    return promo.decide(
        incident={**target, "incident_id": incident.get("incident_id"), "action": incident.get("action"),
                  "action_params": incident.get("action_params"), "outcome": incident.get("outcome"),
                  "verified": incident.get("verified"), "borrowed": incident.get("borrowed")},
        history=history, existing=existing, engineer_approved=True)


def promotion_candidates(driver=None) -> List[dict]:
    """Validated agent fixes whose promotion is open, one entry per variant (column, raw
    value, canonical value) with every incident that made it and what an approval would
    do now: PROMOTED (waiting for an engineer), BLOCKED (cannot now) or
    ALREADY_IN_TABLE (done some other way: an engineer's fix, another promotion)."""
    driver = driver or memory.shared_driver()
    variants, mappings = {}, {}
    for incident in memory.awaiting_promotion(driver, promo.PROMOTABLE):
        target = _target(incident)
        key = _variant(target)
        if key not in variants:   # newest incident first: it stands for the variant
            variants[key] = {**target, "incident_id": incident.get("incident_id"),
                             "table_name": incident.get("table_name"), "incidents": [],
                             "decision": _decide(driver, incident, mappings)}
        variants[key]["incidents"].append(incident.get("incident_id"))
    return list(variants.values())


def promotion_line(candidate: dict) -> str:
    decision = candidate["decision"]
    status = decision["status"]
    what = ("promote" if status == promo.PROMOTED else
            "already in the table" if status == promo.ALREADY_IN_TABLE else f"cannot: {'; '.join(decision['reasons'])}")
    doubts = f" doubts: {'; '.join(decision['overridden'])}" if decision.get("overridden") else ""
    return (f"{candidate['incident_id']:<18} {candidate['column_name']}: {candidate['raw_key']!r} -> "
            f"{candidate['canonical_value']}  healed {decision.get('occurrences', 0)} row(s) in "
            f"{decision.get('batches', 0)} batch(es), {len(candidate['incidents'])} incident(s)  [{what}]{doubts}")


def promote_incident(incident_id: str, engineer: str, note: Optional[str] = None, driver=None,
                     override_decline: bool = False) -> dict:
    """An engineer approves promoting a validated fix of the agent (EL2). Returns the
    promotion decision; exits when there is nothing to promote."""
    driver = driver or memory.shared_driver()
    incident = memory.get_incident(driver, incident_id)
    if not incident:
        sys.exit(f"No incident {incident_id} in Neo4j (see `python -m agent.review promotions`)")
    if incident.get("promotion") == promo.PROMOTED:
        sys.exit(f"Incident {incident_id} is promoted already"
                 + (f" (by {incident['promoted_by']})" if incident.get("promoted_by") else ""))
    decision = _decide(driver, incident)
    if decision["status"] == promo.NOT_APPLICABLE:
        sys.exit(f"Incident {incident_id} has nothing to promote: {'; '.join(decision['reasons'])}")
    if incident.get("promotion") == promo.DECLINED and not override_decline:
        sys.exit(f"An engineer approved incident {incident_id} with --no-promote (for these records only). "
                 f"Add --override-decline to promote it anyway.")
    if incident.get("promotion") == promo.DECLINED:
        decision.setdefault("overridden", []).append("an engineer declined promoting this very fix (--no-promote)")
    reasons = list(decision["reasons"]) + [f"engineer {engineer}: {note}" if note else f"engineer {engineer}"]
    target = _target(incident)
    if decision["status"] == promo.PROMOTED:
        decision["inserted"] = promo.promote(target["column_name"], target["raw_key"], target["canonical_value"],
                                             added_by=f"recovery_agent:engineer:{engineer}",
                                             source=f"recovery_agent:{incident_id}")
        memory.mark_promotion(driver, incident_id, promo.PROMOTED, "; ".join(reasons), promoted_by=engineer,
                              target=target)
    elif decision["status"] == promo.ALREADY_IN_TABLE:
        # The engineer approved this variant and it is in the table, perhaps by an
        # earlier run of this command that wrote the row and then lost Neo4j.
        memory.mark_promotion(driver, incident_id, promo.ALREADY_IN_TABLE, "; ".join(reasons),
                              promoted_by=engineer, target=target)
    else:   # BLOCKED: stays open, so it is offered again once the conflict is gone
        memory.mark_promotion(driver, incident_id, promo.BLOCKED, "; ".join(reasons), target=target)
        decision["siblings"] = []
        return decision
    # Every other incident that made the same variant is decided with it, so the
    # variant leaves `promotions` at once instead of resurfacing under the next one.
    siblings = [i["incident_id"] for i in memory.awaiting_promotion(driver, promo.PROMOTABLE)
                if i.get("incident_id") != incident_id and _variant(_target(i)) == _variant(target)]
    for other in siblings:
        memory.mark_promotion(driver, other, promo.ALREADY_IN_TABLE,
                              f"decided with incident {incident_id}: " + "; ".join(reasons))
    decision["siblings"] = siblings
    return decision


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
        if name in ("approve", "fix"):
            cmd.add_argument("--no-promote", dest="promote", action="store_false",
                             help="heal these records only; do not add the variant to ref_canonical_mappings")
    lst = sub.add_parser("promotions", help="validated fixes of the agent waiting for an engineer to promote them")
    lst.add_argument("--all", action="store_true",
                     help="also list variants that cannot be promoted now (conflicts) or are in the table already")
    prom = sub.add_parser("promote", help="add the variant of a validated fix to ref_canonical_mappings")
    prom.add_argument("incident_id", help="from `promotions` (the workflow's failure_id)")
    prom.add_argument("--by", default=os.environ.get("ENGINEER_NAME"), help="your name (or ENGINEER_NAME)")
    prom.add_argument("--note", help="why it is right (stored on the incident)")
    prom.add_argument("--override-decline", action="store_true",
                      help="promote a fix an engineer approved with --no-promote")
    args = parser.parse_args(argv)

    if args.command == "promotions":
        try:
            candidates = promotion_candidates()
        except Exception as exc:   # Neo4j or the reference table unreachable
            sys.exit(f"Could not read the promotions: {type(exc).__name__}: {exc}")
        waiting = [c for c in candidates if c["decision"]["status"] == promo.PROMOTED]
        other = [c for c in candidates if c["decision"]["status"] != promo.PROMOTED]
        print(f"{len(waiting)} variant(s) waiting for an engineer to approve their promotion")
        for candidate in waiting + (other if args.all else []):
            print(promotion_line(candidate))
        if waiting:
            print("Promote one with: python -m agent.review promote <incident_id> --by NAME")
        if other and not args.all:
            print(f"{len(other)} more cannot be promoted now (a conflict) or are in the table already: --all lists them")
        return
    if args.command == "promote":
        if not (args.by or "").strip():
            sys.exit("Say who approves: --by NAME (or set ENGINEER_NAME)")
        try:
            decision = promote_incident(args.incident_id, args.by.strip(), args.note,
                                        override_decline=args.override_decline)
        except Exception as exc:   # Neo4j or the reference table unreachable: run it again
            sys.exit(f"Not promoted: {type(exc).__name__}: {exc}")
        if decision["status"] == promo.PROMOTED:
            print(f"Promoted {decision['column']}: {decision['raw_value']!r} -> {decision['canonical_value']} "
                  f"into ref_canonical_mappings" + ("" if decision.get("inserted") else " (the row was there already)")
                  + ". Silver 2 maps it from its next micro-batch; quarantined rows that still wait for it "
                    "are released with the replay tool (`python -m tools.replay quarantine`, tools/README.md).")
        else:
            print(f"Not promoted ({decision['status']}): {'; '.join(decision['reasons'])}")
        return

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
                             getattr(args, "force", False), promote=getattr(args, "promote", True))
    _, problems = esc.check_decision(state, message)
    if problems:
        sys.exit("Not sent:\n" + "\n".join(f"  - {p}" for p in problems))
    send(message)
    print(f"Sent {message['decision']} on {message['escalation_id']} by {message['engineer']}"
          + (f": {esc.fix_text(message)}" if message["decision"] == esc.APPLY_FIX else "")
          + (" (not promoted)" if message.get("promote") is False else "")
          + ". The agent applies it; follow it with `docker compose logs -f recovery-agent`.")


if __name__ == "__main__":
    main()
