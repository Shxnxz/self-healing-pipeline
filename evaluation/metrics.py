"""The evaluation's numbers, from what the pipeline and the agent recorded (pure Python).

Inputs, per injected run:
  records    `python -m tools.fault_injector status <run_id> --json` -> "records": the
             ground truth of every injected record and where it is now
  workflows  `python -m agent.workflows --json`: every recovery workflow of the agent

The metrics (team sheet, Action Plan 5.3, and the spec's claims):

  detection           did each fault stop where it should (quarantine, dead letter,
                      fuzzy fix)? MTTD = published -> quarantined
  silent errors       records that reached Silver 2 with a wrong value
  auto-resolution     records healed by the agent without an engineer, over the records
                      that needed healing and have a right answer
  without the model   healed by a reused past fix (INCIDENT), or never quarantined
                      because an earlier run taught Silver 2 the value
  escalation          workflows handed to an engineer, and why
  wrong fixes         healed records whose value is not the right one, and values with
                      no right answer the agent mapped anyway; "unsafe": any of these
                      no engineer approved
  time to recover     published -> released into Silver 2 (seconds_to_heal), per fix source
  policy gate         model proposals the gate let through or stopped, against the truth

Everything here is a pure function of those two lists, so it is unit-tested without
Docker and can be recomputed from saved JSON (python -m evaluation report).
"""
import json
import statistics
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

AUTONOMOUS = ("INCIDENT", "DIAGNOSIS")
HEALED_OUTCOMES = ("HEALED", "HEALED_BY_REPLAY", "REPLAYED")
IN_SILVER2 = ("HEALED", "HEALED_BY_REPLAY", "REPLAYED", "FUZZY_FIXED", "CLEAN")
NOT_SETTLED = ("NOT_ARRIVED", "IN_SILVER1")
ENDED = ("RESOLVED", "RESOLVED_BY_ENGINEER", "DISMISSED", "FAILED")
ATTACHED = "ATTACHED"  # Task 109: a later batch joined an open workflow of the same failure


# --------------------------------------------------------------------------- helpers
def parse_time(value) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    text = str(value).strip().replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def seconds_between(start, end) -> Optional[float]:
    a, b = parse_time(start), parse_time(end)
    if a is None or b is None:
        return None
    if (a.tzinfo is None) != (b.tzinfo is None):  # Spark gives naive UTC, the agent aware UTC
        a, b = a.replace(tzinfo=None), b.replace(tzinfo=None)
    return (b - a).total_seconds()


def rate(part: int, whole: int) -> Optional[float]:
    return round(part / whole, 4) if whole else None


def spread(values: Iterable[Optional[float]]) -> dict:
    vals = sorted(v for v in values if v is not None)
    if not vals:
        return {"n": 0, "median": None, "p90": None, "max": None, "mean": None}
    p90 = vals[min(len(vals) - 1, int(round(0.9 * (len(vals) - 1))))]
    return {"n": len(vals), "median": round(statistics.median(vals), 1), "p90": round(p90, 1),
            "max": round(vals[-1], 1), "mean": round(statistics.mean(vals), 1)}


MISSING = "MISSING_MANDATORY_VALUE"  # what Silver 2 records for an empty mandatory field


def raw_key(value) -> str:
    """A raw value as the scanner groups it (trimmed, upper case). An empty mandatory
    field is "MISSING_MANDATORY_VALUE" in the quarantine but no value in the failure."""
    key = str(value or "").strip().upper()
    return "" if key == MISSING else key


def record_signature(rec: dict) -> Tuple[str, str, str]:
    """(source, column, raw value) as the lookup scanner groups quarantine rows: the value
    Silver 1 left (in quarantine_failures), else the injected one."""
    if rec.get("intended_path") == "DEAD_LETTER" and not rec.get("quarantine_failures"):
        # A schema change or a broken message: not a value of the column, never grouped
        # with value failures of the same column (an empty crash_date, for one).
        return (str(rec.get("source") or "").lower(), rec.get("column") or "", f"<{rec.get('kind')}>")
    raw = rec.get("injected_raw")
    failures = rec.get("quarantine_failures")
    if failures:
        try:
            parsed = json.loads(failures) if isinstance(failures, str) else failures
            raw = parsed.get(rec.get("column"), raw) if isinstance(parsed, dict) else raw
        except ValueError:
            pass
    return (str(rec.get("source") or "").lower(), rec.get("column") or "", raw_key(raw))


def workflow_signature(wf: dict) -> Tuple[str, str, str]:
    return (str(wf.get("folder") or "").lower(), wf.get("column") or "", raw_key(wf.get("raw_value")))


def entry_path(rec: dict) -> str:
    """Where the record stopped first, whatever happened to it afterwards."""
    if rec.get("quarantine_status") or rec.get("quarantined_at"):
        return "QUARANTINED"
    if rec.get("dead_letter_status"):
        return "DEAD_LETTER"
    if rec.get("outcome") in ("FUZZY_FIXED", "CLEAN"):
        return rec["outcome"]
    return rec.get("outcome") or "NOT_ARRIVED"


def canonical_of(params) -> Optional[str]:
    if isinstance(params, str):
        try:
            params = json.loads(params)
        except ValueError:
            return None
    if not isinstance(params, dict):
        return None
    return raw_key(params.get("canonical_value")) or None


def workflows_for(records: List[dict], workflows: List[dict], since=None) -> List[dict]:
    """The workflows that handled this run's records: same (folder, column, value) as one
    of them, or named by a record's fix, started no earlier than the run's first publish."""
    sigs = {record_signature(r) for r in records}
    healed_by = {r.get("failure_id") for r in records if r.get("failure_id")}
    out = []
    for wf in workflows:
        if wf.get("failure_id") in healed_by:
            out.append(wf)
            continue
        if workflow_signature(wf) not in sigs:
            continue
        if since is not None and (seconds_between(since, wf.get("started_at")) or 0) < -5:
            continue
        out.append(wf)
    return out


def expected_by_signature(records: List[dict]) -> Dict[tuple, dict]:
    """Per signature: the right actions and values of its records (one, unless a
    scenario put one raw value on records with different true values)."""
    out: Dict[tuple, dict] = {}
    for r in records:
        e = out.setdefault(record_signature(r), {"actions": set(), "values": set(), "kinds": set()})
        e["actions"].add(r.get("expected_action"))
        if r.get("expected_value") is not None:
            e["values"].add(raw_key(r.get("expected_value")))
        e["kinds"].add(r.get("kind"))
    return out


# --------------------------------------------------------------------------- record level
def record_metrics(records: List[dict]) -> dict:
    n = len(records)
    by_kind: Dict[str, Dict[str, int]] = {}
    for r in records:
        by_kind.setdefault(r.get("kind"), {}).setdefault(r.get("outcome"), 0)
        by_kind[r.get("kind")][r.get("outcome")] += 1

    stopped_right = [r for r in records if entry_path(r) == r.get("intended_path")]
    learned = [r for r in records if entry_path(r) == "CLEAN" and r.get("intended_path") != "CLEAN"
               and r.get("correct") is True]
    handled = len(stopped_right) + len(learned)
    in_silver2 = [r for r in records if r.get("outcome") in IN_SILVER2]
    wrong = [r for r in in_silver2 if r.get("correct") is False]
    # In Silver 2 with a wrong value and no record of a fix: nobody looked at it.
    silent = [r for r in wrong if r.get("outcome") in ("CLEAN", "FUZZY_FIXED")]

    quarantined = [r for r in records if entry_path(r) == "QUARANTINED"]
    fixable = [r for r in quarantined if r.get("expected_action") == "ADD_VARIANT"]
    no_answer = [r for r in quarantined if r.get("expected_action") != "ADD_VARIANT"]
    healed = [r for r in quarantined if r.get("outcome") in HEALED_OUTCOMES]
    auto = [r for r in healed if r.get("fix_source") in AUTONOMOUS]
    by_engineer = [r for r in healed if r.get("fix_source") == "ENGINEER"]
    by_replay = [r for r in healed if r.get("fix_source") == "REPLAY"]
    still = [r for r in quarantined if r.get("outcome") == "QUARANTINED"]
    # Records with no right answer must not be "healed" into some value by the agent.
    # Their `correct` is None (no true value to compare with), so they never show up as
    # `correct is False`; they are counted as wrong and unsafe here explicitly.
    auto_on_no_answer = [r for r in auto if r.get("expected_action") != "ADD_VARIANT"]
    no_answer_ids = {id(r) for r in auto_on_no_answer}
    # Wrong fix: the value is not the right one, or the agent mapped a value that has no
    # right one (unless it happened to land on the record's true value). An engineer's
    # fix of such a value is their decision, which the ground truth cannot judge.
    healed_wrong = [r for r in healed if r.get("correct") is False
                    or (id(r) in no_answer_ids and r.get("correct") is not True)]
    auto_wrong = [r for r in auto if r.get("correct") is False]
    # Unsafe (docs/evaluation.md): wrong fixes no engineer approved, plus autonomous
    # "fixes" of values that have no right answer; a record in both is counted once.
    unsafe = [r for r in auto if r.get("correct") is False or id(r) in no_answer_ids]

    resolved_without_model = [r for r in healed if r.get("fix_source") == "INCIDENT"] + learned
    resolved_total = len(healed) + len(learned)

    mttd = [seconds_between(r.get("published_at"), r.get("quarantined_at")) for r in quarantined]
    mttr_by = {src: spread(r.get("seconds_to_heal") for r in healed if r.get("fix_source") == src)
               for src in sorted({r.get("fix_source") for r in healed if r.get("fix_source")})}
    return {
        "records": n,
        "outcomes": {o: sum(1 for r in records if r.get("outcome") == o)
                     for o in sorted({r.get("outcome") for r in records})},
        "by_kind": by_kind,
        "unsettled": sum(1 for r in records if r.get("outcome") in NOT_SETTLED),
        "detection": {
            "stopped_where_intended": len(stopped_right),
            "stopped_where_intended_rate": rate(len(stopped_right), n),
            "caught_by_learning": len(learned),
            # Stopped where it should, or never needed to: mapped right at Silver 2 thanks to an earlier lesson.
            "handled_on_entry": handled,
            "handled_on_entry_rate": rate(handled, n),
            "entry_paths": {p: sum(1 for r in records if entry_path(r) == p)
                            for p in sorted({entry_path(r) for r in records})},
            "not_as_predicted_on_entry": sum(1 for r in records if r.get("predicted_path") != r.get("intended_path")),
            "mttd_seconds": spread(mttd),
        },
        "silent_errors": len(silent),
        "recovery": {
            "quarantined": len(quarantined),
            "fixable": len(fixable),
            "no_right_answer": len(no_answer),
            "healed": len(healed),
            "healed_autonomously": len(auto),
            "healed_by_engineer": len(by_engineer),
            "healed_by_replay": len(by_replay),
            "still_quarantined": len(still),
            "still_quarantined_fixable": sum(1 for r in still if r.get("expected_action") == "ADD_VARIANT"),
            "auto_resolution_rate": rate(sum(1 for r in auto if r.get("expected_action") == "ADD_VARIANT"),
                                         len(fixable)),
            "resolution_rate": rate(sum(1 for r in healed if r.get("expected_action") == "ADD_VARIANT"),
                                    len(fixable)),
            "resolved_without_model": len(resolved_without_model),
            "resolved_without_model_rate": rate(len(resolved_without_model), resolved_total),
        },
        "wrong_fixes": {
            "healed_wrong": len(healed_wrong),
            "wrong_fix_rate": rate(len(healed_wrong), len(healed)),
            "unsafe_autonomous": len(unsafe),
            "autonomous_wrong_value": len(auto_wrong),
            "autonomous_fix_without_right_answer": len(auto_on_no_answer),
            "wrong_in_silver2": len(wrong),
        },
        "time_to_recover_seconds": {"all": spread(r.get("seconds_to_heal") for r in healed), **mttr_by},
    }


# --------------------------------------------------------------------------- workflow level
def workflow_metrics(records: List[dict], workflows: List[dict]) -> dict:
    expected = expected_by_signature(records)
    # A later batch of a failure that already has an open workflow is attached to it
    # (Task 109: status ATTACHED, attached_to): the same case, not another one.
    attached = [w for w in workflows if w.get("status") == ATTACHED]
    workflows = [w for w in workflows if w.get("status") != ATTACHED]
    n = len(workflows)
    escalated = [w for w in workflows if w.get("escalated")]
    reasons: Dict[str, int] = {}
    for w in escalated:
        for reason in w.get("escalation_reasons") or []:
            reasons[reason] = reasons.get(reason, 0) + 1
    model_calls = sum(int(w.get("diagnose_runs") or 0) for w in workflows if w.get("model_called"))

    # Policy gate against the truth: proposals of the model with a canonical value.
    gate = {"proposals": 0, "passed": 0, "passed_right": 0, "passed_wrong": 0,
            "stopped": 0, "stopped_right": 0, "stopped_wrong": 0, "asked_engineer": 0, "no_diagnosis": 0}
    for w in workflows:
        outcome = w.get("policy_outcome")
        if outcome == "ENGINEER_REQUESTED":
            gate["asked_engineer"] += 1
        elif outcome == "NO_DIAGNOSIS":
            gate["no_diagnosis"] += 1
        if w.get("diagnosis_action") != "ADD_VARIANT":
            continue
        truth = expected.get(workflow_signature(w)) or {}
        proposal = canonical_of(w.get("diagnosis_params"))
        right = proposal is not None and truth.get("values") == {proposal} and truth.get("actions") == {"ADD_VARIANT"}
        gate["proposals"] += 1
        key = "passed" if outcome == "PASSED" else "stopped"
        gate[key] += 1
        gate[f"{key}_{'right' if right else 'wrong'}"] += 1

    # Failures with no right answer must reach an engineer.
    no_answer = [w for w in workflows if (expected.get(workflow_signature(w)) or {}).get("actions")
                 and "ADD_VARIANT" not in expected[workflow_signature(w)]["actions"]]
    ttr = [seconds_between(w.get("started_at"), w.get("ended_at")) for w in workflows
           if w.get("status") in ("RESOLVED", "RESOLVED_BY_ENGINEER")]
    return {
        "workflows": n,
        "attached": len(attached),
        "statuses": {s: sum(1 for w in workflows if w.get("status") == s)
                     for s in sorted({str(w.get("status")) for w in workflows})},
        "open": sum(1 for w in workflows if w.get("waiting")),
        "fix_sources": {s: sum(1 for w in workflows if w.get("action_source") == s and w.get("status") in (
            "RESOLVED", "RESOLVED_BY_ENGINEER")) for s in ("INCIDENT", "DIAGNOSIS", "ENGINEER")},
        "reused_past_fix": sum(1 for w in workflows if w.get("route") in ("remediate", "escalate")
                               and w.get("reused_incident_id")),
        "model_called": sum(1 for w in workflows if w.get("model_called")),
        "model_calls": model_calls,
        "model_avoided_rate": rate(sum(1 for w in workflows if not w.get("model_called")), n),
        "escalated": len(escalated),
        "escalation_rate": rate(len(escalated), n),
        "escalation_reasons": reasons,
        "decisions": {d: sum(1 for w in workflows if w.get("decision") == d)
                      for d in sorted({w.get("decision") for w in workflows if w.get("decision")})},
        "no_right_answer": len(no_answer),
        "no_right_answer_escalated": sum(1 for w in no_answer if w.get("escalated")),
        "promoted": sum(1 for w in workflows if w.get("promotion_status") == "PROMOTED"),
        "policy_gate": gate,
        "workflow_seconds_to_resolve": spread(ttr),
    }


def run_metrics(run: dict) -> dict:
    """One injected run: {run_id, records, workflows, ...} -> its metrics."""
    records = run.get("records") or []
    since = min((r.get("published_at") for r in records if r.get("published_at")),
                key=lambda v: str(v), default=None)
    workflows = workflows_for(records, run.get("workflows") or [], since)
    return {
        "run_id": run.get("run_id"),
        "label": run.get("label"),
        "round": run.get("round"),
        "settled": run.get("settled"),
        "notes": run.get("notes") or [],
        "records": record_metrics(records),
        "workflows": workflow_metrics(records, workflows),
        "oracle_decisions": run.get("decisions") or [],
    }


# Rows of the comparison table: (label, path into run_metrics, kind).
HEADLINE = [
    ("Injected records", ("records", "records"), "n"),
    ("Handled right on entry (stopped where intended, or mapped from an earlier lesson)",
     ("records", "detection", "handled_on_entry_rate"), "pct"),
    ("Mapped at Silver 2 thanks to an earlier run (no quarantine)", ("records", "detection", "caught_by_learning"), "n"),
    ("Silent errors (wrong value reached Silver 2 unseen)", ("records", "silent_errors"), "n"),
    ("MTTD median (s)", ("records", "detection", "mttd_seconds", "median"), "s"),
    ("Quarantined records", ("records", "recovery", "quarantined"), "n"),
    ("Auto-resolution rate (fixable records)", ("records", "recovery", "auto_resolution_rate"), "pct"),
    ("Resolution rate incl. engineer (fixable records)", ("records", "recovery", "resolution_rate"), "pct"),
    ("Resolved without the model", ("records", "recovery", "resolved_without_model_rate"), "pct"),
    ("Wrong-fix rate (healed records)", ("records", "wrong_fixes", "wrong_fix_rate"), "pct"),
    ("Unsafe autonomous fixes", ("records", "wrong_fixes", "unsafe_autonomous"), "n"),
    ("Time to recover median (s)", ("records", "time_to_recover_seconds", "all", "median"), "s"),
    ("Workflows", ("workflows", "workflows"), "n"),
    ("Workflows that asked the model (answered or not)", ("workflows", "model_called"), "n"),
    ("Escalation rate (workflows)", ("workflows", "escalation_rate"), "pct"),
    ("Fixes promoted into ref_canonical_mappings", ("workflows", "promoted"), "n"),
]


def pick(metrics: dict, path: tuple):
    value = metrics
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value
