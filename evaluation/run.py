"""Run the evaluation experiment on the evaluation stack and write its report.

One round = inject a scenario (tools.fault_injector), let the pipeline and the agent
work on it while the oracle engineer answers the cases (evaluation/oracle.py), wait
until nothing moves any more, then record where every injected record ended
(`status`) and what every workflow did (`agent.workflows`). The same scenario is
injected again in the next round: the stack keeps what it learned, so round 2
measures the learning loop (values promoted into ref_canonical_mappings, incidents
reused without the model).
"""
import json
import os
import time
from datetime import datetime, timezone
from typing import List, Optional

from evaluation import metrics, oracle, report, stack

DEFAULT_OUT = os.path.join(stack.REPO, "eval", "results")


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _relevant(records: List[dict], workflows: List[dict]) -> List[dict]:
    since = min((r.get("published_at") for r in records if r.get("published_at")), key=str, default=None)
    return metrics.workflows_for(records, workflows, since)


def _snapshot(records: List[dict], workflows: List[dict]) -> tuple:
    return (tuple(sorted((r["record_id"], r.get("outcome"), r.get("quarantine_status")) for r in records)),
            tuple(sorted((w["failure_id"], str(w.get("status")), tuple(w.get("next") or [])) for w in workflows)))


def settled_reason(records: List[dict], workflows: List[dict], open_ok: set) -> Optional[str]:
    """None when the run has come to rest, else what is still moving."""
    moving = [r for r in records if r.get("outcome") in metrics.NOT_SETTLED]
    if moving:
        return f"{len(moving)} record(s) not through Silver 2 yet"
    running = [w for w in workflows if w.get("next") and not w.get("waiting")]
    if running:
        return f"{len(running)} workflow(s) running"
    waiting = [w for w in workflows if w.get("waiting") and w["failure_id"] not in open_ok]
    if waiting:
        return f"{len(waiting)} case(s) waiting for a decision"
    sigs = {metrics.workflow_signature(w) for w in workflows}
    unseen = [r for r in records if r.get("outcome") == "QUARANTINED" and metrics.record_signature(r) not in sigs]
    if unseen:
        return f"{len(unseen)} quarantined record(s) not picked up by a workflow yet"
    return None


def run_round(scenario: str, run_id: str, label: str, round_no: int, engineer: str, promote: bool,
              timeout: float, poll: float, quiet: float, seed: int, log=stack.say) -> dict:
    notes: List[str] = []
    if stack.run_exists(run_id):
        log(f"\n== Round {round_no}: {run_id} was injected already, measuring it again")
    else:
        log(f"\n== Round {round_no}: injecting {scenario!r} as {run_id}")
        out = stack.inject(scenario, run_id, seed)
        for line in out.strip().splitlines()[-4:]:
            log("   " + line)
    injected_at = _now()
    started = time.monotonic()
    decisions: List[dict] = []
    decided, open_ok = set(), set()
    status, workflows = {"records": []}, []
    last_snapshot, quiet_since, settled = None, None, False
    next_status = 0.0
    while time.monotonic() - started < timeout:
        workflows = stack.workflows()
        records = status.get("records") or []
        relevant = _relevant(records, workflows) if records else []
        if engineer == "oracle" and records:
            # A case is answered once per escalation round (a fix that fails Validate comes back).
            todo = [w for w in relevant if (w["failure_id"], w.get("escalation_rounds")) not in decided]
            for d in oracle.decisions(todo, records, promote):
                done = stack.review(d["command"])
                decisions.append({**d, "decided_at": _now(), "exit": done.returncode,
                                  "output": (done.stdout or done.stderr or "").strip()[-300:]})
                if done.returncode == 0:
                    decided.add((d["failure_id"], d.get("round")))
                log(f"   oracle: {d['failure_id']} -> {d['why']} (exit {done.returncode})")
        if engineer == "oracle" and records:
            expected = metrics.expected_by_signature(records)
            open_ok = {w["failure_id"] for w in relevant if w.get("waiting")
                       and oracle.decide(w, expected, promote) is None}
        elif engineer == "none":
            open_ok = {w["failure_id"] for w in relevant if w.get("waiting")}
        if time.monotonic() >= next_status:
            status = stack.status(run_id)
            records = status.get("records") or []
            relevant = _relevant(records, workflows)
            next_status = time.monotonic() + max(poll * 3, 45)
            why = settled_reason(records, relevant, open_ok)
            snap = _snapshot(records, relevant)
            if why is None and snap == last_snapshot:
                if quiet_since and time.monotonic() - quiet_since >= quiet:
                    settled = True
                    break
            else:
                quiet_since = time.monotonic() if why is None else None
            last_snapshot = snap
            summary = status.get("summary") or {}
            log(f"   [{int(time.monotonic() - started)}s] {summary.get('outcomes')} | "
                f"{len(relevant)} workflow(s) | {why or 'at rest, confirming'}")
        time.sleep(poll)
    if not settled:
        notes.append(f"stopped waiting after {timeout:.0f}s: {settled_reason(status.get('records') or [], _relevant(status.get('records') or [], workflows), open_ok)}")
        status = stack.status(run_id)
        workflows = stack.workflows()
    left_open = sorted(open_ok)
    if left_open:
        notes.append(f"left open on purpose (no automatic answer): {', '.join(left_open)}")
    return {"run_id": run_id, "label": label, "round": round_no, "scenario": scenario,
            "engineer": engineer, "promote": promote, "injected_at": injected_at, "measured_at": _now(),
            "settled": settled, "notes": notes, "decisions": decisions,
            "summary": status.get("summary"), "records": status.get("records") or [], "workflows": workflows}


def write(out_dir: str, rounds: List[dict], title: str, context: List[str]) -> str:
    os.makedirs(out_dir, exist_ok=True)
    for r in rounds:
        with open(os.path.join(out_dir, f"round-{r['round']}.json"), "w", encoding="utf-8") as fh:
            json.dump(r, fh, indent=1, default=str, ensure_ascii=False)
    computed = [metrics.run_metrics(r) for r in rounds]
    with open(os.path.join(out_dir, "metrics.json"), "w", encoding="utf-8") as fh:
        fh.write(report.dump(computed))
    path = os.path.join(out_dir, "report.md")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(report.render(computed, title, context))
    return path


def load(out_dir: str) -> List[dict]:
    rounds = []
    for name in sorted(os.listdir(out_dir)):
        if name.startswith("round-") and name.endswith(".json"):
            with open(os.path.join(out_dir, name), encoding="utf-8") as fh:
                rounds.append(json.load(fh))
    rounds.sort(key=lambda r: r.get("round") or 0)
    if not rounds:
        raise stack.StackError(f"no round-*.json in {out_dir}")
    return rounds


def context_lines(rounds: List[dict]) -> List[str]:
    first = rounds[0]
    models = sorted({w.get("diagnosis_model") for r in rounds for w in r.get("workflows") or []
                     if w.get("diagnosis_model")})
    statuses = sorted({w.get("diagnosis_status") for r in rounds for w in r.get("workflows") or []
                       if w.get("diagnosis_status")})
    return [
        f"Scenario `{first.get('scenario')}`, {len(rounds)} round(s) on one evaluation stack (learning kept "
        f"between rounds). Engineer: `{first.get('engineer')}`"
        + ("" if first.get("promote", True) else ", decisions with `--no-promote`") + ".",
        f"Reasoning model seen in diagnoses: {', '.join(models) or 'none'}; diagnosis statuses: "
        f"{', '.join(statuses) or 'none'} (UNAVAILABLE means the model was not reachable).",
    ]
