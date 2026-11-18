"""Markdown report of an evaluation: the headline table across rounds, then each round."""
import json
from datetime import datetime, timezone
from typing import List

from evaluation.metrics import HEADLINE, pick


def fmt(value, kind: str = "n") -> str:
    if value is None:
        return "-"
    if kind == "pct":
        return f"{100 * value:.1f}%"
    if kind == "s":
        return f"{value:.0f}"
    return str(value)


def _table(header: List[str], rows: List[List[str]]) -> List[str]:
    out = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    out += ["| " + " | ".join(str(c) for c in row) + " |" for row in rows]
    return out


def _counts(d: dict) -> str:
    return ", ".join(f"{k} {v}" for k, v in d.items() if v) or "none"


def headline(runs: List[dict]) -> List[str]:
    header = ["Metric"] + [f"Round {m.get('round') or i + 1} (`{m['run_id']}`)" for i, m in enumerate(runs)]
    rows = [[label] + [fmt(pick(m, path), kind) for m in runs] for label, path, kind in HEADLINE]
    return _table(header, rows)


def round_section(m: dict) -> List[str]:
    rec, wf = m["records"], m["workflows"]
    det, recov, wrong = rec["detection"], rec["recovery"], rec["wrong_fixes"]
    out = [f"## Round {m.get('round') or ''}: `{m['run_id']}`", ""]
    if not m.get("settled", True):
        out += ["**Did not settle before the timeout**: some records were still moving when it was measured.", ""]
    out += [f"{rec['records']} injected records. Outcomes: {_counts(rec['outcomes'])}.", ""]
    out += ["### Where each kind of fault ended", ""]
    kinds = sorted(rec["by_kind"])
    outcomes = sorted({o for k in kinds for o in rec["by_kind"][k]})
    out += _table(["Kind"] + outcomes, [[k] + [rec["by_kind"][k].get(o, 0) or "" for o in outcomes] for k in kinds])
    out += ["", "### Detection", "",
            f"- First stop as intended: {det['stopped_where_intended']} of {rec['records']} "
            f"({fmt(det['stopped_where_intended_rate'], 'pct')}); first stops: {_counts(det['entry_paths'])}. "
            f"With the records an earlier lesson let through correctly: {det['handled_on_entry']} "
            f"({fmt(det['handled_on_entry_rate'], 'pct')}).",
            f"- Never quarantined because an earlier run taught Silver 2 the value: {det['caught_by_learning']}.",
            f"- Records the injector predicted would not take the intended path: {det['not_as_predicted_on_entry']}.",
            f"- Time to detect (published to quarantined), seconds: median {fmt(det['mttd_seconds']['median'], 's')}, "
            f"p90 {fmt(det['mttd_seconds']['p90'], 's')}, n={det['mttd_seconds']['n']}.",
            f"- Silent errors (a wrong value reached Silver 2 without quarantine or a fix): {rec['silent_errors']}.",
            "", "### Recovery", "",
            f"- Quarantined: {recov['quarantined']} ({recov['fixable']} with a right answer, "
            f"{recov['no_right_answer']} without).",
            f"- Healed: {recov['healed']} (autonomously {recov['healed_autonomously']}, by the engineer "
            f"{recov['healed_by_engineer']}, by replay {recov['healed_by_replay']}); still quarantined "
            f"{recov['still_quarantined']} ({recov['still_quarantined_fixable']} of them fixable).",
            f"- Auto-resolution rate: {fmt(recov['auto_resolution_rate'], 'pct')}; with the engineer: "
            f"{fmt(recov['resolution_rate'], 'pct')}.",
            f"- Resolved without the model (reused fix or learned earlier): {recov['resolved_without_model']} "
            f"({fmt(recov['resolved_without_model_rate'], 'pct')}).",
            f"- Wrong fixes: {wrong['healed_wrong']} of {recov['healed']} healed ({fmt(wrong['wrong_fix_rate'], 'pct')}); "
            f"unsafe (no engineer approved): {wrong['unsafe_autonomous']}, of which autonomous fixes to a wrong "
            f"value: {wrong.get('autonomous_wrong_value', '-')}; autonomous fixes of values with no "
            f"right answer: {wrong['autonomous_fix_without_right_answer']}.",
            ""]
    ttr = rec["time_to_recover_seconds"]
    out += _table(["Time to recover (s)", "n", "median", "p90", "max"],
                  [[src, t["n"], fmt(t["median"], "s"), fmt(t["p90"], "s"), fmt(t["max"], "s")]
                   for src, t in ttr.items()])
    gate = wf["policy_gate"]
    out += ["", "### Recovery workflows", "",
            f"- {wf['workflows']} workflows: {_counts(wf['statuses'])}; {wf['open']} still waiting for an engineer"
            + (f"; {wf['attached']} later batch(es) attached to an open one" if wf.get("attached") else "") + ".",
            f"- Fixed by: {_counts(wf['fix_sources'])}; a past fix reused in {wf['reused_past_fix']}.",
            f"- The model was asked in {wf['model_called']} workflows ({wf['model_calls']} calls, answered or "
            f"not); not needed in {fmt(wf['model_avoided_rate'], 'pct')} of workflows.",
            f"- Escalated: {wf['escalated']} ({fmt(wf['escalation_rate'], 'pct')}); reasons: "
            f"{_counts(wf['escalation_reasons'])}; decisions: {_counts(wf['decisions'])}.",
            f"- Failures with no right answer: {wf['no_right_answer']}, of which escalated "
            f"{wf['no_right_answer_escalated']}.",
            f"- Promoted into ref_canonical_mappings: {wf['promoted']}.",
            f"- Policy gate on the model's fix proposals: {gate['proposals']} proposals, passed {gate['passed']} "
            f"(right {gate['passed_right']}, wrong {gate['passed_wrong']}), stopped {gate['stopped']} "
            f"(right {gate['stopped_right']}, wrong {gate['stopped_wrong']}); the model asked for an engineer "
            f"{gate['asked_engineer']} times; no usable diagnosis {gate['no_diagnosis']} times.",
            ""]
    if m.get("oracle_decisions"):
        out += ["### The oracle engineer's decisions", ""]
        out += _table(["Failure", "Decision", "Escalation reasons"],
                      [[d["failure_id"], d["why"], ", ".join(r for r in d.get("reasons") or [] if r)]
                       for d in m["oracle_decisions"]])
        out += [""]
    if m.get("notes"):
        out += ["### Notes", ""] + [f"- {n}" for n in m["notes"]] + [""]
    return out


def render(runs: List[dict], title: str = "Evaluation", context: List[str] = ()) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    out = [f"# {title}", "", f"Generated {stamp} by `python -m evaluation`.", ""]
    out += list(context) + ([""] if context else [])
    out += ["## Headline", ""] + headline(runs) + [""]
    out += ["How each number is computed: `evaluation/metrics.py` and docs/evaluation.md.", ""]
    for m in runs:
        out += round_section(m)
    return "\n".join(out) + "\n"


def dump(runs: List[dict]) -> str:
    return json.dumps(runs, indent=1, default=str, ensure_ascii=False)
