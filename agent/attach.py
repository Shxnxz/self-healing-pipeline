"""One open workflow per problem: later batches attach to it (Task 109).

The quarantine trigger (lookup/scanner.py) claims each micro-batch's rows and
publishes them as a failure of their own, and its failure_id includes the batch ids.
Without this module a problem that recurs in every batch ("Jule" in batches 7, 8 and
9) would start a new workflow, a new model call and a new engineer case for each one.

So before the agent starts a workflow for a failure it looks for a workflow of the
SAME SIGNATURE that is still open, and if there is one the failure attaches to it:

  signature   source, failure type, folder, layers, column and the normalised value:
              the key lookup/stack.py merge_failures uses for one window
  open        the workflow waits for an engineer, or stopped part-way before Validate
              (the agent was killed). A workflow that reached Validate or ended is
              not open: its group is fixed, so a later batch starts a new workflow
              (which then usually reuses the verified fix).

Attaching adds the batch to the open workflow's `failure`: batch ids, rows,
sample record ids, and `attached_failure_ids`. Remediate stages the failure's records
from those batches, so the engineer's fix (or the retried automated path) covers the
new rows too, and Learn records every batch in the incident. The engineer's case is
updated to show the rows now waiting. The attached failure gets a finished workflow of
its own with status ATTACHED and `attached_to`, so a redelivery of it is a duplicate
and `python -m agent.show <failure_id>` says where it went.

The index of open workflows lives in memory: the agent rebuilds it from the saved
workflows when it starts (`OpenWorkflows.scan`), and every workflow it runs or resumes
updates it.
"""
import json
from typing import Dict, List, Optional

from agent.reference import failure_batches
from agent.state import STATUS_ATTACHED, step

NAME = "consumer"
SAMPLE_IDS = 5
# Next steps of a workflow that may still take more rows: nothing is staged yet, or an
# engineer's decision (which stages again) is still to come. At validate or learn the
# group is fixed, so it is not open any more.
ATTACHABLE_NEXT = {"deterministic_lookup", "diagnose", "policy_gate", "remediate", "escalate", "engineer_review"}


def _key(value) -> str:
    return (value or "").strip().upper()


def signature(failure: Optional[dict]) -> Optional[str]:
    """The problem a failure is about, the same for every batch it shows up in."""
    if not isinstance(failure, dict) or not failure.get("failure_id"):
        return None
    value = failure.get("signature_value")
    if value is None:
        value = failure.get("raw_value")
    key = {
        "source": failure.get("source"), "type": failure.get("failure_type"),
        "folder": (failure.get("folder") or "").lower(), "detected": failure.get("detected_layer"),
        "origin": failure.get("origin_layer"), "origin_affected": bool(failure.get("origin_affected")),
        "column": failure.get("column_name"), "value": _key(value),
        "mandatory": bool(failure.get("mandatory")),
    }
    return json.dumps(key, sort_keys=True)


def is_open(saved) -> bool:
    """The workflow can still take more rows (see ATTACHABLE_NEXT)."""
    values = saved.values or {}
    if not values or values.get("status") == STATUS_ATTACHED:
        return False
    return bool(saved.next) and saved.next[0] in ATTACHABLE_NEXT


def merge(base: dict, extra: dict) -> dict:
    """`base` failure with the batches, rows and samples of `extra` added."""
    old, new = set(failure_batches(base)), set(failure_batches(extra))
    batches = sorted(old | new)
    merged = dict(base)
    merged["batch_id"] = ",".join(batches) or None
    merged["evidence"] = {**(base.get("evidence") or {}), "quarantine_batches": batches}
    rows_a, rows_b = base.get("rows_affected"), extra.get("rows_affected")
    if rows_a is not None or rows_b is not None:
        # Disjoint batches: the rows add up. Overlapping ones (the trigger published a
        # batch again after a crash) cannot be split, so the larger count is kept: it
        # is a lower bound, and Remediate stages every waiting row of the batches anyway.
        merged["rows_affected"] = (rows_a or 0) + (rows_b or 0) if not (old & new) else max(rows_a or 0, rows_b or 0)
    merged["sample_record_ids"] = list(dict.fromkeys(
        list(base.get("sample_record_ids") or []) + list(extra.get("sample_record_ids") or [])))[:SAMPLE_IDS]
    merged["trusted"] = bool(base.get("trusted", True)) and bool(extra.get("trusted", True))
    merged["attached_failure_ids"] = list(base.get("attached_failure_ids") or []) + [extra["failure_id"]]
    return merged


def _case_update(escalation: Optional[dict], failure: dict) -> Optional[dict]:
    """The open case with the failure's new rows and batches, or None when there is no case."""
    if not isinstance(escalation, dict) or not isinstance(escalation.get("case"), dict):
        return None
    case = dict(escalation["case"])
    shown = dict(case.get("failure") or {})
    for name in ("rows_affected", "batch_id", "sample_record_ids", "attached_failure_ids"):
        if failure.get(name) not in (None, [], ""):
            shown[name] = failure[name]
    case["failure"] = shown
    return {**escalation, "case": case}


def attach(graph, target_id: str, lookup_id: Optional[str], item: dict) -> dict:
    """Add the stack item's failure to the open workflow `target_id`. Returns the summary."""
    failure = item["failure"]
    failure_id = failure["failure_id"]
    target = {"configurable": {"thread_id": target_id}}
    saved = graph.get_state(target)
    values = saved.values
    merged = merge(values["failure"], failure)
    rows = merged.get("rows_affected")
    update = {"failure": merged,
              "trace": [step(NAME, f"failure {failure_id} of batch(es) {failure.get('batch_id') or '-'} "
                                   f"attached: same problem, now {rows if rows is not None else '?'} row(s) "
                                   f"in batch(es) {merged['batch_id'] or '-'}")]}
    priority, extra = values.get("priority") or {}, item.get("priority") or {}
    if (extra.get("score") or 0) > (priority.get("score") or 0):
        update["priority"] = {**priority, "score": extra.get("score"), "tier": extra.get("tier")}
    case = _case_update(values.get("escalation"), merged)
    if case is not None:
        update["escalation"] = case
    waiting = bool(saved.interrupts)
    graph.update_state(target, update)
    # Writing the state ends the pending interrupt; running the workflow on puts the
    # engineer_review question back (nothing else runs), or continues a workflow the
    # agent was killed in.
    state = graph.invoke(None, target)

    # The attached failure's own record: finished, so a redelivery is a duplicate.
    graph.update_state({"configurable": {"thread_id": failure_id}}, {
        "lookup_id": lookup_id, "failure_id": failure_id, "failure": failure,
        "priority": {"rank": item.get("rank"), "score": extra.get("score"), "tier": extra.get("tier")},
        "lookup_route": item.get("route"), "route_reason": item.get("route_reason"),
        "status": STATUS_ATTACHED, "attached_to": target_id,
        "trace": [step(NAME, f"attached to the open workflow {target_id}"
                             + (" (waiting for an engineer)" if waiting else ""))],
    }, as_node="learn")
    return {"failure_id": failure_id, "attached_to": target_id, "state": state}


class OpenWorkflows:
    """Signature -> the open workflow of that problem (in memory, see the module doc)."""

    def __init__(self, graph):
        self.graph = graph
        self.by_signature: Dict[str, str] = {}

    def _saved(self, failure_id: str):
        return self.graph.get_state({"configurable": {"thread_id": failure_id}})

    def scan(self, thread_ids: List[str]) -> int:
        for failure_id in thread_ids:
            self.note(failure_id)
        return len(self.by_signature)

    def note(self, failure_id: Optional[str], saved=None) -> None:
        """Record the workflow as open, or forget it when it is not open any more."""
        if not failure_id:
            return
        if saved is None:
            saved = self._saved(failure_id)
        sig = signature((saved.values or {}).get("failure"))
        if sig is None:
            return
        if is_open(saved):
            current = self.by_signature.get(sig)
            if current is None or current == failure_id or not is_open(self._saved(current)):
                self.by_signature[sig] = failure_id
        elif self.by_signature.get(sig) == failure_id:
            del self.by_signature[sig]

    def find(self, failure: dict) -> Optional[str]:
        """The open workflow of the same problem as `failure`, if there is one."""
        sig = signature(failure)
        target = self.by_signature.get(sig) if sig else None
        if target is None or target == failure.get("failure_id"):
            return None
        if not is_open(self._saved(target)):
            del self.by_signature[sig]
            return None
        return target
