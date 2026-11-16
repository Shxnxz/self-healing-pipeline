"""The evaluation's engineer: answers open cases from the injected ground truth.

An escalated case waits for a person (python -m agent.review). An evaluation run
cannot wait for one, and the question it asks is what the system does with a correct
answer, so this "oracle" engineer answers each case of the run the way a careful
engineer would, knowing the truth of every injected record:

  right answer is ADD_VARIANT to V    the case's proposed fix is V -> approve it,
                                      otherwise -> fix <failure> V
  no right answer (unknown value,     dismiss: there is no canonical value to map to
  unseen format, missing date)
  records of one value disagree       dismiss: one mapping cannot be right for all
  schema change (RENAME_COLUMN)       left open: the engineer replays the batch with
                                      tools.replay (shown in the demo, not automated)
  not one of this run's values        left open: not part of the experiment

Its answers are instant, so the times measured on the engineer path leave out how
long a real engineer takes to read the case: a lower bound.
"""
from typing import Dict, List, Optional

from evaluation.metrics import canonical_of, expected_by_signature, workflow_signature

ENGINEER = "evaluation-oracle"


def decide(workflow: dict, expected: Dict[tuple, dict], promote: bool = True) -> Optional[dict]:
    """The decision for one waiting workflow, or None to leave it open.

    Returns {"command": [...agent.review args], "why": text}."""
    if not workflow.get("waiting"):
        return None
    failure_id = workflow["failure_id"]
    truth = expected.get(workflow_signature(workflow))
    if not truth:
        return None
    actions, values = truth["actions"], truth["values"]
    tail = ["--by", ENGINEER] + ([] if promote else ["--no-promote"])
    if actions == {"ADD_VARIANT"} and len(values) == 1:
        value = next(iter(values))
        pending = canonical_of(workflow.get("action_params")) if workflow.get("action") == "ADD_VARIANT" else None
        if pending == value:
            return {"command": ["approve", failure_id, *tail, "--note", f"oracle: {value} is right"],
                    "why": f"approve the proposed {value}"}
        return {"command": ["fix", failure_id, value, *tail, "--note", "oracle: ground truth"],
                "why": f"fix to {value}" + (f" (proposed {pending})" if pending else "")}
    if "RENAME_COLUMN" in actions:
        return None
    if "ADD_VARIANT" in actions or len(values) > 1:
        why = "records of this value have different true values"
    else:
        why = "no canonical value is right for it"
    return {"command": ["dismiss", failure_id, "--by", ENGINEER, "--note", f"oracle: {why}"],
            "why": f"dismiss: {why}"}


def decisions(workflows: List[dict], records: List[dict], promote: bool = True) -> List[dict]:
    expected = expected_by_signature(records)
    out = []
    for wf in workflows:
        d = decide(wf, expected, promote)
        if d:
            out.append({**d, "failure_id": wf["failure_id"], "round": wf.get("escalation_rounds"),
                        "reasons": wf.get("escalation_reasons")})
    return out
