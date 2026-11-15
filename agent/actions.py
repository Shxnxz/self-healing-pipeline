"""The pre-approved action set (spec AP2, RS1).

Every action the reasoning model may propose is listed here, and nothing else is
accepted. The policy gate lets an action run without an engineer only when it is
marked `safe`: its effect can be undone and it cannot touch records outside the
failure group. Everything else is still a useful proposal, but an engineer decides.

Diagnose, the policy gate and Remediate share this module, so the model's vocabulary,
the safe set and what gets executed cannot drift apart. What executing an action
means is in agent/executors.py; an action without an executor there cannot run.

`action_params` per action (the same shape the incident memory stores, so a reused
past fix and a fresh diagnosis look alike to Remediate):

  ADD_VARIANT     {"canonical_value": "JULY"}
  RENAME_COLUMN   {"renames": {"<source column>": "<expected column>"}}
  ESCALATE        {}
"""
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

ADD_VARIANT = "ADD_VARIANT"
RENAME_COLUMN = "RENAME_COLUMN"
ESCALATE = "ESCALATE"

# silver2_job.py writes this instead of a raw value when a mandatory field is empty.
MISSING_MANDATORY = "MISSING_MANDATORY_VALUE"


@dataclass(frozen=True)
class Action:
    name: str
    safe: bool
    description: str  # shown to the reasoning model
    why: str          # why it is, or is not, safe to run without an engineer


CATALOGUE: Dict[str, Action] = {a.name: a for a in (
    Action(
        ADD_VARIANT, True,
        "Accept the raw value as another spelling of ONE canonical value of the same column. "
        "Every record of the failure gets that canonical value.",
        "Reversible and contained: the mapping is tried as an in-memory overlay on the "
        "quarantined rows of this failure only, the rows stay in quarantine unless Validate "
        "passes, and the mapping reaches ref_canonical_mappings only through Learn's promotion.",
    ),
    Action(
        RENAME_COLUMN, False,
        "Read a renamed source column under the column name the pipeline expects.",
        "Changes how every later batch of the table is read, not only the failed records.",
    ),
    Action(
        ESCALATE, False,
        "Hand the failure to a data engineer. Use it when the evidence does not single out "
        "one fix, or when no other allowed action fits.",
        "Not a fix: it asks for an engineer.",
    ),
)}

SAFE_ACTIONS = frozenset(name for name, a in CATALOGUE.items() if a.safe)


def safe_actions(only: Optional[List[str]] = None) -> frozenset:
    """The safe set in force. `only` (config POLICY_SAFE_ACTIONS) can narrow it, for
    example to nothing so that every diagnosis is reviewed. It can never widen it:
    an action becomes safe by changing this file, where the change is reviewed."""
    if only is None:
        return SAFE_ACTIONS
    return SAFE_ACTIONS & {name.strip().upper() for name in only}


def allowed_for(failure: dict, canonical_values: List[str]) -> List[str]:
    """Actions that can apply to this failure at all; the model chooses among these."""
    allowed = []
    if _is_vocabulary_failure(failure) and canonical_values:
        allowed.append(ADD_VARIANT)
    if failure.get("family") == "SCHEMA" and failure.get("source_only") and failure.get("expected_only"):
        allowed.append(RENAME_COLUMN)
    return allowed + [ESCALATE]


def _is_vocabulary_failure(failure: dict) -> bool:
    raw = (failure.get("raw_value") or "").strip()
    return (failure.get("family") == "VALUE" and bool(failure.get("column_name"))
            and bool(raw) and raw != MISSING_MANDATORY and not failure.get("mandatory"))


def check_params(action: str, params, failure: dict,
                 canonical_values: List[str]) -> Tuple[Optional[dict], List[str]]:
    """Check a proposal against the failure it is for.

    Returns (clean params, []) when it holds, or (None, [what is wrong]). The clean
    params are rebuilt from the checked values, so nothing else the model wrote
    reaches Remediate.
    """
    params = params if isinstance(params, dict) else {}
    if action == ADD_VARIANT:
        return _check_add_variant(params, failure, canonical_values)
    if action == RENAME_COLUMN:
        return _check_rename_column(params, failure)
    if action == ESCALATE:
        return {}, []
    return None, [f"{action!r} is not a pre-approved action"]


def _check_add_variant(params: dict, failure: dict, canonical_values: List[str]):
    column = failure.get("column_name")
    if not _is_vocabulary_failure(failure):
        return None, ["ADD_VARIANT needs a value failure with a raw value on one column"]
    if not canonical_values:
        return None, [f"no canonical values are known for column {column!r}"]
    value = params.get("canonical_value")
    if not isinstance(value, str) or value not in canonical_values:
        return None, [f"{value!r} is not a canonical value of {column}"]
    return {"canonical_value": value}, []


def _check_rename_column(params: dict, failure: dict):
    renames = params.get("renames")
    source_only, expected_only = failure.get("source_only") or [], failure.get("expected_only") or []
    if failure.get("family") != "SCHEMA":
        return None, ["RENAME_COLUMN needs a schema failure"]
    if not isinstance(renames, dict) or not renames:
        return None, ["RENAME_COLUMN needs at least one rename"]
    errors = []
    for source, expected in renames.items():
        if source not in source_only:
            errors.append(f"{source!r} is not an unexpected column of the source")
        if expected not in expected_only:
            errors.append(f"{expected!r} is not a column the pipeline is missing")
    if len(set(renames.values())) != len(renames):
        errors.append("two source columns are renamed to the same column")
    return (None, errors) if errors else (dict(renames), [])
