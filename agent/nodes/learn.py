"""Learn (spec EL2, EL3, KG2, TA3): store the outcome in Neo4j and decide promotion.

Reads   everything
Writes  learned = {"status": STORED | SKIPPED, "written", "incident_id", "outcome",
                   "resolved_by", "verified", "borrowed", "embedded", "attempts",
                   "promotion": {...}, "learned_at"}, status

The last step of every workflow that has an outcome, automated or human (EL3). By the
time it runs Validate has already released the healed records into Silver 2 and marked
them RESOLVED; Learn writes nothing to the pipeline's data tables except the variant it
promotes.

1. Failed attempts. Every fix that was staged and did not hold in Validate (a round
   of escalation.history, or the last round) becomes an incident with outcome
   FAILED_VALIDATION, unless the same fix held later in this workflow. The lookup lists
   those as previous attempts and never reuses them for the same signature.
2. The outcome, one incident per workflow (incident_id = failure_id, so writing it
   again updates it):

     status                 reached Learn by                 outcome    action       resolved_by  verified
     RESOLVED               Validate passed (no engineer)    SUCCESS    the fix      LOOKUP / LLM  true
     RESOLVED_BY_ENGINEER   APPLY_FIX that passed Validate   SUCCESS    the fix      ENGINEER      true
     RESOLVED_BY_ENGINEER   RESOLVED_MANUALLY                SUCCESS    MANUAL       ENGINEER      true
     DISMISSED              DISMISS                          DISMISSED  DISMISS      ENGINEER      false
     FAILED                 a last step kept raising          (nothing is stored, nobody decided)

   With the incident go its provenance: failure_id, execution_id, batch_ids, the fix's
   source and how the lookup matched it, approved_by, the engineer's note and
   `borrowed`. A fix the lookup borrowed from a similar incident (or from one whose fix
   was itself borrowed) that no engineer approved is stored with borrowed = true, so a
   later exact match on it still asks for approval on P1 (deterministic_lookup.py) and
   it never counts towards promotion. Approved by an engineer, it is not borrowed.
   The incident is embedded with the lookup's model and text when LOOKUP_EMBED_MODEL is
   set; if the model cannot be reached it is stored without a vector.
3. Promotion into ref_canonical_mappings (agent/promotion.py, EL2): at once for an
   engineer's approved fix, otherwise once the same fix has recurred
   (PROMOTE_MIN_OCCURRENCES rows in PROMOTE_MIN_BATCHES batches), never for an
   ambiguous value. The decision is written on the incident too.

Neo4j or the reference table failing raises, so the graph retries Learn and then ends
the workflow FAILED with `error.node = learn`. Every write is idempotent, and the
consumer runs Learn alone again when the failure is delivered again (agent/consumer.py,
"relearn").
"""
import json
import urllib.request
from datetime import datetime, timezone
from typing import List, Optional

from agent import actions, config, promotion as promo, reference
from agent import escalation as esc
from agent.memory import incidents as memory
from agent.state import (STATUS_DISMISSED, STATUS_FAILED, STATUS_RESOLVED, STATUS_RESOLVED_BY_ENGINEER,
                         RecoveryState, step)

NAME = "learn"
STORED, SKIPPED = "STORED", "SKIPPED"

# action["source"] -> resolved_by
_RESOLVED_BY = {"INCIDENT": memory.BY_LOOKUP, "DIAGNOSIS": memory.BY_LLM, esc.SOURCE_ENGINEER: memory.BY_ENGINEER}


def embed(text: str) -> Optional[List[float]]:
    """The vector of `text` with the lookup's model (lookup/embedding.py), or None when
    no model is set. Raises when the model cannot be reached."""
    if not config.EMBED_MODEL:
        return None
    body = json.dumps({"model": config.EMBED_MODEL, "input": text}).encode()
    request = urllib.request.Request(config.EMBED_BASE_URL.rstrip("/") + "/api/embed", data=body,
                                     headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(request, timeout=config.EMBED_TIMEOUT_SECONDS) as response:
        return json.loads(response.read())["embeddings"][0]


def _table(state: RecoveryState) -> Optional[str]:
    failure = state.get("failure") or {}
    for candidate in ((state.get("lineage_context") or {}).get("detected_table"),
                      (state.get("validation") or {}).get("target_table"),
                      (state.get("execution") or {}).get("target_table")):
        if candidate:
            return candidate
    folder = (failure.get("folder") or "").strip().lower()
    layer = failure.get("detected_layer")
    return f"{layer}_{folder}" if layer and folder else layer or "unknown"


def _decision(state: RecoveryState) -> dict:
    escalation = state.get("escalation") or {}
    return (escalation.get("decision") or {}) if escalation.get("status") == esc.DECIDED else {}


def _rounds(state: RecoveryState) -> List[dict]:
    """Every escalation round of the workflow, oldest first, with the fix it was about."""
    escalation = state.get("escalation") or {}
    rounds = list(escalation.get("history") or [])
    if escalation.get("escalation_id"):
        rounds.append({"round": escalation.get("round"), "reason": escalation.get("reason"),
                       "details": escalation.get("details"), "decision": escalation.get("decision"),
                       "proposed_fix": (escalation.get("case") or {}).get("proposed_fix")})
    return rounds


def _same_fix(a: Optional[dict], b: Optional[dict]) -> bool:
    return bool(a and b) and a.get("action") == b.get("action") and \
        json.dumps(a.get("action_params") or {}, sort_keys=True) == json.dumps(b.get("action_params") or {}, sort_keys=True)


def failed_attempts(state: RecoveryState, final_fix: Optional[dict]) -> List[dict]:
    """Fixes that were staged and failed Validate in this workflow, one per round."""
    attempts = []
    for entry in _rounds(state):
        fix = entry.get("proposed_fix")
        if entry.get("reason") != esc.VALIDATION_FAILED or not fix or not fix.get("action"):
            continue
        if _same_fix(fix, final_fix):   # it held in the end (the data changed in between)
            continue
        attempts.append({"round": entry.get("round"), "fix": fix, "details": list(entry.get("details") or [])})
    return attempts


def _borrowed(state: RecoveryState, action: dict) -> bool:
    """A reused fix taken from a similar incident, or from one whose fix was itself
    borrowed, with no engineer's approval (agent/nodes/deterministic_lookup.py)."""
    if action.get("source") != "INCIDENT" or action.get("approved_by"):
        return False
    match = (state.get("incident_context") or {}).get("match") or {}
    from agent.nodes.deterministic_lookup import is_borrowed
    return is_borrowed(match)


def outcome(state: RecoveryState) -> Optional[dict]:
    """The incident this workflow stores, or None when there is nothing to store."""
    status = state.get("status") or STATUS_RESOLVED   # the automated path leaves it to Learn
    failure = state.get("failure") or {}
    decision = _decision(state)
    action = state.get("action") or {}
    validation = state.get("validation") or {}
    diagnosis = state.get("diagnosis") or {}
    match = (state.get("incident_context") or {}).get("match") or {}
    note = decision.get("note")
    validated = validation.get("passed") is True

    base = {
        "failure_type": failure.get("failure_type") or "UNKNOWN",
        "table_name": _table(state),
        "column_name": failure.get("column_name"),
        "raw_value": failure.get("signature_value") if failure.get("signature_value") is not None
        else failure.get("raw_value"),
        "rows_affected": int(validation.get("rows_staged") or (state.get("execution") or {}).get("rows_affected")
                             or failure.get("rows_affected") or 0),
    }
    reasons = [r.get("reason") for r in _rounds(state) if r.get("reason")]
    extra = {
        "failure_id": state.get("failure_id"),
        "lookup_id": state.get("lookup_id"),
        "priority_tier": (state.get("priority") or {}).get("tier"),
        "batch_ids": [promo.batch_key(base["table_name"], b) for b in reference.failure_batches(failure)],
        "escalation_reasons": reasons or None,
        "engineer": decision.get("engineer"),
        "engineer_note": note,
        "workflow_status": status,
    }

    if status in (STATUS_RESOLVED, STATUS_RESOLVED_BY_ENGINEER) and validated and action.get("action"):
        source = action.get("source")
        borrowed = _borrowed(state, action)
        if source == "DIAGNOSIS":
            text = diagnosis.get("hypothesis") or "diagnosed by the reasoning model"
        elif source == "INCIDENT":
            text = (f"reused the fix of incident {action.get('incident_id')} "
                    f"({match.get('matched_by') or 'UNKNOWN'} match, score {match.get('score')})")
        else:
            text = note or f"fix decided by {action.get('approved_by') or decision.get('engineer')}"
        approved = action.get("approved_by")   # set by engineer_review on APPLY_FIX
        return {**base, "outcome": memory.SUCCESS, "verified": True, "diagnosis": text,
                "action": action["action"], "action_params": action.get("action_params") or {},
                "resolved_by": _RESOLVED_BY.get(source, memory.BY_ENGINEER),
                "confidence": diagnosis.get("confidence") if source == "DIAGNOSIS" else None,
                "extra": {**extra, "execution_id": validation.get("execution_id")
                          or (state.get("execution") or {}).get("execution_id"),
                          "fix_source": source,
                          # how the lookup found the past fix, also when an engineer approved it
                          "matched_by": match.get("matched_by") if action.get("incident_id") else None,
                          "reused_incident_id": action.get("incident_id"), "approved_by": approved,
                          "borrowed": borrowed,
                          "promote_declined": decision.get("promote") is False or None}}
    if status == STATUS_RESOLVED_BY_ENGINEER and decision.get("decision") == esc.RESOLVED_MANUALLY:
        return {**base, "outcome": memory.SUCCESS, "verified": True, "diagnosis": note or "fixed by hand",
                "action": memory.MANUAL, "action_params": {}, "resolved_by": memory.BY_ENGINEER,
                "confidence": None, "extra": {**extra, "fix_source": esc.SOURCE_ENGINEER,
                                              "approved_by": decision.get("engineer"), "borrowed": False}}
    if status == STATUS_DISMISSED:
        proposed = (state.get("escalation") or {}).get("case", {}).get("proposed_fix") or {}
        return {**base, "outcome": memory.DISMISSED, "verified": False, "diagnosis": note or "dismissed",
                "action": memory.DISMISS, "action_params": {}, "resolved_by": memory.BY_ENGINEER,
                "confidence": None, "extra": {**extra, "fix_source": esc.SOURCE_ENGINEER, "borrowed": False,
                                              "dismissed_fix": esc.fix_text(proposed) if proposed else None}}
    return None


def _write(driver, incident_id: str, record: dict, embedding) -> None:
    memory.write_incident(
        driver, incident_id=incident_id, table_name=record["table_name"], column_name=record["column_name"],
        raw_value=record["raw_value"], failure_type=record["failure_type"], diagnosis=record["diagnosis"],
        action=record["action"], action_params=record["action_params"], outcome=record["outcome"],
        resolved_by=record["resolved_by"], confidence=record["confidence"], verified=record["verified"],
        rows_affected=record["rows_affected"], embedding=embedding,
        signature_text=memory.signature_text(record["table_name"], record["column_name"], record["raw_value"],
                                             record["failure_type"]),
        extra={k: v for k, v in record["extra"].items()})


def _promotion_target(state: RecoveryState, record: dict) -> dict:
    """(column, RAW, canonical) exactly as validated: the overlay Remediate built."""
    overlay = (state.get("execution") or {}).get("overlay") or {}
    column = (state.get("validation") or {}).get("column") or record["column_name"]
    pairs = overlay.get(column) or {}
    raw, canonical = next(iter(pairs.items())) if len(pairs) == 1 else (None, None)
    canonical = canonical or (record.get("action_params") or {}).get("canonical_value")
    return {"column_name": column, "raw_key": promo.raw_key(raw if raw is not None else record["raw_value"]),
            "canonical_value": canonical}


def learn(state: RecoveryState) -> dict:
    status = state.get("status") or STATUS_RESOLVED
    now = datetime.now(timezone.utc).isoformat()
    record = outcome(state)
    final_fix = {"action": record["action"], "action_params": record["action_params"]} if record else None
    attempts = failed_attempts(state, final_fix) if status != STATUS_FAILED else []
    if record is None and not attempts:
        why = ("a last step kept raising and nobody decided; nothing to store" if status == STATUS_FAILED
               else f"nothing to store for status {status}")
        return {"learned": {"status": SKIPPED, "written": False, "reasons": [why], "learned_at": now},
                "status": status, "trace": [step(NAME, why)]}

    driver = memory.shared_driver()
    failure_id = state.get("failure_id")
    written_attempts = []
    failure = state.get("failure") or {}
    for attempt in attempts:
        fix = attempt["fix"]
        attempt_record = {
            "failure_type": failure.get("failure_type") or "UNKNOWN", "table_name": _table(state),
            "column_name": failure.get("column_name"),
            "raw_value": failure.get("signature_value") if failure.get("signature_value") is not None
            else failure.get("raw_value"),
            "rows_affected": int(failure.get("rows_affected") or 0),
            "outcome": memory.FAILED_VALIDATION, "verified": False,
            "diagnosis": "; ".join(attempt["details"]) or "validation failed",
            "action": fix["action"], "action_params": fix.get("action_params") or {},
            "resolved_by": _RESOLVED_BY.get(fix.get("source"), memory.BY_ENGINEER),
            "confidence": fix.get("confidence"),
            "extra": {"failure_id": failure_id, "attempt_round": attempt["round"], "fix_source": fix.get("source"),
                      "reused_incident_id": fix.get("incident_id"), "borrowed": False},
        }
        attempt_id = f"{failure_id}:attempt:{attempt['round']}"
        _write(driver, attempt_id, attempt_record, None)
        written_attempts.append({"incident_id": attempt_id, "round": attempt["round"],
                                 "fix": esc.fix_text(fix)})

    if record is None:
        note = f"stored {len(written_attempts)} failed attempt(s); no outcome for status {status}"
        return {"learned": {"status": STORED, "written": True, "incident_id": None, "attempts": written_attempts,
                            "promotion": None, "learned_at": now},
                "status": status, "trace": [step(NAME, note)]}

    embedding, embedded, embed_error = None, False, None
    if record["outcome"] == memory.SUCCESS and config.EMBED_MODEL:
        try:
            embedding = embed(memory.signature_text(record["table_name"], record["column_name"],
                                                    record["raw_value"], record["failure_type"]))
            embedded = bool(embedding)
        except Exception as exc:   # the vector only widens recall; the incident still counts
            embed_error = f"{type(exc).__name__}: {exc}"
    incident_id = failure_id
    _write(driver, incident_id, record, embedding)

    promotion = None
    if record["outcome"] == memory.SUCCESS and record["action"] == actions.ADD_VARIANT:
        target = _promotion_target(state, record)
        decision = _decision(state)
        engineer_approved = (state.get("action") or {}).get("source") == esc.SOURCE_ENGINEER and \
            decision.get("decision") == esc.APPLY_FIX
        key = memory.signature_key(record["table_name"], record["column_name"], record["raw_value"])
        history = memory.signature_history(driver, key)
        existing = promo.current_mapping(target["column_name"], target["raw_key"])
        promotion = promo.decide(
            incident={**target, "incident_id": incident_id, "action": record["action"],
                      "action_params": record["action_params"], "outcome": record["outcome"],
                      "verified": record["verified"], "borrowed": record["extra"].get("borrowed")},
            history=history, existing=existing, engineer_approved=engineer_approved,
            promote_requested=decision.get("promote") is not False)
        if promotion["status"] == promo.PROMOTED:
            who = (f"engineer:{(state.get('action') or {}).get('approved_by') or decision.get('engineer')}"
                   if promotion["route"] == promo.ENGINEER_APPROVED else "recurrence")
            promotion["inserted"] = promo.promote(target["column_name"], target["raw_key"], target["canonical_value"],
                                                  added_by=f"recovery_agent:{who}",
                                                  source=f"recovery_agent:{incident_id}")
        memory.mark_promotion(driver, incident_id, promotion["status"], "; ".join(promotion["reasons"]))

    learned = {"status": STORED, "written": True, "incident_id": incident_id, "outcome": record["outcome"],
               "resolved_by": record["resolved_by"], "verified": record["verified"],
               "borrowed": record["extra"].get("borrowed"), "embedded": embedded, "embed_error": embed_error,
               "attempts": written_attempts, "promotion": promotion, "learned_at": now}
    note = f"incident {incident_id}: {record['outcome']} {record['action']} by {record['resolved_by']}"
    if record["extra"].get("borrowed"):
        note += " (borrowed, not approved)"
    if written_attempts:
        note += f"; {len(written_attempts)} failed attempt(s)"
    if promotion:
        note += f"; promotion {promotion['status']}: {'; '.join(promotion['reasons'])}"
    if embed_error:
        note += f"; stored without a vector ({embed_error})"
    return {"learned": learned, "status": status, "trace": [step(NAME, note)]}
