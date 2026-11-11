"""Hand-made recovery.lookup stack items and fake nodes for the agent tests."""


def stack_item(failure_id="f1", route="diagnose", match=None, rank=1, value="Jule"):
    """One item of a recovery.lookup stack, shaped like lookup/stack.py builds it."""
    return {
        "rank": rank,
        "route": route,
        "route_reason": "exact match 1.00 -> reuse ADD_VARIANT" if match else "no verified incident in memory",
        "priority": {"score": 42.0, "tier": "P2"},
        "failure": {
            "failure_id": failure_id, "source": "QUARANTINE", "failure_type": "UNRESOLVED_VALUE",
            "family": "VALUE", "folder": "Crash1", "column_name": "crash_month",
            "raw_value": value, "signature_value": value, "rows_affected": 10, "trusted": True,
        },
        "lineage_context": {"origin_table": "silver1_crash1", "detected_table": "silver2_crash1",
                            "blast_radius": {"score": 42.0, "tier": "P2"}},
        "incident_context": {"reusable": bool(match), "match": match, "candidates": [],
                             "previous_attempts": []},
    }


PAST_FIX = {"incident_id": "i1", "matched_by": "EXACT", "score": 1.0,
            "action": "ADD_VARIANT", "action_params": {"canonical_value": "JULY"}}


def lookup_message(*items, lookup_id="L1"):
    return {"schema_version": "1.0", "lookup_id": lookup_id, "stack": list(items)}


def path(state):
    return [entry["node"] for entry in state["trace"]]


# --- fake nodes: what the real steps will return once they are built -----------
def trace(node):
    return [{"node": node, "at": "t", "note": "fake"}]


def remediate_ok(state):
    return {"execution": {"status": "SUCCESS"}, "trace": trace("remediate")}


def validate_ok(state):
    return {"validation": {"passed": True, "reasons": []}, "trace": trace("validate")}


def validate_fails(state):
    return {"validation": {"passed": False, "reasons": ["new failure in weather_condition"]},
            "trace": trace("validate")}


def diagnose_july(state):
    return {"diagnosis": {"hypothesis": "typo of July", "confidence": 0.93,
                          "proposed_action": {"action": "ADD_VARIANT",
                                              "action_params": {"canonical_value": "JULY"}}},
            "trace": trace("diagnose")}


def gate_passes(state):
    proposed = state["diagnosis"]["proposed_action"]
    return {"policy": {"passed": True, "reasons": []},
            "action": {**proposed, "source": "DIAGNOSIS", "incident_id": None},
            "trace": trace("policy_gate")}


def gate_passes_without_action(state):
    return {"policy": {"passed": True, "reasons": []}, "trace": trace("policy_gate")}


# --- Task 103: what the Delta readers and the reasoning model return ---------------
MONTHS = ["APRIL", "AUGUST", "DECEMBER", "FEBRUARY", "JANUARY", "JULY", "JUNE", "MARCH", "MAY",
          "NOVEMBER", "OCTOBER", "SEPTEMBER"]
CANONICAL = {"crash_month": MONTHS, "weather_condition": ["CLEAR", "CLOUDY", "FOG", "RAIN", "SNOW", "UNKNOWN"],
             "crash_day_of_week": ["FRIDAY", "MONDAY", "SATURDAY", "SUNDAY", "THURSDAY", "TUESDAY", "WEDNESDAY"]}


def canonical_values(column, path=None):
    return CANONICAL.get(column, [])


def july_samples(failure, limit=None, path=None):
    return [{"crash_date": "07/04/2019 10:15:00 PM", "crash_month": "Jule"},
            {"crash_date": "07/21/2019 08:00:00 AM", "crash_month": "Jule"}]


def schema_item(failure_id="s1", trusted=True):
    """A Silver 1 schema failure as the Qwen triage hands it over: two renamed columns."""
    item = stack_item(failure_id=failure_id)
    item["failure"].update({
        "source": "QWEN", "failure_type": "SCHEMA_MISMATCH", "family": "SCHEMA", "column_name": None,
        "raw_value": None, "trusted": trusted, "expected_only": ["crash_date", "weather_condition"],
        "source_only": ["CrashDate", "Weather"],
        "signature_value": "EXPECTED_ONLY=crash_date,weather_condition;SOURCE_ONLY=CrashDate,Weather",
    })
    return item


class FakeModel:
    """Stands in for llm.chat_json: answers with `reply` and keeps what it was asked."""

    def __init__(self, reply=None, error=None):
        self.reply, self.error, self.calls = reply, error, []

    def __call__(self, system, user, schema):
        self.calls.append({"system": system, "user": user, "schema": schema})
        if self.error:
            raise self.error
        return self.reply


JULY_REPLY = {"hypothesis": "Typo of July: both sample records have a crash_date in July.",
              "action": "ADD_VARIANT", "canonical_value": "JULY", "confidence": 0.93}
