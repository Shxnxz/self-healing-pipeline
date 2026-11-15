"""One real diagnosis, to confirm the reasoning model works before relying on it.

  docker compose exec recovery-agent python -m agent.check_model

Sends a fixed example (crash_month "Jule", three records dated in June) to the
configured model through the real Diagnose and policy gate. It needs Ollama and
the model, but no Kafka, Neo4j or Delta tables. Exit code 0 when the model
answered in the asked form.
"""
import json
import sys
import time

from agent import config, evidence
from agent.nodes.diagnose import OK, make_diagnose
from agent.nodes.policy_gate import make_policy_gate

RECORDS = [{"crash_date": "2024-06-22", "crash_day_of_week": "Sa", "crash_month": "Jule"},
           {"crash_date": "06/23/2024 08:10:00 AM", "crash_day_of_week": "Su", "crash_month": "Jule"},
           {"crash_date": "2024-06-19", "crash_day_of_week": "We", "crash_month": "Jule"}]
STATE = {
    "failure": {"failure_id": "check-model", "source": "QUARANTINE", "failure_type": "UNRESOLVED_VALUE",
                "family": "VALUE", "folder": "crash1", "column_name": "crash_month", "raw_value": "Jule",
                "rows_affected": 3, "trusted": True, "sample_record_ids": ["r1", "r2", "r3"]},
    "lineage_context": {"origin_table": "silver1_crash1", "detected_table": "silver2_crash1",
                        "downstream": [{"name": "gold"}]},
    "incident_context": {"candidates": [], "previous_attempts": []},
}


def main() -> int:
    readers = {"canonical_values": lambda column: list(evidence.MONTHS),
               "sample_records": lambda failure, limit=None: RECORDS}
    print(f"Asking {config.DIAGNOSE_MODEL} at {config.DIAGNOSE_BASE_URL} "
          f"(think={config.DIAGNOSE_THINK}, timeout {config.DIAGNOSE_TIMEOUT_SECONDS:.0f} s) ...", flush=True)
    started = time.monotonic()
    diagnosis = make_diagnose(**readers)(STATE)["diagnosis"]
    seconds = time.monotonic() - started
    policy = make_policy_gate(**readers)({**STATE, "diagnosis": diagnosis})["policy"]

    print(json.dumps({k: diagnosis[k] for k in ("status", "hypothesis", "confidence", "proposed_action", "reasons")},
                     indent=2, ensure_ascii=False))
    print(f"policy gate: {policy['outcome']}" + "".join(f"\n  - {reason}" for reason in policy["reasons"]))
    if diagnosis["status"] != OK:
        print(f"FAILED after {seconds:.1f} s: no diagnosis ({diagnosis['status']}).")
        return 1
    expected = (diagnosis["proposed_action"] or {}).get("action_params") == {"canonical_value": "JUNE"}
    print(f"OK in {seconds:.1f} s: the model answered in the asked form"
          + ("." if expected else ", but did not choose JUNE, which the three dates show."))
    return 0


if __name__ == "__main__":
    sys.exit(main())
