"""Classify, persist and optionally publish incidents before a batch completes."""
import os
import sys
import traceback
from datetime import datetime, timezone

from .classifier import QwenClassifier
from .handoff import build_incident, publish_incident, save_incident


def capture_exception(exc, *, job_name, batch_id=None, table=None,
                      dead_letter_path=None, context=None):
    if os.getenv('QWEN_ENABLED', 'false').lower() != 'true':
        return None
    trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    classification = QwenClassifier().classify(
        trace, job_name=job_name, batch_id=batch_id, table=table)
    incident = build_incident(classification, error_text=trace,
                             dead_letter_path=dead_letter_path, context=context)
    incident['created_at'] = datetime.now(timezone.utc).isoformat()
    save_incident(incident, os.getenv('QWEN_INCIDENT_DIR', 'incidents'))
    publish_incident(incident)
    print(f"[qwen] {job_name} batch={batch_id}: {incident['failure_type']} "
          f"({incident['classification_status']})", file=sys.stderr)
    return incident


def run_job(main, *, job_name):
    try:
        return main()
    except Exception as exc:
        try:
            capture_exception(exc, job_name=job_name)
        except Exception as recording_error:
            # Keep the original job failure; batch callbacks propagate handoff failures.
            print(f'[qwen] Incident recording failed: {type(recording_error).__name__}',
                  file=sys.stderr)
        raise
