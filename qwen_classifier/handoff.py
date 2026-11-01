"""Versioned incident messages and acknowledged delivery; no recovery logic."""
import hashlib
import json
import os
import uuid
from pathlib import Path


def build_incident(classification, *, error_text, dead_letter_path=None, context=None):
    """Keep source evidence separate from model predictions and graph identifiers."""
    context = dict(context or {})
    identity = {
        'job': classification.get('job_name'), 'batch': classification.get('batch_id'),
        'dead_letter': dead_letter_path, 'error': error_text, 'context': context,
    }
    incident_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    needs_review = classification.get('needs_review', True) or not dead_letter_path
    status = classification.get('classification_status', 'UNAVAILABLE')
    return {
        **classification,
        'schema_version': '1.0',
        'needs_review': needs_review,
        'incident_id': incident_id,
        'error_text': error_text,
        'dead_letter_path': dead_letter_path,  # Compatibility with existing readers.
        'dead_letter_ref': {
            'path': dead_letter_path, 'batch_id': classification.get('batch_id'),
            'source_table': classification.get('table'),
            'expected_only': context.get('expected_only', []),
            'source_only': context.get('source_only', []),
            'error_class': context.get('detected_error_class'),
        },
        'source_topic': context.get('source_topic'),
        'affected_dataset': context.get('affected_dataset'),
        'affected_record_count': context.get('affected_record_count'),
        'affected_columns': {
            'expected_only': context.get('expected_only', []),
            'source_only': context.get('source_only', []),
            'reported_column': classification.get('column'),
        },
        'detected_error_class': context.get('detected_error_class'),
        'handoff_status': 'READY' if status == 'OK' and not needs_review else 'NEEDS_REVIEW',
    }


def save_incident(incident, directory):
    """Atomically persist before delivery. Storage failures must propagate."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / (incident['incident_id'] + '.json')
    temporary = directory / (target.name + '.' + uuid.uuid4().hex + '.tmp')
    try:
        temporary.write_text(json.dumps(incident, ensure_ascii=False, indent=2), encoding='utf-8')
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def publish_incident(incident):
    """Optional at-least-once delivery. Consumers deduplicate by incident_id."""
    if os.getenv('QWEN_KAFKA_ENABLED', 'false').lower() != 'true':
        return
    from confluent_kafka import Producer

    producer = Producer({
        'bootstrap.servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        'enable.idempotence': True, 'acks': 'all', 'delivery.timeout.ms': 30000,
    })
    delivered = []
    def acknowledge(error, message):
        delivered.append(error)
    producer.produce(
        os.getenv('QWEN_INCIDENT_TOPIC', 'recovery.incidents'),
        key=incident['incident_id'].encode(),
        value=json.dumps(incident, ensure_ascii=False).encode(),
        on_delivery=acknowledge,
    )
    remaining = producer.flush(35)
    if remaining or not delivered or delivered[0] is not None:
        raise RuntimeError('Incident Kafka delivery was not acknowledged')
