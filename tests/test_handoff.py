"""Failure-path and contract tests without a running model or Kafka broker."""
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from qwen_classifier.handoff import build_incident, publish_incident
from qwen_classifier.integration import capture_exception


class HandoffTests(unittest.TestCase):
    def classification(self, **changes):
        return dict(failure_type='SCHEMA_MISMATCH', table='Crash4', column=None,
                    job_name='Silver1SchemaValidation', batch_id=0,
                    classification_status='OK', needs_review=False, **changes)

    def test_stable_identity_and_structured_evidence(self):
        context = dict(source_topic='crashes.crash4', expected_only=['crash_date'],
                       source_only=['accident_date'], affected_record_count=1000,
                       affected_dataset='bronze')
        first = build_incident(self.classification(), error_text='schema error', dead_letter_path='/delta/dead', context=context)
        second = build_incident(self.classification(), error_text='schema error', dead_letter_path='/delta/dead', context=context)
        self.assertEqual(first['incident_id'], second['incident_id'])
        self.assertEqual(first['affected_columns']['source_only'], ['accident_date'])
        self.assertEqual(first['affected_record_count'], 1000)
        self.assertEqual(first['handoff_status'], 'READY')
        self.assertEqual(json.loads(json.dumps(first)), first)
        second = build_incident(self.classification(), error_text='different error', context=context)
        self.assertNotEqual(first['incident_id'], second['incident_id'])

    def test_unavailable_and_unknown_require_review(self):
        for status, review in [('UNAVAILABLE', True), ('INVALID_OUTPUT', True), ('OK', True)]:
            result = self.classification()
            result.update(classification_status=status, needs_review=review)
            self.assertEqual(build_incident(result, error_text='x')['handoff_status'], 'NEEDS_REVIEW')

    @patch('qwen_classifier.integration.QwenClassifier')
    def test_storage_failure_propagates(self, classifier):
        classifier.return_value.classify.return_value = self.classification()
        with patch.dict(os.environ, QWEN_ENABLED='true'):
            with patch('qwen_classifier.handoff.Path.write_text', side_effect=OSError('disk full')):
                with tempfile.TemporaryDirectory() as directory:
                    with patch.dict(os.environ, QWEN_INCIDENT_DIR=directory):
                        with self.assertRaises(OSError):
                            capture_exception(ValueError('x'), job_name='job')

    @patch('qwen_classifier.integration.publish_incident', side_effect=RuntimeError('broker down'))
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_delivery_failure_keeps_local_incident_and_propagates(self, classifier, publish):
        classifier.return_value.classify.return_value = self.classification()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory):
                with self.assertRaises(RuntimeError):
                    capture_exception(ValueError('x'), job_name='job')
            self.assertEqual(len(list(Path(directory).glob('*.json'))), 1)

    def test_acknowledged_and_failed_delivery(self):
        incident = build_incident(self.classification(), error_text='x')
        for error in (None, 'broker error'):
            producer = Mock()
            producer.produce.side_effect = lambda *args, **kwargs: kwargs['on_delivery'](error, None)
            producer.flush.return_value = 0
            with patch.dict(sys.modules, confluent_kafka=SimpleNamespace(Producer=Mock(return_value=producer))):
                with patch.dict(os.environ, QWEN_KAFKA_ENABLED='true', KAFKA_BOOTSTRAP_SERVERS='kafka:9092'):
                    if error:
                        with self.assertRaises(RuntimeError):
                            publish_incident(incident)
                    else:
                        publish_incident(incident)
                        self.assertEqual(producer.produce.call_args.kwargs['key'], incident['incident_id'].encode())

    def test_missing_dead_letter_requires_review(self):
        event = build_incident(self.classification(), error_text='startup error')
        self.assertTrue(event['needs_review'])
        self.assertEqual(event['handoff_status'], 'NEEDS_REVIEW')
