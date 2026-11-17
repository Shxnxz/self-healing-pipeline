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
from qwen_classifier.integration import capture_exception, outbox, run_job


class HandoffTests(unittest.TestCase):
    def classification(self, **changes):
        return dict(failure_type='SCHEMA_DRIFT', table='Crash4', column=None,
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
            with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory, QWEN_ASYNC='false'):
                with self.assertRaises(RuntimeError):
                    capture_exception(ValueError('x'), job_name='job')
            self.assertEqual(len(list(Path(directory).glob('*.json'))), 1)

    @patch('qwen_classifier.integration.Outbox.start')
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_async_call_returns_before_classifying_and_survives_a_broker_outage(self, classifier, start):
        """Task 109: foreachBatch only writes the outbox file; the model call and the
        delivery happen on the worker, which keeps the file until Kafka took the incident."""
        classifier.return_value.classify.return_value = self.classification()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory, QWEN_ASYNC='true'):
                queued = capture_exception(ValueError('x'), job_name='job', batch_id=4, table='Crash4')
                classifier.return_value.classify.assert_not_called()
                self.assertEqual(queued['handoff_status'], 'PENDING')
                start.assert_called_once()
                box = outbox()
                [pending] = box.pending()
                self.assertEqual(pending.stem, queued['incident_id'])
                with patch('qwen_classifier.integration.publish_incident', side_effect=RuntimeError('broker down')):
                    self.assertEqual(box.drain(), (0, 1))
                self.assertEqual(len(box.pending()), 1)              # kept for the next attempt
                saved = Path(directory) / (queued['incident_id'] + '.json')
                self.assertTrue(saved.exists())                     # classified once, kept
                with patch('qwen_classifier.integration.publish_incident') as publish:
                    self.assertEqual(box.drain(), (1, 0))
                self.assertEqual(classifier.return_value.classify.call_count, 1)   # not classified again
                published = publish.call_args.args[0]
                self.assertEqual(published['incident_id'], queued['incident_id'])
                self.assertEqual(published, json.loads(saved.read_text()))
                self.assertEqual(box.pending(), [])

    @patch('qwen_classifier.integration.Outbox.start')
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_two_drivers_never_take_the_same_outbox_file(self, classifier, start):
        """Both Spark drivers share incidents/: a file is claimed by rename before it is
        classified, so the other driver skips it (review of #37)."""
        from qwen_classifier.integration import Outbox
        classifier.return_value.classify.return_value = self.classification()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory, QWEN_ASYNC='true'):
                capture_exception(ValueError('x'), job_name='job', batch_id=1)
                first, second = Outbox(directory), Outbox(directory)
                [item] = first.pending()
                claimed = first.claim(item)
                self.assertIsNotNone(claimed)
                self.assertEqual(second.pending(), [])             # the other driver sees nothing
                self.assertIsNone(second.claim(item))
                self.assertEqual(second.drain(), (0, 0))
                first.release(claimed)                             # a failed attempt gives it back
                with patch('qwen_classifier.integration.publish_incident') as publish:
                    self.assertEqual(second.drain(), (1, 0))
                self.assertEqual(publish.call_count, 1)
                self.assertEqual(classifier.return_value.classify.call_count, 1)

    @patch('qwen_classifier.integration.Outbox.start')
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_claims_of_a_stopped_driver_are_put_back(self, classifier, start):
        from qwen_classifier.integration import Outbox
        classifier.return_value.classify.return_value = self.classification()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory, QWEN_ASYNC='true'):
                capture_exception(ValueError('mine'), job_name='job', batch_id=1)
                capture_exception(ValueError('other host'), job_name='job', batch_id=2)
                box = Outbox(directory)
                mine, other = box.pending()
                box.claim(mine)                                    # this host, before a restart
                taken = other.with_name(other.name + '.claimed.other-host-7')
                os.rename(other, taken)
                self.assertEqual(box.recover(stale_seconds=3600), 1)   # the other host may be alive
                self.assertEqual(len(box.pending()), 1)
                os.utime(taken, (0, 0))                           # ...until its claim is stale
                self.assertEqual(box.recover(stale_seconds=3600), 1)
                self.assertEqual(len(box.pending()), 2)

    @patch('qwen_classifier.integration.QwenClassifier')
    def test_async_and_sync_give_the_same_incident_id(self, classifier):
        classifier.return_value.classify.side_effect = lambda text, **kw: dict(
            self.classification(), job_name=kw['job_name'], batch_id=kw['batch_id'])
        error = ValueError('same')
        ids = []
        for mode in ('true', 'false'):
            with tempfile.TemporaryDirectory() as directory:
                with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory, QWEN_ASYNC=mode):
                    with patch('qwen_classifier.integration.Outbox.start'):
                        ids.append(capture_exception(error, job_name='job', batch_id=2)['incident_id'])
        self.assertEqual(ids[0], ids[1])

    @patch('qwen_classifier.integration.QwenClassifier')
    def test_run_job_drains_what_a_stopped_driver_left(self, classifier):
        classifier.return_value.classify.return_value = self.classification()
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory, QWEN_ASYNC='true'):
                with patch('qwen_classifier.integration.Outbox.start'):
                    capture_exception(ValueError('left over'), job_name='job', batch_id=1)
                with patch('qwen_classifier.integration.Outbox.start') as start:
                    self.assertEqual(run_job(lambda: 'ran', job_name='job'), 'ran')
                start.assert_called_once()

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
