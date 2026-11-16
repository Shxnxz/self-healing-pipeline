import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError
from qwen_classifier import QwenClassifier
from qwen_classifier.integration import capture_exception, run_job

ERROR='[UNRESOLVED_COLUMN.WITH_SUGGESTION] Column `crash_month` cannot be resolved'
GOOD=dict(failure_type='MISSING_COLUMN', table=None, column='crash_month', error_class='UNRESOLVED_COLUMN.WITH_SUGGESTION')
def reply(pred):
    return io.BytesIO(json.dumps(dict(done=True,message=dict(content=json.dumps(pred)))).encode())

class Tests(unittest.TestCase):
    @patch('qwen_classifier.classifier.urlopen')
    def test_valid(self, call):
        call.return_value=reply(GOOD)
        result=QwenClassifier().classify(ERROR,batch_id=1)
        self.assertEqual(result['column'],'crash_month')
        self.assertFalse(result['needs_review'])
        self.assertEqual(result['classification_status'],'OK')
    @patch('qwen_classifier.classifier.urlopen')
    def test_invalid(self, call):
        for prediction in (dict(GOOD,table='invented'),dict(GOOD,action='DROP'),
                           dict(GOOD,error_class='UNRESOLVED_COLUMN'),dict(GOOD,column=[])):
            call.return_value=reply(prediction)
            self.assertEqual(QwenClassifier().classify(ERROR)['classification_status'],'INVALID_OUTPUT')
    @patch('qwen_classifier.classifier.urlopen',side_effect=URLError('offline'))
    def test_offline(self,call):
        result=QwenClassifier().classify(ERROR)
        self.assertEqual(result['classification_status'],'UNAVAILABLE')
        self.assertTrue(result['needs_review'])
    @patch('qwen_classifier.classifier.urlopen')
    def test_bad_json(self,call):
        call.return_value=io.BytesIO(b'not json')
        self.assertEqual(QwenClassifier().classify(ERROR)['classification_status'],'INVALID_OUTPUT')
    @patch('qwen_classifier.classifier.urlopen')
    def test_unknown_and_truncated(self,call):
        call.return_value=reply(dict(GOOD,failure_type='UNKNOWN'))
        self.assertTrue(QwenClassifier().classify(ERROR)['needs_review'])
        call.return_value=reply(GOOD)
        self.assertTrue(QwenClassifier().classify(ERROR+'x'*20000)['needs_review'])
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_disabled(self,client):
        with patch.dict(os.environ,QWEN_ENABLED='false'):
            self.assertIsNone(capture_exception(ValueError('x'),job_name='silver2'))
        client.assert_not_called()
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_preserves_error_and_writes_incident(self,client):
        client.return_value.classify.return_value=dict(GOOD,classification_status='OK')
        original=RuntimeError('original')
        def fail(): raise original
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ,QWEN_ENABLED='true',QWEN_INCIDENT_DIR=directory):
                with self.assertRaises(RuntimeError) as caught:
                    run_job(fail,job_name='silver2')
            self.assertIs(caught.exception,original)
            self.assertEqual(len(list(Path(directory).glob('*.json'))),1)
    def test_blank(self):
        with self.assertRaises(ValueError): QwenClassifier().classify('')

    @patch('qwen_classifier.classifier.urlopen')
    def test_source_table_cannot_be_overridden(self, call):
        call.return_value = reply(dict(GOOD, table='OtherTable'))
        result = QwenClassifier().classify(ERROR + ' join OtherTable', table='Crash4')
        self.assertEqual(result['table'], 'Crash4')
        self.assertEqual(result['classification_status'], 'INVALID_OUTPUT')

    # --- Task 109: grounding ------------------------------------------------------
    SUGGESTED = ('[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name '
                 '`crash_mnth` cannot be resolved. Did you mean one of the following? [`crash_month`, `crash_date`].')

    @patch('qwen_classifier.classifier.urlopen')
    def test_column_from_a_suggestion_list_is_not_evidence(self, call):
        call.return_value = reply(dict(GOOD, column='crash_month'))
        result = QwenClassifier().classify(self.SUGGESTED)
        self.assertEqual(result['classification_status'], 'INVALID_OUTPUT')
        self.assertTrue(result['needs_review'])
        call.return_value = reply(dict(GOOD, column='crash_mnth'))
        self.assertEqual(QwenClassifier().classify(self.SUGGESTED)['classification_status'], 'OK')

    @patch('qwen_classifier.classifier.urlopen')
    def test_column_must_be_a_whole_name(self, call):
        call.return_value = reply(dict(GOOD, column='crash'))   # only inside `crash_month`
        self.assertEqual(QwenClassifier().classify(ERROR)['classification_status'], 'INVALID_OUTPUT')

    @patch('qwen_classifier.classifier.urlopen')
    def test_column_must_be_the_unresolved_one(self, call):
        text = ERROR + ' while reading crash_date from the plan'
        call.return_value = reply(dict(GOOD, column='crash_date'))    # in the text, but not the failing one
        self.assertEqual(QwenClassifier().classify(text)['classification_status'], 'INVALID_OUTPUT')

    @patch('qwen_classifier.classifier.urlopen')
    def test_schema_drift_is_the_schema_type(self, call):
        call.return_value = reply(dict(GOOD, failure_type='SCHEMA_MISMATCH', column=None))
        self.assertEqual(QwenClassifier().classify(ERROR)['classification_status'], 'INVALID_OUTPUT')
        call.return_value = reply(dict(GOOD, failure_type='SCHEMA_DRIFT', column=None))
        self.assertEqual(QwenClassifier().classify(ERROR)['failure_type'], 'SCHEMA_DRIFT')

    @patch('qwen_classifier.integration.publish_incident')
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_background_worker_records_the_incident(self, client, publish):
        import time
        client.return_value.classify.return_value = dict(GOOD, classification_status='OK')
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, QWEN_ENABLED='true', QWEN_INCIDENT_DIR=directory, QWEN_ASYNC='true'):
                queued = capture_exception(ValueError('x'), job_name='silver2', batch_id=3)
                waiting, deadline = Path(directory) / 'outbox', time.time() + 5
                while list(waiting.glob('*.json')) and time.time() < deadline:
                    time.sleep(0.02)
                self.assertEqual(publish.call_count, 1)
                self.assertTrue((Path(directory) / (queued['incident_id'] + '.json')).exists())
                self.assertEqual(list((Path(directory) / 'outbox').glob('*.json')), [])
