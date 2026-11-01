import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('schema_guard', Path(__file__).resolve().parents[1]/'spark-jobs/schema_guard.py')
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


class Tests(unittest.TestCase):
    def test_equal_columns_any_order(self):
        self.assertEqual(m.schema_difference(['b', 'a'], ['a', 'b']), ([], []))

    def test_renamed_missing_extra_and_case(self):
        self.assertEqual(m.schema_difference(['accident_date', 'WEATHER', 'extra'],
                                            ['crash_date', 'weather']),
                         (['crash_date', 'weather'], ['WEATHER', 'accident_date', 'extra']))

    def test_error_has_source_evidence(self):
        msg = str(m.schema_error('Crash4', ['crash_date'], ['accident_date']))
        self.assertIn('[SCHEMA_MISMATCH]', msg)
        self.assertIn('Crash4', msg)
        self.assertIn('accident_date', msg)
