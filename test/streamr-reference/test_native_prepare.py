"""Preparation must preserve contracts and isolate incomplete profile output."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('native_prepare', ROOT/'deploy/streamr-native/prepare.py')
prepare = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare)


class PrepareTests(unittest.TestCase):
    def test_kafka_queries_preserve_source_time_and_isolate_partial_outputs(self):
        queries = prepare.render('owned-broker:9092', 'arc-test')
        self.assertEqual(set(queries), {'identity','unified','merges','sessions','profile-core','closed-metrics'})
        for query in queries.values():
            self.assertNotIn('{{', query)
            self.assertIn("'read_committed'", query)
            self.assertIn("INTERVAL '5 seconds'", query)
            self.assertNotIn('arrival_seq', query)
            self.assertNotIn('ORDER BY source_offset', query)
        self.assertIn("topic='arc-test-profile-core-diagnostic'", queries['profile-core'])
        self.assertNotIn("topic='arc-test-profiles'", queries['profile-core'])
        self.assertIn('FIRST_VALUE(canonical_id)', queries['sessions'])

    def test_sql_literal_quote_cannot_inject_connector_options(self):
        query = prepare.render_fixture('profile-core', "/tmp/input'file", '/tmp/output')
        self.assertIn("path='/tmp/input''file'", query)
        self.assertNotIn('connector=\'kafka\'', query)
        self.assertNotIn('{{', query)

    def test_manifest_does_not_claim_readiness_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'bundle'
            manifest = prepare.prepare(path, 'broker:9092', 'arc-test')
            self.assertFalse(manifest['complete_mvp'])
            self.assertFalse(manifest['production_ready'])
            self.assertEqual(manifest['required_backend'], 'rocksdb')
            self.assertEqual(set(manifest['required_engine_contracts']), {'STR-29','STR-60','STR-64','STR-146'})
            payload = json.loads((path/'profile-core-request.json').read_text())
            self.assertEqual(payload['udfs'], [])
            self.assertEqual(payload['parallelism'], 1)
            with self.assertRaises(FileExistsError):
                prepare.prepare(path, 'broker:9092', 'arc-test')

    def test_field_coverage_matches_consumer_without_invented_values(self):
        fields = json.loads((ROOT/'deploy/streamr-native/profile-native/fields.json').read_text())
        actual = {row['name'] for row in fields['output_fields']}
        pending = set(fields['pending_application_fields'])
        self.assertEqual(len(actual), 20)
        self.assertEqual(len(pending), 13)
        self.assertFalse(actual & pending)
        consumer = (ROOT/'query-api/src/streaming/profile_decode.rs').read_text().split('];', 1)[0]
        import re
        self.assertEqual(actual | pending, set(re.findall(r'^    "([a-z_0-9]+)",$', consumer, re.M)))


if __name__ == '__main__':
    unittest.main()
