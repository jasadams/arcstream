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

    def test_existing_raw_topic_preserves_isolated_groups_and_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp)/'bundle'
            manifest = prepare.prepare(path, 'broker:9092', 'arc-test',
                                       profile_release=True, raw_events_topic='existing.events')
            self.assertEqual(manifest['raw_events_topic'], 'existing.events')
            identity = (path/'identity.sql').read_text()
            self.assertIn("topic = 'existing.events'", identity)
            self.assertIn("'arc-test-identity-owner'", identity)
            self.assertIn("'arc-test-identity-capture'", identity)
            self.assertIn("topic='arc-test-profiles'", (path/'profiles.sql').read_text())
            with self.assertRaises(ValueError):
                prepare.prepare(Path(temp)/'invalid', 'broker:9092', 'arc-test',
                                fixture=('sessions', '/in', '/out'), raw_events_topic='existing.events')

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
        release = json.loads((ROOT/'deploy/streamr-native/profile-native/release-fields.json').read_text())
        released = {row['name'] for row in release['output_fields']}
        deferred = set(release['deferred_session_fields'] + release['omitted_publication_fields'])
        self.assertEqual(released, set(re.findall(r'^    "([a-z_0-9]+)",$', consumer, re.M)))
        self.assertEqual(actual | pending, released | deferred)
        self.assertFalse(released & deferred)

    def test_scoped_release_emits_complete_profiles_without_diagnostic_producers(self):
        queries = prepare.render('owned-broker:9092', 'arc-test', profile_release=True)
        self.assertEqual(set(queries), {'identity', 'unified', 'merges', 'sessions', 'profiles'})
        self.assertIn("topic='arc-test-profiles'", queries['profiles'])
        self.assertNotIn('diagnostic', queries['profiles'])
        self.assertIn("SESSION(INTERVAL '30 minutes')", queries['profiles'])
        self.assertNotIn('EVENT_TIMESTAMP()', queries['profiles'])
        self.assertNotIn('changed_fields', queries['profiles'])
        self.assertNotIn('sink.recovery_max_bytes', queries['profiles'])
        self.assertIn("format='json'", queries['profiles'])
        self.assertIn('MERGE INTO profile_scalar_snapshot', queries['profiles'])

    def test_release_oracle_preserves_absence_fractional_average_and_quiet_expiry(self):
        directory = ROOT/'test/streamr-reference/profile-native/release'
        stages = json.loads((directory/'expected-prefixes.json').read_text())
        fields = json.loads((ROOT/'deploy/streamr-native/profile-native/release-fields.json').read_text())
        keys = {row['name'] for row in fields['output_fields']}
        for stage in stages:
            for row in stage['values']:
                self.assertEqual(set(row), keys)
        self.assertIsNone(stages[0]['values'][0]['avg_session_duration_sec'])
        self.assertEqual(stages[5]['values'], stages[4]['values'])
        person_a = lambda stage: [r for r in stage['values'] if r['tenant_id'] == 'tenant-a'][-1]
        self.assertEqual(person_a(stages[6])['avg_session_duration_sec'], 5.0)
        self.assertEqual(person_a(stages[8])['avg_session_duration_sec'], 5.0)
        self.assertEqual(person_a(stages[9])['avg_session_duration_sec'], 3.75)
        self.assertEqual(person_a(stages[10])['avg_session_duration_sec'], 3.75)
        self.assertEqual(person_a(stages[10])['events_1d'], 0)
        self.assertEqual(person_a(stages[10])['total_events'], 7)

    def test_native_flaredb_schema_matches_complete_wire_and_fractional_average(self):
        fields = json.loads((ROOT/'deploy/streamr-native/profile-native/release-fields.json').read_text())
        config = prepare.render_flaredb_profiles('owned-broker:9092', 'arc-test')
        schema = {field['name']: field for field in config['schema']}
        self.assertEqual(set(schema), {field['name'] for field in fields['output_fields']})
        self.assertEqual(len(schema), 22)
        for field in fields['output_fields']:
            name = field['name']
            expected_type = ('Timestamp(Millisecond, None)' if name in {'first_seen', 'last_seen'}
                             else {'TEXT': 'Utf8', 'TEXT[]': 'List(item:Utf8:true)', 'BIGINT': 'Int64', 'DOUBLE': 'Float64'}[field['sql_type']])
            self.assertEqual(schema[name]['type'], expected_type, name)
            self.assertEqual(schema[name]['nullable'], field.get('nullable', False), name)
        self.assertEqual(schema['avg_session_duration_sec'],
                         {'name': 'avg_session_duration_sec', 'type': 'Float64', 'nullable': True})
        self.assertEqual(config['upsertConfig'], {'mode': 'FULL', 'primary_key_columns': ['tenant_id', 'canonical_id']})
        # FlareDB catalog::data_type_from_string parses List(name:type:nullable);
        # ingestion::deserialize accepts JSON arrays only for native List fields.
        # Utf8 would reject each complete native profile row containing ranks.
        for name in ('top_pages', 'top_features'):
            self.assertEqual(schema[name]['type'], 'List(item:Utf8:true)')
        self.assertEqual(config['tableName'], 'profiles')
        self.assertEqual(config['ingestionConfig'], {'topic': 'arc-test-profiles', 'brokers': 'owned-broker:9092',
                                                    'groupId': 'arc-test-flaredb-profiles'})
        stages = json.loads((ROOT/'test/streamr-reference/profile-native/release/expected-prefixes.json').read_text())
        values = [row['avg_session_duration_sec'] for stage in stages for row in stage['values']]
        self.assertIn(None, values)
        self.assertIn(3.75, values)

    def test_release_bundle_emits_native_flaredb_config_without_claiming_ingestion(self):
        import hashlib
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)/'bundle'
            broker = 'owned-"broker:9092'
            manifest = prepare.prepare(directory, broker, 'arc-test', profile_release=True)
            path = directory/'flaredb/profiles.json'
            config = json.loads(path.read_text())
            self.assertEqual(config['ingestionConfig'], {'topic': 'arc-test-profiles', 'brokers': broker,
                                                        'groupId': 'arc-test-flaredb-profiles'})
            self.assertEqual(manifest['required_flaredb_contracts'], ['FLA-177', 'FLA-178', 'FLA-179'])
            self.assertEqual(manifest['analytics_configs']['flaredb/profiles.json'], hashlib.sha256(path.read_bytes()).hexdigest())
            self.assertIn('deploy/streamr-native/flaredb/profiles.json', manifest['templates_sha256'])
            self.assertIn('actual FlareDB ingestion not validated', manifest['analytics_validation'])
            self.assertFalse(manifest['production_ready'])
            self.assertNotIn('{{', path.read_text())
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp)/'diagnostic'
            prepare.prepare(directory, 'broker:9092', 'arc-test')
            self.assertFalse((directory/'flaredb').exists())



if __name__ == '__main__':
    unittest.main()
