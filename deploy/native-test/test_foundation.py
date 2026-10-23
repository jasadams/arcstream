import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

HERE = Path(__file__).resolve().parent

def load(name):
    spec = importlib.util.spec_from_file_location(name, HERE / (name + '.py'))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

class FoundationTests(unittest.TestCase):
    def test_all_queries_validated_before_first_create(self):
        bootstrap = load('bootstrap')
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            for stage in bootstrap.STAGES:
                Path(directory, stage + '-request.json').write_text(json.dumps(dict(name=stage, query=stage, udfs=[], checkpoint_interval_micros=2)))
            def request(path, payload=None):
                calls.append(path)
                if path.endswith('validate_query'):
                    return {'errors': ['bad query'] if payload['query'] == 'profiles' else []}
                raise AssertionError('must not create or inspect until all queries validate')
            with self.assertRaises(ValueError):
                bootstrap.bootstrap(directory, request)
        self.assertEqual(len(calls), 5)

    def test_existing_jobs_are_idempotent_and_conflicts_do_not_mutate(self):
        bootstrap = load('bootstrap')
        with tempfile.TemporaryDirectory() as directory:
            payloads = [dict(name=s, query=s, udfs=[], checkpoint_interval_micros=2) for s in bootstrap.STAGES]
            for stage, payload in zip(bootstrap.STAGES, payloads):
                Path(directory, stage + '-request.json').write_text(json.dumps(payload))
            def request(path, payload=None):
                if path.endswith('validate_query'):
                    return {'errors': []}
                if path.startswith('pipelines?'):
                    return {'data': payloads, 'has_more': False}
                raise AssertionError('must not create existing or conflicting graph')
            self.assertEqual(bootstrap.bootstrap(directory, request), payloads)
            payloads[-1]['query'] = 'conflict'
            with self.assertRaises(ValueError):
                bootstrap.bootstrap(directory, request)

    def test_only_digest_pins_accepted(self):
        render = load('render')
        env = {k: 'registry/image@sha256:' + 'a' * 64 for k in render.IMAGES}
        env.update(NATIVE_NAMESPACE='native-test-23', KAFKA_BROKERS='broker:9092', RAW_EVENTS_TOPIC='raw-events', NATIVE_TEST_PORT='3300')
        render.validate_environment(env)
        env['STREAMR_IMAGE'] = 'registry/image:latest'
        with self.assertRaises(ValueError):
            render.validate_environment(env)

if __name__ == '__main__':
    unittest.main()
