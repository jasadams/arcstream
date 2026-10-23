import importlib.util
import json
import os
import re
import subprocess
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
                    return {'data': payloads, 'hasMore': False}
                raise AssertionError('must not create existing or conflicting graph')
            self.assertEqual(bootstrap.bootstrap(directory, request), payloads)
            payloads[-1]['query'] = 'conflict'
            with self.assertRaises(ValueError):
                bootstrap.bootstrap(directory, request)

    def test_current_wire_pagination_keeps_snake_case_cursor_and_payloads(self):
        bootstrap = load('bootstrap')
        calls = []
        with tempfile.TemporaryDirectory() as directory:
            payloads = [dict(id='p'+str(i), name=stage, query=stage, udfs=[],
                             checkpoint_interval_micros=2)
                        for i, stage in enumerate(bootstrap.STAGES)]
            for stage, payload in zip(bootstrap.STAGES, payloads):
                Path(directory, stage+'-request.json').write_text(json.dumps(payload))
            def request(path, payload=None):
                calls.append(path)
                if path.endswith('validate_query'):
                    return {'errors': []}
                if path == 'pipelines?limit=100':
                    page = json.loads((HERE/'fixtures/streamr-empty-pipelines.json').read_text())
                    page.update(data=payloads[:2], hasMore=True)
                    return page
                if path == 'pipelines?limit=100&starting_after=p1':
                    return {'data': payloads[2:], 'hasMore': False}
                raise AssertionError('unexpected API request: '+path)
            self.assertEqual(bootstrap.bootstrap(directory, request), payloads)
        self.assertEqual(calls[-2:], ['pipelines?limit=100',
                                     'pipelines?limit=100&starting_after=p1'])

    def test_rendered_streamr_ports_override_the_native_image_default(self):
        render = load('render')
        env = {**os.environ, **{key: 'registry/image@sha256:' + 'a'*64 for key in render.IMAGES},
               'NATIVE_NAMESPACE': 'native-test-ports', 'KAFKA_BROKERS': 'broker:9092',
               'RAW_EVENTS_TOPIC': 'raw-events', 'NATIVE_TEST_PORT': '18080'}
        rendered = subprocess.check_output(
            ['docker', 'compose', '-f', str(HERE/'compose.yaml'), 'config'], env=env, text=True)
        # Inspect the actual provider's normalized service; both Docker Compose
        # and the maintained Podman provider render YAML on this interface.
        section = re.search(r'(?ms)^  streamr:\n(.*?)(?=^  [a-zA-Z_-]+:|^volumes:|\Z)', rendered).group(1)
        self.assertRegex(section, r'''(?m)^\s+ARROYO__API__HTTP_PORT: ['"]?8000['"]?$''')
        self.assertRegex(section, r'''(?m)^\s+ARROYO__API__RUN_HTTP_PORT: ['"]?8000['"]?$''')
        self.assertNotIn('ARROYO__COMPILER__', section)

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
