#!/usr/bin/env python3
"""Build a source-only test deployment bundle. Makes no network or service calls."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess

HERE = Path(__file__).resolve().parent
IMAGES = ('STREAMR_IMAGE', 'FLAREDB_IMAGE', 'QUERY_API_IMAGE', 'DASHBOARD_IMAGE', 'PYTHON_IMAGE', 'GATEWAY_IMAGE')


def validate_environment(env):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', env.get('NATIVE_NAMESPACE', '')):
        raise ValueError('NATIVE_NAMESPACE must be a fresh 1..63-character lowercase namespace')
    for name in IMAGES:
        if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', env.get(name, '')):
            raise ValueError(name + ' requires an explicit sha256 image digest')
    for name in ('KAFKA_BROKERS', 'RAW_EVENTS_TOPIC'):
        if not env.get(name):
            raise ValueError(name + ' is required')
    port = int(env.get('NATIVE_TEST_PORT', '0'))
    if not 1024 <= port <= 65535:
        raise ValueError('NATIVE_TEST_PORT must be an unprivileged dedicated port')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, default=HERE.parents[1],
                        help='checkout containing the maintained native SQL/schema renderer')
    parser.add_argument('--output', type=Path, default=HERE / 'bundle')
    args = parser.parse_args()
    validate_environment(os.environ)
    bundle = args.output
    bundle.mkdir(exist_ok=False)
    resources = json.loads((args.source_root / 'deploy/streamr-native/runtime-resources.json').read_text())
    config = {
        'api': {'bind-address': '0.0.0.0', 'http-port': 8000, 'run-http-port': 8000},
        'controller': {'bind-address': '127.0.0.1', 'scheduler': 'process'},
        'hostname': '127.0.0.1', 'job-controller': 'controller',
        'database': {'type': 'sqlite', 'sqlite': {'path': '/state/catalog.sqlite'}},
        'checkpoint-url': '/state/checkpoints', 'default-checkpoint-interval': '2s',
        'disable-telemetry': True,
        'process-scheduler': {'shutdown-with-controller': True, 'slots-per-process': 64},
        **resources,
    }
    config['worker']['bind-address'] = '127.0.0.1'
    config['worker']['disk-sql-state']['directory'] = '/state/rocksdb'
    (bundle / 'streamr.json').write_text(json.dumps(config, indent=2) + '\n')
    subprocess.run(['python3', str(args.source_root / 'deploy/streamr-native/prepare.py'),
                    '--output', str(bundle / 'queries'), '--brokers', os.environ['KAFKA_BROKERS'],
                    '--namespace', os.environ['NATIVE_NAMESPACE'], '--profile-release',
                    '--raw-events-topic', os.environ['RAW_EVENTS_TOPIC']], check=True)
    tables = bundle / 'flaredb'
    tables.mkdir()
    # Current maintained prepare.py owns the reviewed flat22 profile schema.
    profile = json.loads((bundle / 'queries/flaredb/profiles.json').read_text())
    (tables / 'profiles.json').write_text(json.dumps(profile, indent=2) + '\n')
    for table, suffix in [('events', 'unified-events'), ('sessions', 'sessions')]:
        definition = json.loads((args.source_root / 'deploy/config/flaredb-tables' / (table + '.json')).read_text())
        definition['ingestionConfig'] = {'topic': os.environ['NATIVE_NAMESPACE'] + '-' + suffix,
                                         'brokers': os.environ['KAFKA_BROKERS']}
        (tables / (table + '.json')).write_text(json.dumps(definition, indent=2) + '\n')
    (bundle / 'images.json').write_text(json.dumps({k: os.environ[k] for k in IMAGES}, indent=2) + '\n')
    sources = [HERE / name for name in ('render.py', 'bootstrap.py', 'compose.yaml', 'gateway.conf')]
    sources += [args.source_root / 'deploy/streamr-native/runtime-resources.json']
    sources += [args.source_root / 'deploy/config/flaredb-tables' / (table + '.json')
                for table in ('events', 'sessions')]
    receipt = {
        'status': 'rendered; runtime validation pending',
        'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.source_root, text=True).strip(),
        'source_files_sha256': {str(path.resolve()): hashlib.sha256(path.read_bytes()).hexdigest() for path in sources},
        'rendered_files_sha256': {str(path.relative_to(bundle)): hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in sorted(bundle.rglob('*')) if path.is_file()},
    }
    (bundle / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')


if __name__ == '__main__':
    main()
