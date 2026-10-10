#!/usr/bin/env python3
"""Prepare a pinned isolated attempt; execution is explicitly opt-in."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import time

PIN = '9baf16501c77a5a30bf87c50c2d3693a14986ae92d253731cda5370953e6d2ed'

def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--case', choices=['scalar-pages', 'map-diagnostic', 'dynamic-counts', 'dynamic-map-counts', 'dynamic-map-cdc'], required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--sql-testing', type=Path, required=True)
    parser.add_argument('--execute', action='store_true', help='root-owned execution; default only prepares')
    args = parser.parse_args()
    binary = args.sql_testing.resolve()
    if sha(binary) != PIN:
        parser.error('SQL ELF differs from explicitly approved pin ' + PIN)
    root = Path(__file__).resolve().parents[3]
    assets = root / 'deploy/streamr-native/session-projection'
    out = args.evidence_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    source = assets / 'closed-sessions.jsonl'
    (out / 'input.jsonl').write_bytes(source.read_bytes())
    sql = (assets / (args.case + '.sql')).read_text().replace('@INPUT@', str(out / 'input.jsonl')).replace('@OUTPUT@', str(out / 'output.jsonl'))
    (out / 'query.sql').write_text(sql)
    env = {k: v for k, v in os.environ.items() if not k.startswith('STREAMR_')}
    cleared = sorted(k for k in os.environ if k.startswith('STREAMR_'))
    overrides = {'STREAMR_TEST_NATIVE_WINDOWS': '1', 'STREAMR_TEST_TYPED_SQL': '1', 'STREAMR_TEST_BACKEND': 'memory', 'STREAMR_TEST_CHECKPOINT_MODE': 'controller', 'STREAMR_TEST_SOURCE_BATCH_ROWS': '1', 'STREAMR_TEST_EXECUTION_BYTES': '16777216', 'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT': '3', 'STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS': '3', 'STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS': '0', 'STREAMR_CAPTURE_EXPECTED_ROWS': '3', 'STREAMR_CAPTURE_CHECKPOINT_EPOCH': '1', 'STREAMR_CAPTURE_QUERY': str(out / 'query.sql'), 'STREAMR_CAPTURE_OUTPUT': str(out / 'output.jsonl'), 'TMPDIR': str(out), 'STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS': '180'}
    if args.case in {'dynamic-counts', 'dynamic-map-counts', 'dynamic-map-cdc'}:
        # COUNT and STRING_AGG are two independent native aggregate owners.
        overrides.update({'STREAMR_TEST_NATIVE_AGGREGATES': '1',
                          'STREAMR_TEST_MAX_OPEN_DATABASES': '2',
                          'STREAMR_TEST_MAX_SNAPSHOTS': '2'})
    checkpoint_expected = 0 if args.case in {'dynamic-counts', 'dynamic-map-counts', 'dynamic-map-cdc'} else 3
    if args.case == 'dynamic-map-cdc':
        checkpoint_expected = 1
        overrides['STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT'] = '1'
    source_prefix_rows = int(overrides['STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT'])
    overrides['STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS'] = str(checkpoint_expected)
    env.update(overrides)
    argv = [str(binary), '--ignored', '--exact', 'smoke_tests::external_sql_checkpoint_capture', '--nocapture', '--test-threads=1']
    receipt = {'case': args.case, 'argv': argv, 'env': overrides, 'cleared_inherited_streamr_keys': cleared, 'binary_sha256': PIN, 'query_sha256': sha(out / 'query.sql'), 'input_sha256': sha(source), 'outer_timeout_seconds': 240, 'expected': {'source_prefix_rows': source_prefix_rows, 'initial_rows': 3, 'checkpoint_prefix_rows': checkpoint_expected, 'recovered_rows': 3, 'full12_comparison': args.case in {'dynamic-counts', 'dynamic-map-counts', 'dynamic-map-cdc'}}, 'status': 'prepared only'}
    (out / 'command.json').write_text(json.dumps(receipt, indent=2) + '\n')
    # Re-enter this runner in a fresh directory so execution cannot bypass safety.
    cmd = [sys.executable, str(Path(__file__).resolve()), '--case', args.case, '--evidence-dir', str(out) + '-executed', '--sql-testing', str(binary), '--execute']
    (out / 'run.sh').write_text('#!/bin/sh\nset -eu\nexec ' + shlex.join(cmd) + '\n')
    if not args.execute:
        print(out / 'run.sh')
        return
    result = {'case': args.case, 'status': 'FAILED', 'full12_qualified': False, 'started_at_unix': time.time(), 'binary_sha256': PIN}
    try:
        # Recheck immediately before process launch.
        assert sha(binary) == PIN, 'binary changed after preparation'
        with (out / 'capture.log').open('w') as log:
            process = subprocess.run(argv, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=240)
        result['returncode'] = process.returncode
        assert process.returncode == 0, 'capture process failed'
        log = (out / 'capture.log').read_text()
        assert len(re.findall(r'test result: ok\. 1 passed; 0 failed;', log)) == 1, 'expected exactly one passed test'
        assert re.search(r'CAPTURE_RESULT phase=initial rows=3 ', log), 'missing initial capture proof'
        assert re.search(r'CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=' + str(source_prefix_rows) + r' committed_rows=' + str(checkpoint_expected) + r' rows=3 ', log), 'missing recovery/source-prefix proof'
        assert 'CAPTURE_CHECKPOINT path=' in log, 'missing selected checkpoint proof'
        prefix = out / 'output.checkpoint-1.jsonl'
        assert prefix.exists(), 'missing checkpoint committed output prefix'
        prefix_bytes = prefix.read_bytes()
        prefix_rows = [json.loads(line) for line in prefix.read_text().splitlines() if line.strip()]
        assert len(prefix_rows) == checkpoint_expected, 'wrong exact committed prefix cardinality'
        recovered_bytes = (out / 'output.jsonl').read_bytes()
        assert recovered_bytes.startswith(prefix_bytes), 'recovery rewrote committed prefix bytes'
        if checkpoint_expected == 3:
            assert recovered_bytes == prefix_bytes, 'recovery added output despite complete3-row checkpoint'
            assert (out / 'output.initial.jsonl').read_bytes() == prefix_bytes, 'checkpoint values differ from initial completed projection'
        if args.case == 'dynamic-map-cdc':
            check = subprocess.run([sys.executable, str(Path(__file__).with_name('compare_cdc.py')), str(prefix), '--prefix'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15)
            (out / 'checkpoint-prefix.comparison.log').write_text(check.stdout)
            assert check.returncode == 0, 'committed first closure differs from independent oracle'
        assert sha(out / 'input.jsonl') == receipt['input_sha256'], 'immutable input changed'
        result['source_prefix_rows'] = source_prefix_rows
        result['checkpoint_prefix_rows'] = checkpoint_expected
        result['checkpoint_prefix_sha256'] = sha(prefix)
        result['checkpoint_prefix_bytes_preserved'] = True
        result['output_files'] = {}
        for name in ['output.initial.jsonl', 'output.jsonl']:
            path = out / name
            rows = [json.loads(s) for s in path.read_text().splitlines() if s.strip()]
            assert len(rows) == 3, 'complete output must contain exactly3 rows'
            result['output_files'][name] = {'sha256': sha(path), 'rows': len(rows)}
            if args.case in {'dynamic-counts', 'dynamic-map-counts', 'dynamic-map-cdc'}:
                check = subprocess.run([sys.executable, str(Path(__file__).with_name('compare_cdc.py' if args.case == 'dynamic-map-cdc' else 'compare.py')), str(path)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15)
                (out / (name + '.comparison.log')).write_text(check.stdout)
                assert check.returncode == 0, 'full12 comparison failed: ' + name
            elif args.case == 'scalar-pages':
                expected = [json.loads(s) for s in (Path(__file__).with_name('expected.jsonl')).read_text().splitlines()]
                raw = [json.loads(s) for s in source.read_text().splitlines()]
                want = [{**e, 'event_types': r['event_types']} for e, r in zip(expected, raw)]
                assert sorted(json.dumps(r, sort_keys=True) for r in rows) == sorted(json.dumps(r, sort_keys=True) for r in want), 'scalar diagnostic fields differ'
            else:
                want = [{'session_id': r['session_id'], 'diagnostic_map': {'diagnostic': 1}} for r in [json.loads(s) for s in source.read_text().splitlines()]]
                assert sorted(json.dumps(r, sort_keys=True) for r in rows) == sorted(json.dumps(r, sort_keys=True) for r in want), 'map diagnostic output differs'
        result['status'] = 'PASSED'
        result['full12_qualified'] = args.case in {'dynamic-counts', 'dynamic-map-counts', 'dynamic-map-cdc'}
        result['qualification_scope'] = 'finite immutable fixture values and operator recovery only; no stateless/resource/Kafka transaction claim'
    except Exception as error:
        result['error'] = str(error)
    finally:
        result['finished_at_unix'] = time.time()
        (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))
    if result['status'] != 'PASSED':
        raise SystemExit(1)

if __name__ == '__main__':
    main()
