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
    parser.add_argument('--case', choices=['full-session-plain', 'full-session-cdc', 'full-session-windowed-plain'], required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    parser.add_argument('--sql-testing', type=Path, required=True)
    parser.add_argument('--executable-receipt', type=Path, help='JSON with binary_sha256 and full source_commit; required for a new executable pin')
    parser.add_argument('--source-commit', help='expected full source commit; required together with --executable-receipt')
    parser.add_argument('--backend', choices=['memory','rocksdb'], default='memory')
    parser.add_argument('--checkpoint-mode', choices=['controller','leader'], default='controller')
    parser.add_argument('--source-batch-rows', type=int, choices=[1,8], default=1)
    parser.add_argument('--execute', action='store_true', help='root-owned execution; default only prepares')
    args = parser.parse_args()
    binary = args.sql_testing.resolve()
    expected_pin = PIN
    executable_receipt = None
    executable_receipt_bytes = None
    executable_receipt_path = None
    source_commit = None
    if (args.executable_receipt is None) != (args.source_commit is None):
        parser.error('--executable-receipt and --source-commit must be supplied together')
    if args.executable_receipt is not None:
        def unique_pairs(items):
            result = {}
            for key, value in items:
                if key in result:
                    raise ValueError('duplicate receipt key: ' + key)
                result[key] = value
            return result
        try:
            executable_receipt_path = args.executable_receipt.resolve()
            executable_receipt_bytes = executable_receipt_path.read_bytes()
            executable_receipt = json.loads(executable_receipt_bytes, object_pairs_hook=unique_pairs)
            if not isinstance(executable_receipt, dict):
                raise ValueError('receipt must be a JSON object')
            expected_pin = executable_receipt['binary_sha256']
            source_commit = executable_receipt['source_commit']
            if not isinstance(expected_pin, str) or re.fullmatch(r'[0-9a-f]{64}', expected_pin) is None:
                raise ValueError('binary_sha256 must be 64 lowercase hexadecimal characters')
            if not isinstance(source_commit, str) or re.fullmatch(r'[0-9a-f]{40}', source_commit) is None:
                raise ValueError('source_commit must be the full 40 lowercase hexadecimal commit')
            if source_commit != args.source_commit:
                raise ValueError('receipt source_commit differs from --source-commit')
        except (OSError, ValueError, KeyError, TypeError) as error:
            parser.error('invalid executable receipt: ' + str(error))
    if sha(binary) != expected_pin:
        parser.error('SQL ELF differs from expected pin ' + expected_pin)
    root = Path(__file__).resolve().parents[3]
    assets = root / 'deploy/streamr-native/session-full-cdc'
    out = args.evidence_dir.resolve()
    out.mkdir(parents=True, exist_ok=False)
    receipt_provenance = None
    if executable_receipt_bytes is not None:
        (out / 'executable-receipt.json').write_bytes(executable_receipt_bytes)
        receipt_provenance = {'supplied_path': str(executable_receipt_path),
                              'sha256': hashlib.sha256(executable_receipt_bytes).hexdigest(),
                              'preserved_path': str(out / 'executable-receipt.json')}
    source = assets / 'input.jsonl'
    (out / 'input.jsonl').write_bytes(source.read_bytes())
    sql = (assets / (args.case + '.sql')).read_text().replace('@INPUT@', str(out / 'input.jsonl')).replace('@OUTPUT@', str(out / 'output.jsonl'))
    (out / 'query.sql').write_text(sql)
    env = {k: v for k, v in os.environ.items() if not k.startswith(('STREAMR_', 'ARROYO__'))}
    cleared = sorted(k for k in os.environ if k.startswith(('STREAMR_', 'ARROYO__')))
    overrides = {'STREAMR_TEST_NATIVE_WINDOWS': '1', 'STREAMR_TEST_TYPED_SQL': '1', 'STREAMR_TEST_BACKEND': args.backend, 'STREAMR_TEST_CHECKPOINT_MODE': args.checkpoint_mode, 'STREAMR_TEST_SOURCE_BATCH_ROWS': str(args.source_batch_rows), 'STREAMR_TEST_EXECUTION_BYTES': '16777216', 'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT': '3', 'STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS': '3', 'STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS': '0', 'STREAMR_CAPTURE_EXPECTED_ROWS': '3', 'STREAMR_CAPTURE_CHECKPOINT_EPOCH': '1', 'STREAMR_CAPTURE_QUERY': str(out / 'query.sql'), 'STREAMR_CAPTURE_OUTPUT': str(out / 'output.jsonl'), 'TMPDIR': str(out), 'STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS': '180'}
    # SESSION plus two downstream aggregates; actual planner receipts decide kinds.
    overrides.update({'STREAMR_TEST_NATIVE_AGGREGATES': '1',
                      'STREAMR_TEST_MAX_OPEN_DATABASES': '3',
                      'STREAMR_TEST_MAX_SNAPSHOTS': '3', 'STREAMR_TEST_SCAN_PAGE_BYTES': '4194304'})
    checkpoint_expected = 0 if args.source_batch_rows == 8 else 1
    overrides['STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT'] = '11'
    source_prefix_rows = int(overrides['STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT'])
    overrides['STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS'] = str(checkpoint_expected)
    env.update(overrides)
    argv = [str(binary), '--ignored', '--exact', 'smoke_tests::external_sql_checkpoint_capture', '--nocapture', '--test-threads=1']
    receipt = {'case': args.case, 'backend': args.backend, 'checkpoint_mode': args.checkpoint_mode, 'source_batch_rows': args.source_batch_rows, 'argv': argv, 'env': overrides, 'cleared_inherited_streamr_arroyo_keys': cleared, 'binary_sha256': expected_pin, 'source_commit': source_commit, 'executable_receipt': receipt_provenance, 'query_sha256': sha(out / 'query.sql'), 'input_sha256': sha(source), 'outer_timeout_seconds': 240, 'expected': {'source_prefix_rows': source_prefix_rows, 'initial_rows': 3, 'checkpoint_prefix_rows': checkpoint_expected, 'recovered_rows': 3, 'full12_comparison': True}, 'status': 'prepared only'}
    (out / 'command.json').write_text(json.dumps(receipt, indent=2) + '\n')
    # Re-enter this runner in a fresh directory so execution cannot bypass safety.
    cmd = [sys.executable, str(Path(__file__).resolve()), '--case', args.case, '--evidence-dir', str(out) + '-executed', '--sql-testing', str(binary), '--backend', args.backend, '--checkpoint-mode', args.checkpoint_mode, '--source-batch-rows', str(args.source_batch_rows), '--execute']
    if source_commit is not None:
        cmd.extend(['--executable-receipt', str(out / 'executable-receipt.json'), '--source-commit', source_commit])
    (out / 'run.sh').write_text('#!/bin/sh\nset -eu\nexec ' + shlex.join(cmd) + '\n')
    if not args.execute:
        print(out / 'run.sh')
        return
    result = {'case': args.case, 'status': 'FAILED', 'full12_qualified': False, 'backend': args.backend, 'checkpoint_mode': args.checkpoint_mode, 'source_batch_rows': args.source_batch_rows, 'started_at_unix': time.time(), 'binary_sha256': expected_pin, 'source_commit': source_commit, 'executable_receipt': receipt_provenance}
    try:
        # Recheck immediately before process launch.
        assert sha(binary) == expected_pin, 'binary changed after preparation'
        if executable_receipt_path is not None:
            assert sha(executable_receipt_path) == receipt_provenance['sha256'], 'executable receipt changed after preparation'
        with (out / 'capture.log').open('w') as log:
            process = subprocess.run(argv, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=240)
        result['returncode'] = process.returncode
        assert process.returncode == 0, 'capture process failed'
        log = (out / 'capture.log').read_text()
        assert len(re.findall(r'test result: ok\. 1 passed; 0 failed;', log)) == 1, 'expected exactly one passed test'
        assert re.search(r'CAPTURE_RESULT phase=initial rows=3 ', log), 'missing initial capture proof'
        assert re.search(r'CAPTURE_RESULT phase=recovered checkpoint=1 input_rows_before_checkpoint=' + str(source_prefix_rows) + r' committed_rows=' + str(checkpoint_expected) + r' rows=3 ', log), 'missing recovery/source-prefix proof'
        assert f'CAPTURE_CONFIG backend={args.backend} checkpoint_mode={args.checkpoint_mode} ' in log, 'configuration receipt differs'
        metadata_type = 'CheckpointManifest' if args.checkpoint_mode == 'leader' else 'CheckpointMetadata'
        assert re.search(r'CAPTURE_CHECKPOINT path=.+ metadata=' + metadata_type + r' \{.*epoch: 1', log), 'missing selected protocol checkpoint proof'
        if args.case == 'full-session-windowed-plain':
            operators = re.findall(r'CAPTURE_OPERATOR .* kind=(\w+) parallelism=1', log)
            assert operators.count('SessionWindowAggregate') == 1 and operators.count('TumblingWindowAggregate') == 2, 'native window operator inventory differs'
            assert 'UpdatingAggregate' not in operators, 'unexpected retained updating aggregate'
            result['native_window_owners'] = 3
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
        if checkpoint_expected == 0:
            assert prefix_bytes == b'', 'zero-row committed prefix must have exactly zero bytes'
        else:
            check = subprocess.run([sys.executable, str(Path(__file__).with_name('compare_cdc.py' if args.case == 'full-session-cdc' else 'compare.py')), str(prefix), '--prefix'], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15)
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
            check = subprocess.run([sys.executable, str(Path(__file__).with_name('compare_cdc.py' if args.case == 'full-session-cdc' else 'compare.py')), str(path)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=15)
            (out / (name + '.comparison.log')).write_text(check.stdout)
            assert check.returncode == 0, 'full12 comparison failed: ' + name
        result['status'] = 'PASSED'
        result['full12_qualified'] = True
        result['qualification_scope'] = 'native SESSION source fixture plus full12 projection and operator recovery only; actual planner receipts decide downstream kinds; no production Kafka transaction claim'
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
