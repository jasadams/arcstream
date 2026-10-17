#!/usr/bin/env python3
"""Real production CLI identity recovery; invoke in a pinned compatible runtime.

Uses existing single_file wait_for_control: one fixture record advances per
checkpoint. The production worker is SIGKILLed only after event ten is durably
committed; the committed prefix must survive and a measured fresh suffix must follow recovery.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import signal
import shutil
import subprocess
import time
import urllib.error

from native_identity_capture import prepare
from streamr_capture import adapt
from compare_identity import compare, read_records


def load(path):
    spec = importlib.util.spec_from_file_location('engine_recovery', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def proof(h, case, epoch, owner):
    """Decode real controller metadata and source/sink committed global state."""
    import pyarrow.parquet as parquet
    base = case/'checkpoints'/owner['job']/'checkpoints'/f'checkpoint-{epoch:07}'
    cp = h.wire((base/'metadata').read_bytes())
    h.require(h.one(cp, 1) == owner['job'].encode() and h.one(cp, 2) == epoch,
              'checkpoint job/epoch differs')
    artifacts, globals_, typed = [], {}, []
    for name in cp.get(6, []):
        op = h.wire((base/f'operator-{name.decode()}'/'metadata').read_bytes())
        for table, encoded in h.pairs(op, 13).items():
            state = h.wire(encoded)
            kind, payload = h.one(state, 1, 0), h.wire(h.one(state, 2, b''))
            if kind == 4:
                subtasks = h.pairs(payload, 2, numeric=True)
                h.require(set(subtasks) == {0}, 'typed state checkpoint ownership differs')
                subtask = h.wire(subtasks[0])
                h.require(h.one(subtask, 7, 0) == epoch, 'typed state checkpoint epoch differs')
                files = []
                for encoded_file in subtask.get(9, []):
                    descriptor = h.wire(encoded_file)
                    relative = h.one(descriptor, 1).decode()
                    path = case/'checkpoints'/relative
                    digest = h.sha(path)
                    h.require(bytes.fromhex(digest) == h.one(descriptor, 3), 'typed checkpoint checksum differs')
                    h.require(path.stat().st_size == h.one(descriptor, 2), 'typed checkpoint size differs')
                    artifacts.append(dict(path=relative, sha256=digest))
                    files.append(relative)
                typed.append(dict(operator=name.decode(), table=table.decode(), kind=kind, files=files))
                continue
            h.require(kind == 1, 'unexpected checkpoint table kind')
            if table != b'f':
                continue
            for relative in payload.get(1, []):
                path = case/'checkpoints'/relative.decode()
                artifacts.append(dict(path=relative.decode(), sha256=h.sha(path)))
                values = parquet.read_table(path, columns=['key', 'value']).to_pylist()
                for row in values:
                    size, pos = h.bincode_uint(row['key'])
                    key = row['key'][pos:].decode()
                    h.require(size == len(row['key'])-pos, 'global key encoding differs')
                    value, end = h.bincode_uint(row['value'])
                    h.require(end == len(row['value']), 'global value encoding differs')
                    h.require(key not in globals_, 'duplicate source/sink key')
                    globals_[key] = value
    source = globals_.get(str(case/'input.jsonl'))
    offset = globals_.get(str(case/'streamr-internal.jsonl'))
    h.require(source is not None and offset is not None and len(typed) == 2, 'missing checkpoint owners')
    data = (case/'streamr-internal.jsonl').read_bytes()[:offset]
    h.require(len(data) == offset and data.endswith(b'\n'), 'committed sink prefix unavailable')
    records = [json.loads(line) for line in data.splitlines()]
    h.require(len(records) == source, 'source line state and committed sink cardinality differ')
    return dict(epoch=epoch, source_lines=source, sink_offset=offset,
                typed_owners=typed, artifacts=artifacts), data


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--case', required=True, type=Path)
    p.add_argument('--engine-helper', required=True, type=Path)
    p.add_argument('--binary', required=True, type=Path)
    p.add_argument('--config-template', required=True, type=Path)
    p.add_argument('--backend', choices=['memory', 'rocksdb'], default='rocksdb')
    p.add_argument('--port', type=int, default=18417)
    p.add_argument('--timeout', type=int, default=240)
    a = p.parse_args()
    h = load(a.engine_helper)
    case = a.case.resolve()
    h.require(not case.exists(), 'case must be new')
    prepare(Path(__file__).resolve().parents[2]/'flink/identity-resolution/src/test/resources/reference/identity-input.json', case, case)
    config = json.loads(a.config_template.read_text())
    config['checkpoint-url'] = str(case/'checkpoints')
    config['database']['sqlite']['path'] = str(case/'catalog.sqlite')
    config['default-checkpoint-interval'] = '3s'
    config['api'].update({'http-port': a.port, 'run-http-port': a.port})
    config['worker']['sql-state-backend'] = a.backend
    config['worker']['disk-sql-state']['directory'] = str(case/'live-state')
    h.write(case/'config.yaml', config)
    expected = Path(__file__).resolve().parents[2]/'flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl'
    h.write(case/'input-provenance.json', dict(files={str(path): h.sha(path) for path in
        (a.engine_helper, a.config_template, case/'config.yaml', case/'query.sql',
         case/'input.jsonl', expected, Path(__file__))}, binary_sha256=h.sha(a.binary.resolve())))
    command = [str(a.binary), '--config', str(case/'config.yaml'), 'run', '--name',
               'arc17-native-identity', '--parallelism', '1', '--state-dir',
               str(case/'checkpoints'), str(case/'query.sql')]
    h.enable_subreaper()
    result = dict(status='failed', command=command, backend=a.backend,
                  binary_sha256=h.sha(a.binary.resolve()), harness_sha256=h.sha(Path(__file__)))
    owned, timeline, fault, replacement, committed = {}, [], None, None, None
    started = time.monotonic()
    with (case/'runtime.log').open('wb') as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                 start_new_session=True, cwd=case)
        try:
            while time.monotonic()-started < a.timeout:
                ps = h.session_processes(child.pid)
                owned.update({h.identity(x): x for x in ps})
                workers = [w for x in ps if (w := h.worker(x, a.binary.resolve()))]
                try:
                    pipelines = h.api(a.port, 'pipelines')['data']
                    if not pipelines:
                        time.sleep(.15)
                        continue
                    pipeline = pipelines[0]
                    jobs = h.api(a.port, f"pipelines/{pipeline['id']}/jobs")['data']
                    if not jobs:
                        time.sleep(.15)
                        continue
                    job = jobs[0]
                    timeline.append(dict(elapsed=time.monotonic()-started, job=job, workers=workers))
                    h.require(job['state'] != 'Failed', 'production job failed')
                    if fault is None and job['state'] == 'Running' and len(workers) == 1:
                        checkpoints = h.api(a.port, f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints")['data']
                        completed = sorted((x for x in checkpoints if x['finish_time'] is not None), key=lambda x:x['epoch'], reverse=True)
                        if completed:
                            selected, data = proof(h, case, completed[0]['epoch'], workers[0])
                            if selected['source_lines'] == 10:
                                shutil.copytree(case/'checkpoints'/workers[0]['job']/'checkpoints'/f"checkpoint-{selected['epoch']:07}", case/'selected-checkpoint-metadata')
                                for artifact in selected['artifacts']:
                                    destination = case/'selected-checkpoint-objects'/artifact['path']
                                    destination.parent.mkdir(parents=True, exist_ok=True)
                                    shutil.copy2(case/'checkpoints'/artifact['path'], destination)
                                h.write(case/'checkpoint-before-loss.json', selected)
                                (case/'committed-prefix.jsonl').write_bytes(data)
                                committed = data
                                target = workers[0]
                                h.require(h.terminate_identity(target, signal.SIGSTOP), 'worker exited before snapshot pause')
                                pause_deadline = time.monotonic()+2
                                while time.monotonic() < pause_deadline:
                                    stopped = h.process(target['pid'])
                                    if stopped and h.identity(stopped) == h.identity(target) and stopped['state'] == 'T':
                                        break
                                    time.sleep(.01)
                                else:
                                    raise ValueError('worker did not stop before physical output snapshot')
                                physical = (case/'streamr-internal.jsonl').read_bytes()
                                (case/'physical-before-loss.jsonl').write_bytes(physical)
                                prekill_rows = [json.loads(line) for line in physical[:physical.rfind(b'\n')+1].splitlines()]
                                h.require(len(prekill_rows) < 13, 'no physically fresh suffix remains before fault')
                                target = workers[0]
                                h.require(h.terminate_identity(target, signal.SIGKILL), 'worker exited before SIGKILL')
                                fault = dict(worker=target, checkpoint=selected, elapsed=time.monotonic()-started, physical_before_loss_event_ids=[row['event_id'] for row in prekill_rows], physical_before_loss_sha256=h.sha(case/'physical-before-loss.jsonl'))
                                h.write(case/'fault.json', fault)
                            h.require(selected['source_lines'] <= 10, 'missed event-ten fault boundary')
                    if fault:
                        candidates = [w for w in workers if h.identity(w) != h.identity(fault['worker']) and w['job'] == fault['worker']['job'] and w['pipeline'] == fault['worker']['pipeline'] and w['generation'] > fault['worker']['generation']]
                        if candidates:
                            replacement = candidates[0]
                        if job['state'] == 'Finished':
                            h.require(replacement is not None, 'no observed recovered worker generation')
                            output = case/'streamr-internal.jsonl'
                            h.require(output.read_bytes().startswith(committed), 'committed canonical IDs/output bytes changed')
                            rows = [json.loads(line) for line in output.read_text().splitlines()]
                            h.require(len(rows) == 13, 'full identity output cardinality differs')
                            prior_ids = set(fault['physical_before_loss_event_ids'])
                            fresh_ids = [row['event_id'] for row in rows if row['event_id'] not in prior_ids]
                            h.require(fresh_ids, 'no measured fresh recovered output')
                            actual = adapt(rows)
                            (case/'actual.jsonl').write_text(''.join(json.dumps(x, sort_keys=True)+'\n' for x in actual))
                            expected = Path(__file__).resolve().parents[2]/'flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl'
                            oracle = compare(read_records(expected), actual)
                            result.update(status='passed', fault=fault, replacement=replacement,
                                          oracle=oracle, fresh_recovered_events=len(fresh_ids), fresh_recovered_event_ids=fresh_ids,
                                          committed_prefix_sha256=h.sha(case/'committed-prefix.jsonl'))
                            break
                except (urllib.error.URLError, TimeoutError, ConnectionError):
                    pass
                h.require(child.poll() is None, 'top-level CLI exited prematurely')
                time.sleep(.15)
            h.require(result['status'] == 'passed', 'recovery deadline exceeded')
        except BaseException as error:
            result['error'] = f'{type(error).__name__}: {error}'
            raise
        finally:
            result['cleanup'] = h.cleanup(child, owned)
            if not result['cleanup']['complete']:
                result['status'] = 'failed'
            result['elapsed_seconds'] = time.monotonic()-started
            h.write(case/'timeline.json', timeline)
            h.write(case/'result.json', result)
    h.require(result['status'] == 'passed', 'cleanup failed')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
