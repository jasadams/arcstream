#!/usr/bin/env python3
"""Validate native SQL through the public API and actual worker-loss recovery.

No application UDFs, compiler setup, outbox adapters or timer feedback are used.
Run inside the qualified runtime image through the maintained shared queue.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import re
import signal
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def digest(path):
    with Path(path).open('rb') as source:
        return hashlib.file_digest(source, 'sha256').hexdigest()


def records_equal(actual, expected, unordered=False):
    """Compare complete records, retaining every duplicate and field."""
    actual = [json.dumps(row, sort_keys=True) for row in actual]
    expected = [json.dumps(row, sort_keys=True) for row in expected]
    return sorted(actual) == sorted(expected) if unordered else actual == expected


def strict_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON key: ' + key)
        result[key] = value
    return result


def decode_kafka_offset(h, key, value):
    def signed(data):
        raw, size = h.bincode_uint(data)
        return (raw >> 1) ^ -(raw & 1), size
    partition, key_size = signed(key)
    stored_partition, start = signed(value)
    offset, end = signed(value[start:])
    if partition != 0 or stored_partition != partition or key_size != len(key) or start + end != len(value) or offset < 0:
        raise ValueError('expected one complete partition-zero Kafka next-offset record')
    return offset


def check_cdc(data, expected, keys):
    """Check every complete image/chain, with exact expected multiplicity."""
    previous, images, traces = {}, [], {}
    for line in data.splitlines():
        row = json.loads(line, object_pairs_hook=strict_object)
        if set(row) != {'before', 'after', 'op'} or not isinstance(row['after'], dict):
            raise ValueError('expected complete create/update CDC envelope, no deletes')
        after, before = row['after'], row['before']
        key = tuple(json.dumps(after[field], sort_keys=True) for field in keys)
        wanted_before = previous.get(key)
        if not records_equal([before], [wanted_before]) or row['op'] != ('c' if wanted_before is None else 'u'):
            raise ValueError('CDC before/after chain differs')
        previous[key] = after
        images.append(after)
        traces.setdefault(key, []).append(after)
    # Intermediate inconsistent images are never discarded, even when a later
    # image happens to match. Sorting only removes cross-key ordering claims.
    if not records_equal(images, expected, unordered=True):
        raise ValueError('complete CDC image multiplicity differs')
    wanted_final = {}
    wanted_traces = {}
    for row in expected:
        key = tuple(json.dumps(row[field], sort_keys=True) for field in keys)
        wanted_final[key] = row
        wanted_traces.setdefault(key, []).append(row)
    if any(not records_equal(traces.get(key, []), rows) for key, rows in wanted_traces.items()):
        raise ValueError('per-key CDC postimage sequence differs')
    if not records_equal(list(previous.values()), list(wanted_final.values()), unordered=True):
        raise ValueError('CDC final values differ')
    return images


def validate_stages(stages, input_count, fault_prefix):
    prefixes = [stage['input_lines'] for stage in stages]
    if not prefixes or prefixes[0] != fault_prefix or prefixes[-1] != input_count or any(
            type(value) is not int or value <= 0 for value in prefixes) or any(
            left >= right for left, right in zip(prefixes, prefixes[1:])):
        raise ValueError('stages must increase from fault prefix through complete input')
    if not all(isinstance(stage.get('value'), dict) for stage in stages):
        raise ValueError('every stage requires an explicit complete metric value')


def request(port, path, payload=None, timeout=600, method=None):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{port}/api/v1/{path}', data=data,
                                 headers={'Content-Type': 'application/json'}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f'API {path}: HTTP {error.code}: {error.read().decode()}') from error


def checkpoint(h, case, owner, epoch, kafka=False):
    """Verify actual source/sink offsets and referenced typed artifacts."""
    import pyarrow.parquet as parquet
    base = case/'checkpoints'/owner['job']/'checkpoints'/f'checkpoint-{epoch:07}'
    cp = h.wire((base/'metadata').read_bytes())
    h.require(h.one(cp, 1) == owner['job'].encode() and h.one(cp, 2) == epoch,
              'checkpoint identity mismatch')
    globals_, artifacts, typed, windows, offsets = {}, [], [], [], []
    for name in cp.get(6, []):
        # Object-store checkpoint paths encode reserved operator-name characters
        # (for example SESSION's `SessionWindow<1800s>`).
        operator_path = urllib.parse.quote(name.decode(), safe='')
        op = h.wire((base/f'operator-{operator_path}'/'metadata').read_bytes())
        for table, encoded in h.pairs(op, 13).items():
            state = h.wire(encoded)
            kind, payload = h.one(state, 1, 0), h.wire(h.one(state, 2, b''))
            if kind in (3, 4):
                subs = h.pairs(payload, 2, numeric=True)
                h.require(set(subs) == {0}, 'typed ownership mismatch')
                sub = h.wire(subs[0]); h.require(h.one(sub, 7, 0) == epoch, 'typed epoch mismatch')
                files = []
                for item in sub.get(9, []):
                    descriptor = h.wire(item)
                    relative = urllib.parse.quote(h.one(descriptor, 1).decode(), safe='/')
                    path = case/'checkpoints'/relative
                    h.require(path.stat().st_size == h.one(descriptor, 2), 'artifact size mismatch')
                    h.require(bytes.fromhex(digest(path)) == h.one(descriptor, 3), 'artifact checksum mismatch')
                    artifacts.append(relative); files.append(relative)
                entry = {'operator': name.decode(), 'namespace': table.decode(), 'files': files}
                (windows if kind == 3 else typed).append(entry)
            elif kind == 1 and table in (b'f', b'k'):
                for relative in payload.get(1, []):
                    path = case/'checkpoints'/relative.decode(); artifacts.append(relative.decode())
                    for row in parquet.read_table(path, columns=['key', 'value']).to_pylist():
                        if table == b'k':
                            offsets.append(decode_kafka_offset(h, row['key'], row['value']))
                            continue
                        size, start = h.bincode_uint(row['key'])
                        key = row['key'][start:].decode()
                        h.require(size == len(row['key'])-start, 'global key mismatch')
                        value, end = h.bincode_uint(row['value'])
                        h.require(end == len(row['value']), 'global encoding mismatch')
                        h.require(key not in globals_, 'duplicate source/sink key')
                        globals_[key] = value
    source, offset = globals_.get(str(case/'input.jsonl')), globals_.get(str(case/'output.jsonl'))
    if kafka:
        h.require(len(offsets) <= 1, 'expected exactly one Kafka source partition owner')
        source = offsets[0] if offsets else 0
    h.require(source is not None and offset is not None, 'source/sink checkpoint missing')
    data = (case/'output.jsonl').read_bytes()[:offset]
    h.require(len(data) == offset and (not data or data.endswith(b'\n')), 'incomplete committed output')
    return {'epoch': epoch, 'source_lines': source, 'sink_offset': offset,
            'typed_owners': typed, 'window_owners': windows, 'artifacts': artifacts}, data


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--case', type=Path, required=True)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--engine-helper', type=Path, required=True)
    p.add_argument('--config-template', type=Path, required=True)
    p.add_argument('--query', type=Path, required=True)
    p.add_argument('--input', type=Path, required=True)
    p.add_argument('--expected', type=Path, required=True)
    p.add_argument('--binary-receipt', type=Path)
    p.add_argument('--backend', choices=['memory', 'rocksdb'], default='rocksdb')
    p.add_argument('--state-row-bytes',type=int)
    p.add_argument('--scan-page-bytes',type=int)
    p.add_argument('--typed-decoded-bytes',type=int)
    p.add_argument('--working-event-bytes',type=int)
    p.add_argument('--scope-bytes',type=int)
    p.add_argument('--typed-resident-bytes',type=int)
    p.add_argument('--typed-owners',type=int,default=0)
    p.add_argument('--window-owners',type=int,default=0)
    p.add_argument('--fault-prefix', type=int, default=2)
    p.add_argument('--require-committed-output', action='store_true')
    p.add_argument('--port', type=int, default=18419)
    p.add_argument('--timeout', type=int, default=1200)
    p.add_argument('--compile-only', action='store_true')
    p.add_argument('--unordered-output', action='store_true',
                   help='compare complete-record multiplicity when cross-key order is unspecified')
    p.add_argument('--kafka-rpk', type=Path)
    p.add_argument('--kafka-broker')
    p.add_argument('--kafka-topic')
    p.add_argument('--expected-prefixes', type=Path,
                   help='JSON stages: input_lines and complete expected CDC postimage value')
    p.add_argument('--cdc-keys', nargs='+', help='key fields of complete expected CDC images')
    a = p.parse_args()
    kafka = a.kafka_rpk is not None
    kafka_options = [a.kafka_rpk, a.kafka_broker, a.kafka_topic, a.expected_prefixes, a.cdc_keys]
    if any(kafka_options) and not all(kafka_options):
        p.error('Kafka fixture mode requires rpk, broker, topic, prefixes and CDC keys')
    stages = json.loads(a.expected_prefixes.read_text()) if kafka else []
    spec = importlib.util.spec_from_file_location('streamr_recovery', a.engine_helper)
    h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
    case = a.case.resolve(); case.mkdir(parents=True, exist_ok=False)
    shutil.copy2(Path(__file__),case/'supervisor.py')
    shutil.copy2(a.input, case/'input.jsonl'); shutil.copy2(a.expected, case/'expected.jsonl')
    input_lines = (case/'input.jsonl').read_text().splitlines()
    if kafka:
        h.require(not a.compile_only, 'Kafka fixture mode requires actual staged execution')
        validate_stages(stages, len(input_lines), a.fault_prefix)
        shutil.copy2(a.expected_prefixes, case/'expected-prefixes.json')
        expected_images = [json.loads(line, object_pairs_hook=strict_object) for line in (case/'expected.jsonl').read_text().splitlines()]
        h.require(records_equal([stage['value'] for stage in stages], expected_images), 'stage oracle differs from expected images')
    if not a.compile_only:
        h.require(0<a.fault_prefix<len((case/'input.jsonl').read_bytes().splitlines()),'fault prefix must leave a nonempty source suffix')
    query = a.query.read_text().replace('{{INPUT}}', str(case/'input.jsonl')).replace('{{OUTPUT}}', str(case/'output.jsonl'))
    if kafka:
        for name, value in [('BROKER', a.kafka_broker), ('INPUT_TOPIC', a.kafka_topic), ('GROUP_ID', case.name)]:
            query = query.replace('{{' + name + '}}', value.replace("'", "''"))
    (case/'query.sql').write_text(query)
    config = json.loads(a.config_template.read_text())
    config['checkpoint-url'] = str(case/'checkpoints')
    config['database']['sqlite']['path'] = str(case/'catalog.sqlite')
    config['default-checkpoint-interval'] = '3s'
    config['api'].update({'http-port': a.port, 'run-http-port': a.port})
    config['worker']['sql-state-backend'] = a.backend
    config['worker']['disk-sql-state']['directory'] = str(case/'live-state')
    if a.state_row_bytes is not None:
        h.require(a.state_row_bytes>0,'state row limit must be positive')
        config['worker']['typed-sql-state']['row-bytes']=a.state_row_bytes
        config['worker']['disk-sql-state']['max-row-bytes']=a.state_row_bytes
    if a.typed_decoded_bytes is not None:
        h.require(a.typed_decoded_bytes>0,'typed decode limit must be positive')
        config['worker']['typed-sql-state']['decoded-bytes']=a.typed_decoded_bytes
    if a.scan_page_bytes is not None:
        h.require(a.scan_page_bytes>0,'scan page pool must be positive')
        config['worker']['live-state-resources']['scan-page-bytes']=a.scan_page_bytes
    if a.working_event_bytes is not None:
        h.require(a.working_event_bytes>0,'working event limit must be positive')
        config['worker']['typed-sql-state']['max-working-event-bytes']=a.working_event_bytes
    if a.scope_bytes is not None:
        h.require(a.scope_bytes>0,'scope limit must be positive')
        config['worker']['typed-sql-state']['scope-bytes']=a.scope_bytes
    if a.typed_resident_bytes is not None:
        h.require(a.typed_resident_bytes>0,'typed resident limit must be positive')
        config['worker']['typed-sql-state']['max-resident-bytes']=a.typed_resident_bytes
    config.pop('compiler', None)
    h.write(case/'config.yaml', config)
    command = [str(a.binary.resolve()), '--config', str(case/'config.yaml'), 'cluster']
    engine_receipt=json.loads(a.binary_receipt.read_text()) if a.binary_receipt else None
    if engine_receipt:
        expected_hash=engine_receipt.get('binary_sha256', engine_receipt.get('production_binary_sha256'))
        h.require(expected_hash==digest(a.binary),'binary receipt mismatch')
    result = {'status':'failed','engine_receipt':engine_receipt,'command':command,'backend':a.backend,'unordered_output':a.unordered_output,
              'prepared_files':{str(case/name):digest(case/name) for name in ['config.yaml','query.sql','input.jsonl','expected.jsonl','supervisor.py']},
              'binary_sha256':digest(a.binary),
              'files':{str(path):digest(path) for path in [a.query,a.input,a.expected,a.engine_helper,Path(__file__)]}}
    if kafka:
        result.update(kafka_topic=a.kafka_topic, cdc_keys=a.cdc_keys,
                      stages_sha256=digest(a.expected_prefixes), kafka_commands=[])
    stage_index, produced, stop_requested = 0, 0, False
    def produce_stage():
        nonlocal produced
        end = stages[stage_index]['input_lines']
        argv = [str(a.kafka_rpk), 'topic', 'produce', a.kafka_topic, '-X', 'brokers=' + a.kafka_broker]
        output = subprocess.run(argv, input=''.join(line+'\n' for line in input_lines[produced:end]),
                                text=True, capture_output=True, timeout=30)
        result['kafka_commands'].append(dict(command=argv, exit=output.returncode, stdout=output.stdout, stderr=output.stderr,
                                             first_input_line=produced+1, last_input_line=end))
        h.write(case/'kafka-commands.json', result['kafka_commands'])
        h.require(output.returncode == 0, 'Kafka fixture production failed: ' + output.stderr)
        produced = end
    timeline, owned, prefix, fault, replacement, pipeline = [], {}, None, None, None, None
    started = time.monotonic(); h.enable_subreaper()
    clean = {k:v for k,v in os.environ.items() if not k.startswith(('STREAMR_TEST_','STREAMR_CAPTURE_','ARROYO__'))}
    clean['RUST_LOG'] = 'info'
    with (case/'runtime.log').open('wb') as log:
        child = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT, cwd=case,
                                 env=clean, start_new_session=True)
        try:
            while time.monotonic()-started < 90:
                h.require(child.poll() is None, 'cluster exited before API ready')
                try: request(a.port,'pipelines'); break
                except (urllib.error.URLError, RuntimeError, ConnectionError): time.sleep(.2)
            else: raise ValueError('API startup timeout')
            validation = request(a.port, 'pipelines/validate_query', {'query':query,'udfs':[]})
            h.write(case/'query-validation.json',validation)
            h.require(not validation['errors'],'query validation failed')
            payload = {'name':'arcstream-native-sql-validation', 'query':query,'udfs':[],'parallelism':1,
                       'checkpoint_interval_micros':3000000}
            h.write(case/'pipeline-request.json',payload)
            pipeline = request(a.port,'pipelines',payload)
            h.write(case/'pipeline-response.json',pipeline)
            if a.compile_only:
                result['status']='compiled'
            while not a.compile_only and time.monotonic()-started < a.timeout:
                ps = h.session_processes(child.pid); owned.update({h.identity(x):x for x in ps})
                workers = [w for x in ps if (w := h.worker(x,a.binary.resolve()))]
                jobs = request(a.port,f"pipelines/{pipeline['id']}/jobs",timeout=10)['data']
                if not jobs: time.sleep(.15); continue
                job=jobs[0]; timeline.append({'elapsed':time.monotonic()-started,'job':job,'workers':workers})
                h.require(job['state']!='Failed','pipeline failed')
                h.require(fault is not None or job['state']!='Finished','job finished before planned fault')
                h.require(fault is not None or job['state']!='Recovering','unexpected recovery before planned fault; inspect runtime error')
                if kafka and not produced and job['state']=='Running':
                    produce_stage()
                if not fault and job['state']=='Running' and len(workers)==1:
                    cps=request(a.port,f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints",timeout=10)['data']
                    completed=sorted((x for x in cps if x['finish_time'] is not None),key=lambda x:x['epoch'],reverse=True)
                    if completed:
                        selected,data=checkpoint(h,case,workers[0],completed[0]['epoch'],kafka=kafka)
                        h.require(len(selected['typed_owners'])==a.typed_owners,'typed owner inventory mismatch')
                        h.require(len(selected['window_owners'])==a.window_owners,'window owner inventory mismatch')
                        h.require(selected['source_lines']<=a.fault_prefix,'missed fault source prefix')
                        if selected['source_lines']==a.fault_prefix:
                            if kafka:
                                count = len(data.splitlines())
                                check_cdc(data, [stage['value'] for stage in stages[:min(count,1)]], a.cdc_keys)
                                if count < 1:
                                    time.sleep(.15)
                                    continue
                            checkpoint_dir=case/'checkpoints'/workers[0]['job']/'checkpoints'/f"checkpoint-{selected['epoch']:07}"
                            shutil.copytree(checkpoint_dir,case/'selected-checkpoint-metadata')
                            prefix=data; (case/'committed-prefix.jsonl').write_bytes(data)
                            if a.require_committed_output:
                                h.require(bool(prefix), 'selected checkpoint has no committed output')
                            target=workers[0]
                            h.require(h.terminate_identity(target,signal.SIGSTOP),'worker exited before pause')
                            pause_deadline=time.monotonic()+2
                            while time.monotonic()<pause_deadline:
                                stopped=h.process(target['pid'])
                                if stopped and h.identity(stopped)==h.identity(target) and stopped['state']=='T':break
                                time.sleep(.01)
                            else:raise ValueError('worker pause not confirmed')
                            physical=(case/'output.jsonl').read_bytes()
                            physical_slots=len(physical.splitlines())
                            expected_count=len((case/'expected.jsonl').read_bytes().splitlines())
                            h.require(not physical or physical.endswith(b'\n'),'partial physical record at pause')
                            physical_result_slots=physical_slots
                            h.require(physical_result_slots<expected_count,'no fresh physical result slot remains')
                            (case/'physical-before-loss.jsonl').write_bytes(physical)
                            fault={'worker':target,'checkpoint':selected,'physical_bytes':len(physical),'physical_record_slots':physical_slots,'physical_result_slots':physical_result_slots,'physical_sha256':digest(case/'physical-before-loss.jsonl')}
                            for relative in selected['artifacts']:
                                dest=case/'retained-checkpoint-objects'/relative; dest.parent.mkdir(parents=True,exist_ok=True)
                                shutil.copy2(case/'checkpoints'/relative,dest)
                            h.require(h.terminate_identity(target,signal.SIGKILL),'worker exited before kill')
                            h.write(case/'fault.json',fault)
                if fault:
                    fresh=[w for w in workers if h.identity(w)!=h.identity(fault['worker']) and w['job']==fault['worker']['job'] and w['pipeline']==fault['worker']['pipeline'] and w['generation']>fault['worker']['generation']]
                    if fresh: replacement=fresh[0]
                    if kafka and replacement and job['state']=='Running' and not stop_requested:
                        runtime=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',(case/'runtime.log').read_text(errors='replace'))
                        restored=any('restoring checkpoint' in line.lower() and fault['worker']['job'] in line
                                     and re.search(r'epoch=' + str(fault['checkpoint']['epoch']) + r'(?:\D|$)',line)
                                     for line in runtime.splitlines())
                        if restored:
                            cps=request(a.port,f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints",timeout=10)['data']
                            completed=sorted((x for x in cps if x['finish_time'] is not None),key=lambda x:x['epoch'],reverse=True)
                            if completed:
                                selected, committed=checkpoint(h,case,replacement,completed[0]['epoch'],kafka=True)
                                h.require(len(selected['typed_owners'])==a.typed_owners and len(selected['window_owners'])==a.window_owners,
                                          'recovered owner inventory differs')
                                h.require(selected['source_lines']<=produced, 'Kafka source passed produced fixture input')
                                h.require(committed.startswith(prefix), 'committed output bytes changed during Kafka stage')
                                if selected['source_lines']==produced:
                                    count = len(committed.splitlines())
                                    wanted = [stage['value'] for stage in stages[:stage_index+1]]
                                    check_cdc(committed, wanted[:count], a.cdc_keys)
                                    if count < len(wanted):
                                        time.sleep(.15)
                                        continue
                                    h.write(case/f'stage-{stage_index}-checkpoint.json', selected)
                                    (case/f'stage-{stage_index}-committed.jsonl').write_bytes(committed)
                                    if stage_index+1<len(stages):
                                        stage_index += 1
                                        produce_stage()
                                    else:
                                        request(a.port,f"pipelines/{pipeline['id']}",{'stop':'checkpoint'},timeout=10,method='PATCH')
                                        stop_requested = True
                    if job['state']==('Stopped' if kafka else 'Finished'):
                        h.require(not kafka or stop_requested, 'Kafka job stopped before explicit final checkpoint stop')
                        h.require(replacement is not None,'fresh worker generation absent')
                        data=(case/'output.jsonl').read_bytes()
                        h.require(data.startswith(prefix),'committed output bytes changed')
                        raw=[json.loads(x) for x in data.splitlines()]
                        actual=raw
                        expected=[json.loads(x) for x in (case/'expected.jsonl').read_text().splitlines()]
                        if kafka:
                            actual=check_cdc(data,expected,a.cdc_keys)
                        else:
                            h.require(records_equal(actual,expected,a.unordered_output),'exact application output differs')
                        h.require(len(raw)>fault['physical_record_slots'],'no physically fresh recovered raw records')
                        h.require(len(actual)>fault['physical_result_slots'],'no physically fresh recovered application records')
                        runtime=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',(case/'runtime.log').read_text(errors='replace'))
                        restore=[line for line in runtime.splitlines() if 'restoring checkpoint' in line.lower() and fault['worker']['job'] in line]
                        h.require(any(re.search(r'epoch=' + str(fault['checkpoint']['epoch']) + r'(?:\D|$)',line) for line in restore),'selected checkpoint restore not logged')
                        result.update(status='passed',fault=fault,replacement=replacement,rows=len(actual),raw_rows=len(raw),
                                      committed_bytes=len(prefix),fresh_output_rows=len(actual)-fault['physical_result_slots'],fresh_suffix_bytes=len(data)-fault['physical_bytes'],restore_log_lines=restore)
                        break
                h.require(child.poll() is None,'cluster exited prematurely'); time.sleep(.15)
            h.require(result['status'] in ['passed','compiled'],'runtime timeout')
        except BaseException as error:
            result['error']=f'{type(error).__name__}: {error}'; raise
        finally:
            result['cleanup']=h.cleanup(child,owned)
            if not result['cleanup']['complete']: result['status']='failed'
            result['seconds']=time.monotonic()-started
            h.write(case/'timeline.json',timeline);h.write(case/'result.json',result)
    h.require(result['status'] in ['passed','compiled'],'cleanup failed');print(json.dumps(result))


if __name__=='__main__': main()
