#!/usr/bin/env python3
"""Exercise application Rust UDFs through Streamr's public cluster/API/compiler.

Run in the pinned Bookworm image under the maintained shared queue. Supply the
qualified CLI, matching frozen Streamr sources at /app (local plugin contract),
shared Cargo target/cache mounts, and an independently prepared expected JSONL.
No application logic or UDF compilation implementation lives in this supervisor.
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
import urllib.request


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def request(port, path, payload=None, timeout=600):
    data = None if payload is None else json.dumps(payload).encode()
    req = urllib.request.Request(f'http://127.0.0.1:{port}/api/v1/{path}', data=data,
                                 headers={'Content-Type': 'application/json'})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        raise RuntimeError(f'API {path}: HTTP {error.code}: {error.read().decode()}') from error


def images(rows, field):
    if field is None: return rows
    result=[]
    for row in rows:
        value=row.get(field,[])
        if not isinstance(value,list): raise ValueError('declared output field is not an array')
        result.extend(value)
    return result


def checkpoint(h, case, owner, epoch):
    """Verify actual source/sink offsets and referenced typed artifacts."""
    import pyarrow.parquet as parquet
    base = case/'checkpoints'/owner['job']/'checkpoints'/f'checkpoint-{epoch:07}'
    cp = h.wire((base/'metadata').read_bytes())
    h.require(h.one(cp, 1) == owner['job'].encode() and h.one(cp, 2) == epoch,
              'checkpoint identity mismatch')
    globals_, artifacts, typed = {}, [], []
    for name in cp.get(6, []):
        op = h.wire((base/f'operator-{name.decode()}'/'metadata').read_bytes())
        for table, encoded in h.pairs(op, 13).items():
            state = h.wire(encoded)
            kind, payload = h.one(state, 1, 0), h.wire(h.one(state, 2, b''))
            if kind == 4:
                subs = h.pairs(payload, 2, numeric=True)
                h.require(set(subs) == {0}, 'typed ownership mismatch')
                sub = h.wire(subs[0]); h.require(h.one(sub, 7, 0) == epoch, 'typed epoch mismatch')
                files = []
                for item in sub.get(9, []):
                    descriptor = h.wire(item)
                    relative = h.one(descriptor, 1).decode()
                    path = case/'checkpoints'/relative
                    h.require(path.stat().st_size == h.one(descriptor, 2), 'artifact size mismatch')
                    h.require(bytes.fromhex(digest(path)) == h.one(descriptor, 3), 'artifact checksum mismatch')
                    artifacts.append(relative); files.append(relative)
                typed.append({'operator': name.decode(), 'namespace': table.decode(), 'files': files})
            elif kind == 1 and table == b'f':
                for relative in payload.get(1, []):
                    path = case/'checkpoints'/relative.decode(); artifacts.append(relative.decode())
                    for row in parquet.read_table(path, columns=['key', 'value']).to_pylist():
                        size, start = h.bincode_uint(row['key'])
                        key = row['key'][start:].decode()
                        h.require(size == len(row['key'])-start, 'global key mismatch')
                        value, end = h.bincode_uint(row['value'])
                        h.require(end == len(row['value']), 'global encoding mismatch')
                        h.require(key not in globals_, 'duplicate source/sink key')
                        globals_[key] = value
    source, offset = globals_.get(str(case/'input.jsonl')), globals_.get(str(case/'output.jsonl'))
    h.require(source is not None and offset is not None, 'source/sink checkpoint missing')
    data = (case/'output.jsonl').read_bytes()[:offset]
    h.require(len(data) == offset and data.endswith(b'\n'), 'incomplete committed output')
    return {'epoch': epoch, 'source_lines': source, 'sink_offset': offset,
            'typed_owners': typed, 'artifacts': artifacts,
            'timers_offset':globals_.get(str(case/'timers.jsonl'))}, data


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--case', type=Path, required=True)
    p.add_argument('--binary', type=Path, required=True)
    p.add_argument('--engine-helper', type=Path, required=True)
    p.add_argument('--config-template', type=Path, required=True)
    p.add_argument('--query', type=Path, required=True)
    p.add_argument('--udf', type=Path, action='append', default=[])
    p.add_argument('--input', type=Path, required=True)
    p.add_argument('--expected', type=Path, required=True)
    p.add_argument('--plugin-source-commit')
    p.add_argument('--binary-receipt', type=Path)
    p.add_argument('--expected-timers', type=Path)
    p.add_argument('--backend', choices=['memory', 'rocksdb'], default='rocksdb')
    p.add_argument('--result-array-field')
    p.add_argument('--timer-array-field')
    p.add_argument('--expected-raw-rows',type=int)
    p.add_argument('--state-row-bytes',type=int)
    p.add_argument('--scan-page-bytes',type=int)
    p.add_argument('--typed-decoded-bytes',type=int)
    p.add_argument('--typed-owners',type=int,default=1)
    p.add_argument('--fault-prefix', type=int, default=2)
    p.add_argument('--port', type=int, default=18419)
    p.add_argument('--timeout', type=int, default=1200)
    p.add_argument('--compile-only', action='store_true')
    a = p.parse_args()
    spec = importlib.util.spec_from_file_location('streamr_recovery', a.engine_helper)
    h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
    case = a.case.resolve(); case.mkdir(parents=True, exist_ok=False)
    shutil.copy2(Path(__file__),case/'supervisor.py')
    shutil.copy2(a.input, case/'input.jsonl'); shutil.copy2(a.expected, case/'expected.jsonl')
    if a.expected_timers: shutil.copy2(a.expected_timers,case/'expected-timers.jsonl')
    if not a.compile_only:
        h.require(0<a.fault_prefix<len((case/'input.jsonl').read_bytes().splitlines()),'fault prefix must leave a nonempty source suffix')
    query = a.query.read_text().replace('{{INPUT}}', str(case/'input.jsonl')).replace('{{OUTPUT}}', str(case/'output.jsonl')).replace('{{TIMERS}}', str(case/'timers.jsonl'))
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
    build = case/'udf-build'; build.mkdir()
    # Official compiler reads build-dir/target/release/libudf.so. This symlink
    # preserves its supported path while reusing the canonical shared target.
    shutil.copy2('/app/Cargo.lock', build/'Cargo.lock')
    (build/'target').symlink_to('/app/target/milestone2-runtime', target_is_directory=True)
    config['compiler'] = {'bind-address':'127.0.0.1', 'rpc-port': 9190,
                          'install-rustc':False, 'install-clang':False,
                          'use-local-udf-crate':True, 'build-dir':str(build),
                          'artifact-url':str(case/'artifacts')}
    h.write(case/'config.yaml', config)
    udfs = [{'definition': path.read_text(), 'language':'rust'} for path in a.udf]
    for i, path in enumerate(a.udf): shutil.copy2(path, case/f'udf-{i}.rs')
    command = [str(a.binary.resolve()), '--config', str(case/'config.yaml'), 'cluster']
    engine_receipt=json.loads(a.binary_receipt.read_text()) if a.binary_receipt else None
    if engine_receipt:
        h.require(engine_receipt['production_binary_sha256']==digest(a.binary),'production binary receipt mismatch')
    result = {'status':'failed','plugin_source_commit':a.plugin_source_commit,'engine_receipt':engine_receipt,'command':command,'backend':a.backend,
              'prepared_files':{str(case/name):digest(case/name) for name in ['config.yaml','query.sql','input.jsonl','expected.jsonl','supervisor.py']},
              'binary_sha256':digest(a.binary),'compiler_profile':'release (existing official compiler contract)',
              'plugin_source':{str(path):digest(path) for path in Path('/app/crates/arroyo-udf').rglob('*') if path.is_file()},
              'workspace_lock_sha256':digest('/app/Cargo.lock'),
              'files':{str(path):digest(path) for path in [a.query,a.input,a.expected,a.engine_helper,Path(__file__),*a.udf]}}
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
            for i, udf in enumerate(udfs):
                validation = request(a.port, 'udfs/validate', udf)
                h.write(case/f'udf-{i}-validation.json', validation)
                h.require(not validation['errors'], 'official Rust UDF validation failed')
            validation = request(a.port, 'pipelines/validate_query', {'query':query,'udfs':udfs})
            h.write(case/'query-validation.json',validation)
            h.require(not validation['errors'],'query validation failed')
            payload = {'name':'arc17-native-profile', 'query':query,'udfs':udfs,'parallelism':1,
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
                if not fault and job['state']=='Running' and len(workers)==1:
                    cps=request(a.port,f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints",timeout=10)['data']
                    completed=sorted((x for x in cps if x['finish_time'] is not None),key=lambda x:x['epoch'],reverse=True)
                    if completed:
                        selected,data=checkpoint(h,case,workers[0],completed[0]['epoch'])
                        h.require(len(selected['typed_owners'])==a.typed_owners,'typed owner inventory mismatch')
                        h.require(selected['source_lines']<=a.fault_prefix,'missed fault source prefix')
                        if selected['source_lines']==a.fault_prefix:
                            checkpoint_dir=case/'checkpoints'/workers[0]['job']/'checkpoints'/f"checkpoint-{selected['epoch']:07}"
                            shutil.copytree(checkpoint_dir,case/'selected-checkpoint-metadata')
                            prefix=data; (case/'committed-prefix.jsonl').write_bytes(data)
                            if a.expected_timers and not a.timer_array_field:
                                h.require(selected['timers_offset'] is not None,'timer sink checkpoint offset absent')
                                timer_prefix=(case/'timers.jsonl').read_bytes()[:selected['timers_offset']]
                                h.require(len(timer_prefix)==selected['timers_offset'] and (not timer_prefix or timer_prefix.endswith(b'\n')),'incomplete committed timer output')
                                (case/'committed-timers-prefix.jsonl').write_bytes(timer_prefix)
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
                            if a.result_array_field:
                                h.require(not physical or physical.endswith(b'\n'),'partial physical outbox record at pause')
                                physical_result_slots=len(images([json.loads(x) for x in physical.splitlines()],a.result_array_field))
                            else:physical_result_slots=physical_slots
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
                    if job['state']=='Finished':
                        h.require(replacement is not None,'fresh worker generation absent')
                        data=(case/'output.jsonl').read_bytes()
                        h.require(data.startswith(prefix),'committed output bytes changed')
                        raw=[json.loads(x) for x in data.splitlines()]
                        if a.expected_raw_rows is not None:h.require(len(raw)==a.expected_raw_rows,'raw outbox cardinality differs')
                        actual=images(raw,a.result_array_field)
                        expected=[json.loads(x) for x in (case/'expected.jsonl').read_text().splitlines()]
                        h.require(json.dumps(actual,sort_keys=True)==json.dumps(expected,sort_keys=True),'exact application output differs')
                        h.require(len(raw)>fault['physical_record_slots'],'no physically fresh recovered raw records')
                        h.require(len(actual)>fault['physical_result_slots'],'no physically fresh recovered application records')
                        if a.expected_timers:
                            if a.timer_array_field:actual_timers=images(raw,a.timer_array_field)
                            else:
                                timer_data=(case/'timers.jsonl').read_bytes()
                                h.require(timer_data.startswith((case/'committed-timers-prefix.jsonl').read_bytes()),'committed timer bytes changed')
                                actual_timers=[json.loads(x) for x in timer_data.splitlines()]
                            expected_timers=[json.loads(x) for x in (case/'expected-timers.jsonl').read_text().splitlines()]
                            h.require(json.dumps(actual_timers,sort_keys=True)==json.dumps(expected_timers,sort_keys=True),'exact timer seed output differs')
                            result['timer_seed_rows']=len(actual_timers)
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
            for name in ['Cargo.toml','Cargo.lock']:
                if (build/name).exists(): result[name+'_sha256']=digest(build/name)
            result['udf_artifacts']={str(path.relative_to(case)):digest(path) for path in (case/'artifacts').rglob('*') if path.is_file()}
            h.write(case/'timeline.json',timeline);h.write(case/'result.json',result)
    h.require(result['status'] in ['passed','compiled'],'cleanup failed');print(json.dumps(result))


if __name__=='__main__': main()
