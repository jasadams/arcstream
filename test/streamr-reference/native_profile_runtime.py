#!/usr/bin/env python3
"""Staged raw Kafka -> native SQL -> transactional Kafka recovery experiment.

Expectations are literal cumulative append snapshots, never derived from output.
An isolated outer wrapper must own both topics and the compact recovery topic.
"""
import argparse
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
import urllib.parse
import native_sql_runtime as n


def validate_stages(stages, count, fault_prefix, recovery=True):
    prefixes = [s['input_lines'] for s in stages]
    if (not prefixes or prefixes[0] != fault_prefix or prefixes[-1] != count
            or any(type(p) is not int or p <= 0 for p in prefixes)
            or any(a >= b for a, b in zip(prefixes, prefixes[1:]))):
        raise ValueError('stages must increase from fault prefix to complete input')
    previous = []
    for stage in stages:
        values = stage.get('values')
        if not isinstance(values, list) or not all(isinstance(x, dict) for x in values):
            raise ValueError('each stage needs literal cumulative values')
        if not n.records_equal(values[:len(previous)], previous):
            raise ValueError('cumulative oracle rewrites earlier snapshots')
        previous = values
    if recovery and (not stages[0]['values'] or len(stages[-1]['values']) <= len(stages[0]['values'])):
        raise ValueError('fault requires committed prefix and fresh suffix')


def check_snapshots(actual, expected, keys, partial=False):
    """Ignore cross-key order only; preserve complete rows and per-key sequence."""
    def traces(rows):
        result = {}
        for row in rows:
            if not isinstance(row, dict):
                raise ValueError('snapshot must be an object')
            key = tuple(json.dumps(row[k], sort_keys=True) for k in keys)
            result.setdefault(key, []).append(row)
        return result
    wanted, observed = traces(expected), traces(actual)
    for key, rows in observed.items():
        reference = wanted.get(key, [])
        if not n.records_equal(rows, reference[:len(rows)] if partial else reference):
            raise ValueError('wrong/extra/duplicate snapshot or per-key sequence')
    if not partial and set(wanted) != set(observed):
        raise ValueError('missing keyed snapshots')
    return len(actual) == len(expected)


def checkpoint(h, case, owner, epoch):
    """Verify native artifact ownership/hash and checkpointed Kafka next-offset."""
    import pyarrow.parquet as parquet
    base = case/'checkpoints'/owner['job']/'checkpoints'/f'checkpoint-{epoch:07}'
    cp = h.wire((base/'metadata').read_bytes())
    h.require(h.one(cp, 1) == owner['job'].encode() and h.one(cp, 2) == epoch,
              'checkpoint identity mismatch')
    offsets, artifacts, owners = [], [], []
    for name in cp.get(6, []):
        path = base/('operator-'+urllib.parse.quote(name.decode(), safe=''))/'metadata'
        op = h.wire(path.read_bytes())
        for table, encoded in h.pairs(op, 13).items():
            state = h.wire(encoded)
            kind, payload = h.one(state, 1, 0), h.wire(h.one(state, 2, b''))
            if kind in (3, 4):
                subs = h.pairs(payload, 2, numeric=True)
                h.require(set(subs) == {0}, 'unexpected parallel ownership')
                sub = h.wire(subs[0])
                h.require(h.one(sub, 7, 0) == epoch, 'native epoch mismatch')
                files = []
                for item in sub.get(9, []):
                    d = h.wire(item)
                    relative = urllib.parse.quote(h.one(d, 1).decode(), safe='/')
                    artifact = case/'checkpoints'/relative
                    h.require(artifact.stat().st_size == h.one(d, 2), 'artifact size differs')
                    h.require(bytes.fromhex(n.digest(artifact)) == h.one(d, 3), 'artifact hash differs')
                    files.append(relative); artifacts.append(relative)
                owners.append(dict(operator=name.decode(), namespace=table.decode(), kind=kind, files=files))
            elif kind == 1 and table == b'k':
                for relative in payload.get(1, []):
                    relative = relative.decode(); artifacts.append(relative)
                    for row in parquet.read_table(case/'checkpoints'/relative, columns=['key','value']).to_pylist():
                        offsets.append(n.decode_kafka_offset(h, row['key'], row['value']))
    h.require(len(offsets) <= 1, 'one Kafka source partition required')
    source_lines = offsets[0] if offsets else 0
    return dict(epoch=epoch, source_lines=source_lines, owners=owners, artifacts=artifacts)


def parse_capture(stdout, minimum_end):
    """Keep Kafka transport metadata; only control batches are non-app records."""
    rows, records, previous = [], [], -1
    for line in stdout.splitlines():
        partition, offset, control, payload = line.split(' ', 3)
        partition, offset, control = int(partition), int(offset), int(control)
        if partition != 0 or offset <= previous or control not in (0, 1):
            raise ValueError('invalid partition/offset/control in finite capture')
        previous = offset
        decoded = bytes.fromhex(payload)
        records.append(dict(partition=partition, offset=offset, control=bool(control), payload_hex=payload))
        if not control:
            rows.append(json.loads(decoded, object_pairs_hook=n.strict_object))
    if minimum_end > 0 and previous < minimum_end - 1:
        raise ValueError('finite capture did not reach captured broker high watermark')
    return rows, records


def read_committed(rpk, broker, topic, timeout=30):
    """Natural finite broker-end completion, never partial timed observation.

    Pinned rpk v24.3.7 :end snapshots broker end offsets and internally retains
    control records to terminate at transaction markers. Empty partitions are
    checked against broker metadata/start/end offsets before natural exit.
    Open transactions may prevent completion: timeout fails the experiment.
    """
    if timeout <= 0:
        raise TimeoutError('experiment deadline reached before Kafka capture')
    deadline = time.monotonic() + timeout
    describe = [str(rpk), 'topic', 'describe', topic, '-X', 'brokers='+broker,
                '--format', 'json', '--stable']
    metadata = subprocess.run(describe, text=True, capture_output=True,
                              check=True, timeout=max(.01, deadline-time.monotonic()))
    described = json.loads(metadata.stdout)
    if len(described) != 1:
        raise ValueError('expected exactly one described topic')
    partitions = described[0]['partitions']
    if len(partitions) != 1 or partitions[0]['partition'] != 0:
        raise ValueError('finite fixture requires partition zero only')
    partition = partitions[0]
    if partition.get('error') or partition.get('load_error') or partition['log_start_offset'] != 0:
        raise ValueError('Kafka topic history absent or metadata failed')
    end = partition['high_watermark']
    if type(end) is not int or end < 0:
        raise ValueError('invalid broker high watermark')
    argv = [str(rpk), 'topic', 'consume', topic, '-X', 'brokers='+broker,
            '--partitions', '0', '--offset', ':end', '--read-committed',
            '--print-control-records', '--format', '%p %o %a{control-bit} %v{hex}\\n']
    captured = subprocess.run(argv, text=True, capture_output=True, check=True,
                              timeout=max(.01, deadline-time.monotonic()))
    if captured.stderr and re.search(r'(ERR:|unknown flag|unable to|failed to|error:)', captured.stderr, re.I):
        raise ValueError('finite read-committed consumption error: '+captured.stderr)
    rows, records = parse_capture(captured.stdout, end)
    return rows, dict(command=argv, metadata_command=describe,
                      metadata=described, exit=captured.returncode,
                      stdout=captured.stdout, stderr=captured.stderr,
                      records=records, minimum_captured_end=end,
                      completion='natural-finite-broker-end')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('case','binary','binary-receipt','engine-helper','config-template','query','input','expected-prefixes','kafka-rpk'):
        p.add_argument('--'+name, type=Path, required=True)
    for name in ('kafka-broker','kafka-topic','kafka-output-topic'):
        p.add_argument('--'+name, required=True)
    p.add_argument('--keys', nargs='+', default=['tenant_id','canonical_id'])
    p.add_argument('--fault-prefix', type=int, default=1)
    p.add_argument('--no-recovery', action='store_true',help='staged diagnostic only; no worker-loss or recovery claim')
    p.add_argument('--typed-owners', type=int, default=1)
    p.add_argument('--disk-owners', type=int, default=4)
    p.add_argument('--port', type=int, default=18428)
    p.add_argument('--timeout', type=int, default=360)
    a = p.parse_args()
    stages = json.loads(a.expected_prefixes.read_text())
    lines = a.input.read_text().splitlines()
    validate_stages(stages, len(lines), a.fault_prefix, recovery=not a.no_recovery)
    spec = importlib.util.spec_from_file_location('engine_recovery', a.engine_helper)
    h = importlib.util.module_from_spec(spec); spec.loader.exec_module(h)
    case = a.case.resolve(); case.mkdir(parents=True, exist_ok=False)
    for path, name in ((a.input,'input.jsonl'),(a.query,'query.sql'),(a.expected_prefixes,'expected-prefixes.json'),(Path(__file__),'supervisor.py')):
        shutil.copy2(path, case/name)
    config = json.loads(a.config_template.read_text())
    config['checkpoint-url'] = str(case/'checkpoints')
    config['database']['sqlite']['path'] = str(case/'catalog.sqlite')
    config['default-checkpoint-interval'] = '3s'
    config['api'].update({'http-port':a.port,'run-http-port':a.port})
    config['worker']['sql-state-backend'] = 'rocksdb'
    config['worker']['disk-sql-state']['directory'] = str(case/'live-state')
    config.pop('compiler', None); h.write(case/'config.yaml', config)
    receipt = json.loads(a.binary_receipt.read_text())
    h.require(n.digest(a.binary) == receipt.get('binary_sha256', receipt.get('production_binary_sha256')), 'binary hash mismatch')
    result = dict(status='failed', recovery_required=not a.no_recovery, binary_receipt=receipt, binary_sha256=n.digest(a.binary),
                  files={str(x):n.digest(x) for x in (a.query,a.input,a.expected_prefixes,a.engine_helper,Path(__file__),Path(n.__file__))},
                  kafka_commands=[],stages=[])
    command = [str(a.binary.resolve()),'--config',str(case/'config.yaml'),'cluster']
    result['command'] = command
    timeline, owned, fault, replacement, prefix = [], {}, None, None, None
    stage_index, produced, stopped = 0, 0, False
    started = time.monotonic(); h.enable_subreaper()
    def remaining():
        value = a.timeout - (time.monotonic()-started)
        if value <= 0: raise TimeoutError('experiment deadline reached')
        return value
    def request(path, payload=None, timeout=10, method=None):
        return n.request(a.port,path,payload,timeout=min(timeout,remaining()),method=method)
    def capture():
        capture_id = len(result.setdefault('captures', []))
        filename = f'capture-{capture_id}.json'
        try:
            rows, receipt = read_committed(a.kafka_rpk,a.kafka_broker,a.kafka_output_topic,timeout=min(30,remaining()))
        except BaseException as error:
            text = lambda value: value.decode(errors='replace') if isinstance(value, bytes) else value
            result['captures'].append(dict(file=filename,complete=False))
            h.write(case/filename,dict(complete=False,error=type(error).__name__+': '+str(error),
                                      command=getattr(error,'cmd',None),
                                      stdout=text(getattr(error,'stdout',None)),
                                      stderr=text(getattr(error,'stderr',None))))
            raise
        result['captures'].append(dict(file=filename,rows=len(rows),complete=True))
        h.write(case/filename,receipt)
        return rows, receipt
    clean = {k:v for k,v in os.environ.items() if not k.startswith(('STREAMR_TEST_','STREAMR_CAPTURE_','ARROYO__'))}
    clean['RUST_LOG'] = 'info'
    def produce():
        nonlocal produced
        end = stages[stage_index]['input_lines']
        argv = [str(a.kafka_rpk),'topic','produce',a.kafka_topic,'-X','brokers='+a.kafka_broker]
        run = subprocess.run(argv,input=''.join(x+'\n' for x in lines[produced:end]),text=True,capture_output=True,timeout=min(30,remaining()))
        result['kafka_commands'].append(dict(command=argv,exit=run.returncode,stdout=run.stdout,stderr=run.stderr,input_start=produced+1,input_end=end))
        h.write(case/'kafka-commands.json',result['kafka_commands'])
        h.require(run.returncode == 0,'Kafka produce failed: '+run.stderr); produced = end
    with (case/'runtime.log').open('wb') as log:
        child = subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,cwd=case,env=clean,start_new_session=True)
        try:
            while time.monotonic()-started < min(90,a.timeout):
                h.require(child.poll() is None,'cluster exited before API ready')
                try: request('pipelines',timeout=2); break
                except (OSError,RuntimeError): time.sleep(.2)
            else: raise ValueError('API startup timeout')
            query = (case/'query.sql').read_text()
            validation = request('pipelines/validate_query',{'query':query,'udfs':[]})
            h.write(case/'query-validation.json',validation)
            h.require(not validation['errors'],'query validation failed')
            payload = dict(name='arcstream-native-profile-validation',query=query,udfs=[],parallelism=1,checkpoint_interval_micros=3000000)
            h.write(case/'pipeline-request.json',payload)
            pipeline = request('pipelines',payload); h.write(case/'pipeline-response.json',pipeline)
            while time.monotonic()-started < a.timeout:
                ps = h.session_processes(child.pid); owned.update({h.identity(x):x for x in ps})
                workers = [w for x in ps if (w := h.worker(x,a.binary.resolve()))]
                jobs = request(f"pipelines/{pipeline['id']}/jobs",timeout=10)['data']
                if not jobs: time.sleep(.15); continue
                job = jobs[0]; timeline.append(dict(elapsed=time.monotonic()-started,job=job,workers=workers))
                h.require(job['state'] != 'Failed','pipeline failed')
                if not fault:
                    h.require(job['state'] not in ('Recovering','Finished') and (job['state']!='Stopped' or stopped),
                              'unexpected state before planned fault or diagnostic completion')
                elif fault:
                    fresh = [w for w in workers if h.identity(w) != h.identity(fault['worker']) and w['job'] == fault['worker']['job'] and w['pipeline'] == fault['worker']['pipeline'] and w['generation'] > fault['worker']['generation']]
                    if fresh: replacement = fresh[0]
                if job['state'] == 'Running' and not produced: produce()
                if job['state'] == 'Running' and len(workers) == 1 and (not fault or replacement):
                    cps = request(f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints",timeout=10)['data']
                    completed = sorted((x for x in cps if x['finish_time'] is not None),key=lambda x:x['epoch'],reverse=True)
                    if completed:
                        selected = checkpoint(h,case,workers[0],completed[0]['epoch'])
                        h.require(selected['source_lines'] <= produced,'source offset passed produced input')
                        if selected['source_lines'] == produced:
                            h.require(sum(x['kind']==4 for x in selected['owners'])==a.typed_owners,'typed owner inventory differs')
                            h.require(sum(x['kind']==3 for x in selected['owners'])==a.disk_owners,'disk-keyed owner inventory differs')
                            actual, argv = capture()
                            result['consume_command'] = argv
                            wanted = stages[stage_index]['values']
                            complete = check_snapshots(actual,wanted,a.keys,partial=True)
                            if prefix is not None:
                                h.require(n.records_equal(actual[:len(prefix)],prefix),'committed Kafka prefix changed')
                            if complete:
                                observation = dict(stage=stage_index,checkpoint=selected,rows=actual)
                                h.write(case/f'stage-{stage_index}.json',observation)
                                if not fault and not a.no_recovery:
                                    target = workers[0]
                                    h.require(h.terminate_identity(target,signal.SIGSTOP),'worker exited before pause')
                                    deadline=time.monotonic()+2
                                    while time.monotonic()<deadline:
                                        current=h.process(target['pid'])
                                        if current and h.identity(current)==h.identity(target) and current['state']=='T': break
                                        time.sleep(.01)
                                    else: raise ValueError('worker pause not confirmed')
                                    latest = request(f"pipelines/{pipeline['id']}/jobs/{job['id']}/checkpoints",timeout=10)['data']
                                    latest = max((x for x in latest if x['finish_time'] is not None),key=lambda x:x['epoch'])
                                    selected = checkpoint(h,case,target,latest['epoch'])
                                    h.require(selected['source_lines']==produced,'paused checkpoint source prefix changed')
                                    paused, _ = capture()
                                    check_snapshots(paused,wanted,a.keys)
                                    prefix=paused
                                    fault=dict(worker=target,checkpoint=selected,committed_rows=prefix)
                                    metadata=case/'checkpoints'/target['job']/'checkpoints'/f"checkpoint-{selected['epoch']:07}"
                                    shutil.copytree(metadata,case/'selected-checkpoint-metadata')
                                    for relative in selected['artifacts']:
                                        dest=case/'retained-checkpoint-objects'/relative; dest.parent.mkdir(parents=True,exist_ok=True)
                                        shutil.copy2(case/'checkpoints'/relative,dest)
                                    h.require(h.terminate_identity(target,signal.SIGKILL),'worker exited before kill')
                                    h.write(case/'fault.json',fault)
                                else:
                                    runtime=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',(case/'runtime.log').read_text(errors='replace'))
                                    restore=[] if a.no_recovery else [x for x in runtime.splitlines() if 'restoring checkpoint' in x.lower() and fault['worker']['job'] in x and re.search(r'epoch='+str(fault['checkpoint']['epoch'])+r'(?:\D|$)',x)]
                                    if not a.no_recovery:
                                        h.require(bool(restore),'selected checkpoint restoration not logged')
                                    result['stages'].append(observation)
                                    if stage_index+1 < len(stages):
                                        stage_index+=1; produce()
                                    elif not stopped:
                                        request(f"pipelines/{pipeline['id']}",{'stop':'checkpoint'},timeout=10,method='PATCH'); stopped=True
                if job['state'] == 'Stopped':
                    h.require(stopped and (a.no_recovery or replacement is not None),'unexpected stop/no fresh worker')
                    final,_=capture()
                    check_snapshots(final,stages[-1]['values'],a.keys)
                    if not a.no_recovery:
                        h.require(len(final)>len(prefix),'no fresh recovered application result')
                    h.write(case/'committed-output.json',final)
                    result.update(status='passed',fault=fault,replacement=replacement,rows=len(final),fresh_rows=None if a.no_recovery else len(final)-len(prefix),restore_log_lines=restore,
                                  validation_scope='staged-diagnostic-no-recovery' if a.no_recovery else 'kafka-committed-output-recovery')
                    break
                h.require(child.poll() is None,'cluster exited'); time.sleep(.15)
            h.require(result['status']=='passed','runtime timeout')
        except BaseException as error:
            result['error']=type(error).__name__+': '+str(error); raise
        finally:
            result['cleanup']=h.cleanup(child,owned)
            if not result['cleanup']['complete']: result['status']='failed'
            result['seconds']=time.monotonic()-started
            h.write(case/'timeline.json',timeline); h.write(case/'result.json',result)
    h.require(result['status']=='passed','cleanup failed'); print(json.dumps(result))


if __name__=='__main__': main()
