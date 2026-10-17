#!/usr/bin/env python3
"""Isolated real Kafka/native application graph qualification.

Host mode owns a private broker/network and compatible engine container. Inner
mode uses only public APIs/connectors, existing compiler, native checkpoints and
read-committed finite Kafka offset scans. Invoke through scripts/dev-build.
"""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def write(path, value):
    Path(path).write_text(json.dumps(value,indent=2,sort_keys=True)+'\n')


def require(value, message):
    if not value: raise ValueError(message)


def api(port, path, body=None, timeout=1200):
    request=urllib.request.Request(f'http://127.0.0.1:{port}/api/v1/{path}',
        data=None if body is None else json.dumps(body).encode(),
        headers={'Content-Type':'application/json'})
    compiler_deadline=time.monotonic()+90
    while True:
        try:
            with urllib.request.urlopen(request,timeout=timeout) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            detail=error.read().decode()
            if path in ('udfs/validate','pipelines/validate_query') and error.code==503 and 'compiler-service not available' in detail and time.monotonic()<compiler_deadline:
                time.sleep(.5)
                continue
            raise RuntimeError(f'{path}: HTTP{error.code}: {detail}') from error


def run(command, **kwargs):
    process=subprocess.run(command,text=True,capture_output=True,**kwargs)
    require(process.returncode==0,f'command failed: {command}: {process.stderr}')
    return process.stdout


def checkpoint(h, case, worker, epoch, label):
    """Source Kafka offsets, sink recovery epoch and typed artifact inventory."""
    import pyarrow.parquet as parquet
    base=case/'checkpoints'/worker['job']/'checkpoints'/f'checkpoint-{epoch:07}'
    cp=h.wire((base/'metadata').read_bytes())
    require(h.one(cp,1)==worker['job'].encode() and h.one(cp,2)==epoch,'checkpoint identity differs')
    typed,offsets,sinks,objects=[],[],[],[]
    def signed(data):
        value,pos=h.bincode_uint(data)
        return (value>>1)^-(value&1),pos
    for name in cp.get(6,[]):
        op=h.wire((base/f'operator-{name.decode()}'/'metadata').read_bytes())
        for table,encoded in h.pairs(op,13).items():
            state=h.wire(encoded);kind=h.one(state,1,0);payload=h.wire(h.one(state,2,b''))
            if kind==4:
                subs=h.pairs(payload,2,numeric=True)
                require(set(subs)=={0},'typed subtask differs')
                sub=h.wire(subs[0]);require(h.one(sub,7,0)==epoch,'typed epoch differs')
                files=[]
                for encoded_file in sub.get(9,[]):
                    descriptor=h.wire(encoded_file);relative=h.one(descriptor,1).decode()
                    path=case/'checkpoints'/relative
                    require(path.stat().st_size==h.one(descriptor,2),'typed object size differs')
                    require(bytes.fromhex(sha(path))==h.one(descriptor,3),'typed object checksum differs')
                    objects.append(relative);files.append(relative)
                typed.append(dict(operator=name.decode(),table=table.decode(),files=files))
            elif kind==1 and table in (b'k',b'i'):
                for relative in payload.get(1,[]):
                    path=case/'checkpoints'/relative.decode();objects.append(relative.decode())
                    for row in parquet.read_table(path,columns=['key','value']).to_pylist():
                        data=row['value']
                        if table==b'k':
                            partition,pos=signed(data);offset,tail=signed(data[pos:])
                            require(partition==0 and pos+tail==len(data),'Kafka source encoding differs')
                            offsets.append(dict(operator=name.decode(),partition=partition,next_offset=offset))
                        else:
                            size,pos=h.bincode_uint(data)
                            require(size==len(data)-pos,'Kafka sink encoding differs')
                            saved=json.loads(data[pos:]);require(saved['checkpoint_epoch']==epoch,'sink epoch differs')
                            sinks.append(saved)
    require(len(typed)==2 and len(offsets)==1 and len(sinks)==1,'profile owner checkpoint inventory differs')
    preserved=case/('selected-checkpoint-'+label);preserved.mkdir(exist_ok=False)
    shutil.copytree(base,preserved/'metadata')
    receipts=[]
    for relative in dict.fromkeys(objects):
        source=case/'checkpoints'/relative;destination=preserved/'objects'/relative
        destination.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,destination)
        receipts.append(dict(path=relative,sha256=sha(destination),bytes=destination.stat().st_size))
    return dict(epoch=epoch,source=offsets[0],sink=sinks[0],typed=typed,artifacts=receipts)


def inner(a):
    case=a.case.resolve();h=load('engine_recovery',a.engine_helper)
    prepare=load('application_prepare',ROOT/'deploy/streamr-native/prepare_mvp.py')
    oracle=load('independent_application_oracle',case/'oracle.py')
    sys_path=str(ROOT/'test/streamr-reference')
    import sys
    sys.path.insert(0,sys_path)
    from compare_identity import compare,read_records
    fixture=json.loads((case/'identity-input.json').read_text())
    events=[step['payload'] for step in fixture['steps'] if step['op']=='event']
    prepared=case/'pipelines'
    manifest=prepare.prepare(prepared,'broker:9092',a.namespace,'replay')
    data_topics=[row['name'] for row in manifest['topics'] if not row['name'].endswith('-commits')]
    # Broker resources belong to host mode; this process only uses its copied CLI.
    rpk=[str(case/'rpk')]
    commands=[]
    def broker(*args,data=None,timeout=90):
        command=rpk+list(args)+['-X','brokers=broker:9092']
        process=subprocess.run(command,input=data,text=True,capture_output=True,timeout=timeout)
        commands.append(dict(command=command,exit=process.returncode,stderr=process.stderr))
        write(case/'broker-commands.json',commands)
        require(process.returncode==0,f'rpk failed: {process.stderr}')
        return process.stdout
    for compact in [False,True]:
        topics=[row['name'] for row in manifest['topics'] if row['name'].endswith('-commits')==compact]
        broker('topic','create',*topics,'--partitions','1','--replicas','1','--topic-config',
               'cleanup.policy='+('compact' if compact else 'delete'))
    config=json.loads(a.config_template.read_text())
    config.update({'checkpoint-url':str(case/'checkpoints'),'default-checkpoint-interval':'2s'})
    config['database']['sqlite']['path']=str(case/'catalog.sqlite')
    config['api'].update({'bind-address':'127.0.0.1','http-port':a.port,'run-http-port':a.port})
    config['pipeline'].update({'source-batch-size':1,'source-batch-linger':'50ms','update-aggregate-flush-interval':'100ms'})
    config['worker']['sql-state-backend']=a.backend
    config['worker']['disk-sql-state'].update({'directory':str(case/'live-state'),'max-row-bytes':32768})
    config['worker']['typed-sql-state'].update({'row-bytes':32768,'decoded-bytes':262144,'scope-bytes':1048576,'page-entries':16,'max-working-event-bytes':8388608,'max-resident-bytes':16777216})
    config['worker']['live-state-resources'].update({'max-open-databases':8,'max-snapshots':8,'scan-page-bytes':8388608,'queued-write-bytes':67108864})
    config['worker']['window-state'].update({'partial-bytes':262144,'page-bytes':524288,'write-bytes':2097152,'page-entries':16})
    build=case/'udf-build';build.mkdir();shutil.copy2('/app/Cargo.lock',build/'Cargo.lock')
    (build/'target').symlink_to('/app/target/milestone2-runtime',target_is_directory=True)
    config['compiler']={'bind-address':'127.0.0.1','rpc-port':9190,'install-rustc':False,'install-clang':False,
                        'use-local-udf-crate':True,'build-dir':str(build),'artifact-url':str(case/'artifacts')}
    write(case/'config.yaml',config)
    command=[str(a.binary),'--config',str(case/'config.yaml'),'cluster']
    result=dict(status='failed',command=command,binary_sha256=sha(a.binary),plugin_source_commit=a.plugin_source_commit,
                files={str(path):sha(path) for path in [Path(__file__),a.engine_helper,a.config_template,case/'config.yaml',case/'oracle.py',
                       case/'identity-input.json',case/'identity-expected.jsonl',case/'rpk',
                       ROOT/'deploy/streamr-native/prepare_mvp.py',*sorted((ROOT/'deploy/streamr-native').rglob('*.rs')),*sorted((ROOT/'deploy/streamr-native').rglob('*.sql'))]},
                plugin_source_files={str(path):sha(path) for path in [Path('/app/Cargo.toml'),Path('/app/Cargo.lock'),*sorted(Path('/app/crates/arroyo-udf').rglob('*')),*sorted(Path('/app/crates/arroyo-compiler-service').rglob('*'))] if path.is_file()})
    owned={};started=time.monotonic();h.enable_subreaper();timeline=[]
    counters=Counter()
    def snapshot(suffix,label,committed=True):
        topic=a.namespace+'-'+suffix
        read_options=['--read-committed'] if committed else []
        output=broker('topic','consume',topic,*read_options,'--offset',':end',
                      '--fetch-max-wait','500ms','--format','%p\t%o\t%v\n')
        counters[label]+=1
        path=case/'topics'/f'{label}-{counters[label]:04}.tsv';path.parent.mkdir(exist_ok=True)
        path.write_text(output)
        rows=[];offset=-1
        for line in output.splitlines():
            partition,current,value=line.split('\t',2)
            require(partition=='0' and int(current)>offset,'topic offset order differs')
            offset=int(current);rows.append(json.loads(value))
        return rows,output,offset
    def jobs(pipelines):
        values={}
        for name,pipeline in pipelines.items():
            rows=api(a.port,f"pipelines/{pipeline['id']}/jobs",timeout=15)['data']
            require(rows and rows[0]['state'] not in ('Failed','Error'),f'failed pipeline {name}: {rows}')
            values[name]=rows[0]
        return values
    def wait(predicate,label,seconds=180):
        deadline=time.monotonic()+seconds
        while time.monotonic()<deadline:
            require(child.poll() is None,'engine cluster exited')
            ps=h.session_processes(child.pid);owned.update({h.identity(p):p for p in ps})
            value=predicate()
            if value: return value
            time.sleep(.5)
        raise ValueError('deadline: '+label)
    def await_rows(suffix,count,label):
        def ready():
            rows,data,offset=snapshot(suffix,label)
            require(len(rows)<=count,f'extra {suffix} records: {len(rows)}>{count}')
            return (rows,data,offset) if len(rows)==count else None
        return wait(ready,label,300)
    def latest_profiles(rows):
        return {(row['tenant_id'],row['canonical_id']):row for row in oracle.verify_cdc(rows)}
    clean={k:v for k,v in os.environ.items() if not k.startswith(('ARROYO__','STREAMR_TEST_','STREAMR_CAPTURE_'))}
    clean['RUST_LOG']='info'
    with (case/'runtime.log').open('wb') as log:
        child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,cwd=case,env=clean)
        try:
            def api_ready():
                try: api(a.port,'pipelines',timeout=2);return True
                except (urllib.error.URLError,RuntimeError,ConnectionError): return False
            wait(api_ready,'API',90)
            for item in manifest['pipelines']:
                payload=json.loads((prepared/item['request']).read_text())
                for index,udf in enumerate(payload['udfs']):
                    validation=api(a.port,'udfs/validate',udf)
                    write(prepared/(item['name']+f'-udf-{index}-validation.json'),validation)
                    require(not validation.get('errors'),'official Rust UDF rejected: '+item['name'])
                response=api(a.port,'pipelines/validate_query',dict(query=payload['query'],udfs=payload['udfs']))
                write(prepared/(item['name']+'-validation.json'),response)
                require(not response.get('errors') and response.get('graph'),'query rejected: '+item['name'])
            submitted=prepare.submit(prepared,f'http://127.0.0.1:{a.port}/api/v1')
            pipelines=submitted['pipelines']
            require(set(pipelines)=={row['name'] for row in manifest['pipelines']},'submitted native graph differs from manifest')
            wait(lambda: all(job['state']=='Running' for job in jobs(pipelines).values()),'all jobs Running',180)
            def inventory():
                rows=[];cursor=None
                while True:
                    page=api(a.port,'pipelines?limit=100'+('&starting_after='+urllib.parse.quote(cursor,safe='') if cursor else ''))
                    rows.extend(page['data'])
                    require(len({row['id'] for row in rows})==len(rows),'repeated pipeline pagination identity')
                    if not page.get('has_more',False):return rows
                    require(page['data'],'pipeline pagination did not advance')
                    cursor=page['data'][-1]['id']
            initial_ids=sorted(row['id'] for row in inventory())
            repeated=prepare.submit(prepared,f'http://127.0.0.1:{a.port}/api/v1')
            require({key:row['id'] for key,row in repeated['pipelines'].items()}=={key:row['id'] for key,row in pipelines.items()},'repeated submission changed pipeline identities')
            require(sorted(row['id'] for row in inventory())==initial_ids,'repeated submission created pipelines')
            write(case/'repeated-submission.json',dict(status='passed',pipeline_ids=initial_ids,pipelines=repeated['pipelines']))
            broker('topic','produce',a.namespace+'-raw-events',data=''.join(json.dumps(event)+'\n' for event in events[:10]))
            await_rows('unified-events',10,'initial-unified')
            await_rows('profile-updates',10,'initial-profile-flat')
            outbox,_,_=await_rows('profile-outbox',10,'initial-outbox')
            await_rows('session-outbox',10,'initial-session-outbox')
            signals,_,source_offset=await_rows('profile-signals',10,'initial-signals')
            profile_id=pipelines['profiles']['id']
            def target_ready():
                job=jobs(pipelines)['profiles']
                workers=[w for p in h.session_processes(child.pid) if (w:=h.worker(p,a.binary.resolve())) and w['pipeline']==profile_id]
                cps=api(a.port,f"pipelines/{profile_id}/jobs/{job['id']}/checkpoints")['data']
                complete=sorted((cp for cp in cps if cp['finish_time'] is not None),key=lambda cp:cp['epoch'])
                return (workers[0],complete[-1]) if len(workers)==1 and complete else None
            target,selected=wait(target_ready,'profile checkpoint')
            require(h.terminate_identity(target,signal.SIGSTOP),'worker vanished before pause')
            wait(lambda:(p:=h.process(target['pid'])) and h.identity(p)==h.identity(target) and p['state']=='T','frozen worker',5)
            time.sleep(.25)
            job=jobs(pipelines)['profiles']
            cps=api(a.port,f"pipelines/{profile_id}/jobs/{job['id']}/checkpoints")['data']
            selected=max((cp for cp in cps if cp['finish_time'] is not None),key=lambda cp:cp['epoch'])
            proof=checkpoint(h,case,target,selected['epoch'],'profiles')
            require(proof['source']['next_offset']==source_offset+1,'checkpoint does not cover exact initial signals')
            write(case/'checkpoint-before-loss.json',proof)
            prefixes={}
            for topic in data_topics:
                suffix=topic[len(a.namespace)+1:]
                _,data,_=snapshot(suffix,'before-loss-'+suffix);prefixes[suffix]=data
                (case/'topics'/('committed-prefix-'+suffix+'.tsv')).write_text(data)
            require(h.terminate_identity(target,signal.SIGKILL),'worker vanished before SIGKILL')
            write(case/'fault.json',dict(worker=target,checkpoint=proof))
            def recovered():
                job=jobs(pipelines)['profiles']
                workers=[w for p in h.session_processes(child.pid) if (w:=h.worker(p,a.binary.resolve())) and w['pipeline']==profile_id and w['generation']>target['generation'] and w['job']==target['job'] and h.identity(w)!=h.identity(target)]
                return workers[0] if workers and job['state']=='Running' else None
            replacement=wait(recovered,'replacement profile worker',180)
            # The session owner is a separate stateful application pipeline.
            # Exercise its actual restoration before admitting the fresh suffix.
            _,_,session_source_offset=await_rows('session-signals',10,'initial-session-signals')
            session_pipeline=pipelines['sessions']['id']
            def session_target_ready():
                job=jobs(pipelines)['sessions']
                workers=[w for p in h.session_processes(child.pid) if (w:=h.worker(p,a.binary.resolve())) and w['pipeline']==session_pipeline]
                cps=api(a.port,f"pipelines/{session_pipeline}/jobs/{job['id']}/checkpoints")['data']
                complete=[cp for cp in cps if cp['finish_time'] is not None]
                return workers[0] if len(workers)==1 and complete else None
            session_target=wait(session_target_ready,'session checkpoint')
            require(h.terminate_identity(session_target,signal.SIGSTOP),'session worker vanished before pause')
            wait(lambda:(p:=h.process(session_target['pid'])) and h.identity(p)==h.identity(session_target) and p['state']=='T','frozen session worker',5)
            time.sleep(.25)
            job=jobs(pipelines)['sessions']
            cps=api(a.port,f"pipelines/{session_pipeline}/jobs/{job['id']}/checkpoints")['data']
            session_epoch=max((cp['epoch'] for cp in cps if cp['finish_time'] is not None))
            session_proof=checkpoint(h,case,session_target,session_epoch,'sessions')
            require(session_proof['source']['next_offset']==session_source_offset+1,'session checkpoint source prefix differs')
            write(case/'session-checkpoint-before-loss.json',session_proof)
            require(h.terminate_identity(session_target,signal.SIGKILL),'session worker vanished before SIGKILL')
            def session_recovered():
                job=jobs(pipelines)['sessions']
                workers=[w for p in h.session_processes(child.pid) if (w:=h.worker(p,a.binary.resolve())) and w['pipeline']==session_pipeline and w['generation']>session_target['generation'] and w['job']==session_target['job'] and h.identity(w)!=h.identity(session_target)]
                return workers[0] if workers and job['state']=='Running' else None
            session_replacement=wait(session_recovered,'replacement session worker',180)
            broker('topic','produce',a.namespace+'-raw-events',data=''.join(json.dumps(event)+'\n' for event in events[10:]))
            unified,_,_=await_rows('unified-events',13,'identity13-unified')
            merges,_,_=await_rows('identity-merges',2,'identity13-merges')
            actual=[dict(stream='unified-events',payload=row) for row in unified]+[dict(stream='identity-merges',payload=row) for row in merges]
            identity=compare(read_records(case/'identity-expected.jsonl'),actual)
            await_rows('profile-updates',13,'identity13-profile-flat')
            cdc,_,_=await_rows('profile-changelog',13,'identity13-profile-cdc')
            latest=latest_profiles(cdc)
            active=oracle.verify_profiles(unified,list(latest.values()),stage='active')
            after_recovery={}
            for suffix,prefix in prefixes.items():
                _,data,_=snapshot(suffix,'recovered-'+suffix)
                require(data.startswith(prefix),'committed topic prefix changed: '+suffix)
                after_recovery[suffix]=dict(prefix_sha256=hashlib.sha256(prefix.encode()).hexdigest(),prefix_bytes=len(prefix.encode()),recovered_bytes=len(data.encode()))
            result['completed_gates']=dict(identity=identity,profiles_active=active,
                profile_checkpoint=proof,profile_replacement=replacement,
                session_checkpoint=session_proof,session_replacement=session_replacement,
                topic_continuity=after_recovery)
            # Reserved controls enter internal execution only. Public unified
            # analytics remain the exact thirteen business records.
            marker=dict(events[-1],event_id='arc-clock-close',event_type='__arc_clock__',
                        tenant_id='__arc_clock__',canonical_id='__arc_clock__',anonymous_id='clock',
                        user_id=None,session_id=None,event_time='2026-01-01 00:31:00.000')
            broker('topic','produce',a.namespace+'-execution-events',data=json.dumps(marker)+'\n')
            def closed_ready():
                rows,_,_=snapshot('profile-changelog','closed-profiles')
                require(len(rows)<=18,'extra finite profile lifecycle output')
                if len(rows)<18:return None
                latest=latest_profiles(rows)
                try:
                    verdict=oracle.verify_profiles(unified,list(latest.values()),stage='closed')
                    return dict(verdict=verdict,latest=latest,rows=rows)
                except (AssertionError,ValueError): return None
            closed=wait(closed_ready,'native profile timeout feedback',300)
            sessions,_,_=await_rows('sessions',4,'closed-sessions')
            session_result=oracle.verify_sessions(unified,sessions)
            # New real input after recovery and timer closure must reuse the
            # same identity and start a second lifecycle segment.
            fresh=dict(events[-1],event_id='identity-14',event_time='2026-01-01 00:32:00.000',
                       event_type='page_view',session_id='recovered-session',page_url='/recovered',
                       feature_name='recovery-feature',country='NZ',device_type='mobile',browser='Safari',
                       properties='{"recovered":true}')
            prior=next(row for row in unified if row['event_id']=='identity-13')
            key=(prior['tenant_id'],prior['canonical_id']);before=closed['latest'][key]
            broker('topic','produce',a.namespace+'-raw-events',data=json.dumps(fresh)+'\n')
            final_unified,_,_=await_rows('unified-events',14,'fresh-unified')
            fresh_actual=next(row for row in final_unified if row['event_id']=='identity-14')
            require(fresh_actual['canonical_id']==prior['canonical_id'],'fresh recovered identity changed')
            require(fresh_actual==dict(fresh,canonical_id=prior['canonical_id']),'fresh full unified payload differs')
            def fresh_ready():
                rows,_,_=snapshot('profile-changelog','fresh-profile')
                require(len(rows)<=19,'extra finite recovered profile output')
                current=latest_profiles(rows).get(key)
                if len(rows)==19 and current and current['total_events']==before['total_events']+1:return current
                return None
            fresh_profile=wait(fresh_ready,'fresh native profile state',300)
            require(fresh_profile['total_sessions']==before['total_sessions']+1 and fresh_profile['current_session_active'],'fresh session reset/start differs')
            require(fresh_profile['first_seen']==before['first_seen'] and fresh_profile['avg_session_duration_sec']==before['avg_session_duration_sec'],'fresh recovered retained state differs')
            for field in ['events_1d','events_7d','events_30d','events_90d','sessions_1d','sessions_7d','sessions_30d','sessions_90d','page_views']:
                require(fresh_profile[field]==before[field]+1,'fresh profile counter differs: '+field)
            require(fresh_profile['current_session_duration_sec']==0 and fresh_profile['last_page']=='/recovered','fresh profile values differ')
            wanted=dict(before)
            for field in ['total_events','total_sessions','events_1d','events_7d','events_30d','events_90d',
                          'sessions_1d','sessions_7d','sessions_30d','sessions_90d','page_views']:
                wanted[field]+=1
            selected=[row for row in final_unified if (row['tenant_id'],row['canonical_id'])==key]
            for input_field,output_field,limit in [('page_url','top_pages',5),('feature_name','top_features',3)]:
                counts=Counter(row[input_field] for row in selected if row.get(input_field))
                wanted[output_field]=sorted(counts,key=lambda value:(-counts[value],value))[:limit]
            wanted.update(last_seen=oracle.ms(fresh['event_time']),current_session_active=True,
                          current_session_duration_sec=0,last_page='/recovered',last_country='NZ',
                          last_device='mobile',last_browser='Safari',action='update',trigger='event',
                          changed_fields=['total_events','total_sessions','last_seen','page_views','last_page','last_country','last_device','last_browser'])
            require(type(fresh_profile['updated_at']) is int and oracle.ms(fresh_profile['timestamp'])==fresh_profile['updated_at'],'fresh observation metadata differs')
            wanted.update(updated_at=None,timestamp=None)
            normalized=dict(fresh_profile,updated_at=None,timestamp=None)
            require(json.dumps(normalized,sort_keys=True)==json.dumps(wanted,sort_keys=True),'fresh complete full33 expected snapshot differs')
            final_cdc,_,_=await_rows('profile-changelog',19,'final-cdc')
            flat,_,_=await_rows('profile-updates',19,'final-profile-flat')
            encode=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'))
            require(list(map(encode,flat))==[encode(row['after']) for row in final_cdc],'native flat publication differs from complete CDC after-images')
            require(len(snapshot('sessions','final-sessions')[0])==4,'extra session summary before its deadline')
            require(len(snapshot('identity-merges','final-merges')[0])==2,'extra identity merge')
            runtime=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',(case/'runtime.log').read_text(errors='replace'))
            restore=[line for line in runtime.splitlines() if 'restoring checkpoint' in line.lower() and target['job'] in line]
            require(any(re.search(r"\bepoch="+str(proof['epoch'])+r"\b",line) for line in restore),'selected checkpoint restoration not confirmed')
            session_restore=[line for line in runtime.splitlines() if 'restoring checkpoint' in line.lower() and session_target['job'] in line]
            require(any(re.search(r"\bepoch="+str(session_proof['epoch'])+r"\b",line) for line in session_restore),'session checkpoint restoration not confirmed')
            result.update(status='passed',identity=identity,profiles_active=active,profiles_closed=closed['verdict'],sessions=session_result,
                          checkpoint=proof,fault_worker=target,replacement=replacement,restore_log_lines=restore,
                          session_recovery=dict(checkpoint=session_proof,fault_worker=session_target,replacement=session_replacement,restore_log_lines=session_restore),
                          topic_continuity=after_recovery,fresh_event=fresh_actual,fresh_profile=fresh_profile,
                          pipeline_count=len(pipelines),scope='Native Kafka application graph correctness/worker-loss; no production cutover or throughput claim')
        except BaseException as error:
            result['error']=repr(error)
            diagnostics=[]
            for topic in data_topics:
                suffix=topic.removeprefix(a.namespace+'-')
                for committed in [True,False]:
                    label='failure-'+('committed-' if committed else 'uncommitted-')+suffix
                    try:
                        rows,raw,offset=snapshot(suffix,label,committed=committed)
                        diagnostics.append(dict(topic=topic,committed=committed,rows=len(rows),last_offset=offset))
                    except BaseException as diagnostic_error:
                        diagnostics.append(dict(topic=topic,committed=committed,error=repr(diagnostic_error)))
            result['failure_topic_diagnostics']=diagnostics
            raise
        finally:
            result['cleanup']=h.cleanup(child,owned)
            if not result['cleanup']['complete']:result['status']='failed'
            result['elapsed_seconds']=time.monotonic()-started
            write(case/'result.json',result)
    require(result['status']=='passed','graph qualification failed')
    print(json.dumps(result))


def host(a):
    require(os.environ.get('RUST_BUILD_QUEUE_HELD'),'run via maintained scripts/dev-build shared queue')
    case=a.case.resolve();case.mkdir(parents=True,exist_ok=False)
    suffix=a.namespace.removeprefix('arc-mvp-')
    require(re.fullmatch('[a-z0-9][a-z0-9-]{0,45}',suffix),'invalid isolated namespace')
    network=a.namespace+'-network';broker=a.namespace+'-broker';engine=a.namespace+'-engine'
    commands=[]
    def command(argv,**kwargs):
        process=subprocess.run(argv,text=True,capture_output=True,**kwargs)
        commands.append(dict(command=argv,exit=process.returncode,stdout=process.stdout,stderr=process.stderr));write(case/'host-commands.json',commands)
        require(process.returncode==0,f'host command failed: {argv}: {process.stderr}')
        return process.stdout
    source=command(['git','-C',str(a.engine_source),'rev-parse','HEAD']).strip()
    require(source==a.plugin_source_commit,'UDF source checkout differs')
    require(not command(['git','-C',str(a.engine_source),'status','--porcelain']).strip(),'UDF source checkout is dirty')
    receipt=json.loads(a.binary_receipt.read_text());require(sha(a.binary)==receipt['production_binary_sha256'],'qualified binary hash differs')
    engine_image=json.loads(command(['podman','image','inspect',a.image]))[0]
    broker_image=json.loads(command(['podman','image','inspect',a.broker_image]))[0]
    write(case/'host-source.json',dict(engine_plugin_source=source,binary_sha256=sha(a.binary),
          binary_receipt=receipt,image_id=engine_image['Id'],broker_image_id=broker_image['Id'],
          engine_image=engine_image,broker_image=broker_image))
    # Resolve mutable references once, then execute precisely the inspected images.
    a.image=engine_image['Id'];a.broker_image=broker_image['Id']
    shutil.copy2(Path(__file__),case/'supervisor.py');shutil.copy2(a.oracle,case/'oracle.py')
    reference=ROOT/'flink/identity-resolution/src/test/resources/reference'
    shutil.copy2(reference/'identity-input.json',case/'identity-input.json');shutil.copy2(reference/'identity-expected.jsonl',case/'identity-expected.jsonl')
    for kind,name in [('network',network),('container',broker),('container',engine)]:
        require(subprocess.run(['podman',kind,'exists',name],capture_output=True).returncode==1,'isolated resource already exists: '+name)
    started=False;created_network=None;network_id=None;broker_id=None;engine_cid=case/'engine.cid'
    try:
        created_network=command(['podman','network','create',network]).strip()
        network_receipt=json.loads(command(['podman','network','inspect',created_network]))[0]
        network_id=network_receipt.get('id',network_receipt.get('Id'))
        require(network_id,'created network has no resource ID')
        write(case/'network-source.json',network_receipt)
        broker_id=command(['podman','run','-d','--name',broker,'--network',network,'--network-alias','broker',
                 '--entrypoint','/usr/bin/rpk',a.broker_image,'redpanda','start','--mode=dev-container',
                 '--smp=1','--memory=512M','--reserve-memory=0M','--overprovisioned',
                 '--kafka-addr=internal://0.0.0.0:9092','--advertise-kafka-addr=internal://broker:9092']).strip()
        started=True
        for _ in range(90):
            response=subprocess.run(['podman','exec',broker,'rpk','cluster','info','-X','brokers=localhost:9092'],capture_output=True,text=True)
            if response.returncode==0:break
            time.sleep(1)
        else:raise ValueError('isolated broker did not start')
        command(['podman','cp',broker+':/opt/redpanda/libexec/rpk',str(case/'rpk')])
        argv=['podman','run','--rm','--cidfile',str(engine_cid),'--name',engine,'--network',network,'--security-opt','label=disable',
              '-v',str(ROOT)+':'+str(ROOT)+':ro','-v',str(case)+':'+str(case)+':rw',
              '-v',str(a.qualification_root)+':'+str(a.qualification_root)+':ro',
              '-v',str(a.engine_source)+':/app:ro','-v','/home/jason/repos/streamr/target:/app/target:rw',
              '-v','streamr-cargo-registry:/usr/local/cargo/registry','-v','streamr-cargo-git:/usr/local/cargo/git',
              '-v','streamr-sccache:/var/cache/sccache','-v',str(a.pyarrow_root)+':'+str(a.pyarrow_root)+':ro',
              '-e','RUST_BUILD_QUEUE_HELD='+os.environ['RUST_BUILD_QUEUE_HELD'],
              '-e','LD_LIBRARY_PATH='+str(a.binary.parent.parent/'lib'),'-e','PYTHONPATH='+str(a.pyarrow_root),
              '-e','CARGO_BUILD_JOBS=4','-e','CARGO_INCREMENTAL=0',a.image,
              'python3',str(ROOT/'test/streamr-reference/native_mvp_kafka.py'),'--inner','--case',str(case),
              '--binary',str(a.binary),'--engine-helper','/app/scripts/test-native-process-recovery.py',
              '--config-template',str(a.config_template),'--plugin-source-commit',a.plugin_source_commit,
              '--namespace',a.namespace,'--backend',a.backend,'--port',str(a.port)]
        with (case/'container.log').open('w') as log:
            process=subprocess.run(argv,stdout=log,stderr=subprocess.STDOUT,timeout=a.timeout)
        write(case/'container-command.json',dict(command=argv,exit=process.returncode))
        require(process.returncode==0,'native Kafka graph failed; see retained container/runtime logs')
    finally:
        if started:
            logs=subprocess.run(['podman','logs',broker],capture_output=True,text=True);(case/'broker.log').write_text(logs.stdout+logs.stderr)
        cleanup=[];remaining=[]
        if created_network and not network_id:
            remaining.append('created network identity unresolved: '+created_network)
        owned_containers=[value for value in [engine_cid.read_text().strip() if engine_cid.exists() else None,broker_id] if value]
        for container_id in owned_containers:
            process=subprocess.run(['podman','rm','-f',container_id],capture_output=True,text=True)
            cleanup.append(dict(resource=container_id,exit=process.returncode,stdout=process.stdout,stderr=process.stderr))
            if subprocess.run(['podman','container','exists',container_id],capture_output=True).returncode==0:remaining.append(container_id)
        if network_id:
            process=subprocess.run(['podman','network','rm',network_id],capture_output=True,text=True)
            cleanup.append(dict(resource=network_id,exit=process.returncode,stdout=process.stdout,stderr=process.stderr))
            if subprocess.run(['podman','network','exists',network_id],capture_output=True).returncode==0:remaining.append(network_id)
        write(case/'host-cleanup.json',dict(actions=cleanup,remaining=remaining,complete=not remaining))
        require(not remaining,'isolated Kafka graph resources remain after cleanup')


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case',type=Path,required=True)
    parser.add_argument('--namespace',required=True)
    parser.add_argument('--binary',type=Path,required=True)
    parser.add_argument('--config-template',type=Path,required=True)
    parser.add_argument('--engine-helper',type=Path)
    parser.add_argument('--engine-source',type=Path)
    parser.add_argument('--plugin-source-commit',required=True)
    parser.add_argument('--binary-receipt',type=Path)
    parser.add_argument('--oracle',type=Path)
    parser.add_argument('--inner',action='store_true')
    parser.add_argument('--backend',choices=['memory','rocksdb'],default='rocksdb')
    parser.add_argument('--port',type=int,default=18421)
    parser.add_argument('--timeout',type=int,default=2400)
    parser.add_argument('--image',default='bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e')
    parser.add_argument('--broker-image',default='docker.io/redpandadata/redpanda:v24.3.7')
    parser.add_argument('--qualification-root',type=Path,default=Path('/home/jason/qa-evidence/str126-qualification-20261017'))
    parser.add_argument('--pyarrow-root',type=Path,default=Path('/home/jason/qa-evidence/str32-20261010-52391450/python-runtime'))
    args=parser.parse_args()
    inner(args) if args.inner else host(args)
