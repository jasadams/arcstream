#!/usr/bin/env python3
"""Owned five-pipeline native graph qualification; no application computation."""
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
import uuid
import native_profile_runtime as p
import native_sql_runtime as n


def normalized(rows, aliases):
    result=[]
    for row in rows:
        row=dict(row)
        for field in ('canonical_id','old_canonical_id'):
            if field in row:
                key=(row['tenant_id'],row[field])
                if key not in aliases: raise ValueError('unknown canonical identity')
                row[field]=aliases[key]
        result.append(row)
    return result


def identity_oracle(raw, accepted, unified):
    """Canonical UUIDs are nondeterministic; equivalence classes are literal."""
    expected=[x for x in raw if x['event_id'] in accepted]
    if len(unified)!=len(expected): raise ValueError('unified event count differs')
    aliases={}; owners={}
    for want,got in zip(expected,unified):
        if set(got)!=set(want)|{'canonical_id'}: raise ValueError('unified schema differs')
        if {k:v for k,v in got.items() if k!='canonical_id'}!=want: raise ValueError('raw payload or event order differs')
        canonical=got['canonical_id'];uuid.UUID(canonical)
        if canonical in owners and owners[canonical]!=got['tenant_id']:raise ValueError('canonical reused across tenants')
        owners[canonical]=got['tenant_id']
        aliases[(got['tenant_id'],canonical)]='c1'
    for tenant in {x['tenant_id'] for x in expected}:
        if sum(k[0]==tenant for k in aliases)!=1:raise ValueError('anonymous/user binding equivalence differs')
    return aliases


def decode_profiles(rows):
    """Validate every native CDC transition; only map its public c/u action."""
    current={};decoded=[]
    for envelope in rows:
        if not isinstance(envelope,dict) or not {'before','after','op'} <= set(envelope):
            raise ValueError('profile requires native CDC envelope')
        before,after,op=(envelope[k] for k in ('before','after','op'))
        if op not in ('c','u') or not isinstance(after,dict) or len(after)!=21 or 'action' in after:
            raise ValueError('unexpected profile CDC operation/schema')
        key=(after['tenant_id'],after['canonical_id'])
        if op=='c':
            if before is not None or key in current:raise ValueError('duplicate or invalid create')
        elif before!=current.get(key) or key not in current:
            raise ValueError('CDC before image does not equal preceding complete image')
        current[key]=dict(after);decoded.append(dict(after,action={'c':'create','u':'update'}[op]))
    return decoded


def check_sessions(rows, expected, aliases):
    rows=normalized(rows,aliases)
    p.check_snapshots(rows,expected,['tenant_id','session_id'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ('case','binary','binary-receipt','engine-helper','config-template','queries','fixtures','kafka-rpk'):
        parser.add_argument('--'+key,type=Path,required=True)
    parser.add_argument('--namespace',required=True);parser.add_argument('--kafka-broker',required=True)
    parser.add_argument('--timeout',type=int,default=720);parser.add_argument('--port',type=int,default=18428)
    a=parser.parse_args();case=a.case.resolve();case.mkdir(parents=True,exist_ok=False)
    spec=importlib.util.spec_from_file_location('engine_recovery',a.engine_helper);h=importlib.util.module_from_spec(spec);spec.loader.exec_module(h)
    raw=[json.loads(x) for x in (a.fixtures/'input.jsonl').read_text().splitlines()]
    stages=json.loads((a.fixtures/'expected-prefixes.json').read_text());p.validate_stages(stages,len(raw),1)
    config=json.loads(a.config_template.read_text());config['checkpoint-url']=str(case/'checkpoints');config['database']['sqlite']['path']=str(case/'catalog.sqlite')
    config['default-checkpoint-interval']='3s';config['api'].update({'http-port':a.port,'run-http-port':a.port});config['worker']['sql-state-backend']='rocksdb';config['worker']['disk-sql-state']['directory']=str(case/'live-state');config.pop('compiler',None);h.write(case/'config.yaml',config)
    receipt=json.loads(a.binary_receipt.read_text());h.require(n.digest(a.binary)==receipt.get('binary_sha256',receipt.get('production_binary_sha256')),'binary hash mismatch')
    result=dict(status='failed',binary_receipt=receipt,files={str(x):n.digest(x) for x in (Path(__file__),Path(p.__file__),Path(n.__file__),a.engine_helper,a.config_template,a.fixtures/'input.jsonl',a.fixtures/'expected-prefixes.json',*(a.queries/(name+'-request.json') for name in ('identity','unified','merges','sessions','profiles')))},stages=[],faults=[],captures=[])
    started=time.monotonic();owned={};pipelines={};prefixes={};capture_receipts={};timeline=[];h.enable_subreaper()
    def remaining():
        r=a.timeout-(time.monotonic()-started)
        if r<=0:raise TimeoutError('graph deadline reached')
        return r
    def request(path,payload=None,method=None):return n.request(a.port,path,payload,timeout=min(10,remaining()),method=method)
    def capture(suffix):
        rows,receipt=p.read_committed(a.kafka_rpk,a.kafka_broker,a.namespace+'-'+suffix,min(30,remaining()))
        capture_receipts[suffix]=receipt
        filename=f"capture-{len(result['captures'])}-{suffix}.json";h.write(case/filename,receipt);result['captures'].append(filename)
        if suffix in prefixes and not n.records_equal(rows[:len(prefixes[suffix])],prefixes[suffix]):raise ValueError('committed prefix changed: '+suffix)
        return rows
    command=[str(a.binary),'--config',str(case/'config.yaml'),'cluster'];result['command']=command
    clean={k:v for k,v in os.environ.items() if not k.startswith(('STREAMR_TEST_','STREAMR_CAPTURE_','ARROYO__'))};clean['RUST_LOG']='info'
    with (case/'runtime.log').open('wb') as log:
      child=subprocess.Popen(command,stdout=log,stderr=subprocess.STDOUT,cwd=case,env=clean,start_new_session=True)
      try:
        while True:
            h.require(child.poll() is None,'cluster exited before API')
            try:request('pipelines');break
            except (OSError,RuntimeError):remaining();time.sleep(.2)
        for name in ('profiles','sessions','merges','unified','identity'):
            payload=json.loads((a.queries/(name+'-request.json')).read_text());payload['checkpoint_interval_micros']=3000000
            valid=request('pipelines/validate_query',{'query':payload['query'],'udfs':[]});h.write(case/(name+'-validation.json'),valid);h.require(not valid['errors'],'query rejected: '+name)
            pipelines[name]=request('pipelines',payload)
        h.write(case/'pipelines.json',pipelines)
        def inventory():
            ps=h.session_processes(child.pid);owned.update({h.identity(x):x for x in ps});workers=[w for x in ps if (w:=h.worker(x,a.binary.resolve()))]
            jobs={name:request(f"pipelines/{pipe['id']}/jobs")['data'][0] for name,pipe in pipelines.items()}
            for name,job in jobs.items():h.require(job['state']!='Failed','pipeline failed: '+name)
            timeline.append(dict(elapsed=time.monotonic()-started,jobs=jobs,workers=workers));return jobs,workers
        def wait_ready():
            while True:
                jobs,workers=inventory()
                if all(j['state']=='Running' for j in jobs.values()):return jobs,workers
                remaining();time.sleep(.2)
        def selected(name,jobs,workers):
            job=jobs[name];matches=[w for w in workers if w['job']==job['id'] and w['pipeline']==pipelines[name]['id']]
            if len(matches)!=1:raise ValueError('expected exactly one actual owner process for '+name)
            cps=request(f"pipelines/{pipelines[name]['id']}/jobs/{job['id']}/checkpoints")['data'];done=[x for x in cps if x['finish_time'] is not None]
            if not done:return None,None
            owner=matches[0];cp=p.checkpoint(h,case,owner,max(x['epoch'] for x in done))
            if name=='identity':h.require(sum(x['kind']==4 for x in cp['owners'])==2,'identity must checkpoint both native typed tables')
            if name=='profiles':h.require(any(x['kind']==3 for x in cp['owners']),'profiles disk-backed aggregate inventory absent')
            return owner,cp
        wait_ready()
        for index,stage in enumerate(stages):
            previous=0 if index==0 else stages[index-1]['input_lines'];payload=''.join(json.dumps(x)+'\n' for x in raw[previous:stage['input_lines']])
            argv=[str(a.kafka_rpk),'topic','produce',a.namespace+'-raw-events','-X','brokers='+a.kafka_broker]
            produced=subprocess.run(argv,input=payload,text=True,capture_output=True,check=True,timeout=min(30,remaining()));h.write(case/f'produce-{index}.json',dict(command=argv,stdout=produced.stdout,stderr=produced.stderr,input=payload))
            while True:
                jobs,workers=wait_ready();owner,cp=selected('identity',jobs,workers)
                if cp and cp['source_lines']==stage['input_lines']:
                    unified=capture('unified-events')
                    if len(unified)>len(stage['accepted_event_ids']):raise ValueError('extra unified output')
                    if len(unified)==len(stage['accepted_event_ids']):
                        aliases=identity_oracle(raw,stage['accepted_event_ids'],unified)
                        profiles=capture('profiles');complete=p.check_snapshots(normalized(decode_profiles(profiles),aliases),stage['values'],['tenant_id','canonical_id'],partial=True)
                        sessions=capture('sessions')
                        if len(sessions)>len(stage['sessions']):raise ValueError('extra completed session')
                        merges=capture('identity-merges');h.require(not merges,'unexpected identity merge')
                        profile_owner,profile_cp=selected('profiles',jobs,workers)
                        data_offsets=[r['offset'] for r in capture_receipts['unified-events']['records'] if not r['control']]
                        expected_profile_next=max(data_offsets)+1
                        if complete and len(sessions)==len(stage['sessions']) and profile_cp and profile_cp['source_lines']==expected_profile_next:
                            check_sessions(sessions,stage['sessions'],aliases);break
                remaining();time.sleep(.25)
            result['stages'].append(dict(input_lines=stage['input_lines'],profiles=profiles,unified=unified,sessions=sessions,identity_checkpoint=cp))
            h.write(case/f'stage-{index}.json',result['stages'][-1])
            # Kill separate actual owners while committed profile/session state is open.
            if index in (1,4):
                name={1:'profiles',4:'identity'}[index]
                while True:
                    jobs,workers=wait_ready();target,cp=selected(name,jobs,workers)
                    expected_offset=stage['input_lines'] if name=='identity' else expected_profile_next
                    if cp and cp['source_lines']==expected_offset:break
                    remaining();time.sleep(.2)
                h.require(h.terminate_identity(target,signal.SIGSTOP),'pause failed')
                while True:
                    current=h.process(target['pid'])
                    if current and h.identity(current)==h.identity(target) and current['state']=='T':break
                    remaining();time.sleep(.01)
                jobs,workers=inventory();_,cp=selected(name,jobs,workers)
                h.require(cp['source_lines']==expected_offset,'paused checkpoint source boundary differs')
                for topic in ('unified-events','profiles','sessions','identity-merges'):prefixes[topic]=capture(topic)
                h.require(bool(prefixes['profiles']),'empty committed profile fault prefix')
                metadata=case/'checkpoints'/target['job']/'checkpoints'/f"checkpoint-{cp['epoch']:07}"
                shutil.copytree(metadata,case/(name+'-selected-checkpoint'))
                for relative in cp['artifacts']:
                    dest=case/(name+'-retained-artifacts')/relative;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(case/'checkpoints'/relative,dest)
                h.require(h.terminate_identity(target,signal.SIGKILL),'kill failed')
                while True:
                    jobs,workers=inventory();fresh=[w for w in workers if w['job']==target['job'] and w['pipeline']==target['pipeline'] and w['generation']>target['generation'] and h.identity(w)!=h.identity(target)]
                    if fresh and jobs[name]['state']=='Running':break
                    remaining();time.sleep(.2)
                text=re.sub(r'\x1b\[[0-?]*[ -/]*[@-~]','',(case/'runtime.log').read_text(errors='replace'))
                restore=[line for line in text.splitlines() if 'restoring checkpoint' in line.lower() and target['job'] in line and re.search(r'epoch='+str(cp['epoch'])+r'(?:\D|$)',line)]
                h.require(bool(restore),'selected checkpoint restore absent')
                result['faults'].append(dict(pipeline=name,worker=target,replacement=fresh,checkpoint=cp,committed_prefixes=dict(prefixes),restore=restore));h.write(case/'faults.json',result['faults'])
        for name,pipe in pipelines.items():request(f"pipelines/{pipe['id']}",{'stop':'checkpoint'},method='PATCH')
        while True:
            jobs,workers=inventory()
            if all(j['state']=='Stopped' for j in jobs.values()):break
            remaining();time.sleep(.2)
        final=capture('profiles');aliases=identity_oracle(raw,stages[-1]['accepted_event_ids'],capture('unified-events'));p.check_snapshots(normalized(decode_profiles(final),aliases),stages[-1]['values'],['tenant_id','canonical_id']);check_sessions(capture('sessions'),stages[-1]['sessions'],aliases);h.require(not capture('identity-merges'),'unexpected final merge')
        h.require(len(final)>len(prefixes['profiles']),'no fresh profile after recovery');result.update(status='passed',profiles=final,identity_merges='empty: directed merge not exercised')
      except BaseException as error:result['error']=type(error).__name__+': '+str(error);raise
      finally:
        result['cleanup']=h.cleanup(child,owned)
        if not result['cleanup']['complete']:result['status']='failed'
        result['seconds']=time.monotonic()-started;h.write(case/'timeline.json',timeline);h.write(case/'result.json',result)
    h.require(result['status']=='passed','cleanup failed');print(json.dumps(result))

if __name__=='__main__':main()
