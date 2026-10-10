#!/usr/bin/env python3
"""Prepare-only by default; --execute is coordinator-owned serial runtime."""
import argparse, hashlib, json, os, pathlib, subprocess, re
ROOT=pathlib.Path(__file__).resolve().parent
REPO=ROOT.parents[2]
def strict_pairs(pairs):
    result={}
    for key,value in pairs:
        assert key not in result, ('duplicate JSON key',key)
        result[key]=value
    return result

def recovered_committed_rows(log):
    records=[line for line in log.splitlines() if line.startswith('CAPTURE_RESULT phase=recovered ')]
    assert len(records)==1, ('expected exactly one recovered capture receipt',records)
    fields=dict(re.findall(r'(\w+)=([^ ]+)',records[0]))
    assert fields['input_rows_before_checkpoint']=='2' and fields['checkpoint']=='1'
    committed=int(fields['committed_rows']); assert committed>0
    return committed

def typed_equal(actual,wanted):
    return type(actual) is type(wanted) and (all(type(actual[k]) is type(v) and actual[k]==v for k,v in wanted.items()) if isinstance(wanted,dict) and set(actual)==set(wanted) else actual==wanted)

def compare(path, expected, committed_rows=None):
    previous={}; last_prefix={}; count=0
    for line in path.read_text().splitlines():
        item=json.loads(line,object_pairs_hook=strict_pairs); assert set(item)=={'before','after','op'}
        before=item['before']; after=item['after']; assert isinstance(after,dict), ('unexpected deletion/null',path,count)
        key=(after['tenant_id'],after['canonical_id']); assert item['op']==('c' if before is None else 'u')
        assert (before is None if key not in previous else typed_equal(before,previous[key])), ('typed CDC chain mismatch',path,count,key)
        candidates=[]
        for snapshot in expected:
            if snapshot['source_prefix']<last_prefix.get(key,0): continue
            for wanted in snapshot['rows']:
                if (wanted['tenant_id'],wanted['canonical_id'])==key and typed_equal(after,wanted): candidates.append(snapshot['source_prefix'])
        assert candidates, ('complete typed25 row not any independent monotonic prefix',path,count,key,after)
        last_prefix[key]=min(candidates);previous[key]=after;count+=1
        if committed_rows is not None and count==committed_rows:
            checkpoint={(r['tenant_id'],r['canonical_id']):r for r in expected[1]['rows']}
            assert previous==checkpoint, ('committed checkpoint state not exact source-prefix2',path,count)
    assert count and previous=={(r['tenant_id'],r['canonical_id']):r for r in expected[-1]['rows']}, ('full final25 state mismatch',path)
    if committed_rows is not None: assert count>committed_rows, ('missing recovered suffix',path)
    return {'rows':count,'keys':len(previous),'all25_typed_fields':True,'exact_perkey_CDC_chain':True,'committed_source_prefix':2 if committed_rows else None}

def executable_provenance(receipt_bytes, expected_source_commit, binary):
    assert re.fullmatch('[0-9a-f]{40}',expected_source_commit), 'expected source commit must be full40hex'
    receipt=json.loads(receipt_bytes,object_pairs_hook=strict_pairs)
    commits=[receipt[k] for k in ('source_commit','commit','head') if k in receipt]
    assert commits and all(value==expected_source_commit for value in commits), ('source commit mismatch',commits,expected_source_commit)
    digest=hashlib.sha256(binary.read_bytes()).hexdigest()
    hashes=[receipt[k] for k in ('binary_sha256','executable_sha256','sql_binary_sha256') if k in receipt]
    assert hashes and all(value==digest for value in hashes), ('executable receipt hash mismatch',hashes,digest)
    if 'build_exit' in receipt:
        assert type(receipt['build_exit']) is int and receipt['build_exit']==0, 'build_exit must be integer0'
    return receipt,digest

def checkpoint_proof(target, expected, committed_rows):
    checkpoint=target/'output.checkpoint-1.jsonl'; recovered=target/'output.jsonl'
    prefix=checkpoint.read_bytes(); suffix=recovered.read_bytes()
    assert prefix and prefix.endswith(b'\n'), 'checkpoint must contain complete JSONL records'
    assert len(prefix.splitlines())==committed_rows, ('checkpoint physical count mismatch',committed_rows)
    assert suffix.startswith(prefix), 'recovered output changed committed checkpoint bytes'
    assert len(suffix.splitlines())>committed_rows, 'recovered continuation suffix missing'
    checkpoint_comparison=compare(checkpoint,expected[:2])
    return {'physical_rows':committed_rows,'checkpoint_bytes':len(prefix),'checkpoint_sha256':hashlib.sha256(prefix).hexdigest(),'recovered_byte_prefix_identical':True,'fresh_suffix_rows':len(suffix.splitlines())-committed_rows,'comparison':checkpoint_comparison}

def main():
    p=argparse.ArgumentParser(); p.add_argument('--run-dir',required=True); p.add_argument('--binary',required=True); p.add_argument('--execute',action='store_true'); p.add_argument('--executable-receipt'); p.add_argument('--expected-source-commit'); p.add_argument('--backend',choices=['memory','rocksdb'],default='memory'); p.add_argument('--checkpoint-mode',choices=['controller','leader'],default='controller'); p.add_argument('--batch-rows',type=int,choices=[1,8],default=1); p.add_argument('--query',default=str(REPO/'deploy/streamr-native/profile-session-counts/query.sql'));  a=p.parse_args()
    if bool(a.executable_receipt)!=bool(a.expected_source_commit): p.error('--executable-receipt and --expected-source-commit must be supplied together')
    receipt=json.loads((ROOT/'preparation-receipt.json').read_text(),object_pairs_hook=strict_pairs)
    binary=pathlib.Path(a.binary).resolve(strict=True)
    receipt_bytes=None; selected_receipt=None
    if a.executable_receipt:
        receipt_path=pathlib.Path(a.executable_receipt).resolve(strict=True)
        receipt_bytes=receipt_path.read_bytes()
        selected_receipt,digest=executable_provenance(receipt_bytes,a.expected_source_commit,binary)
    else:
        digest=hashlib.sha256(binary.read_bytes()).hexdigest(); assert digest==receipt['binary_sha256']
    target=pathlib.Path(a.run_dir).resolve(); target.mkdir(parents=True,exist_ok=False)
    if receipt_bytes is not None: (target/'executable-receipt.original.json').write_bytes(receipt_bytes)
    (target/'expected-prefixes.json').write_bytes((ROOT/'expected-prefixes.json').read_bytes())
    (target/'input.jsonl').write_bytes((ROOT/'input.jsonl').read_bytes())
    query=pathlib.Path(a.query).read_text().replace('{{INPUT}}',str(target/'input.jsonl')).replace('{{OUTPUT}}',str(target/'output.jsonl'))
    (target/'query.sql').write_text(query)
    env={'STREAMR_TEST_TYPED_SQL':'1','STREAMR_TEST_AGGREGATE_FLUSH_SECONDS':'3600','STREAMR_TEST_EXECUTION_BYTES':'16777216','STREAMR_TEST_MAX_OPEN_DATABASES':'6','STREAMR_TEST_MAX_SNAPSHOTS':'6','STREAMR_TEST_SCAN_PAGE_BYTES':str(5*1024*1024 if a.backend=='rocksdb' else 4*1024*1024),'STREAMR_TEST_QUEUED_WRITE_BYTES':str(64*1024*1024 if a.backend=='rocksdb' else 32*1024*1024),'STREAMR_TEST_BACKEND':a.backend,'STREAMR_TEST_CHECKPOINT_MODE':a.checkpoint_mode,'STREAMR_TEST_SOURCE_BATCH_ROWS':str(a.batch_rows),'STREAMR_CAPTURE_QUERY':str(target/'query.sql'),'STREAMR_CAPTURE_OUTPUT':str(target/'output.jsonl'),'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT':'2','STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS':'2','STREAMR_CAPTURE_MAX_INITIAL_ROWS':'32','STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS':'2','STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS':'32','STREAMR_CAPTURE_EXPECTED_ROWS':'4','STREAMR_CAPTURE_MAX_ROWS':'32','STREAMR_CAPTURE_CHECKPOINT_EPOCH':'1','STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS':'180','TMPDIR':str(target)}
    env.update({'ARROYO__WORKER__AGGREGATE_STATE__KEY_BYTES': '512', 'ARROYO__WORKER__AGGREGATE_STATE__VALUE_BYTES': '32768', 'ARROYO__WORKER__AGGREGATE_STATE__PAGE_BYTES': '131072', 'ARROYO__WORKER__AGGREGATE_STATE__PAGE_ENTRIES': '64', 'ARROYO__WORKER__AGGREGATE_STATE__WRITE_BYTES': '2097152', 'ARROYO__WORKER__AGGREGATE_STATE__WRITE_OPERATIONS': '128', 'ARROYO__WORKER__AGGREGATE_STATE__OVERLAY_BYTES': '2097152', 'ARROYO__WORKER__AGGREGATE_STATE__MAX_PENDING_OUTPUT_ROWS': '64', 'ARROYO__WORKER__AGGREGATE_STATE__MAX_PENDING_OUTPUT_BYTES': '524288', 'ARROYO__WORKER__AGGREGATE_STATE__MAX_RESIDENT_BYTES': '134217728', 'ARROYO__WORKER__EXECUTION_RESOURCES__MEMORY_BYTES': '16777216', 'ARROYO__WORKER__EXECUTION_RESOURCES__MAX_BATCH_BYTES': '1048576'})
    if a.backend=='rocksdb': env['ARROYO__WORKER__AGGREGATE_STATE__MAX_PENDING_OUTPUT_BYTES']='131072'
    argv=[str(binary),'--ignored','--exact','smoke_tests::external_sql_checkpoint_capture','--nocapture','--test-threads=1']
    plan={'argv':argv,'env':env,'binary_sha256':digest,'status':'prepared','scope':'25-field transition/calendar diagnostic; timeout-reset and full33 application parity unqualified','runner_sha256':hashlib.sha256(pathlib.Path(__file__).read_bytes()).hexdigest(),'executable_receipt':selected_receipt,'executable_receipt_sha256':hashlib.sha256(receipt_bytes).hexdigest() if receipt_bytes is not None else None,'expected_source_commit':a.expected_source_commit,'pin_mode':'explicit executable/source receipt' if receipt_bytes is not None else 'historical preparation receipt pin'}
    (target/'command-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
    if not a.execute: return
    clean_env={k:v for k,v in os.environ.items() if not k.startswith(('STREAMR_TEST_','STREAMR_CAPTURE_','ARROYO__'))}
    plan['status']='failed'
    try:
        assert hashlib.sha256(binary.read_bytes()).hexdigest()==digest, 'binary changed before launch'
        if receipt_bytes is not None:
            assert receipt_path.read_bytes()==receipt_bytes, 'executable receipt changed before launch'
            assert executable_provenance(receipt_bytes,a.expected_source_commit,binary)[1]==digest
        with (target/'runtime.log').open('w') as log:
            result=subprocess.run(argv,env={**clean_env,**env},stdout=log,stderr=subprocess.STDOUT,timeout=210)
        plan['exit']=result.returncode
        log_text=(target/'runtime.log').read_text()
        assert result.returncode==0 and '1 passed;' in log_text, 'runtime failed; inspect preserved log'
        expected=json.loads((target/'expected-prefixes.json').read_text(),object_pairs_hook=strict_pairs)
        committed=recovered_committed_rows(log_text); plan['recovered_committed_rows']=committed
        plan['checkpoint_proof']=checkpoint_proof(target,expected,committed)
        plan['comparisons']={'initial':compare(target/'output.initial.jsonl',expected),'recovered':compare(target/'output.jsonl',expected,committed)}
        plan['status']='pass'
    except Exception as error:
        plan['error']={'type':type(error).__name__,'message':str(error)}
        raise
    finally:
        (target/'result.json').write_text(json.dumps(plan,indent=2)+'\n')
if __name__=='__main__': main()
