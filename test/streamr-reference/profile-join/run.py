#!/usr/bin/env python3
"""Prepare-safe diagnostic for the unchanged original LEFT JOIN application SQL."""
import argparse, collections, hashlib, json, os, pathlib, re, subprocess
ROOT=pathlib.Path(__file__).resolve().parent

def digest(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def strict_pairs(pairs):
    result={}
    for key,value in pairs:
        if key in result: raise AssertionError(('duplicate JSON key',key))
        result[key]=value
    return result

def read_json(path): return json.loads(path.read_text(),object_pairs_hook=strict_pairs)
def row_token(row, schema):
    assert isinstance(row,dict) and set(row)==set(schema), ('20-field schema mismatch',row)
    for key,value in schema.items():
        assert type(row[key]) is type(value), ('wrong field type',key,row[key])
        if isinstance(value,list): assert all(type(x) is str for x in row[key]), ('wrong array member type',key)
    return json.dumps(row,sort_keys=True,separators=(',',':'),ensure_ascii=False)

def compare(path, final_row, checkpoint_row=None, committed_rows=None):
    """Sink-visible relational bag only; intermediate values are NOT prefix oracles."""
    bag=collections.Counter(); records=0; creates=updates=deletes=noops=0
    for line in path.read_text().splitlines():
        item=json.loads(line,object_pairs_hook=strict_pairs);assert set(item)=={'before','after','op'}
        old,new,op=item['before'],item['after'],item['op']
        assert op in ('c','u','d'), ('unsupported CDC op',op)
        assert (old is None and new is not None) if op=='c' else ((old is not None and new is not None) if op=='u' else (old is not None and new is None)), ('invalid CDC envelope',item)
        old_token=row_token(old,final_row) if old is not None else None
        new_token=row_token(new,final_row) if new is not None else None
        if old_token is not None:
            assert bag[old_token]>0, ('before-image absent from current exact bag',records,old)
            bag[old_token]-=1
            if not bag[old_token]: del bag[old_token]
        if new_token is not None: bag[new_token]+=1
        creates+=op=='c';updates+=op=='u';deletes+=op=='d';noops+=op=='u' and old_token==new_token;records+=1
        if committed_rows is not None and records==committed_rows:
            assert bag==collections.Counter({row_token(checkpoint_row,final_row):1}), ('checkpoint source-prefix1 exact typed20 bag mismatch',path,bag)
    assert bag==collections.Counter({row_token(final_row,final_row):1}), ('final source-prefix4 exact typed20 bag mismatch',path,bag)
    assert records>0
    if committed_rows is not None: assert records>committed_rows>0, ('invalid committed boundary',records,committed_rows)
    return {'records':records,'creates':creates,'updates':updates,'deletes':deletes,'noops':noops,'final_multiplicity':1,'all20_types_checked':True,'exact_before_current_bag':True,'intermediate_whole_event_prefix_checked':False,'accepted_application_emission_parity':False}

def committed(log):
    matches=[line for line in log.splitlines() if line.startswith('CAPTURE_RESULT phase=recovered ')]
    assert len(matches)==1, ('one recovered receipt required',matches)
    fields=dict(re.findall(r'(\w+)=([^ ]+)',matches[0]));assert fields['input_rows_before_checkpoint']=='1' and fields['checkpoint']=='1'
    value=int(fields['committed_rows']);assert value>0
    return value

def checkpoint_proof(checkpoint_path, recovered_path, expected, committed_count):
    prefix=checkpoint_path.read_bytes(); recovered=recovered_path.read_bytes()
    assert prefix and prefix.endswith(b'\n'), 'checkpoint prefix must end in complete JSONL record'
    assert len(prefix.splitlines())==committed_count, ('checkpoint physical count mismatch',len(prefix.splitlines()),committed_count)
    assert recovered.startswith(prefix), 'recovered output must preserve exact committed checkpoint bytes'
    assert len(recovered.splitlines())>committed_count, 'recovered suffix missing'
    result=compare(checkpoint_path,expected)
    return {**result,'committed_rows':committed_count,'checkpoint_sha256':hashlib.sha256(prefix).hexdigest(),'checkpoint_bytes':len(prefix),'recovered_byte_prefix_identical':True}

def provenance(receipt, expected_commit, binary):
    assert re.fullmatch('[0-9a-f]{40}',expected_commit), 'expected commit must be full40hex'
    commits=[receipt[k] for k in ('source_commit','commit','head') if k in receipt]
    assert commits and all(x==expected_commit for x in commits), ('source receipt commit mismatch',commits,expected_commit)
    hashes=[receipt[k] for k in ('binary_sha256','executable_sha256','sql_binary_sha256') if k in receipt]
    actual=digest(binary);assert hashes and all(x==actual for x in hashes), ('ELF receipt hash mismatch',hashes,actual)
    assert type(receipt.get('build_exit')) is int and receipt['build_exit']==0, 'successful build receipt required'
    return actual

def environment(target, backend, mode, batch):
    config={'STREAMR_TEST_BACKEND':backend,'STREAMR_TEST_CHECKPOINT_MODE':mode,'STREAMR_TEST_SOURCE_BATCH_ROWS':str(batch),'STREAMR_TEST_NATIVE_UPDATING_JOINS':'1','STREAMR_TEST_TYPED_SQL':'1','STREAMR_TEST_EXECUTION_BYTES':'16777216','STREAMR_TEST_SCAN_PAGE_BYTES':'16777216','STREAMR_TEST_QUEUED_WRITE_BYTES':'100663296','STREAMR_TEST_MAX_OPEN_DATABASES':'8','STREAMR_TEST_MAX_SNAPSHOTS':'8','STREAMR_TEST_AGGREGATE_FLUSH_SECONDS':'3600','STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS':'180','STREAMR_CAPTURE_QUERY':str(target/'query.sql'),'STREAMR_CAPTURE_OUTPUT':str(target/'output.jsonl'),'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT':'1','STREAMR_CAPTURE_CHECKPOINT_EPOCH':'1','TMPDIR':str(target)}
    for name in ('INITIAL_ROWS','CHECKPOINT_ROWS','ROWS'):
        config['STREAMR_CAPTURE_EXPECTED_'+name]='0';config['STREAMR_CAPTURE_MAX_'+name]='512'
    aggregate={'KEY_BYTES':512,'VALUE_BYTES':32768,'PAGE_BYTES':131072,'PAGE_ENTRIES':64,'WRITE_BYTES':2097152,'WRITE_OPERATIONS':128,'OVERLAY_BYTES':1572864,'MAX_PENDING_OUTPUT_ROWS':64,'MAX_PENDING_OUTPUT_BYTES':131072,'MAX_RESIDENT_BYTES':134217728}
    live={'BLOCK_CACHE_BYTES':8388608,'MEMTABLE_BYTES':4194304,'QUEUED_WRITE_BYTES':100663296,'DECODED_VALUE_BYTES':16777216,'SCAN_PAGE_BYTES':16777216,'MAX_BLOCKING_OPERATIONS':2,'MAX_SNAPSHOTS':8,'MAX_OPEN_DATABASES':8,'DISK_RESERVE_BYTES':67108864}
    for prefix,values in [('ARROYO__WORKER__AGGREGATE_STATE__',aggregate),('ARROYO__WORKER__LIVE_STATE_RESOURCES__',live)]: config.update({prefix+k:str(v) for k,v in values.items()})
    config.update({'ARROYO__WORKER__EXECUTION_RESOURCES__MEMORY_BYTES':'16777216','ARROYO__WORKER__EXECUTION_RESOURCES__MAX_BATCH_BYTES':'1048576'})
    return config

def main():
    p=argparse.ArgumentParser();p.add_argument('--run-dir',required=True,type=pathlib.Path);p.add_argument('--binary',required=True,type=pathlib.Path);p.add_argument('--source-receipt',required=True,type=pathlib.Path);p.add_argument('--expected-commit',required=True);p.add_argument('--backend',choices=['memory','rocksdb'],default='memory');p.add_argument('--checkpoint-mode',choices=['controller','leader'],default='controller');p.add_argument('--batch-rows',type=int,choices=[1,8],default=1);p.add_argument('--execute',action='store_true');a=p.parse_args()
    source_bytes=a.source_receipt.read_bytes();source=json.loads(source_bytes,object_pairs_hook=strict_pairs);binary=a.binary.resolve(strict=True);binary_hash=provenance(source,a.expected_commit,binary)
    target=a.run_dir.resolve();target.mkdir(parents=True,exist_ok=False)
    (target/'source-build-receipt.original.json').write_bytes(source_bytes)
    for name in ('input.jsonl','expected-prefixes.json','query-original.sql'): (target/name).write_bytes((ROOT/name).read_bytes())
    query=(ROOT/'query-original.sql').read_text().replace('{{INPUT}}',str(target/'input.jsonl')).replace('{{OUTPUT}}',str(target/'output.jsonl'));(target/'query.sql').write_text(query)
    env=environment(target,a.backend,a.checkpoint_mode,a.batch_rows);argv=[str(binary),'--ignored','--exact','smoke_tests::external_sql_checkpoint_capture','--nocapture','--test-threads=1']
    plan={'status':'prepared','argv':argv,'environment':env,'binary_sha256':binary_hash,'expected_source_commit':a.expected_commit,'source_receipt':source,'source_receipt_sha256':hashlib.sha256(source_bytes).hexdigest(),'artifact_sha256':{name:digest(target/name) for name in ('input.jsonl','query.sql','expected-prefixes.json')},'runner_sha256':digest(pathlib.Path(__file__)),'scope':'exact typed20 checkpoint/final bag +sinkCDC diagnostic only','accepted_application_emission_parity':False}
    (target/'command-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
    if not a.execute:return
    clean={k:v for k,v in os.environ.items() if not k.startswith(('STREAMR_TEST_','STREAMR_CAPTURE_','ARROYO__'))};plan['status']='failed'
    try:
        assert a.source_receipt.read_bytes()==source_bytes, 'source receipt changed before launch'
        assert digest(binary)==binary_hash, 'pinned ELF changed before launch'
        assert provenance(read_json(a.source_receipt),a.expected_commit,binary)==binary_hash
        with (target/'runtime.log').open('w') as log: result=subprocess.run(argv,env={**clean,**env},stdout=log,stderr=subprocess.STDOUT,timeout=210)
        plan['exit']=result.returncode;log=(target/'runtime.log').read_text();assert result.returncode==0 and '1 passed;' in log, 'capture failed; preserve runtime.log'
        expected=read_json(target/'expected-prefixes.json');boundary=committed(log);plan['committed_rows']=boundary
        plan['checkpoint_proof']=checkpoint_proof(target/'output.checkpoint-1.jsonl',target/'output.jsonl',expected[0],boundary)
        plan['comparisons']={'initial':compare(target/'output.initial.jsonl',expected[-1]),'recovered':compare(target/'output.jsonl',expected[-1],expected[0],boundary)};plan['status']='diagnostic-pass'
    except Exception as error:
        plan['error']={'type':type(error).__name__,'message':str(error)};raise
    finally:(target/'result.json').write_text(json.dumps(plan,indent=2)+'\n')
if __name__=='__main__':main()
