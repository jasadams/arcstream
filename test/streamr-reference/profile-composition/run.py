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
    assert fields['input_rows_before_checkpoint']=='1' and fields['checkpoint']=='1'
    committed=int(fields['committed_rows']); assert committed>0
    return committed

def compare(path, expected, committed_rows=None):
    previous=None; last_prefix=0; count=0
    for line in path.read_text().splitlines():
        item=json.loads(line,object_pairs_hook=strict_pairs); assert set(item)=={'before','after','op'}
        before=item['before']; after=item['after']; assert item['op']==('c' if before is None else 'u')
        assert before==previous, ('CDC chain mismatch',path,count)
        assert after is not None, ('unexpected deletion',path,count)
        prefix=after['total_events']; assert type(prefix) is int and last_prefix<=prefix<=len(expected)
        wanted=expected[prefix-1]
        assert set(after)==set(wanted), ('schema',path,count)
        for key,value in wanted.items():
            assert type(after[key]) is type(value) and after[key]==value, ('typed value mismatch',path,count,prefix,key,after[key],value)
        previous=after; last_prefix=prefix; count+=1
        if committed_rows is not None:
            if count==committed_rows: assert after==expected[0], ('checkpoint committed prefix is not exact prefix1',path,count)
    if committed_rows is not None: assert count>committed_rows, ('missing post-checkpoint suffix',path)
    assert count and previous==expected[-1], ('full final mismatch',path)
    return {'rows':count,'final_prefix':last_prefix,'all20_typed_fields':True,'exact_CDC_chain':True}
def main():
    p=argparse.ArgumentParser(); p.add_argument('--run-dir',required=True); p.add_argument('--binary',required=True); p.add_argument('--execute',action='store_true'); p.add_argument('--query',default=str(REPO/'deploy/streamr-native/profile-composition/query.sql'));  a=p.parse_args()
    target=pathlib.Path(a.run_dir).resolve(); target.mkdir(parents=True,exist_ok=False)
    receipt=json.loads((ROOT/'preparation-receipt.json').read_text())
    binary=pathlib.Path(a.binary); digest=hashlib.sha256(binary.read_bytes()).hexdigest(); assert digest==receipt['sql_binary_sha256']
    (target/'input.jsonl').write_bytes((ROOT/'input.jsonl').read_bytes())
    query=pathlib.Path(a.query).read_text().replace('{{INPUT}}',str(target/'input.jsonl')).replace('{{OUTPUT}}',str(target/'output.jsonl'))
    (target/'query.sql').write_text(query)
    env={'STREAMR_TEST_NATIVE_AGGREGATES':'1','STREAMR_TEST_TYPED_SQL':'1','STREAMR_TEST_AGGREGATE_FLUSH_SECONDS':'3600','STREAMR_TEST_EXECUTION_BYTES':'16777216','STREAMR_TEST_MAX_OPEN_DATABASES':'4','STREAMR_TEST_BACKEND':'memory','STREAMR_TEST_CHECKPOINT_MODE':'controller','STREAMR_TEST_SOURCE_BATCH_ROWS':'1','STREAMR_CAPTURE_QUERY':str(target/'query.sql'),'STREAMR_CAPTURE_OUTPUT':str(target/'output.jsonl'),'STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT':'1','STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS':'1','STREAMR_CAPTURE_MAX_INITIAL_ROWS':'32','STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS':'1','STREAMR_CAPTURE_EXPECTED_ROWS':'2','STREAMR_CAPTURE_MAX_ROWS':'32','STREAMR_CAPTURE_CHECKPOINT_EPOCH':'1','STREAMR_TEST_RUNTIME_TIMEOUT_SECONDS':'180','TMPDIR':str(target)}
    argv=[str(binary),'--ignored','--exact','smoke_tests::external_sql_checkpoint_capture','--nocapture','--test-threads=1']
    plan={'argv':argv,'env':env,'binary_sha256':digest,'status':'prepared','scope':'20-field composition diagnostic; not full33 application parity'}
    (target/'command-plan.json').write_text(json.dumps(plan,indent=2)+'\n')
    if not a.execute: return
    clean_env={k:v for k,v in os.environ.items() if not k.startswith(('STREAMR_TEST_','STREAMR_CAPTURE_'))}
    plan['status']='failed'
    try:
        with (target/'runtime.log').open('w') as log:
            result=subprocess.run(argv,env={**clean_env,**env},stdout=log,stderr=subprocess.STDOUT,timeout=210)
        plan['exit']=result.returncode
        log_text=(target/'runtime.log').read_text()
        assert result.returncode==0 and '1 passed;' in log_text, 'runtime failed; inspect preserved log'
        expected=json.loads((ROOT/'expected-prefixes.json').read_text(),object_pairs_hook=strict_pairs)
        committed=recovered_committed_rows(log_text); plan['recovered_committed_rows']=committed
        plan['comparisons']={'initial':compare(target/'output.initial.jsonl',expected),'recovered':compare(target/'output.jsonl',expected,committed)}
        plan['status']='pass'
    except Exception as error:
        plan['error']={'type':type(error).__name__,'message':str(error)}
        raise
    finally:
        (target/'result.json').write_text(json.dumps(plan,indent=2)+'\n')
if __name__=='__main__': main()
