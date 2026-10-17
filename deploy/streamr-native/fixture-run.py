#!/usr/bin/env python3
"""Run the strict identity fixture in the isolated native Kafka evaluation."""
import argparse,json,subprocess,sys,time,urllib.request
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2];HERE=Path(__file__).resolve().parent
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--evidence-dir',type=Path,required=True,help='New directory for this attempt; existing directories are preserved')
parser.add_argument('--api-url',default='http://127.0.0.1:18150/api/v1/')
parser.add_argument('--broker-container',required=True)
options=parser.parse_args()
EVIDENCE=options.evidence_dir
EVIDENCE.mkdir(parents=True,exist_ok=False)
sys.path.insert(0,str(ROOT/'test/streamr-reference'))
from compare_identity import compare,read_records
from streamr_capture import adapt
API=options.api_url.rstrip('/')+'/'
def api(path,payload=None):
 req=urllib.request.Request(API+path,data=None if payload is None else json.dumps(payload).encode(),headers={'Content-Type':'application/json'})
 with urllib.request.urlopen(req,timeout=30) as f:return json.load(f)
def broker(*args,data=None):
 p=subprocess.run(['podman','exec','-i',options.broker_container,'rpk',*args,'-X','brokers=localhost:9092'],input=data,text=True,capture_output=True,timeout=120)
 assert p.returncode==0,p.stderr
 return p.stdout
result={'status':'running'};started=time.time()
try:
 ids={}
 for name in ('identity','unified','merges'):
  query=(HERE/(name+'.sql')).read_text();validation=api('pipelines/validate_query',{'query':query,'udfs':[]});assert not validation.get('errors') and validation.get('graph'),validation
  nodes=validation['graph']['nodes'];owners=sum('fused state-table event owner' in n['description'] for n in nodes);assert owners==(1 if name=='identity' else 0)
  pipeline=api('pipelines',{'name':'Arcstream native '+name,'query':query,'udfs':[],'parallelism':1,'checkpoint_interval_micros':2_000_000,'tags':{'evaluation':'arcstream-native-fixture'}});ids[name]=pipeline['id']
  (EVIDENCE/(name+'-pipeline.json')).write_text(json.dumps(pipeline,indent=2)+'\n')
  deadline=time.time()+120
  while True:
   jobs=api('pipelines/'+pipeline['id']+'/jobs')['data'];job=max(jobs,key=lambda j:j['created_at']) if jobs else None
   if job and job['state']=='Running':break
   assert not job or job['state'] not in ('Failed','Error'),job
   assert time.time()<deadline,job
   time.sleep(1)
 fixture=json.loads((ROOT/'flink/identity-resolution/src/test/resources/reference/identity-input.json').read_text());events=[s['payload'] for s in fixture['steps'] if s['op']=='event']
 broker('topic','produce','arc-native-raw-events',data=''.join(json.dumps(e)+'\n' for e in events))
 for topic,count in [('arc-native-identity-capture',13),('arc-native-unified-events',13),('arc-native-identity-merges',2)]:
  out=broker('topic','consume',topic,'--read-committed','--format','%v\n','--offset','start','--num',str(count));(EVIDENCE/(topic+'.jsonl')).write_text(out)
 capture=[json.loads(l) for l in (EVIDENCE/'arc-native-identity-capture.jsonl').read_text().splitlines() if l.strip()];unified=[json.loads(l) for l in (EVIDENCE/'arc-native-unified-events.jsonl').read_text().splitlines() if l.strip()];merges=[json.loads(l) for l in (EVIDENCE/'arc-native-identity-merges.jsonl').read_text().splitlines() if l.strip()];oracle=read_records(ROOT/'flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl')
 compare(oracle,adapt(capture));actual=[{'stream':'unified-events','payload':e} for e in unified]+[{'stream':'identity-merges','payload':e} for e in merges];comparison=compare(oracle,actual)
 result.update(status='passed',pipelines=ids,comparison=comparison,elapsed_seconds=time.time()-started,scope='Committed minimum13events/two merges verified; full physical scan/recovery/live consumers pending')
except Exception as exc:result.update(status='failed',error=repr(exc),elapsed_seconds=time.time()-started);raise
finally:(EVIDENCE/'result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result),flush=True)
