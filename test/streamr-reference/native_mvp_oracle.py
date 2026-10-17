"""Independent hand-counted app oracle for the strict13-event identity fixture.

No transition UDF import. Only canonical UUID literals come from real output;
event payloads/lineage are first checked against the pinned reference fixture.
Clock-close stage must stay on2026-01-01 after00:30:13 and beforeJan2.
"""
from copy import deepcopy
from datetime import datetime,timezone
import json
from pathlib import Path

REFERENCE_INPUT=Path(__file__).with_name('identity-input.json')
if not REFERENCE_INPUT.exists():
 REFERENCE_INPUT=Path(__file__).resolve().parents[2]/'flink/identity-resolution/src/test/resources/reference/identity-input.json'
ROLE_EVENTS={('tenant-a','c1'):[1,2,3,5,6,7,11,13],('tenant-a','c2'):[4],
 ('tenant-a','c3'):[10],('tenant-b','c1'):[8,12],('tenant-b','c2'):[9]}
CREATE=['canonical_id','tenant_id','total_events','first_seen','last_seen']

def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def ms(value):
 dt=datetime.fromisoformat(value.replace('Z','+00:00'))
 if dt.tzinfo is None:dt=dt.replace(tzinfo=timezone.utc)
 delta=dt-datetime(1970,1,1,tzinfo=timezone.utc)
 return delta.days*86400000+delta.seconds*1000+delta.microseconds//1000

def mapping(unified):
 fixture=json.loads(REFERENCE_INPUT.read_text())
 expected={s['payload']['event_id']:s['payload'] for s in fixture['steps'] if s['op']=='event'}
 assert len(unified)==13 and len({r['event_id'] for r in unified})==13,'complete13 unique unified rows required'
 by_id={r['event_id']:r for r in unified}
 assert set(by_id)==set(expected),'unexpected event IDs'
 for event_id,row in by_id.items():
  assert set(row)==set(expected[event_id])|{'canonical_id'},'unified fields differ'
  assert canonical({k:v for k,v in row.items() if k!='canonical_id'})==canonical(expected[event_id]),'unified application payload differs'
 aliases={}
 for key,numbers in ROLE_EVENTS.items():
  ids={by_id[f'identity-{n:02}']['canonical_id'] for n in numbers}
  assert len(ids)==1 and all(type(v)is str and v for v in ids),'identity lineage differs'
  aliases[key]=next(iter(ids))
 assert len(set(aliases.values()))==5,'distinct identities/tenants share UUID'
 return aliases

def expectations(unified):
 aliases=mapping(unified)
 # role,total events,sessions,first,last,pageviews,features,user,lastpage,top pages,
 # current session start,action,changed fields. Counts are hand-derived.
 rows=[
 ('tenant-a','c1',8,3,1,13,6,2,'alice','/page/13',['/page/1','/page/11','/page/13','/page/3','/page/5'],7,'update',['total_events','last_seen','page_views','last_page']),
 ('tenant-a','c2',1,1,4,4,1,0,'','/page/4',['/page/4'],4,'create',CREATE),
 ('tenant-a','c3',1,1,10,10,1,0,'bob','/page/10',['/page/10'],10,'create',CREATE),
 ('tenant-b','c1',2,1,8,12,1,1,'alice','/page/12',['/page/12','/page/8'],8,'update',['total_events','last_seen','feature_uses','last_page']),
 ('tenant-b','c2',1,1,9,9,0,1,'','/page/9',['/page/9'],9,'create',CREATE)]
 active,closed={},{}
 base=ms('2026-01-01 00:00:00.000')
 for tenant,role,total,sessions,first,last,views,features,user,page,top,start,action,changed in rows:
  p=dict(canonical_id=aliases[(tenant,role)],tenant_id=tenant,user_id=user,
   first_seen=base+first*1000,last_seen=base+last*1000,updated_at=None,
   total_events=total,total_sessions=sessions,
   events_1d=total,events_7d=total,events_30d=total,events_90d=total,
   sessions_1d=sessions,sessions_7d=sessions,sessions_30d=sessions,sessions_90d=sessions,
   avg_session_duration_sec=0,current_session_active=True,current_session_duration_sec=last-start,
   page_views=views,clicks=0,logins=0,feature_uses=features,last_page=page,
   last_country='AU',last_device='desktop',last_browser='Firefox',top_pages=top,
   top_features=['reference-feature'],action=action,changed_fields=list(changed),timestamp=None,trigger='event')
  assert len(p)==33
  key=(tenant,p['canonical_id']);active[key]=p
  q=deepcopy(p);duration=last-start
  q.update(current_session_active=False,current_session_duration_sec=0,
   avg_session_duration_sec=duration,action='update',trigger='session_timeout',
   changed_fields=['avg_session_duration_sec'] if duration else [])
  closed[key]=q
 sessions=[]
 for tenant,session,role,start,end,count,pages,types in [
 ('tenant-a','session-1','c1',1,13,6,['/page/1','/page/11','/page/13','/page/3','/page/5','/page/7'],{'feature_used':1,'page_view':5}),
 ('tenant-a','session-0','c2',4,10,3,['/page/10','/page/4','/page/6'],{'feature_used':1,'page_view':2}),
 ('tenant-b','session-0','c1',8,12,2,['/page/12','/page/8'],{'feature_used':1,'page_view':1}),
 ('tenant-b','session-1','c2',9,9,1,['/page/9'],{'feature_used':1})]:
  sessions.append(dict(session_id=session,canonical_id=aliases[(tenant,role)],tenant_id=tenant,
   start_time=f'2026-01-01 00:00:{start:02}.000',end_time=f'2026-01-01 00:00:{end:02}.000',
   duration_sec=end-start,event_count=count,pages=pages,event_types=types,
   device_type='desktop',browser='Firefox',country='AU'))
 return dict(active=active,closed=closed,sessions=sessions,aliases=aliases)

def verify_profiles(unified,profiles,stage='active'):
 wanted=expectations(unified)[stage]
 assert len(profiles)==5,'expected exact five latest profile rows'
 received={(p['tenant_id'],p['canonical_id']):p for p in profiles}
 assert len(received)==5 and set(received)==set(wanted),'profile identity set differs'
 for key,p in received.items():
  assert set(p)==set(wanted[key]),'full33 fields differ'
  assert type(p['updated_at'])is int and type(p['timestamp'])is str,'observation metadata type differs'
  assert ms(p['timestamp'])==p['updated_at'],'updated_at and timestamp observation clocks disagree'
  q=dict(p);q.update(updated_at=None,timestamp=None)
  assert canonical(q)==canonical(wanted[key]),f'profile business snapshot differs: {key}: {q} expected {wanted[key]}'
 return dict(stage=stage,profiles=5,fields=33,exact_business_values=True,observation_metadata_relation=True)

def verify_sessions(unified,sessions):
 expected=expectations(unified)['sessions']
 assert len(sessions)==4,'expected exact four closed sessions'
 assert sorted(map(canonical,sessions))==sorted(map(canonical,expected)),'full12 session values differ'
 return dict(sessions=4,fields=12,exact=True)

def verify_cdc(records):
 """Check complete literal before-image chains without wall-clock normalization."""
 previous={}
 for row in records:
  assert set(row)=={'before','after','op'} and type(row['after'])is dict,'invalid profile CDC'
  p=row['after'];key=(p['tenant_id'],p['canonical_id'])
  assert canonical(row['before'])==canonical(previous.get(key)),'broken literal profile before chain'
  assert row['op']==('u' if key in previous else 'c'),'wrong profile CDC action'
  previous[key]=p
 return list(previous.values())
