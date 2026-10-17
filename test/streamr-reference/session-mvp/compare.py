#!/usr/bin/env python3
"""Compare complete, typed twelve-field session summaries in fixture order."""
import argparse
from datetime import datetime
import json
from pathlib import Path
import re

FIELDS={'session_id','canonical_id','tenant_id','start_time','end_time','duration_sec',
        'event_count','pages','event_types','device_type','browser','country'}
def pairs(items):
 result={}
 for key,value in items:
  if key in result:raise ValueError(f'duplicate JSON field {key}')
  result[key]=value
 return result
def read(path):
 return [json.loads(line,object_pairs_hook=pairs) for line in Path(path).read_text().splitlines()]
def canonical(value):return json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)
def validate(row):
 if type(row)is not dict or set(row)!=FIELDS:raise ValueError('session must contain exactly all12 fields')
 for name in ['session_id','canonical_id','tenant_id']:
  if type(row[name])is not str or not row[name]:raise ValueError('session identity must be nonempty text')
 for name in ['start_time','end_time']:
  if type(row[name])is not str or not re.fullmatch(r'\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}',row[name]):raise ValueError('session timestamp must retain UTC milliseconds')
  datetime.strptime(row[name],'%Y-%m-%d %H:%M:%S.%f')
 for name in ['duration_sec','event_count']:
  if type(row[name])is not int:raise ValueError('session counts/duration must be integers')
 if row['event_count']<=0 or row['duration_sec']<0:raise ValueError('invalid session counts/duration')
 for name in ['device_type','browser','country']:
  if type(row[name])is not str:raise ValueError('session last dimensions must be text')
 pages=row['pages']
 if type(pages)is not list or any(type(value)is not str or not value for value in pages) or pages!=sorted(set(pages)):
  raise ValueError('session pages must be unique sorted nonempty text')
 types=row['event_types']
 if type(types)is not dict or not types or any(type(key)is not str or not key or type(value)is not int or value<=0 for key,value in types.items()):raise ValueError('invalid arbitrary event type counts')
 if sum(types.values())!=row['event_count']:raise ValueError('event type counts do not account for every event')
 return row

def compare(expected,actual):
 for row in expected+actual:validate(row)
 if canonical(expected)!=canonical(actual):raise ValueError('complete typed session oracle differs')
 return dict(rows=len(actual),fields_per_summary=12,exact_typed_values=True,complete_finite_output=True)
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('expected',type=Path);p.add_argument('actual',type=Path);a=p.parse_args();print(json.dumps(compare(read(a.expected),read(a.actual))))
