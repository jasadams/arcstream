#!/usr/bin/env python3
"""Exact complete 12-field multiset comparison. Reject duplicates and intermediates."""
import argparse,json
from collections import Counter
from pathlib import Path
def pairs(items):
 d={}
 for k,v in items:
  if k in d: raise ValueError('duplicate JSON key '+k)
  d[k]=v
 return d
def read(path):
 return [json.loads(s,object_pairs_hook=pairs) for s in path.read_text().splitlines() if s.strip()]
p=argparse.ArgumentParser();p.add_argument('output',type=Path);p.add_argument('--prefix',action='store_true');a=p.parse_args()
expected=read(Path(__file__).with_name('expected.jsonl')); actual=read(a.output)
if a.prefix: expected=expected[:1]
assert len(actual)==len(expected), f'Complete output has {len(actual)} rows, expected 3; no cropping allowed'
keys=set(expected[0])
for row in actual:
 assert set(row)==keys and len(row)==12,'Incorrect full schema'
 assert type(row['duration_sec']) is int and type(row['event_count']) is int
 assert type(row['event_types']) is dict, 'event_types must be an object, not encoded JSON text or arrays'
 assert all(type(k) is str and type(v) is int and v>0 for k,v in row['event_types'].items())
 assert type(row['pages']) is list and len(set(row['pages']))==len(row['pages'])
# Pages are a set in the reference implementation; event-type object ordering is immaterial.
def canonical(row):
 return json.dumps({**row,'pages':sorted(row['pages'])},sort_keys=True,separators=(',',':'))
assert Counter(map(canonical,actual))==Counter(map(canonical,expected)),'Full12 values, counts, UTC milliseconds or lineage differ'
print('PASS: complete 3 rows, all 12 fields; no duplicates/intermediate rows')
