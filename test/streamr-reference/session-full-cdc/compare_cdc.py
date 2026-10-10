#!/usr/bin/env python3
"""Strict complete CDC chain and independently bounded closure-value diagnostic."""
import argparse
import json
from pathlib import Path

def pairs(items):
    d = {}
    for k,v in items:
        assert k not in d, 'duplicate JSON key: '+k
        d[k] = v
    return d

def read(path):
    return [json.loads(s,object_pairs_hook=pairs) for s in path.read_text().splitlines() if s.strip()]

def key(r):
    return r['tenant_id'],r['session_id'],r['start_time']

p=argparse.ArgumentParser();p.add_argument('output',type=Path);p.add_argument('--prefix',action='store_true',help='exact first independently expected closure only');a=p.parse_args()
expected=read(Path(__file__).with_name('expected.jsonl'))
if a.prefix: expected=expected[:1]
want={key(r):r for r in expected}
state={}
rows=read(a.output)
# No intermediate rows are admitted in this first, exactly3-row capture attempt.
assert len(rows)==len(expected), 'capture physical multiplicity differs from independently declared closure set'
for envelope in rows:
    assert {'before','after','op'} <= set(envelope), 'missing CDC metadata'
    before,after,op=envelope['before'],envelope['after'],envelope['op']
    assert op in {'c','u','d'}, 'unsupported CDC operation'
    for r in [before,after]:
        if r is None: continue
        assert type(r) is dict and set(r)==set(expected[0]) and len(r)==12
        k=key(r);assert k in want, 'unknown closure identity'
        target=want[k]
        # All invariant scalar fields/pages must already be exact; only type-count
        # maps may be partial in a general CDC chain. No arbitrary values allowed.
        assert {n:v for n,v in r.items() if n!='event_types'}=={n:v for n,v in target.items() if n!='event_types'}, 'closure scalar/pages/time value mismatch'
        assert type(r['duration_sec']) is int and type(r['event_count']) is int
        assert type(r['pages']) is list and all(type(v) is str for v in r['pages']) and len(set(r['pages']))==len(r['pages'])
        types=r['event_types'];assert type(types) is dict and types, 'native nonempty event-type object required'
        assert all(n in target['event_types'] and type(v) is int and 0<v<=target['event_types'][n] for n,v in types.items()), 'unknown/overcounted type'
    k=key(after if after is not None else before)
    if op=='c':
        assert before is None and after is not None and k not in state, 'duplicate/invalid create'
        state[k]=after
    elif op=='u':
        assert after is not None and before is not None and key(before)==k
        assert state.get(k)==before and before!=after, 'broken/noop before-chain'
        state[k]=after
    else:
        assert after is None and before is not None and state.get(k)==before, 'broken delete chain'
        del state[k]
assert state==want, 'complete closure state differs from independent all12 oracle'
print(f'PASS: exact{len(expected)} physical CDC records, strict before-chain, full12 final state; updating retained-state diagnostic only')
