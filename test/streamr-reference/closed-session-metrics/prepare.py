#!/usr/bin/env python3
"""Standalone completed-session metrics oracle; prepares files without an engine."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path


def milliseconds(text):
    delta = datetime.fromisoformat(text).replace(tzinfo=timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc)
    return delta.days*86400000+delta.seconds*1000+delta.microseconds//1000


def fixture():
    inputs=[]
    def event(name,tenant,session,at):
        inputs.append(dict(event_id=name,tenant_id=tenant,canonical_id='shared-person',session_id=session,
                           event_type='page_view',page_url='/fixture',device_type=None,browser=None,country=None,
                           event_time=at))
    def progress(name,at,session=''):
        event(name,'progress',session,at)
    event('a-first','tenant-a','reused','2020-02-01 00:00:00.000')
    event('a-end','tenant-a','reused','2020-02-01 00:00:01.900')
    progress('after-a-first','2020-02-01 00:40:00.000')
    event('a-reopen','tenant-a','reused','2020-02-01 01:00:00.000')
    event('a-reopen-end','tenant-a','reused','2020-02-01 01:00:02.900')
    progress('after-a-second','2020-02-01 01:40:00.000')
    event('b-first','tenant-b','reused','2020-02-01 02:00:00.000')
    event('b-end','tenant-b','reused','2020-02-01 02:00:05.000')
    progress('after-b-first','2020-02-01 02:40:00.000',session=None)
    event('a-third','tenant-a','reused','2020-02-03 03:00:00.000')
    event('a-third-end','tenant-a','reused','2020-02-03 03:00:04.000')
    event('a-rejected-late','tenant-a','reused','2020-02-03 02:59:30.000')
    progress('after-a-third','2020-02-03 03:40:00.000')
    # Hand-specified independent values. Milliseconds are retained until the
    # final integer-second average; never average pre-truncated duration_sec.
    expected=[
        dict(tenant_id='tenant-a',canonical_id='shared-person',closed_session_count=1,avg_session_duration_sec=1),
        dict(tenant_id='tenant-a',canonical_id='shared-person',closed_session_count=2,avg_session_duration_sec=2),
        dict(tenant_id='tenant-b',canonical_id='shared-person',closed_session_count=1,avg_session_duration_sec=5),
        dict(tenant_id='tenant-a',canonical_id='shared-person',closed_session_count=3,avg_session_duration_sec=2),
    ]
    prefixes=[
        dict(input_lines=3,source_event_time='2020-02-01 00:40:00.000',completed_duration_ms=1900,value=expected[0]),
        dict(input_lines=6,source_event_time='2020-02-01 01:40:00.000',completed_duration_ms=4800,value=expected[1]),
        dict(input_lines=9,source_event_time='2020-02-01 02:40:00.000',completed_duration_ms=5000,value=expected[2]),
        dict(input_lines=13,source_event_time='2020-02-03 03:40:00.000',completed_duration_ms=8800,value=expected[3]),
    ]
    return dict(inputs=inputs,expected=expected,prefixes=prefixes,final=[expected[3],expected[2]])


def prepare(directory):
    directory=Path(directory);directory.mkdir(parents=True,exist_ok=False)
    data=fixture()
    for filename,rows in [('input.jsonl',data['inputs']),('expected.jsonl',data['expected']),('expected-final.jsonl',data['final'])]:
        (directory/filename).write_text(''.join(json.dumps(row,sort_keys=True)+'\n' for row in rows))
    (directory/'expected-prefixes.json').write_text(json.dumps(data['prefixes'],indent=2,sort_keys=True)+'\n')
    files=['input.jsonl','expected.jsonl','expected-final.jsonl','expected-prefixes.json']
    manifest=dict(status='prepared only; not runtime validation',input_rows=13,expected_metric_images=4,
                  expected_final_profiles=2,watermark='cast(event_time as timestamp) minus exactly 5 seconds; older activity rejected',
                  average_rule='floor(sum(completed duration milliseconds) / completed session count / 1000)',
                  scope='completed-session count and average only, not full profile parity',
                  source_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  files_sha256={name:hashlib.sha256((directory/name).read_bytes()).hexdigest() for name in files})
    (directory/'manifest.json').write_text(json.dumps(manifest,indent=2,sort_keys=True)+'\n')
    return manifest


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('directory',type=Path)
    print(json.dumps(prepare(parser.parse_args().directory)))
