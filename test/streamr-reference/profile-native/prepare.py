#!/usr/bin/env python3
"""Literal application oracle for the prepared native profile core, not a runtime pass."""
import argparse
import hashlib
import json
from pathlib import Path


def event(number, event_time, **values):
    row = dict(event_id=f'profile-{number}', tenant_id='tenant-a',
               canonical_id='person', event_time=event_time, event_type='page_view',
               user_id=None, session_id='session-a', page_url=None, country=None,
               device_type=None, browser=None, feature_name=None)
    row.update(values)
    return row


def profile(**values):
    # Defaults are explicit fixture values, not missing consumer fields filled in.
    row = dict(tenant_id='tenant-a', canonical_id='person', first_seen=1580515201000,
               last_seen=1580515201000, total_events=1, page_views=1, clicks=0,
               logins=0, feature_uses=0, user_id='', last_page='/b',
               last_country='', last_device='desktop', last_browser='',
               events_1d=1, events_7d=1, events_30d=1, events_90d=1,
               top_pages=['/b'], top_features=[])
    row.update(values)
    return row


INPUT = [
    event(1, '2020-02-01 00:00:01.000', page_url='/b', device_type='desktop'),
    # Three seconds earlier, admitted by five-second allowance. Its raw FOR date
    # is Jan31, even though maximum observed time is Feb1 and AS progress Jan31.
    event(2, '2020-01-31 23:59:58.000', page_url='/a', device_type='mobile',
          user_id='earlier-user'),
    event(3, '2020-02-01 00:00:02.000', page_url='/a', device_type='',
          user_id='latest-user', country='AU', browser='Firefox'),
    event(4, '2020-02-01 00:00:03.000', page_url='/b', device_type=None),
    event(5, '2020-02-01 00:00:04.000', page_url='/b', device_type='tablet'),
    # Fourteen seconds behind observed progress; must not change any metric.
    event(6, '2020-01-31 23:59:50.000', page_url='/late', device_type='late'),
    # Same canonical text under another tenant; advances real event time Feb2.
    event(7, '2020-02-02 00:00:06.000', tenant_id='tenant-b', page_url='/other',
          device_type='phone', session_id=None),
]

A1 = profile()
A2 = profile(first_seen=1580515198000, total_events=2, page_views=2,
             user_id='earlier-user', events_1d=1, events_7d=1,
             events_30d=1, events_90d=1, top_pages=['/a', '/b'])
A3 = profile(first_seen=1580515198000, last_seen=1580515202000,
             total_events=3, page_views=3, user_id='latest-user', last_page='/a',
             last_country='AU', last_browser='Firefox', events_1d=2,
             events_7d=3, events_30d=3, events_90d=3,
             top_pages=['/a', '/b'])
A4 = profile(first_seen=1580515198000, last_seen=1580515203000,
             total_events=4, page_views=4, user_id='latest-user',
             last_country='AU', last_browser='Firefox', events_1d=3,
             events_7d=4, events_30d=4, events_90d=4,
             top_pages=['/a', '/b'])
A5 = profile(first_seen=1580515198000, last_seen=1580515204000,
             total_events=5, page_views=5, user_id='latest-user',
             last_country='AU', last_device='tablet', last_browser='Firefox',
             events_1d=4, events_7d=5, events_30d=5, events_90d=5,
             top_pages=['/b', '/a'])
A_QUIET = dict(A5, events_1d=0)
B1 = profile(tenant_id='tenant-b', first_seen=1580601606000,
             last_seen=1580601606000, last_page='/other', last_device='phone',
             top_pages=['/other'])

# Every list is a set of complete keyed snapshots at a committed prefix.
# It imposes no cross-key emission ordering and is not an engine-derived trace.
CASES = [
    dict(input_lines=1, name='first-event', rows=[A1]),
    dict(input_lines=2, name='admitted-earlier-event-raw-trigger-date', rows=[A2]),
    dict(input_lines=3, name='date-crossing-and-nonblank-last', rows=[A3]),
    dict(input_lines=4, name='ranking-tie', rows=[A4]),
    dict(input_lines=5, name='ranking-reversal', rows=[A5]),
    dict(input_lines=6, name='late-event-no-change', rows=[A5]),
    dict(input_lines=7, name='quiet-expiry-and-tenant-isolation',
         rows=[A_QUIET, B1]),
]


def prepare(destination):
    destination.mkdir(parents=True, exist_ok=False)
    artifacts = {
        'input.jsonl': ''.join(json.dumps(row, sort_keys=True) + '\n' for row in INPUT),
        'expected-prefixes.json': json.dumps(CASES, indent=2, sort_keys=True) + '\n',
        'expected-final.jsonl': ''.join(json.dumps(row, sort_keys=True) + '\n'
                                         for row in CASES[-1]['rows']),
    }
    for name, content in artifacts.items():
        (destination / name).write_text(content)
    manifest = {'status': 'prepared-not-executed', 'input_events': len(INPUT),
                'fields_per_profile': 20,
                'sha256': {name: hashlib.sha256(content.encode()).hexdigest()
                           for name, content in artifacts.items()}}
    (destination / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('destination', type=Path)
    print(json.dumps(prepare(parser.parse_args().destination), indent=2))
