#!/usr/bin/env python3
"""Prepare application expectations for native SESSION; never execute an engine."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
def product_oracle():
    """Standalone product oracle: five-second disorder, native SESSION closure."""
    def event(name, tenant, session, canonical, at, kind, **metadata):
        return dict(kind='event', event_id=name, tenant_id=tenant, session_id=session,
                    canonical_id=canonical, event_time=at, event_type=kind, **metadata)
    def progress(name, at):
        return dict(kind='progress', event_id=name, tenant_id='progress', session_id='',
                    canonical_id='progress', event_time=at, event_type='page_view')
    inputs = [
        event('a1','tenant-a','shared','first-a','2020-01-31 23:59:00.000','page_view',page_url='/first',device_type='desktop',browser='Firefox',country='AU'),
        event('a2','tenant-a','shared','later-a','2020-01-31 23:58:30.000','custom_event',page_url='/late',browser='Chrome',country=''),
        event('b1','tenant-b','shared','first-b','2020-02-01 00:00:00.000','click',page_url='/tenant',device_type='tablet',browser='Safari',country='NZ'),
        event('a3','tenant-a','shared','ignored-canonical','2020-02-01 00:01:00.000','custom_event',page_url='/first',device_type='mobile',browser='',country='US'),
        progress('progress1','2020-02-01 00:32:00.000'),
        event('a4','tenant-a','shared','new-a','2020-02-01 00:40:00.000','login',page_url='/new',device_type='phone',browser='Safari',country='GB'),
        event('b2','tenant-b','shared','new-b','2020-02-01 00:41:00.000','login',page_url='/newb'),
        event('a5','tenant-a','shared','ignored-after-reopen','2020-02-01 00:20:00.000','click',page_url='/oldlater',country=''),
        progress('progress2','2020-02-01 01:12:00.000'),
        event('empty','tenant-a',None,'ignored','2020-02-01 01:20:00.000','page_view',page_url='/excluded'),
        event('pre','tenant-pre','pre','pre-canonical','1969-12-31 23:59:59.000','custom',page_url='/before'),
    ]
    # Explicit expectations: a2, a5 and pre are older than the five-second
    # watermark and are dropped. They never mutate a closed/reopened session.
    expected = [
        dict(tenant_id='tenant-a',session_id='shared',canonical_id='first-a',start_time='2020-01-31 23:59:00.000',end_time='2020-02-01 00:01:00.000',duration_sec=120,event_count=2,pages=['/first'],event_types={'custom_event':1,'page_view':1},device_type='mobile',browser='Firefox',country='US'),
        dict(tenant_id='tenant-b',session_id='shared',canonical_id='first-b',start_time='2020-02-01 00:00:00.000',end_time='2020-02-01 00:00:00.000',duration_sec=0,event_count=1,pages=['/tenant'],event_types={'click':1},device_type='tablet',browser='Safari',country='NZ'),
        dict(tenant_id='tenant-a',session_id='shared',canonical_id='new-a',start_time='2020-02-01 00:40:00.000',end_time='2020-02-01 00:40:00.000',duration_sec=0,event_count=1,pages=['/new'],event_types={'login':1},device_type='phone',browser='Safari',country='GB'),
        dict(tenant_id='tenant-b',session_id='shared',canonical_id='new-b',start_time='2020-02-01 00:41:00.000',end_time='2020-02-01 00:41:00.000',duration_sec=0,event_count=1,pages=['/newb'],event_types={'login':1},device_type='',browser='',country=''),
    ]
    return dict(inputs=inputs, expected=expected)


def milliseconds(text):
    delta = datetime.fromisoformat(text).replace(tzinfo=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def source_rows(signals):
    rows = []
    for signal in signals:
        at = signal['event_time']
        rows.append(dict(event_id=signal['event_id'], session_id=signal.get('session_id'),
                         tenant_id=signal['tenant_id'], canonical_id=signal['canonical_id'],
                         event_type=signal.get('event_type', ''),
                         **{key: signal.get(key) for key in ('page_url', 'device_type', 'browser', 'country')},
                         event_time=at))
    return rows


DESCRIPTIONS = {
    'chronological': 'One session, ordered activity; another tenant advances real source event time past its deadline.',
    'late-first-arrival': 'Thirty-second-late activity is dropped by the five-second product watermark.',
    'reopened-late': 'After closure and reopening at 00:40, twenty-minute-late a5 is dropped.',
    'tenant-reopen': 'Four native summaries, tenant isolation, reopening, null session exclusion and rejection of ancient activity.',
    'five-second-cutoff': 'Accept five-second-disordered activity, reject activity 5001 milliseconds behind source progress.',
    'event-time-ordering': 'Admitted out-of-order rows choose identity/start and final metadata by event time, not arrival.',
    'equal-time-ties': 'Equal event-time FIRST keeps first arrival; LAST prefers later write under STR-60.',
    'large-pages': 'Ten admitted events with long repeated pages exceed the old 512-byte grouping-key budget; early scalar pages normalization preserves the full summary.',
    'chronological-recovery': 'Three sequential sessions with distinct identities and source-progress closures; fault after first committed summary while later summaries remain.',
}


def fixture(case):
    data = product_oracle()
    by_id = {row['event_id']: row for row in data['inputs'] if row['kind'] in ('event', 'progress')}
    def progress(name, at):
        return dict(kind='progress', event_id=name, tenant_id='progress', session_id='', canonical_id='progress', event_time=at, event_type='page_view')
    if case == 'large-pages':
        # Input generation is compact; all twelve expected output fields below
        # are independent literals, never computed by an aggregation oracle.
        signals = []
        for index in range(10):
            signals.append(dict(kind='event', event_id=f'large-{index:03}',
                                tenant_id='tenant-long', session_id='long-session',
                                canonical_id='long-first' if index == 0 else f'later-{index}',
                                event_time=f'2020-02-01 00:{index//60:02}:{index%60:02}.000',
                                event_type=('page_view','click','feature_used')[index % 3],
                                page_url=('/catalog/product/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '/checkout/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '/home/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc')[index % 3],
                                device_type='desktop' if index == 0 else '',
                                browser='Firefox' if index == 0 else None,
                                country='AU' if index == 0 else ''))
        signals[5].update(device_type='mobile', browser='Chrome', country='GB')
        signals[9].update(device_type='tablet', browser='Safari', country='NZ')
        signals.append(progress('close-large', '2020-02-01 00:40:00.000'))
        expected = [dict(tenant_id='tenant-long', session_id='long-session',
                         canonical_id='long-first', start_time='2020-02-01 00:00:00.000',
                         end_time='2020-02-01 00:00:09.000', duration_sec=9,
                         event_count=10, pages=['/catalog/product/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '/checkout/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '/home/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'],
                         event_types={'page_view':4,'click':3,'feature_used':3},
                         device_type='tablet', browser='Safari', country='NZ')]
    elif case == 'chronological-recovery':
        signals = []
        expected = []
        # Explicit application oracle: do not derive expected aggregation from
        # the generated events or from native SESSION output.
        for index, hour in enumerate((0, 2, 4), 1):
            start = f'2020-02-01 {hour:02}:00:00.000'
            end = f'2020-02-01 {hour:02}:02:00.000'
            session = f'recovery-{index}'
            first = dict(by_id['a1'], event_id=f'r{index}-first',
                         tenant_id='tenant-recovery', session_id=session, canonical_id=f'first-{index}',
                         event_time=start)
            second = dict(by_id['a3'], event_id=f'r{index}-second',
                          tenant_id='tenant-recovery', session_id=session, canonical_id=f'later-{index}',
                          event_time=end)
            signals.extend([first, second, progress(f'close-{index}', f'2020-02-01 {hour:02}:40:00.000')])
            # Extra real source progress records ensure the first checkpoint
            # cannot read ahead directly to the final application's result.
            if index < 3:
                signals.extend(progress(f'progress-{index}-{minute}', f'2020-02-01 {hour:02}:{minute:02}:00.000')
                               for minute in (41, 42, 43, 44))
            expected.append(dict(tenant_id='tenant-recovery', session_id=session,
                                 canonical_id=f'first-{index}', start_time=start, end_time=end,
                                 duration_sec=120, event_count=2, pages=['/first'],
                                 event_types={'custom_event': 1, 'page_view': 1},
                                 device_type='mobile', browser='Firefox', country='US'))
    elif case == 'event-time-ordering':
        first = dict(by_id['a1'], event_id='ordered-middle', event_time='2020-02-01 00:00:04.000')
        earlier = dict(by_id['a2'], event_id='ordered-first', event_time='2020-02-01 00:00:02.000', canonical_id='earliest', device_type='phone', browser='Earlier', country='NZ')
        latest = dict(by_id['a3'], event_id='ordered-last', event_time='2020-02-01 00:00:06.000', device_type='tablet', browser='Latest', country='GB')
        later_arrival = dict(by_id['a2'], event_id='ordered-final-arrival', event_time='2020-02-01 00:00:03.000', device_type='wrong-arrival', browser='WrongArrival', country='US')
        signals = [first, earlier, latest, later_arrival, progress('close','2020-02-01 00:40:00.000')]
        expected = [dict(data['expected'][0],canonical_id='earliest',start_time='2020-02-01 00:00:02.000',end_time='2020-02-01 00:00:06.000',duration_sec=4,event_count=4,pages=['/first','/late'],event_types={'page_view':1,'custom_event':3},device_type='tablet',browser='Latest',country='GB')]
    elif case == 'equal-time-ties':
        first = dict(by_id['a1'],event_id='tie-first',event_time='2020-02-01 00:00:04.000')
        last = dict(by_id['a3'],event_id='tie-last',event_time='2020-02-01 00:00:04.000',browser='LastTie')
        signals = [first,last,progress('close','2020-02-01 00:40:00.000')]
        expected = [dict(data['expected'][0],start_time='2020-02-01 00:00:04.000',end_time='2020-02-01 00:00:04.000',duration_sec=0,browser='LastTie')]
    elif case == 'five-second-cutoff':
        first = dict(by_id['a1'], event_id='cutoff-first', event_time='2020-02-01 00:00:10.000')
        inside = dict(by_id['a2'], event_id='cutoff-inside', event_time='2020-02-01 00:00:05.000', event_type='click', page_url='/inside')
        outside = dict(by_id['a2'], event_id='cutoff-outside', event_time='2020-02-01 00:00:04.999', page_url='/outside', browser='Rejected')
        last = dict(by_id['a3'], event_id='cutoff-last', event_time='2020-02-01 00:00:12.000')
        signals = [first, inside, outside, last, progress('close', '2020-02-01 00:40:00.000')]
        expected = [dict(data['expected'][0], canonical_id='later-a', start_time='2020-02-01 00:00:05.000', end_time='2020-02-01 00:00:12.000', duration_sec=7, event_count=3, pages=['/first','/inside'], event_types={'page_view':1,'click':1,'custom_event':1}, browser='Firefox')]
    elif case == 'tenant-reopen':
        signals = data['inputs'] + [progress('final-progress', '2020-02-01 02:00:00.000')]
        expected = data['expected']
    elif case == 'chronological':
        signals = [by_id['a1'], by_id['a3'], progress('close', '2020-02-01 00:40:00.000')]
        expected = [dict(data['expected'][0], event_count=2, pages=['/first'],
                         event_types={'custom_event': 1, 'page_view': 1}, browser='Firefox')]
    elif case == 'late-first-arrival':
        signals = [by_id['a1'], by_id['a2'], by_id['a3'], progress('close', '2020-02-01 00:40:00.000')]
        expected = [data['expected'][0]]
    elif case == 'reopened-late':
        signals = [by_id['a1'], by_id['a3'], by_id['progress1'], by_id['a4'], by_id['a5'],
                   progress('close', '2020-02-01 01:20:00.000')]
        expected = [dict(data['expected'][0], event_count=2, pages=['/first'],
                         event_types={'custom_event': 1, 'page_view': 1}, browser='Firefox'), data['expected'][2]]
    else:
        raise ValueError(case)
    return dict(description=DESCRIPTIONS[case], inputs=source_rows(signals), expected=deepcopy(expected))


def prepare(directory, case):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    data = fixture(case)
    for name, rows in [('input.jsonl', data['inputs']), ('expected.jsonl', data['expected'])]:
        (directory / name).write_text(''.join(json.dumps(row, sort_keys=True) + '\n' for row in rows))
    manifest = dict(case=case, description=data['description'], input_rows=len(data['inputs']),
                    expected_rows=len(data['expected']), progress_contract='Ordinary empty-session events advance raw source event time; no special clock protocol.',
                    source_watermark='Product contract: event_ts minus 5 seconds; older events are dropped.',
                    output_order='No cross-key ordering contract. Compare complete records with multiplicity; preserve physical committed bytes for recovery.',
                    status='prepared only; expected is application oracle, not a claimed native result',
                    source_sha256={str(Path(__file__)): hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
                    files_sha256={name: hashlib.sha256((directory/name).read_bytes()).hexdigest() for name in ['input.jsonl', 'expected.jsonl']})
    if case == 'chronological-recovery':
        manifest['recommended_fault_prefix'] = 3
        manifest['recovery_expectation'] = 'First summary committed before fault; two later summaries remain. Runner must verify this gate, never assume scheduling.'
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--case', choices=DESCRIPTIONS, default='chronological')
    args = parser.parse_args()
    print(json.dumps(prepare(args.directory, args.case)))
