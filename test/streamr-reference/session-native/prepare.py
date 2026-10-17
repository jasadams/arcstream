#!/usr/bin/env python3
"""Prepare application expectations for native SESSION; never execute an engine."""
import argparse
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ORIGINAL = HERE.parent / 'session-mvp' / 'fixture.py'


def original():
    spec = importlib.util.spec_from_file_location('session_contract', ORIGINAL)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.fixture()


def milliseconds(text):
    delta = datetime.fromisoformat(text).replace(tzinfo=timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000


def source_rows(signals):
    rows = []
    for signal in signals:
        if signal['kind'] not in ('event', 'clock'):
            continue  # Feedback controls are not inputs to native SESSION.
        clock = signal['kind'] == 'clock'
        at = signal['event_time']
        rows.append(dict(event_id=signal['generation'], session_id=signal.get('session_id') or '',
                         tenant_id=signal['tenant_id'], canonical_id=signal['canonical_id'],
                         event_type=signal.get('event_type', ''),
                         **{key: signal.get(key) for key in ('page_url', 'device_type', 'browser', 'country')},
                         event_ts=at, event_ms=milliseconds(at), clock_ts=at,
                         arrival_seq=len(rows), is_clock=clock))
    return rows


DESCRIPTIONS = {
    'chronological': 'One session, ordered activity; another tenant advances real source event time past its deadline.',
    'late-first-arrival': 'Earlier timestamp arrives second: preserve first-arrival identity/start and last-arrival nonempty metadata.',
    'reopened-late': 'After closure and reopening at 00:40, a5 timestamp00:20 belongs to reopened state in the application contract; native timestamp SESSION may diverge.',
    'complete-original': 'All original five summaries, tenant isolation, stale/duplicate feedback independence, null session exclusion and pre-epoch boundary.',
    'chronological-recovery': 'Three sequential sessions with distinct identities and source-progress closures; fault after first committed summary while later summaries remain.',
}


def fixture(case):
    data = original()
    by_id = {row['generation']: row for row in data['inputs'] if row['kind'] in ('event', 'clock')}
    def clock(name, at):
        return dict(kind='clock', generation=name, tenant_id='progress', session_id='', canonical_id='progress', event_time=at)
    if case == 'chronological-recovery':
        signals = []
        expected = []
        # Explicit application oracle: do not derive expected aggregation from
        # the generated events or from native SESSION output.
        for index, hour in enumerate((0, 2, 4), 1):
            start = f'2020-02-01 {hour:02}:00:00.000'
            end = f'2020-02-01 {hour:02}:02:00.000'
            session = f'recovery-{index}'
            first = dict(by_id['a1'], generation=f'r{index}-first', event_id=f'r{index}-first',
                         tenant_id='tenant-recovery', session_id=session, canonical_id=f'first-{index}',
                         event_time=start, logical_time=start, session_source_ms=milliseconds(start))
            second = dict(by_id['a3'], generation=f'r{index}-second', event_id=f'r{index}-second',
                          tenant_id='tenant-recovery', session_id=session, canonical_id=f'later-{index}',
                          event_time=end, logical_time=end, session_source_ms=milliseconds(end))
            signals.extend([first, second, clock(f'close-{index}', f'2020-02-01 {hour:02}:40:00.000')])
            # Extra real source progress records ensure the first checkpoint
            # cannot read ahead directly to the final application's result.
            if index < 3:
                signals.extend(clock(f'progress-{index}-{minute}', f'2020-02-01 {hour:02}:{minute:02}:00.000')
                               for minute in (41, 42, 43, 44))
            expected.append(dict(tenant_id='tenant-recovery', session_id=session,
                                 canonical_id=f'first-{index}', start_time=start, end_time=end,
                                 duration_sec=120, event_count=2, pages=['/first'],
                                 event_types={'custom_event': 1, 'page_view': 1},
                                 device_type='mobile', browser='Firefox', country='US'))
    elif case == 'complete-original':
        signals = data['inputs'] + [clock('final-progress', '2020-02-01 02:00:00.000')]
        expected = data['expected']
    elif case == 'chronological':
        signals = [by_id['a1'], by_id['a3'], clock('close', '2020-02-01 00:40:00.000')]
        expected = [dict(data['expected'][0], event_count=2, pages=['/first'],
                         event_types={'custom_event': 1, 'page_view': 1}, browser='Firefox')]
    elif case == 'late-first-arrival':
        signals = [by_id['a1'], by_id['a2'], by_id['a3'], clock('close', '2020-02-01 00:40:00.000')]
        expected = [data['expected'][0]]
    elif case == 'reopened-late':
        signals = [by_id['a1'], by_id['a3'], by_id['clock1'], by_id['a4'], by_id['a5'],
                   clock('close', '2020-02-01 01:20:00.000')]
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
                    expected_rows=len(data['expected']), clock_contract='clock_ts equals raw event_time; no synthetic dispatch or lateness offset',
                    source_watermark='Candidate SQL must declare its actual lateness separately; these fixtures do not choose it.',
                    output_order='Expected order follows application closure; cross-key native ordering must be reported explicitly.',
                    status='prepared only; expected is application oracle, not a claimed native result',
                    source_sha256={str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in [ORIGINAL, Path(__file__)]},
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
