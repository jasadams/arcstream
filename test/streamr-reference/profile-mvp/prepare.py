#!/usr/bin/env python3
"""Prepare exact full-profile inputs/oracles and the public API UDF payload."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from fixture import fixture, live_fixture, boundary_fixture, feedback_fixture

ROOT = Path(__file__).resolve().parents[3]


def write_rows(path, rows):
    path.write_text(''.join(json.dumps(row, sort_keys=True, separators=(',', ':')) + '\n' for row in rows))


def prepare(directory, case='replay'):
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=False)
    data = {'live':live_fixture,'replay':fixture,'boundary':boundary_fixture,'feedback':feedback_fixture}[case]()
    write_rows(directory/'input.jsonl', data['inputs'])
    write_rows(directory/'expected.jsonl', data['expected'])
    write_rows(directory/'expected-timer-seeds.jsonl', data['timer_seeds'])
    # Independent transport oracle: one persisted monotone pulse per raw
    # event/clock; returned controls retain the previous pulse and progress.
    project_timers=[];maximum=None;dispatch=None
    def text_time(value):
        return datetime.fromtimestamp(value/1000,timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    for signal in data['inputs']:
        if signal['kind'] in ('event','clock'):
            maximum=max(maximum if maximum is not None else signal['source_ms'],signal['source_ms'])
            dispatch=max(dispatch+2000 if dispatch is not None else -(2**63),
                         signal['effective_ms']+31*86400000,signal['source_ms']+31*86400000)
        assert maximum is not None and dispatch is not None
        if signal['kind']=='event':
            project_timers.extend(row for row in data['timer_seeds'] if row['generation']==signal['generation'])
        progress=text_time(maximum-5001)
        project_timers.append(dict(tenant_id='',canonical_id='',session_id='',generation='arc-technical-clock',
            kind='clock',deadline_time=progress,progress_time=progress,dispatch_time=text_time(dispatch),
            observed_ms=None,replay_mode=False))
    write_rows(directory/'expected-project-timer-seeds.jsonl',project_timers)
    source = ROOT/'deploy/streamr-native/profile-mvp'
    query = (source/'owner.sql').read_text().replace('{{INPUT}}', str(directory/'input.jsonl').replace("'", "''")).replace('{{OUTPUT}}', str(directory/'outbox.jsonl').replace("'", "''"))
    (directory/'owner.sql').write_text(query)
    project=(source/'project.sql').read_text().replace('{{INPUT}}',str(directory/'outbox.jsonl')).replace('{{OUTPUT}}',str(directory/'output.jsonl')).replace('{{TIMERS}}',str(directory/'timer-seeds.jsonl'))
    (directory/'project.sql').write_text(project)
    udf = (source/'profile_transition.rs').read_text()
    timer_udf=(ROOT/'deploy/streamr-native/timer_outbox.rs').read_text()
    (directory/'timer_outbox.rs').write_text(timer_udf)
    payload = dict(name='Arcstream full profile fixture', query=query,
                   udfs=[dict(language='rust', definition=udf)], parallelism=1,
                   checkpoint_interval_micros=2_000_000)
    (directory/'project-pipeline-payload.json').write_text(json.dumps(dict(name='Arcstream profile projector fixture',query=project,udfs=[dict(language='rust',definition=timer_udf)],parallelism=1,checkpoint_interval_micros=2_000_000),indent=2)+'\n')
    (directory/'pipeline-payload.json').write_text(json.dumps(payload, indent=2)+'\n')
    manifest = dict(scope='All33 finite owner fixture; native timer and live connector acceptance separate',
                    source_events=len(data['timer_seeds'])//4, input_signals=len(data['inputs']),
                    output_rows=len(data['expected']),outbox_rows=len(data['inputs']),timer_seed_rows=len(data['timer_seeds']),project_timer_seed_rows=len(project_timers),
                    fixture_case=case,clock_mode='live' if case == 'live' else 'replay',checkpoint_after_input=data['checkpoint_after'], checkpoint_output_rows=data['checkpoint_after'],
                    files={str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                           for path in [ROOT/'deploy/streamr-native/timer_outbox.rs',source/'profile_transition.rs', source/'owner.sql',
                                        source/'timers.sql',source/'project.sql', Path(__file__), Path(__file__).with_name('fixture.py')]})
    (directory/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--case',choices=['replay','live','boundary','feedback'],default='replay')
    args = parser.parse_args()
    print(json.dumps(prepare(args.directory,args.case)))
