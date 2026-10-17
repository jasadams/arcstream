#!/usr/bin/env python3
"""Prepare exact full-profile inputs/oracles and the public API UDF payload."""
import argparse
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
    source = ROOT/'deploy/streamr-native/profile-mvp'
    query = (source/'owner.sql').read_text().replace('{{INPUT}}', str(directory/'input.jsonl').replace("'", "''")).replace('{{OUTPUT}}', str(directory/'outbox.jsonl').replace("'", "''"))
    (directory/'owner.sql').write_text(query)
    project=(source/'project.sql').read_text().replace('{{INPUT}}',str(directory/'outbox.jsonl')).replace('{{OUTPUT}}',str(directory/'output.jsonl')).replace('{{TIMERS}}',str(directory/'timer-seeds.jsonl'))
    (directory/'project.sql').write_text(project)
    udf = (source/'profile_transition.rs').read_text()
    payload = dict(name='Arcstream full profile fixture', query=query,
                   udfs=[dict(language='rust', definition=udf)], parallelism=1,
                   checkpoint_interval_micros=2_000_000)
    (directory/'pipeline-payload.json').write_text(json.dumps(payload, indent=2)+'\n')
    manifest = dict(scope='All33 finite owner fixture; native timer and live connector acceptance separate',
                    source_events=len(data['timer_seeds'])//4, input_signals=len(data['inputs']),
                    output_rows=len(data['expected']),outbox_rows=len(data['inputs']),timer_seed_rows=len(data['timer_seeds']),
                    fixture_case=case,clock_mode='live' if case == 'live' else 'replay',checkpoint_after_input=data['checkpoint_after'], checkpoint_output_rows=data['checkpoint_after'],
                    files={str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                           for path in [source/'profile_transition.rs', source/'owner.sql',
                                        source/'timers.sql',source/'project.sql', Path(__file__), Path(__file__).with_name('fixture.py')]})
    (directory/'manifest.json').write_text(json.dumps(manifest, indent=2)+'\n')
    return manifest


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('directory', type=Path)
    parser.add_argument('--case',choices=['replay','live','boundary','feedback'],default='replay')
    args = parser.parse_args()
    print(json.dumps(prepare(args.directory,args.case)))
