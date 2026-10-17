#!/usr/bin/env python3
"""Prepare the finite full12 session native owner/project acceptance fixture."""
import argparse,hashlib,json
from pathlib import Path
from fixture import fixture
ROOT=Path(__file__).resolve().parents[3]
def rows(path,values):path.write_text(''.join(json.dumps(value,sort_keys=True)+'\n' for value in values))
def prepare(directory):
 directory=Path(directory).resolve();directory.mkdir(parents=True,exist_ok=False);data=fixture()
 rows(directory/'input.jsonl',data['inputs']);rows(directory/'expected.jsonl',data['expected']);rows(directory/'expected-timers.jsonl',data['timer_seeds'])
 source=ROOT/'deploy/streamr-native/session-mvp'
 for name in ['owner.sql','project.sql','timers.sql','session_transition.rs']:
  (directory/name).write_text((source/name).read_text())
 manifest=dict(input_rows=len(data['inputs']),output_rows=len(data['expected']),timer_seed_rows=len(data['timer_seeds']),
               checkpoint_after_input=data['checkpoint_after'],typed_owners=2,
               semantics='Full12 raw-source session lifecycle, late admission, prior-watermark catchup, native DELETE, same session reuse, tenant isolation',
               files={str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in [*source.glob('*'),Path(__file__),Path(__file__).with_name('fixture.py')] if path.is_file()})
 (directory/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n');return manifest
if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('directory',type=Path);a=p.parse_args();print(json.dumps(prepare(a.directory)))
