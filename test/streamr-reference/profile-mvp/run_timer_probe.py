#!/usr/bin/env python3
"""Retain a runnable late-timer native SQL attempt; no engine changes.

--execute requires the maintained shared queue reservation. The harness admits a
range of physical output counts; the separate semantic oracle requires BOTH
deadlines, so a dropped late seed remains a failed application attempt.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[3]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--case-dir',type=Path,required=True)
    p.add_argument('--binary',type=Path,required=True)
    p.add_argument('--executable-receipt',type=Path,required=True)
    p.add_argument('--source-commit',required=True)
    p.add_argument('--env-template',type=Path,required=True)
    p.add_argument('--execute',action='store_true')
    a = p.parse_args()
    case = a.case_dir.resolve()
    binary = a.binary.resolve(strict=True)
    receipt = json.loads(a.executable_receipt.read_text())
    assert receipt['binary_sha256'] == digest(binary)
    assert receipt.get('source_commit',receipt.get('commit',receipt.get('head'))) == a.source_commit
    case.mkdir(parents=True,exist_ok=False)
    rows = []
    for generation,base,progress,kind in [
        ('first','2020-02-01 00:35:00.000','2020-02-01 00:05:00.000','session_timeout'),
        ('late','2020-02-01 00:32:00.000','2020-02-01 00:05:00.000','session_timeout'),
        ('overdue','2020-02-01 00:00:00.000','2020-02-01 00:05:00.000','session_timeout'),
        ('clock','2020-02-01 02:00:00.000','2020-02-01 02:00:00.000','clock')]:
        rows.append(dict(tenant_id='tenant-a',canonical_id='person-a',generation=generation,
                         kind=kind,deadline_time=base,progress_time=progress,
                         observed_ms=1893456000000,replay_mode=True))
    (case/'input.jsonl').write_text(''.join(json.dumps(row)+'\n' for row in rows))
    template = ROOT/'deploy/streamr-native/profile-mvp/timers.sql'
    query = template.read_text().replace('{{INPUT}}',str(case/'input.jsonl')).replace('{{OUTPUT}}',str(case/'output.jsonl'))
    (case/'query.sql').write_text(query)
    expected = [dict(tenant_id='tenant-a',canonical_id='person-a',generation=generation,
                     kind='session_timeout',logical_time=at,observed_ms=1893456000000,replay_mode=True)
                for generation,at in [('late','2020-02-01 00:32:00.000'),('first','2020-02-01 00:35:00.000'),('overdue','2020-02-01 00:00:00.000')]]
    (case/'expected.json').write_text(json.dumps(expected,indent=2)+'\n')
    env = json.loads(a.env_template.read_text())['env']
    env.update(STREAMR_CAPTURE_QUERY=str(case/'query.sql'),STREAMR_CAPTURE_OUTPUT=str(case/'output.jsonl'),
               STREAMR_CAPTURE_INPUT_ROWS_BEFORE_CHECKPOINT='2',STREAMR_CAPTURE_EXPECTED_INITIAL_ROWS='3',
               STREAMR_CAPTURE_MAX_INITIAL_ROWS='3',STREAMR_CAPTURE_EXPECTED_CHECKPOINT_ROWS='0',
               STREAMR_CAPTURE_MAX_CHECKPOINT_ROWS='0',STREAMR_CAPTURE_EXPECTED_ROWS='3',
               STREAMR_CAPTURE_MAX_ROWS='3',STREAMR_TEST_NATIVE_WINDOWS='1',TMPDIR=str(case))
    command = [str(binary),'--ignored','--exact','smoke_tests::external_sql_checkpoint_capture','--nocapture','--test-threads=1']
    plan = dict(command=command,env=env,binary_sha256=digest(binary),source_commit=a.source_commit,
                files={str(path):digest(path) for path in [template,Path(__file__),case/'input.jsonl',case/'expected.json',a.executable_receipt]})
    (case/'command.json').write_text(json.dumps(plan,indent=2)+'\n')
    if not a.execute:
        print(json.dumps(dict(status='prepared',case=str(case))))
        return
    assert os.environ.get('RUST_BUILD_QUEUE_HELD'), 'use maintained scripts/rust-build reservation'
    image='bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e'
    name='arc17-timer-probe-'+str(os.getpid())
    qualified=binary.parent
    argv=['podman','run','--rm','--name',name,'--network','none','--security-opt','label=disable',
          '-v',str(ROOT)+':'+str(ROOT)+':ro','-v',str(qualified)+':'+str(qualified)+':ro',
          '-v',str(case)+':'+str(case)+':rw','-w',str(ROOT)]
    for key,value in env.items():
        argv.extend(['-e',key+'='+value])
    argv.extend([image]+command)
    result=dict(status='failed',container_command=argv)
    try:
        with (case/'runtime.log').open('w') as log:
            process=subprocess.run(argv,stdout=log,stderr=subprocess.STDOUT,timeout=210)
        result['exit']=process.returncode
        if process.returncode == 0:
            actual=[json.loads(line) for line in (case/'output.jsonl').read_text().splitlines()]
            encode=lambda rows: sorted(json.dumps(row,sort_keys=True) for row in rows)
            result.update(actual=actual,expected=expected,
                          semantic_oracle_passed=encode(actual)==encode(expected))
            result['status']='passed' if result['semantic_oracle_passed'] else 'semantic-failure'
    except BaseException as error:
        result['error']=repr(error)
        raise
    finally:
        cleanup=subprocess.run(['podman','rm','-f',name],capture_output=True,text=True)
        result['cleanup_exit']=cleanup.returncode
        (case/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
