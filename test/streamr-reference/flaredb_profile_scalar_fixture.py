#!/usr/bin/env python3
"""Ingest native scalar-profile output into published FlareDB and restart it."""
import argparse
from datetime import datetime, timezone, timedelta
import json
import os
from pathlib import Path
import subprocess
import time
import urllib.request
import uuid

FLARE = 'ghcr.io/jasadams/flaredb:arcstream-2026-10-17-95b58cb@sha256:3c8ea585571f4e12f24761356febc071ee018600bc0a58a1848c44312f913a91'
ARRAYS = {'top_pages', 'top_features', 'changed_fields'}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', type=Path, required=True)
    parser.add_argument('--input', type=Path, required=True, help='actual native projection output JSONL')
    parser.add_argument('--expected', type=Path, required=True, help='original logical full33 profile JSONL')
    a = parser.parse_args()
    if not os.environ.get('RUST_BUILD_QUEUE_HELD'):
        raise ValueError('run through scripts/dev-build shared queue')
    a.case.mkdir(parents=True, exist_ok=False)
    commands, containers, network, volume = [], [], None, None
    result = {'status': 'failed', 'image': FLARE}

    def command(argv, data=None):
        p = subprocess.run(argv, input=data, capture_output=True, text=True, timeout=120)
        commands.append({'argv': argv, 'exit': p.returncode, 'stdout': p.stdout, 'stderr': p.stderr})
        (a.case/'commands.json').write_text(json.dumps(commands, indent=2)+'\n')
        p.check_returncode()
        return p.stdout.strip()

    def query(base, sql):
        request = urllib.request.Request(base+'/query/sql', data=json.dumps({'sql': sql}).encode(),
                                         headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=15) as response:
            return json.load(response)

    expected = [json.loads(row) for row in a.expected.read_text().splitlines()]
    schema = json.loads((Path(__file__).resolve().parents[2]/'deploy/config/flaredb-tables/profiles.json').read_text())
    assert len(schema['schema']) == 33
    timestamp_fields = {f['name'] for f in schema['schema'] if f['type'].startswith('Timestamp')}
    for row in expected:
        for field in timestamp_fields:
            if isinstance(row[field], str):
                value = datetime.fromisoformat(row[field].replace('Z', '+00:00'))
                if value.tzinfo is None:
                    value = value.replace(tzinfo=timezone.utc)
                row[field] = (value.astimezone(timezone.utc)-datetime(1970,1,1,tzinfo=timezone.utc))//timedelta(milliseconds=1)
    expected.sort(key=lambda row: row['canonical_id'])
    columns = [f['name'] for f in schema['schema']]
    select = ', '.join('CAST('+name+' AS BIGINT) AS '+name if name in timestamp_fields else name for name in columns)
    sql = 'SELECT '+select+' FROM profiles ORDER BY canonical_id'
    label = 'arc-scalar-'+uuid.uuid4().hex
    try:
        for kind in ['network', 'volume', 'container']:
            name = label if kind != 'container' else label+'-broker'
            if subprocess.run(['podman', kind, 'exists', name], capture_output=True).returncode != 1:
                raise ValueError('isolated resource preflight failed: '+name)
        network = command(['podman','network','create','--label','arc.scalar='+label,label])
        volume = command(['podman','volume','create','--label','arc.scalar='+label,label])
        flare_image = json.loads(command(['podman','image','inspect',FLARE]))[0]
        broker_image = json.loads(command(['podman','image','inspect','docker.io/redpandadata/redpanda:v24.3.7']))[0]
        result.update(flaredb_image_id=flare_image['Id'], broker_image_id=broker_image['Id'])
        broker = command(['podman','run','-d','--name',label+'-broker','--network',network,
            '--network-alias','broker','--entrypoint','/usr/bin/rpk',broker_image['Id'],
            'redpanda','start','--mode=dev-container','--smp=1','--memory=512M','--reserve-memory=0M',
            '--overprovisioned','--kafka-addr=internal://0.0.0.0:9092','--advertise-kafka-addr=internal://broker:9092'])
        containers.append(broker)
        for _ in range(90):
            if subprocess.run(['podman','exec',broker,'rpk','cluster','info'],capture_output=True).returncode==0:
                break
            time.sleep(.2)
        else:
            raise ValueError('private broker not ready')
        command(['podman','exec',broker,'rpk','topic','create','profiles-scalar','--partitions','1','--replicas','1'])
        configdir = a.case/'config';configdir.mkdir()
        schema['ingestionConfig'].update(topic='profiles-scalar',brokers='broker:9092',maxRowsPerSegment=1)
        (configdir/'profiles.json').write_text(json.dumps(schema,indent=2)+'\n')
        flare = command(['podman','run','-d','--network',network,'--stop-signal','SIGINT','--memory','1536m',
            '-v',volume+':/data','-v',str(configdir.resolve())+':/config:ro','-p','127.0.0.1::8099',flare_image['Id'],
            '--data-dir','/data','--http','0.0.0.0:8099','--load-tables','/config','--memory-limit','512M'])
        containers.append(flare)
        base = 'http://'+command(['podman','port',flare,'8099/tcp']).splitlines()[0]
        command(['podman','exec','-i',broker,'rpk','topic','produce','profiles-scalar'],a.input.read_text())
        observations = []
        def verify():
            deadline = time.monotonic()+90
            while time.monotonic()<deadline:
                try:
                    response = query(base, sql)
                    data = response['result_table']; names = data['data_schema']['column_names']
                    actual = [dict(zip(names, row)) for row in data['rows']]
                    for row in actual:
                        for field in ARRAYS:
                            if isinstance(row[field], str):
                                row[field] = json.loads(row[field])
                    if actual == expected:
                        observations.append(response)
                        return
                except Exception as error:
                    result['last_observation_error'] = str(error)
                time.sleep(.3)
            raise ValueError('full33 stored profile values differ from explicit logical expected results')
        verify()
        command(['podman','stop','--time','30',flare])
        command(['podman','start',flare])
        base = 'http://'+command(['podman','port',flare,'8099/tcp']).splitlines()[0]
        verify()
        result.update(status='passed', profiles=len(expected), columns=len(columns), observations=observations)
    finally:
        if containers:
            try:
                (a.case/'flaredb.log').write_text(command(['podman','logs',containers[-1]]))
            except Exception:
                pass
        cleanup = []
        for cid in reversed(containers):
            p = subprocess.run(['podman','rm','-f',cid],capture_output=True,text=True)
            cleanup.append({'resource':cid,'exit':p.returncode})
        for kind, name in [('volume',volume),('network',network)]:
            if name:
                p=subprocess.run(['podman',kind,'rm',name],capture_output=True,text=True)
                cleanup.append({'resource':name,'exit':p.returncode})
        result['cleanup'] = cleanup
        if any(item['exit'] != 0 for item in cleanup):
            result['status'] = 'failed-cleanup'
        (a.case/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    assert result['status']=='passed', result
    print(json.dumps({'status':result['status'],'profiles':len(expected),'columns':len(columns)}))


if __name__ == '__main__':
    main()
