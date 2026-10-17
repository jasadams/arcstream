#!/usr/bin/env python3
"""Fresh isolated pinned-image health/SQL smoke. Invoke through the shared queue."""
import argparse
import hashlib
import json
import os
import uuid
from pathlib import Path
import subprocess
import time
import urllib.request


COMMANDS = []
COMMAND_FILE = None

def command(argv):
    process = subprocess.run(argv, capture_output=True, text=True)
    COMMANDS.append({'argv': argv, 'exit': process.returncode, 'stdout': process.stdout, 'stderr': process.stderr})
    if COMMAND_FILE is not None:
        COMMAND_FILE.write_text(json.dumps(COMMANDS, indent=2)+'\n')
    process.check_returncode()
    return process.stdout.strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--case', type=Path, required=True)
    parser.add_argument('--image', default='ghcr.io/jasadams/flaredb:arcstream-2026-10-17-95b58cb@sha256:3c8ea585571f4e12f24761356febc071ee018600bc0a58a1848c44312f913a91')
    parser.add_argument('--source-commit', default='95b58cb1fccc650c3b484e8b5c05e440508ae6ce')
    parser.add_argument('--expected-image-tag', default='arcstream-2026-10-17-95b58cb')
    args = parser.parse_args()
    if not os.environ.get('RUST_BUILD_QUEUE_HELD'):
        raise ValueError('run through maintained scripts/dev-build shared queue')
    if '@sha256:' not in args.image:
        raise ValueError('immutable registry digest required')
    case = args.case.resolve()
    case.mkdir(parents=True, exist_ok=False)
    global COMMAND_FILE
    COMMAND_FILE = case/'commands.json'
    inspected = json.loads(command(['podman', 'image', 'inspect', args.image]))[0]
    if inspected['Config']['User'] != '10001:10001':
        raise ValueError('unexpected image user')
    (case/'image.json').write_text(json.dumps(inspected, indent=2)+'\n')
    cidfile = case/'container.cid'
    result = {'status': 'failed', 'image': args.image, 'source_commit': args.source_commit,
              'source_binding': 'caller-provided published workflow binding; health does not expose git commit',
              'supervisor_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    volume = 'arc17-smoke-'+uuid.uuid4().hex
    volume_created = False
    if subprocess.run(['podman','volume','exists',volume],capture_output=True).returncode != 1:
        raise ValueError('fresh volume already exists or preflight failed')
    try:
        created = command(['podman','volume','create','--label','arc17.smoke='+volume,volume])
        if created != volume:
            raise ValueError('created volume identity differs')
        volume_created = True
        volume_info = json.loads(command(['podman','volume','inspect',volume]))[0]
        if volume_info['Labels'].get('arc17.smoke') != volume:
            raise ValueError('owned volume label differs')
        (case/'volume.json').write_text(json.dumps(volume_info,indent=2)+'\n')
        command(['podman', 'run', '-d', '--cidfile', str(cidfile), '--network', 'bridge',
                 '--memory', '1g', '-v', volume+':/data', '-p', '127.0.0.1::8099', inspected['Id'],
                 '--data-dir', '/data', '--http', '0.0.0.0:8099',
                 '--memory-limit', '512M'])
        cid = cidfile.read_text().strip()
        port = command(['podman', 'port', cid, '8099/tcp']).splitlines()[0]
        base = 'http://'+port
        result.update(port=port, base_url=base)
        deadline = time.monotonic()+90
        while True:
            try:
                with urllib.request.urlopen(base+'/health', timeout=2) as response:
                    health = json.load(response)
                break
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(.2)
        result['health'] = health
        if health['image_tag'] != args.expected_image_tag or health['status'] != 'healthy':
            raise ValueError('unexpected image tag or health status')
        request = urllib.request.Request(base+'/query/sql', data=b'{"sql":"SELECT 1 AS smoke"}',
                                         headers={'Content-Type':'application/json'})
        with urllib.request.urlopen(request, timeout=30) as response:
            result['query'] = json.load(response)
        if result['query']['result_table']['rows'] != [[1]]:
            raise ValueError('SELECT1 result differs')
        command(['podman','restart',cid])
        deadline = time.monotonic()+90
        while True:
            try:
                with urllib.request.urlopen(base+'/health',timeout=2) as response:
                    restarted = json.load(response)
                with urllib.request.urlopen(request,timeout=30) as response:
                    restart_query = json.load(response)
                if restarted['image_tag'] != args.expected_image_tag or restarted['status'] != 'healthy' or restart_query['result_table']['rows'] != [[1]]:
                    raise ValueError('restart health/SQL differs')
                break
            except Exception:
                if time.monotonic() >= deadline: raise
                time.sleep(.2)
        result.update(restart_health=restarted,restart_query=restart_query,restarted_same_owned_volume=volume)
        result['status'] = 'passed'
    except BaseException as error:
        result['error'] = repr(error)
        raise
    finally:
        if cidfile.exists():
            cid = cidfile.read_text().strip()
            logs = subprocess.run(['podman', 'logs', cid], capture_output=True, text=True)
            (case/'runtime.log').write_text(logs.stdout+logs.stderr)
            cleanup = subprocess.run(['podman', 'rm', '-f', cid], capture_output=True, text=True)
            remaining = subprocess.run(['podman', 'container', 'exists', cid], capture_output=True).returncode
            result['cleanup'] = {'removed_id': cid, 'exit': cleanup.returncode, 'complete': remaining == 1}
            if remaining != 1:
                result['status'] = 'failed'
        if volume_created:
            removal=subprocess.run(['podman','volume','rm',volume],capture_output=True,text=True)
            gone=subprocess.run(['podman','volume','exists',volume],capture_output=True).returncode==1
            result['volume_cleanup']={'name':volume,'exit':removal.returncode,'complete':gone}
            if not gone:result['status']='failed'
        (case/'result.json').write_text(json.dumps(result, indent=2)+'\n')
    if result['status'] != 'passed':
        raise ValueError('smoke failed')


if __name__ == '__main__':
    main()
