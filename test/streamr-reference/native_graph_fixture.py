#!/usr/bin/env python3
"""Owned Kafka five-query graph; invoke through the shared build/run queue."""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess


def load_module(name, path):
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def resource_config(base, resource_path):
    """Merge maintained allocations without replacing machine-specific paths."""
    import copy
    config=copy.deepcopy(base)
    resources=json.loads(resource_path.read_text())
    def merge(target, source):
        for key,value in source.items():
            if isinstance(value,dict):
                if key not in target:target[key]={}
                if not isinstance(target[key],dict):raise ValueError('resource config type differs: '+key)
                merge(target[key],value)
            else:target[key]=value
    merge(config,resources)
    return config,dict(path=str(resource_path.resolve()),sha256=hashlib.sha256(resource_path.read_bytes()).hexdigest(),allocations=resources)


def run(argv, **options):
    return subprocess.run(argv, text=True, capture_output=True, check=True, timeout=120, **options).stdout.strip()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('case', 'binary-pin',
                 'engine-helper', 'config-template', 'python-runtime'):
        p.add_argument('--'+name, type=Path, required=True)
    p.add_argument('--runtime-image', required=True)
    p.add_argument('--broker-image', required=True)
    p.add_argument('--fixtures', type=Path, default=Path(__file__).resolve().parent/'graph-release-fixtures')
    p.add_argument('--timeout', type=int, default=720)
    p.add_argument('--scan-diagnostics', action='store_true', help='Enable native opt-in scan counters; no performance qualification')
    a = p.parse_args()
    for selected in (a.runtime_image, a.broker_image):
        if not re.fullmatch(r'(?:sha256:)?[a-f0-9]{64}', selected):
            p.error('image arguments must be immutable image IDs')
    if not os.environ.get('RUST_BUILD_QUEUE_HELD'):
        p.error('use the maintained shared build/run queue')
    case = a.case.resolve(); case.mkdir(parents=True, exist_ok=False)
    token = hashlib.sha256(str(case).encode()).hexdigest()[:12]
    prefix = 'arc-graph-'+token
    network, broker, engine, topic = (prefix+'-'+suffix for suffix in ('network', 'broker', 'engine', 'input'))
    output_topic, recovery_topic = prefix+'-output', prefix+'-recovery'
    broker_id = network_id = None
    cidfile = case/'engine.cid'
    result = {'status': 'failed', 'resources': {}, 'cleanup': []}
    def write(name, value):
        (case/name).write_text(json.dumps(value, indent=2)+'\n')
    write('invocation.json', {'argv':__import__('sys').argv, 'cwd':os.getcwd()})
    try:
        runtime_image = json.loads(run(['podman','image','inspect',a.runtime_image]))[0]['Id']
        broker_image = json.loads(run(['podman','image','inspect',a.broker_image]))[0]['Id']
        write('images.json', {'runtime': runtime_image, 'broker': broker_image})
        if runtime_image.removeprefix('sha256:') != a.runtime_image.removeprefix('sha256:') or broker_image.removeprefix('sha256:') != a.broker_image.removeprefix('sha256:'):
            raise ValueError('resolved image identity differs from requested pin')
        for name in (broker, engine):
            existing = subprocess.run(['podman','container','exists',name], capture_output=True)
            if existing.returncode != 1:
                raise ValueError('test container name already exists or cannot be inspected: '+name)
        network_id = run(['podman','network','create',network])
        network_info = json.loads(run(['podman','network','inspect',network_id]))[0]
        network_id = network_info.get('id', network_info.get('Id'))
        if not network_id:
            raise ValueError('owned network identity absent')
        broker_id = run(['podman','run','-d','--name',broker,'--network',network,
            '--network-alias','broker','--memory','1g','--entrypoint','/usr/bin/rpk',broker_image,
            'redpanda','start','--mode=dev-container','--smp=1','--memory=512M',
            '--reserve-memory=0M','--overprovisioned','--kafka-addr=internal://0.0.0.0:9092',
            '--advertise-kafka-addr=internal://broker:9092'])
        if not re.fullmatch('[a-f0-9]{64}', broker_id):
            raise ValueError('owned broker identity absent')
        result['resources'].update(network=network_id, broker=broker_id)
        import time
        for _ in range(60):
            ready = subprocess.run(['podman','exec',broker_id,'rpk','cluster','info','-X','brokers=localhost:9092'], capture_output=True, timeout=10)
            if ready.returncode == 0:
                break
            time.sleep(.5)
        else:
            raise ValueError('owned broker startup failed')
        topics = {}
        suffixes = ('raw-events','identity-capture','unified-events','identity-merges','sessions','profiles','identity-commits','unified-commits','merges-commits','sessions-commits','profiles-commits')
        for owned_topic in (prefix+'-'+suffix for suffix in suffixes):
            create = ['podman','exec',broker_id,'rpk','topic','create',owned_topic,
                      '-p','1','-r','1','-X','brokers=localhost:9092']
            if owned_topic.endswith('-commits'):
                create += ['-c','cleanup.policy=compact']
            run(create)
            topics[owned_topic] = json.loads(run(['podman','exec',broker_id,'rpk',
                'topic','describe',owned_topic,'--format','json','-X','brokers=localhost:9092']))
        write('topics.json', topics)
        run(['podman','cp',broker_id+':/opt/redpanda/libexec/rpk',str(case/'rpk')])
        repo = Path(__file__).resolve().parents[2]
        helpers = load_module('arcstream_prepare', repo/'deploy/streamr-native/prepare.py')
        queries = case/'queries'
        helpers.prepare(queries, 'broker:9092', prefix, profile_release=True)
        fixture_dir = case/'fixture'
        __import__('shutil').copytree(a.fixtures, fixture_dir)
        resource_path=repo/'deploy/streamr-native/runtime-resources.json'
        fixture_config,resource_receipt=resource_config(json.loads(a.config_template.read_text()),resource_path)
        write('runtime-resources-receipt.json',resource_receipt)
        result['runtime_resources']=resource_receipt
        # Use ordinary application emission cadence, not the qualification
        # harness's one-hour batching/flush controls. Native time is unchanged.
        overrides = {'source-batch-size':1,'source-batch-linger':'50ms',
                     'update-aggregate-flush-interval':'100ms'}
        fixture_config.setdefault('pipeline', {}).update(overrides)
        write('fixture-config.json', fixture_config)
        result['pipeline_config_overrides'] = overrides
        result['worker_config_overrides'] = resource_receipt['allocations']['worker']
        result['files_sha256'] = {str(path):hashlib.sha256(path.read_bytes()).hexdigest() for path in (Path(__file__),repo/'deploy/streamr-native/prepare.py',a.config_template,resource_path)}
        pin = a.binary_pin.resolve()
        argv = ['podman','run','--rm','--name',engine,'--cidfile',str(cidfile),
                '--network',network,'--security-opt','label=disable','--memory','2g']
        mounts = {(repo,repo,'ro'), (pin,pin,'ro'), (case,case,'rw'),
                  (a.engine_helper.resolve(),Path('/runner/helper.py'),'ro'),
                  (a.config_template.resolve(),a.config_template.resolve(),'ro'),
                  (a.python_runtime.resolve(),a.python_runtime.resolve(),'ro')}
        for source,target,mode in sorted(mounts, key=lambda item: str(item[1])):
            argv += ['-v',str(source)+':'+str(target)+':'+mode]
        if a.scan_diagnostics:
            argv += ['-e','STREAMR_LIVE_SCAN_DIAGNOSTICS=1']
        argv += ['-e','LD_LIBRARY_PATH='+str(pin/'package/lib'),
                 '-e','PYTHONPATH='+str(a.python_runtime.resolve()),'-e','PYTHONDONTWRITEBYTECODE=1',
                 runtime_image,'python3',str(repo/'test/streamr-reference/native_graph_runtime.py'),
                 '--case',str(case/'runtime'),'--binary',str(pin/'package/bin/arroyo'),
                 '--binary-receipt',str(pin/'source-manifest.json'),'--engine-helper','/runner/helper.py',
                 '--config-template',str(case/'fixture-config.json'),'--queries',str(queries),
                 '--fixtures',str(fixture_dir),'--namespace',prefix,
                 '--kafka-rpk',str(case/'rpk'),'--kafka-broker','broker:9092',
                 '--port','18428','--timeout',str(a.timeout)]
        write('command.json',argv)
        with (case/'container.log').open('w') as output:
            completed = subprocess.run(argv, stdout=output, stderr=subprocess.STDOUT, timeout=a.timeout+120)
        result['inner_exit'] = completed.returncode
        if completed.returncode != 0:
            raise ValueError('native fixture failed; see container.log and runtime/result.json')
        result['status'] = 'passed'
    except BaseException as error:
        result['error'] = type(error).__name__+': '+str(error)
        raise
    finally:
        def cleanup(argv, resource):
            try:
                cleaned = subprocess.run(argv, capture_output=True, text=True, timeout=30)
                result['cleanup'].append({'resource':resource,'exit':cleaned.returncode})
            except BaseException as error:
                result['cleanup'].append({'resource':resource,'exit':None,'error':str(error)})
        if broker_id:
            try:
                logs = subprocess.run(['podman','logs',broker_id], capture_output=True, text=True, timeout=20)
                (case/'broker.log').write_text(logs.stdout+logs.stderr)
            except BaseException as error:
                result['broker_log_error'] = str(error)
        if cidfile.exists():
            cid = cidfile.read_text().strip()
            if re.fullmatch('[a-f0-9]{64}',cid):
                present = subprocess.run(['podman','container','exists',cid],capture_output=True,timeout=10)
                if present.returncode == 0:
                    cleanup(['podman','rm','--force','--volumes',cid],cid)
                elif present.returncode != 1:
                    result['cleanup'].append({'resource':cid,'exit':None,'error':'container inspection failed'})
            else:
                result['cleanup'].append({'resource':'engine','exit':None,'error':'invalid owned identity'})
        for resource, argv in ((broker_id,['podman','rm','--force','--volumes']), (network_id,['podman','network','rm'])):
            if resource:
                cleanup([*argv,resource],resource)
        if any(row['exit'] != 0 for row in result['cleanup']):
            result['status'] = 'failed'
        write('outer-result.json',result)
    if result['status'] != 'passed':
        raise ValueError('native fixture cleanup failed')
    print(json.dumps(result))


if __name__ == '__main__':
    main()
