#!/usr/bin/env python3
"""Run a small native SQL fixture against an owned, isolated Kafka broker.

Invoke through the maintained shared queue. This creates test resources only
and runs the twenty-field profile candidate through a transactional Kafka sink.
It does not establish full thirty-three-field application readiness.
"""
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


def literal_stages(oracle):
    """Use independent literal snapshots; retain late input without emission."""
    cumulative = []
    stages = []
    for stage in oracle.CASES:
        if stage['input_lines'] <= 5:
            cumulative.append(getattr(oracle, 'A'+str(stage['input_lines'])))
        elif stage['input_lines'] == 7:
            cumulative.extend([oracle.A_QUIET, oracle.B1])
        stages.append(dict(input_lines=stage['input_lines'], name=stage['name'],
                           rows=stage['rows'], values=list(cumulative)))
    return stages


def run(argv, **options):
    return subprocess.run(argv, text=True, capture_output=True, check=True, timeout=120, **options).stdout.strip()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('case', 'query', 'binary-pin',
                 'engine-helper', 'config-template', 'python-runtime'):
        p.add_argument('--'+name, type=Path, required=True)
    p.add_argument('--runtime-image', required=True)
    p.add_argument('--broker-image', required=True)
    p.add_argument('--fault-prefix', type=int, default=1)
    p.add_argument('--input', type=Path)
    p.add_argument('--expected-prefixes', type=Path)
    p.add_argument('--keys', nargs='+', default=['tenant_id','canonical_id'])
    p.add_argument('--typed-owners', type=int, default=1)
    p.add_argument('--disk-owners', type=int, default=4)
    p.add_argument('--no-recovery', action='store_true',
                   help='stage observations only; never claim process recovery')
    a = p.parse_args()
    if (a.input is None) != (a.expected_prefixes is None):
        p.error('--input and --expected-prefixes must be supplied together')
    if min(a.typed_owners,a.disk_owners) < 0:
        p.error('owner counts must be nonnegative')
    if a.no_recovery and a.input is None:
        p.error('diagnostic mode requires explicit input and expected prefixes')
    for selected in (a.runtime_image, a.broker_image):
        if not re.fullmatch(r'(?:sha256:)?[a-f0-9]{64}', selected):
            p.error('image arguments must be immutable image IDs')
    if not os.environ.get('RUST_BUILD_QUEUE_HELD'):
        p.error('use the maintained shared build/run queue')
    case = a.case.resolve(); case.mkdir(parents=True, exist_ok=False)
    token = hashlib.sha256(str(case).encode()).hexdigest()[:12]
    prefix = 'arc-profile-'+token
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
        for owned_topic in (topic, output_topic, recovery_topic):
            create = ['podman','exec',broker_id,'rpk','topic','create',owned_topic,
                      '-p','1','-r','1','-X','brokers=localhost:9092']
            if owned_topic == recovery_topic:
                create += ['-c','cleanup.policy=compact']
            run(create)
            topics[owned_topic] = json.loads(run(['podman','exec',broker_id,'rpk',
                'topic','describe',owned_topic,'--format','json','-X','brokers=localhost:9092']))
        write('topics.json', topics)
        run(['podman','cp',broker_id+':/opt/redpanda/libexec/rpk',str(case/'rpk')])
        repo = Path(__file__).resolve().parents[2]
        helpers = load_module('arcstream_prepare', repo/'deploy/streamr-native/prepare.py')
        fixture_dir = case/'fixture'
        stages_path = case/'cumulative-prefixes.json'
        if a.input is None:
            oracle = load_module('profile_oracle', repo/'test/streamr-reference/profile-native/prepare.py')
            oracle.prepare(fixture_dir)
            stages = literal_stages(oracle)
        else:
            fixture_dir.mkdir()
            (fixture_dir/'input.jsonl').write_bytes(a.input.read_bytes())
            (fixture_dir/'expected-prefixes-original.json').write_bytes(a.expected_prefixes.read_bytes())
            stages = json.loads(a.expected_prefixes.read_text())
        write('cumulative-prefixes.json', stages)
        query_path = case/'query.sql'
        original_query = a.query.read_text()
        (case/'query-original.sql').write_text(original_query)
        query_path.write_text(helpers.replace_once(
            helpers.replace_once(original_query, '{{SOURCE_OPTIONS}}',
                helpers.source_options('broker:9092', topic, topic+'-reader')),
            '{{SINK_OPTIONS}}', helpers.sink_options('broker:9092', output_topic,
                recovery_topic, 'json')))
        if '{{' in query_path.read_text():
            raise ValueError('unresolved SQL template placeholder')
        result['query_template_sha256'] = hashlib.sha256(original_query.encode()).hexdigest()
        fixture_config = json.loads(a.config_template.read_text())
        # Use ordinary application emission cadence, not the qualification
        # harness's one-hour batching/flush controls. Native time is unchanged.
        overrides = {'source-batch-size':1,'source-batch-linger':'50ms',
                     'update-aggregate-flush-interval':'100ms'}
        fixture_config.setdefault('pipeline', {}).update(overrides)
        # This composed query owns multiple native databases. Allocate a shared
        # 4 MiB scan pool with 8 database slots: the engine's checkpoint merge
        # page formula gives 4 MiB // (8 * (8 + 1)) - 32768 = 25486 bytes,
        # enough for two configured 8192-byte rows plus the 1024-byte overhead.
        # Retain native admission validation; this is an explicit allocation.
        live_resources = fixture_config['worker'].setdefault('live-state-resources', {})
        # Typed scan admission reserves 64 entries * 128 KiB decoded * 3 =
        # 24 MiB (typed_table.rs validate_headroom). Allocate a bounded 32 MiB
        # shared decoded pool, covering that page and ordinary scope/read
        # headroom. Keep page entries, encoded rows and backend bounds intact.
        live_resources.update({'max-open-databases':8,'scan-page-bytes':4194304,
                               'decoded-value-bytes':33554432})
        # The eighteen-scalar snapshot is encoded as a complete Arrow IPC
        # schema and row. Its observed 4 KiB typed-row limit failed; allocate
        # 8 KiB independently of the disk backend's unchanged row bound.
        # This is an application allocation, not a measured exact encoded size.
        fixture_config['worker']['typed-sql-state']['row-bytes'] = 8192
        # Actual restored Arrow-row accounting required 65896 bytes, above
        # the qualification fixture's 16384-byte decoded-row limit. Allocate
        # 128 KiB; retain encoded-row and disk bounds. The shared decoded
        # allocation is recorded above.
        fixture_config['worker']['typed-sql-state']['decoded-bytes'] = 131072
        write('fixture-config.json', fixture_config)
        result['pipeline_config_overrides'] = overrides
        result['worker_config_overrides'] = {'live-state-resources.max-open-databases': 8,
                                              'live-state-resources.scan-page-bytes': 4194304,
                                              'live-state-resources.decoded-value-bytes': 33554432,
                                              'typed-sql-state.row-bytes': 8192,
                                              'typed-sql-state.decoded-bytes': 131072}
        result['files_sha256'] = {str(path):hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (Path(__file__), repo/'deploy/streamr-native/prepare.py',
                         repo/'test/streamr-reference/profile-native/prepare.py',
                         a.config_template, query_path, stages_path)}
        pin = a.binary_pin.resolve()
        argv = ['podman','run','--rm','--name',engine,'--cidfile',str(cidfile),
                '--network',network,'--security-opt','label=disable','--memory','2g']
        mounts = {(repo,repo,'ro'), (pin,pin,'ro'), (case,case,'rw'),
                  (a.engine_helper.resolve(),Path('/runner/helper.py'),'ro'),
                  (a.config_template.resolve(),a.config_template.resolve(),'ro'),
                  (a.python_runtime.resolve(),a.python_runtime.resolve(),'ro')}
        for file in (a.query,):
            mounts.add((file.resolve().parent,file.resolve().parent,'ro'))
        for source,target,mode in sorted(mounts, key=lambda item: str(item[1])):
            argv += ['-v',str(source)+':'+str(target)+':'+mode]
        argv += ['-e','LD_LIBRARY_PATH='+str(pin/'package/lib'),
                 '-e','PYTHONPATH='+str(a.python_runtime.resolve()),'-e','PYTHONDONTWRITEBYTECODE=1',
                 runtime_image,'python3',str(repo/'test/streamr-reference/native_profile_runtime.py'),
                 '--case',str(case/'runtime'),'--binary',str(pin/'package/bin/arroyo'),
                 '--binary-receipt',str(pin/'source-manifest.json'),'--engine-helper','/runner/helper.py',
                 '--config-template',str(case/'fixture-config.json'),'--query',str(query_path),
                 '--input',str(fixture_dir/'input.jsonl'),
                 '--expected-prefixes',str(stages_path),
                 '--kafka-rpk',str(case/'rpk'),'--kafka-broker','broker:9092','--kafka-topic',topic,
                 '--kafka-output-topic',output_topic,'--fault-prefix',str(a.fault_prefix),
                 '--keys',*a.keys,'--typed-owners',str(a.typed_owners),
                 '--disk-owners',str(a.disk_owners),'--port','18428','--timeout','360']
        if a.no_recovery:
            argv.append('--no-recovery')
        write('command.json',argv)
        with (case/'container.log').open('w') as output:
            completed = subprocess.run(argv, stdout=output, stderr=subprocess.STDOUT, timeout=480)
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
