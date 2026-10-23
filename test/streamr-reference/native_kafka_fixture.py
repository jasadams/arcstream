#!/usr/bin/env python3
"""Run a small native SQL fixture against an owned, isolated Kafka broker.

Invoke through the maintained shared queue. This creates test resources only;
application SQL and independently specified expectations are supplied as files.
"""
import argparse
import hashlib
import json
import os
import re
from pathlib import Path
import subprocess


def render_connector_template(query, broker, topic, group):
    """Bind an updating-metric template to this run's owned Kafka resources."""
    if '{{SOURCE_OPTIONS}}' not in query and '{{SINK_OPTIONS}}' not in query:
        return query
    if query.count('{{SOURCE_OPTIONS}}') != 1 or query.count('{{SINK_OPTIONS}}') != 1:
        raise ValueError('expected exactly one source and sink connector placeholder')
    literal = lambda value: "'" + str(value).replace("'", "''") + "'"
    source = ("connector='kafka',bootstrap_servers=" + literal(broker)
              + ",topic=" + literal(topic) + ",format='json',type='source',"
              "'source.offset'='earliest','source.read_mode'='read_committed',"
              "'source.group_id'=" + literal(group))
    sink = "connector='single_file',path='{{OUTPUT}}',format='debezium_json',type='sink'"
    return query.replace('{{SOURCE_OPTIONS}}', source).replace('{{SINK_OPTIONS}}', sink)


def run(argv, **options):
    return subprocess.run(argv, text=True, capture_output=True, check=True, timeout=120, **options).stdout.strip()


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ('case', 'query', 'input', 'expected', 'expected-prefixes', 'binary-pin',
                 'engine-helper', 'config-template', 'python-runtime'):
        p.add_argument('--'+name, type=Path, required=True)
    p.add_argument('--runtime-image', required=True)
    p.add_argument('--broker-image', required=True)
    p.add_argument('--fault-prefix', type=int, default=3)
    p.add_argument('--window-owners', type=int, default=2)
    p.add_argument('--cdc-keys', nargs='+', required=True)
    a = p.parse_args()
    if not os.environ.get('RUST_BUILD_QUEUE_HELD'):
        p.error('use the maintained shared build/run queue')
    case = a.case.resolve(); case.mkdir(parents=True, exist_ok=False)
    token = hashlib.sha256(str(case).encode()).hexdigest()[:12]
    prefix = 'arc-native-'+token
    network, broker, engine, topic = (prefix+'-'+suffix for suffix in ('network', 'broker', 'engine', 'input'))
    broker_id = network_id = None
    cidfile = case/'engine.cid'
    result = {'status': 'failed', 'resources': {}, 'cleanup': []}
    def write(name, value):
        (case/name).write_text(json.dumps(value, indent=2)+'\n')
    try:
        runtime_image = json.loads(run(['podman','image','inspect',a.runtime_image]))[0]['Id']
        broker_image = json.loads(run(['podman','image','inspect',a.broker_image]))[0]['Id']
        write('images.json', {'runtime': runtime_image, 'broker': broker_image})
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
        run(['podman','exec',broker_id,'rpk','topic','create',topic,'-p','1','-r','1','-X','brokers=localhost:9092'])
        write('topic.json', json.loads(run(['podman','exec',broker_id,'rpk','topic','describe',topic,'--format','json','-X','brokers=localhost:9092'])))
        run(['podman','cp',broker_id+':/opt/redpanda/libexec/rpk',str(case/'rpk')])
        query_path = case/'query-template.sql'
        original_query = a.query.read_text()
        query_path.write_text(render_connector_template(original_query, 'broker:9092', topic, topic+'-reader'))
        result['query_template_sha256'] = hashlib.sha256(original_query.encode()).hexdigest()
        fixture_config = json.loads(a.config_template.read_text())
        # Use ordinary application emission cadence, not the qualification
        # harness's one-hour batching/flush controls. Native time is unchanged.
        overrides = {'source-batch-size':1,'source-batch-linger':'50ms',
                     'update-aggregate-flush-interval':'100ms'}
        fixture_config.setdefault('pipeline', {}).update(overrides)
        write('fixture-config.json', fixture_config)
        result['pipeline_config_overrides'] = overrides
        repo = Path(__file__).resolve().parents[2]
        pin = a.binary_pin.resolve()
        argv = ['podman','run','--rm','--name',engine,'--cidfile',str(cidfile),
                '--network',network,'--security-opt','label=disable','--memory','2g']
        mounts = {(repo,repo,'ro'), (pin,pin,'ro'), (case,case,'rw'),
                  (a.engine_helper.resolve(),Path('/runner/helper.py'),'ro'),
                  (a.config_template.resolve(),a.config_template.resolve(),'ro'),
                  (a.python_runtime.resolve(),a.python_runtime.resolve(),'ro')}
        for file in (a.query,a.input,a.expected,a.expected_prefixes):
            mounts.add((file.resolve().parent,file.resolve().parent,'ro'))
        for source,target,mode in sorted(mounts, key=lambda item: str(item[1])):
            argv += ['-v',str(source)+':'+str(target)+':'+mode]
        argv += ['-e','LD_LIBRARY_PATH='+str(pin/'package/lib'),
                 '-e','PYTHONPATH='+str(a.python_runtime.resolve()),'-e','PYTHONDONTWRITEBYTECODE=1',
                 runtime_image,'python3',str(repo/'test/streamr-reference/native_sql_runtime.py'),
                 '--case',str(case/'runtime'),'--binary',str(pin/'package/bin/arroyo'),
                 '--binary-receipt',str(pin/'source-manifest.json'),'--engine-helper','/runner/helper.py',
                 '--config-template',str(case/'fixture-config.json'),'--query',str(query_path),
                 '--input',str(a.input.resolve()),'--expected',str(a.expected.resolve()),
                 '--expected-prefixes',str(a.expected_prefixes.resolve()),
                 '--kafka-rpk',str(case/'rpk'),'--kafka-broker','broker:9092','--kafka-topic',topic,
                 '--cdc-keys',*a.cdc_keys,'--fault-prefix',str(a.fault_prefix),
                 '--require-committed-output','--typed-owners','0','--window-owners',str(a.window_owners),
                 '--backend','rocksdb','--port','18428','--timeout','180']
        write('command.json',argv)
        with (case/'container.log').open('w') as output:
            completed = subprocess.run(argv, stdout=output, stderr=subprocess.STDOUT, timeout=240)
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
                    cleanup(['podman','rm','--force',cid],cid)
                elif present.returncode != 1:
                    result['cleanup'].append({'resource':cid,'exit':None,'error':'container inspection failed'})
            else:
                result['cleanup'].append({'resource':'engine','exit':None,'error':'invalid owned identity'})
        for resource, argv in ((broker_id,['podman','rm','--force']), (network_id,['podman','network','rm'])):
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
