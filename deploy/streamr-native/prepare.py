#!/usr/bin/env python3
"""Render native application queries for review; never submit or deploy jobs."""
import argparse
import hashlib
import json
from pathlib import Path
import re
import subprocess

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
SESSION_SOURCE = "WITH(connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true')"
SESSION_SINK = "WITH(connector='single_file',path='{{OUTPUT}}',format='json',type='sink')"


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def source_options(broker, topic, group):
    return ("connector='kafka',bootstrap_servers=" + literal(broker)
            + ",topic=" + literal(topic) + ",format='json',type='source',"
            "'source.offset'='earliest','source.read_mode'='read_committed',"
            "'source.group_id'=" + literal(group))


def sink_options(broker, topic, journal, format_):
    return ("connector='kafka',bootstrap_servers=" + literal(broker)
            + ",topic=" + literal(topic) + ",format=" + literal(format_)
            + ",type='sink','sink.commit_mode'='exactly_once',"
            "'sink.recovery_topic'=" + literal(journal)
            + ","
            "client_configs='transaction.timeout.ms=60000,compression.type=lz4'")


def replace_once(query, old, new):
    if query.count(old) != 1:
        raise ValueError('template boundary changed: ' + old)
    return query.replace(old, new)


def render(broker, namespace, profile_release=False, raw_events_topic=None):
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', namespace):
        raise ValueError('namespace must be 1..63 lowercase letters, numbers or hyphens')
    names = ['raw-events', 'identity-capture', 'unified-events', 'identity-merges',
             'identity-commits', 'unified-commits', 'merges-commits']
    queries = {}
    for stage in ['identity', 'unified', 'merges']:
        query = (HERE / (stage + '.sql')).read_text()
        query = query.replace("'broker:9092'", literal(broker))
        for suffix in names:
            query = query.replace(literal('arc-native-' + suffix), literal(raw_events_topic if suffix == 'raw-events' and raw_events_topic is not None else namespace + '-' + suffix))
        for group in ['identity-owner', 'identity-unified', 'identity-merges']:
            query = query.replace(literal('arc-native-' + group), literal(namespace + '-' + group))
        queries[stage] = query
    session = (HERE / 'session-native/query.sql').read_text()
    session = replace_once(session, SESSION_SOURCE, 'WITH(' + source_options(
        broker, namespace + '-unified-events', namespace + '-sessions') + ')')
    session = replace_once(session, SESSION_SINK, 'WITH(' + sink_options(
        broker, namespace + '-sessions', namespace + '-sessions-commits', 'json') + ')')
    queries['sessions'] = session
    core = (HERE / 'profile-native/core.sql').read_text()
    core = replace_once(core, '{{SOURCE_OPTIONS}}', source_options(
        broker, namespace + '-unified-events', namespace + '-profile-core'))
    core = replace_once(core, '{{SINK_OPTIONS}}', sink_options(
        broker, namespace + '-profile-core-diagnostic', namespace + '-profile-core-commits',
        'json'))
    queries['profile-core'] = core
    closed = (HERE / 'profile-native/closed-metrics.sql').read_text()
    closed = replace_once(closed, '{{SOURCE_OPTIONS}}', source_options(
        broker, namespace + '-unified-events', namespace + '-closed-metrics'))
    closed = replace_once(closed, '{{SINK_OPTIONS}}', sink_options(
        broker, namespace + '-closed-session-metrics-diagnostic',
        namespace + '-closed-metrics-commits', 'debezium_json'))
    queries['closed-metrics'] = closed
    if profile_release:
        release = (HERE / 'profile-native/release.sql').read_text()
        release = replace_once(release, '{{SOURCE_OPTIONS}}', source_options(
            broker, namespace + '-unified-events', namespace + '-profiles'))
        release = replace_once(release, '{{SINK_OPTIONS}}', sink_options(
            broker, namespace + '-profiles', namespace + '-profiles-commits', 'json'))
        del queries['profile-core']
        del queries['closed-metrics']
        queries['profiles'] = release
    for name, query in queries.items():
        if re.search(r'\{\{[^}]+\}\}', query):
            raise ValueError('unresolved placeholder in ' + name)
    return queries


def render_flaredb_profiles(broker, namespace):
    """Render schema only for a dedicated native FlareDB instance; never submit it."""
    if not re.fullmatch(r'[a-z0-9][a-z0-9-]{0,62}', namespace):
        raise ValueError('namespace must be 1..63 lowercase letters, numbers or hyphens')
    config = json.loads((HERE / 'flaredb/profiles.json').read_text())
    config['ingestionConfig'] = dict(topic=namespace + '-profiles', brokers=broker)
    return config


def render_fixture(component, input_path, output_path):
    if component == 'sessions':
        query = (HERE / 'session-native/query.sql').read_text()
        query = replace_once(query, "'{{INPUT}}'", literal(input_path))
        return replace_once(query, "'{{OUTPUT}}'", literal(output_path))
    template = {'profile-core': 'core.sql', 'closed-metrics': 'closed-metrics.sql', 'profiles': 'release.sql'}[component]
    query = (HERE / 'profile-native' / template).read_text()
    query = replace_once(query, '{{SOURCE_OPTIONS}}',
                         "connector='single_file',path=" + literal(input_path)
                         + ",format='json',type='source',wait_for_control='true'")
    format_ = 'debezium_json' if component == 'closed-metrics' else 'json'
    return replace_once(query, '{{SINK_OPTIONS}}',
                        "connector='single_file',path=" + literal(output_path)
                        + ",format=" + literal(format_) + ",type='sink'")


def prepare(directory, broker, namespace, fixture=None, profile_release=False, raw_events_topic=None):
    if raw_events_topic is not None and (fixture or not re.fullmatch(r'[a-zA-Z0-9._-]{1,249}', raw_events_topic) or raw_events_topic in {'.', '..'}):
        raise ValueError('raw-events topic must be a valid Kafka topic and is incompatible with fixture mode')
    queries = ({fixture[0]: render_fixture(*fixture)} if fixture else render(broker, namespace, profile_release, raw_events_topic))
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=False)
    sources = [HERE / (name + '.sql') for name in ['identity', 'unified', 'merges']]
    sources += [HERE / 'session-native/query.sql', HERE / 'profile-native/core.sql',
                HERE / 'profile-native/closed-metrics.sql', HERE / 'profile-native/release.sql', Path(__file__)]
    if profile_release and not fixture:
        sources.append(HERE / 'flaredb/profiles.json')
    manifest = dict(status='prepared; engine-dependent validation not run',
                    complete_mvp=False, production_ready=False, broker=broker, namespace=namespace,
                    required_backend='rocksdb', required_engine_contracts=['STR-29', 'STR-60', 'STR-64', 'STR-146'],
                    profile_output=('22-field first-release profiles; seven live/start fields and four unused publication metadata fields explicitly deferred' if profile_release or fixture and fixture[0] == 'profiles' else 'partial 20-field diagnostic, never the profiles topic'),
                    mode='fixture' if fixture else 'kafka',
                    raw_events_topic=None if fixture else (raw_events_topic or namespace + '-raw-events'),
                    source_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
                    templates_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sources},
                    queries={})
    for name, query in queries.items():
        (directory / (name + '.sql')).write_text(query)
        payload = dict(name=namespace + '-' + name, query=query, udfs=[], parallelism=1,
                       checkpoint_interval_micros=2_000_000)
        (directory / (name + '-request.json')).write_text(json.dumps(payload, indent=2) + '\n')
        manifest['queries'][name] = hashlib.sha256(query.encode()).hexdigest()
    if profile_release and not fixture:
        config_directory = directory / 'flaredb'
        config_directory.mkdir()
        config_text = json.dumps(render_flaredb_profiles(broker, namespace), indent=2) + '\n'
        (config_directory / 'profiles.json').write_text(config_text)
        manifest['analytics_configs'] = {'flaredb/profiles.json': hashlib.sha256(config_text.encode()).hexdigest()}
        manifest['analytics_validation'] = 'schema prepared for a dedicated native instance; actual FlareDB ingestion not validated'
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--brokers', required=True)
    parser.add_argument('--namespace', required=True)
    parser.add_argument('--raw-events-topic', help='existing raw input topic; groups and outputs remain namespaced')
    parser.add_argument('--fixture-component', choices=['sessions', 'profile-core', 'closed-metrics', 'profiles'])
    parser.add_argument('--profile-release', action='store_true',
                        help='render the explicitly scoped 22-field profiles graph; does not deploy')
    parser.add_argument('--fixture-input', type=Path)
    parser.add_argument('--fixture-output', type=Path)
    args = parser.parse_args()
    fixture_args = [args.fixture_component, args.fixture_input, args.fixture_output]
    if any(fixture_args) and not all(fixture_args):
        parser.error('fixture mode requires component, input and output together')
    fixture = ((args.fixture_component, args.fixture_input, args.fixture_output)
               if args.fixture_component else None)
    print(json.dumps(prepare(args.output, args.brokers, args.namespace, fixture, args.profile_release, args.raw_events_topic)))


if __name__ == '__main__':
    main()
