#!/usr/bin/env python3
"""Read-only exact FlareDB qualification for an explicitly declared Kafka prefix."""
import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import urllib.request

FIELDS = ('event_id', 'event_type', 'tenant_id', 'event_time', 'canonical_id',
          'anonymous_id', 'user_id', 'session_id', 'page_url', 'referrer',
          'element_id', 'feature_name', 'device_type', 'browser', 'os', 'country', 'properties')

def require(value, message):
    if not value:
        raise ValueError(message)

def unique_object(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, f'duplicate JSON key {key}')
        result[key] = value
    return result

def loads(text):
    return json.loads(text, object_pairs_hook=unique_object)

def epoch_ms(value):
    if value is None:
        return None
    if type(value) is int:
        return value
    require(type(value) is str, 'event_time must be timestamp string or integer milliseconds')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    delta = parsed.astimezone(timezone.utc) - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000

def key(row):
    return row['tenant_id'], row['event_id']

def normalized(row):
    require(set(row) == set(FIELDS), 'event fields differ from full17-column contract')
    require(all(value is None or type(value) is str for name, value in row.items() if name != 'event_time'), 'text/null field type differs')
    result = dict(row)
    result['event_time'] = epoch_ms(result['event_time'])
    return result

def read_prefix(path, expected_rows):
    rows = []
    offsets = []
    for line in path.read_text().splitlines():
        partition, offset, payload = line.split('\t', 2)
        require(partition == '0', 'historical prefix requires partition0')
        offset = int(offset)
        require(offset >= 0 and (not offsets or offset > offsets[-1]), 'duplicate/unordered Kafka offsets')
        rows.append(normalized(loads(payload)))
        offsets.append(offset)
    require(len(rows) == expected_rows, f'expected prefix{expected_rows} rows, found{len(rows)}')
    require(len({key(row) for row in rows}) == len(rows), 'duplicate expected tenant/event IDs')
    require(all(type(row['event_id']) is str and row['event_id'] for row in rows), 'invalid event ID')
    return rows

def response_rows(response):
    require(type(response) is dict and 'result_table' in response, 'missing result_table')
    table = response['result_table']
    schema = table['data_schema']
    require(schema['column_names'] == list(FIELDS), 'query response column names/order differ')
    require(schema['column_data_types'] == ['Timestamp(ms)' if name == 'event_time' else 'Utf8' for name in FIELDS], 'query response types differ')
    require(type(table['rows']) is list, 'query response rows are not array')
    result = []
    for values in table['rows']:
        require(type(values) is list and len(values) == len(FIELDS), 'query response row shape differs')
        result.append(normalized(dict(zip(FIELDS, values))))
    return result

def compare(expected, actual):
    expected_keys = Counter(map(key, expected))
    actual_keys = Counter(map(key, actual))
    require(expected_keys == actual_keys, f'event multiplicity differs: missing={dict(expected_keys-actual_keys)}, unexpected={dict(actual_keys-expected_keys)}')
    by_key = {key(row): row for row in actual}
    for row in expected:
        require(by_key[key(row)] == row, f'event{key(row)} full payload differs: expected={row}, actual={by_key[key(row)]}')
    return {'rows': len(expected), 'fields_per_row': len(FIELDS), 'timestamp_comparison': 'UTC logical milliseconds', 'properties_comparison': 'exact nullable original string'}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--expected-tsv', type=Path, required=True)
    parser.add_argument('--expected-rows', type=int, default=1389)
    parser.add_argument('--endpoint', default='http://127.0.0.1:18154/query/sql')
    parser.add_argument('--output-dir', type=Path, required=True, help='Fresh directory; existing evidence is preserved')
    parser.add_argument('--batch-ids', type=int, default=128)
    parser.add_argument('--max-response-bytes', type=int, default=32 * 1024 * 1024)
    args = parser.parse_args()
    require(args.expected_rows > 0 and args.batch_ids > 0 and args.max_response_bytes > 0, 'limits must be positive')
    expected = read_prefix(args.expected_tsv, args.expected_rows)
    ids = list(dict.fromkeys(row['event_id'] for row in expected))
    args.output_dir.mkdir(parents=True, exist_ok=False)
    actual = []
    result = {'status': 'running', 'scope': 'Explicit historical event-ID prefix only; not the complete live population', 'expected_sha256': hashlib.sha256(args.expected_tsv.read_bytes()).hexdigest(), 'endpoint': args.endpoint}
    try:
        for index, start in enumerate(range(0, len(ids), args.batch_ids)):
            selected = ids[start:start + args.batch_ids]
            quoted = ','.join("'" + value.replace("'", "''") + "'" for value in selected)
            sql = 'SELECT ' + ', '.join(FIELDS) + ' FROM events WHERE event_id IN (' + quoted + ')'
            request = urllib.request.Request(args.endpoint, data=json.dumps({'sql': sql}).encode(), headers={'Content-Type': 'application/json'})
            with urllib.request.urlopen(request, timeout=30) as response:
                body = response.read(args.max_response_bytes + 1)
                require(len(body) <= args.max_response_bytes, 'SQL response exceeds declared finite byte limit')
                receipt = {'sql': sql, 'status': response.status, 'headers': dict(response.headers), 'body': body.decode()}
            (args.output_dir / f'query-{index:04}.json').write_text(json.dumps(receipt, indent=2) + '\n')
            rows = response_rows(loads(body))
            require(all(row['event_id'] in selected for row in rows), 'query returned IDs outside explicit selection')
            actual.extend(rows)
        result.update(status='passed', comparison=compare(expected, actual), selected_ids=len(ids), queried_rows=len(actual), queries=(len(ids) + args.batch_ids - 1) // args.batch_ids)
    except Exception as error:
        result.update(status='failed', error=repr(error))
        raise
    finally:
        (args.output_dir / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2))

if __name__ == '__main__':
    main()
