#!/usr/bin/env python3
"""Validate the entire prepared graph before idempotent public-API submission."""
import argparse
import json
from pathlib import Path
import time
import urllib.request

STAGES = ('identity', 'unified', 'merges', 'sessions', 'profiles')


def bootstrap(directory, request):
    payloads = [json.loads((Path(directory) / (stage + '-request.json')).read_text()) for stage in STAGES]
    # Validate everything first, including jobs which already exist.
    for payload in payloads:
        result = request('pipelines/validate_query', {'query': payload['query'], 'udfs': payload['udfs']})
        if result['errors']:
            raise ValueError(f"validation failed for {payload['name']}: {result['errors']}")
    existing = {}
    cursor = None
    while True:
        from urllib.parse import urlencode
        page = request('pipelines?' + urlencode({'limit': 100, **({'starting_after': cursor} if cursor else {})}))
        for pipeline in page['data']:
            if pipeline['name'] in existing:
                raise ValueError('duplicate pipeline name: ' + pipeline['name'])
            existing[pipeline['name']] = pipeline
        if not page['has_more']:
            break
        if not page['data']:
            raise ValueError('invalid empty paginated pipeline response')
        cursor = page['data'][-1]['id']
    for payload in payloads:
        old = existing.get(payload['name'])
        if old and (old['query'] != payload['query'] or old['udfs'] != payload['udfs']
                    or old['checkpoint_interval_micros'] != payload['checkpoint_interval_micros']):
            raise ValueError('conflicting existing pipeline: ' + payload['name'])
    return [existing.get(p['name']) or request('pipelines', p) for p in payloads]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api', default='http://streamr:8000/api/v1')
    parser.add_argument('--queries', type=Path, default=Path('/bundle/queries'))
    args = parser.parse_args()
    def request(path, payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        req = urllib.request.Request(args.api.rstrip('/') + '/' + path, data=data,
                                     headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=120) as response:
            return json.load(response)
    for attempt in range(60):
        try:
            request('pipelines?limit=1')
            break
        except (OSError, ValueError):
            if attempt == 59:
                raise
            time.sleep(2)
    print(json.dumps(bootstrap(args.queries, request), indent=2))


if __name__ == '__main__':
    main()
