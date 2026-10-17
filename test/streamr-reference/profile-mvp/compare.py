#!/usr/bin/env python3
"""Exact typed full33 CDC chains; observation clock fixed explicitly in fixture."""
import argparse
from collections import defaultdict
import json
from pathlib import Path


def pairs(values):
    result = {}
    for key, value in values:
        if key in result:
            raise ValueError(f'duplicate JSON key {key}')
        result[key] = value
    return result


def read(path):
    return [json.loads(line, object_pairs_hook=pairs) for line in Path(path).read_text().splitlines()]


def canonical(row):
    # JSON encoding retains exact Boolean/integer/null distinctions unlike ==.
    return json.dumps(row, sort_keys=True, separators=(',', ':'), allow_nan=False)


def chains(rows):
    result = defaultdict(list)
    previous = {}
    for envelope in rows:
        assert type(envelope) is dict and set(envelope) == {'before', 'after', 'op'}, envelope
        after = envelope['after']
        assert type(after) is dict and len(after) == 33, after
        key = (after['tenant_id'], after['canonical_id'])
        assert canonical(envelope['before']) == canonical(previous.get(key)), ('broken CDC before', key)
        assert envelope['op'] == ('u' if key in previous else 'c'), ('wrong operation', key)
        result[key].append(canonical(envelope))
        previous[key] = after
    return dict(result)


def compare(expected, actual):
    wanted, received = chains(expected), chains(actual)
    assert received == wanted, {'expected': wanted, 'actual': received}
    return dict(rows=len(actual), profiles=len(received), fields_per_profile=33,
                exact_typed_cdc_chains=True, complete_finite_output=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('expected', type=Path)
    parser.add_argument('actual', type=Path)
    args = parser.parse_args()
    print(json.dumps(compare(read(args.expected), read(args.actual))))
