#!/usr/bin/env python3
"""Compare captured identity outputs to an independent fixture oracle."""

import argparse
from collections import Counter
import json
from pathlib import Path
import sys


class ComparisonError(ValueError):
    pass


def read_records(path):
    records = []
    for line_number, line in enumerate(Path(path).read_text().splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as error:
            raise ComparisonError(f"{path}:{line_number}: invalid JSON: {error}") from error
        if not isinstance(record, dict) or set(record) != {"stream", "payload"}:
            raise ComparisonError(f"{path}:{line_number}: expected stream/payload envelope")
        if record["stream"] not in {"unified-events", "identity-merges"}:
            raise ComparisonError(f"{path}:{line_number}: unknown stream {record['stream']!r}")
        if not isinstance(record["payload"], dict):
            raise ComparisonError(f"{path}:{line_number}: payload must be an object")
        records.append(record)
    return records


def events_by_key(records):
    events = {}
    for record in records:
        if record["stream"] != "unified-events":
            continue
        payload = record["payload"]
        for field in ("tenant_id", "event_id", "canonical_id"):
            if not isinstance(payload.get(field), str) or not payload[field]:
                raise ComparisonError(f"unified event requires nonempty {field}")
        key = (payload["tenant_id"], payload["event_id"])
        if key in events:
            raise ComparisonError(f"duplicate unified event {key}")
        events[key] = payload
    return events


def normalize(records, event_order):
    events = events_by_key(records)
    if set(events) != set(event_order):
        missing = sorted(set(event_order) - set(events))
        extra = sorted(set(events) - set(event_order))
        raise ComparisonError(f"event keys differ: missing={missing}, unexpected={extra}")
    aliases = {}
    counts = Counter()
    normalized = []
    for key in event_order:
        payload = events[key].copy()
        tenant = payload["tenant_id"]
        identity = (tenant, payload["canonical_id"])
        if identity not in aliases:
            counts[tenant] += 1
            aliases[identity] = f"c{counts[tenant]}"
        payload["canonical_id"] = aliases[identity]
        normalized.append({"stream": "unified-events", "payload": payload})
    for record in records:
        if record["stream"] != "identity-merges":
            continue
        payload = record["payload"].copy()
        tenant = payload.get("tenant_id")
        for field in ("old_canonical_id", "canonical_id"):
            identity = (tenant, payload.get(field))
            if identity not in aliases:
                raise ComparisonError(f"merge {field} references an identity absent from events")
            payload[field] = aliases[identity]
        if payload["old_canonical_id"] == payload["canonical_id"]:
            raise ComparisonError("self-merge is invalid")
        normalized.append({"stream": "identity-merges", "payload": payload})
    return normalized


def compare(expected, actual):
    event_order = list(events_by_key(expected))
    if not event_order:
        raise ComparisonError("oracle contains no unified events")
    owners = {}
    for payload in events_by_key(actual).values():
        identity, tenant = payload["canonical_id"], payload["tenant_id"]
        if identity in owners and owners[identity] != tenant:
            raise ComparisonError("canonical ID reused across tenants")
        owners[identity] = tenant
    expected = normalize(expected, event_order)
    actual = normalize(actual, event_order)
    encode = lambda record: json.dumps(record, sort_keys=True, separators=(",", ":"))
    # Event keys are unique; merge multiplicity and direction must also match.
    expected_counts = Counter(map(encode, expected))
    actual_counts = Counter(map(encode, actual))
    if expected_counts != actual_counts:
        missing = list((expected_counts - actual_counts).elements())
        unexpected = list((actual_counts - expected_counts).elements())
        raise ComparisonError(f"output mismatch:\nmissing={missing}\nunexpected={unexpected}")
    return {"unified_events": len(event_order), "identity_merges": sum(
        record["stream"] == "identity-merges" for record in expected)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("expected", type=Path)
    parser.add_argument("actual", type=Path)
    args = parser.parse_args()
    try:
        result = compare(read_records(args.expected), read_records(args.actual))
    except (ComparisonError, OSError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps({"status": "pass", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
