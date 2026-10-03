#!/usr/bin/env python3
"""Compare session summaries against an independent, fixed-event-time oracle."""

import argparse
import json
from pathlib import Path
import sys


class ComparisonError(ValueError):
    pass


STRING_FIELDS = {
    "session_id", "canonical_id", "tenant_id", "start_time", "end_time",
    "device_type", "browser", "country",
}
INTEGER_FIELDS = {"duration_sec", "event_count"}
SESSION_FIELDS = STRING_FIELDS | INTEGER_FIELDS | {"pages", "event_types"}


def read_records(path):
    def distinct_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ComparisonError(f"{path}: duplicate JSON key {key!r}")
            result[key] = value
        return result

    try:
        return [json.loads(line, object_pairs_hook=distinct_keys)
                for line in Path(path).read_text().splitlines() if line.strip()]
    except json.JSONDecodeError as error:
        raise ComparisonError(f"{path}: invalid JSON: {error}") from error


def group_records(records):
    groups = {}
    last_step = None
    for record in records:
        if not isinstance(record, dict) or set(record) != {"stream", "step", "payload"}:
            raise ComparisonError("unexpected session envelope fields")
        if record["stream"] != "session-summaries":
            raise ComparisonError("expected session-summaries stream")
        step = record["step"]
        if not isinstance(step, str) or not step:
            raise ComparisonError("session step must be a nonempty string")
        if step != last_step and step in groups:
            raise ComparisonError("session emission step must be contiguous")
        last_step = step
        payload = record["payload"]
        if not isinstance(payload, dict) or set(payload) != SESSION_FIELDS:
            raise ComparisonError(f"session payload fields differ at {step}")
        for field in STRING_FIELDS:
            if not isinstance(payload[field], str):
                raise ComparisonError(f"{field} must be a string")
        for field in ("session_id", "canonical_id", "tenant_id"):
            if not payload[field]:
                raise ComparisonError(f"{field} must be nonempty")
        for field in INTEGER_FIELDS:
            if type(payload[field]) is not int:
                raise ComparisonError(f"{field} must be an integer")
        pages = payload["pages"]
        if not isinstance(pages, list) or not all(isinstance(page, str) for page in pages):
            raise ComparisonError("pages must be a native array of strings")
        if len(pages) != len(set(pages)):
            raise ComparisonError("pages must contain distinct entries")
        counts = payload["event_types"]
        if (not isinstance(counts, dict)
                or not all(isinstance(key, str) and type(value) is int
                           for key, value in counts.items())):
            raise ComparisonError("event_types must be a native object of integer counts")
        key = (payload["tenant_id"], payload["session_id"])
        group = groups.setdefault(step, {})
        if key in group:
            raise ComparisonError(f"duplicate session emission at {step}: {key}")
        normalized = payload.copy()
        normalized["pages"] = sorted(pages)
        group[key] = normalized
    return groups


def compare(expected, actual):
    oracle = group_records(expected)
    capture = group_records(actual)
    if not oracle:
        raise ComparisonError("oracle contains no session emissions")
    if list(oracle) != list(capture):
        raise ComparisonError(f"session emission steps/order differ: expected={list(oracle)}, "
                              f"actual={list(capture)}")
    for step, group in oracle.items():
        if set(group) != set(capture[step]):
            raise ComparisonError(f"session keys differ at {step}")
        for key, payload in group.items():
            if payload != capture[step][key]:
                differences = {field: {"expected": payload[field], "actual": capture[step][key][field]}
                               for field in SESSION_FIELDS if payload[field] != capture[step][key][field]}
                raise ComparisonError(f"session mismatch at {step}/{key}: {differences}")
    return {"session_summaries": sum(map(len, oracle.values())),
            "fields_per_summary": len(SESSION_FIELDS)}


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
