#!/usr/bin/env python3
"""Compare profile captures with an independent oracle and explicit clock bounds."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys


class ComparisonError(ValueError):
    pass


INTEGER_FIELDS = {
    "first_seen", "last_seen", "updated_at", "total_events", "total_sessions",
    "events_1d", "events_7d", "events_30d", "events_90d", "sessions_1d",
    "sessions_7d", "sessions_30d", "sessions_90d", "avg_session_duration_sec",
    "current_session_duration_sec", "page_views", "clicks", "logins", "feature_uses",
}
ARRAY_FIELDS = {"top_pages", "top_features", "changed_fields"}
STRING_FIELDS = {
    "canonical_id", "tenant_id", "user_id", "last_page", "last_country", "last_device",
    "last_browser", "action", "timestamp", "trigger",
}
PROFILE_FIELDS = INTEGER_FIELDS | ARRAY_FIELDS | STRING_FIELDS | {"current_session_active"}


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


def _integer(value, field):
    if type(value) is not int:
        raise ComparisonError(f"{field} must be an integer, got {value!r}")
    return value


def _records_by_step(records, captured):
    result = {}
    fields = {"stream", "step", "payload"}
    if captured:
        fields |= {"captured_from_ms", "captured_to_ms"}
    for record in records:
        if not isinstance(record, dict) or set(record) != fields:
            raise ComparisonError("unexpected profile capture envelope fields")
        if record["stream"] != "profile-updates":
            raise ComparisonError("expected profile-updates stream")
        if not isinstance(record["step"], str) or not record["step"]:
            raise ComparisonError("profile step must be a nonempty string")
        if record["step"] in result:
            raise ComparisonError(f"duplicate profile emission for step {record['step']}")
        if not isinstance(record["payload"], dict) or set(record["payload"]) != PROFILE_FIELDS:
            raise ComparisonError(f"profile payload fields differ at {record['step']}")
        if captured:
            lower = _integer(record["captured_from_ms"], "captured_from_ms")
            upper = _integer(record["captured_to_ms"], "captured_to_ms")
            if lower <= 0 or upper < lower:
                raise ComparisonError("invalid emission clock bounds")
            payload = record["payload"]
            for field in INTEGER_FIELDS:
                _integer(payload[field], field)
            for field in STRING_FIELDS:
                if not isinstance(payload[field], str):
                    raise ComparisonError(f"{field} must be a string")
            if type(payload["current_session_active"]) is not bool:
                raise ComparisonError("current_session_active must be a boolean")
            for field in ARRAY_FIELDS:
                if not isinstance(payload[field], list) or not all(
                    isinstance(value, str) for value in payload[field]
                ):
                    raise ComparisonError(f"{field} must be a real array of strings")
            if len(payload["changed_fields"]) != len(set(payload["changed_fields"])):
                raise ComparisonError("duplicate changed_fields entries")
        result[record["step"]] = record
    return result


def _resolve(field, expected, record, base_ms):
    if not isinstance(expected, dict):
        return expected
    if set(expected) == {"base_offset_ms"} and field in {"first_seen", "last_seen"}:
        return base_ms + _integer(expected["base_offset_ms"], "base_offset_ms")
    lower, upper = record["captured_from_ms"], record["captured_to_ms"]
    actual = record["payload"][field]
    if expected == {"clock": "emission_ms"} and field == "updated_at":
        if not lower <= actual <= upper:
            raise ComparisonError(f"updated_at outside emission bounds at {record['step']}")
        return actual
    if expected == {"clock": "emission_timestamp"} and field == "timestamp":
        try:
            parsed = datetime.strptime(actual, "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
        except ValueError as error:
            raise ComparisonError("timestamp must be a valid UTC timestamp with milliseconds") from error
        if parsed.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] != actual:
            raise ComparisonError("timestamp must use the production millisecond format")
        delta = parsed - datetime(1970, 1, 1, tzinfo=timezone.utc)
        milliseconds = delta.days * 86_400_000 + delta.seconds * 1000 + delta.microseconds // 1000
        if not lower <= milliseconds <= upper:
            raise ComparisonError(f"timestamp outside emission bounds at {record['step']}")
        return actual
    if (set(expected) == {"clock", "session_start_offset_ms"}
            and expected["clock"] == "session_duration_seconds"
            and field == "current_session_duration_sec"):
        start = base_ms + _integer(expected["session_start_offset_ms"], "session_start_offset_ms")
        if not record["payload"]["current_session_active"]:
            raise ComparisonError("active session duration marker requires an active session")
        if not (lower - start) // 1000 <= actual <= (upper - start) // 1000:
            raise ComparisonError(f"session duration outside emission bounds at {record['step']}")
        return actual
    raise ComparisonError(f"unsupported oracle marker for {field}: {expected}")


def compare(expected, actual, base_ms):
    _integer(base_ms, "base_time_ms")
    expected_by_step = _records_by_step(expected, False)
    actual_by_step = _records_by_step(actual, True)
    if not expected_by_step:
        raise ComparisonError("oracle contains no profile emissions")
    if list(expected_by_step) != list(actual_by_step):
        raise ComparisonError("profile emission steps/order differ: "
                              f"expected={list(expected_by_step)}, actual={list(actual_by_step)}")
    for step, oracle in expected_by_step.items():
        record = actual_by_step[step]
        resolved = {field: _resolve(field, value, record, base_ms)
                    for field, value in oracle["payload"].items()}
        payload = record["payload"].copy()
        if (not isinstance(resolved["changed_fields"], list)
                or not all(isinstance(value, str) for value in resolved["changed_fields"])
                or len(resolved["changed_fields"]) != len(set(resolved["changed_fields"]))):
            raise ComparisonError("oracle changed_fields must be distinct strings")
        resolved["changed_fields"] = sorted(resolved["changed_fields"])
        payload["changed_fields"] = sorted(payload["changed_fields"])
        if json.dumps(resolved, sort_keys=True) != json.dumps(payload, sort_keys=True):
            differences = {field: {"expected": resolved[field], "actual": payload[field]}
                           for field in PROFILE_FIELDS if resolved[field] != payload[field]}
            raise ComparisonError(f"profile mismatch at {step}: {differences}")
    return {"profile_updates": len(expected_by_step), "fields_per_update": len(PROFILE_FIELDS)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("expected", type=Path)
    parser.add_argument("actual", type=Path)
    parser.add_argument("fixture", type=Path, help="resolved fixture containing base_time_ms")
    args = parser.parse_args()
    try:
        fixture = json.loads(args.fixture.read_text())
        result = compare(read_records(args.expected), read_records(args.actual), fixture["base_time_ms"])
    except (ComparisonError, OSError, KeyError, json.JSONDecodeError) as error:
        print(str(error), file=sys.stderr)
        return 1
    print(json.dumps({"status": "pass", **result}, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
