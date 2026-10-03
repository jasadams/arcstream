#!/usr/bin/env python3
"""Prepare identity SQL input, or adapt real SQL rows to reference envelopes."""

import argparse
import json
from pathlib import Path


RAW_FIELDS = {
    "event_id", "event_type", "tenant_id", "event_time", "anonymous_id", "user_id",
    "session_id", "page_url", "referrer", "element_id", "feature_name", "device_type",
    "browser", "os", "country", "properties",
}
UNIFIED_FIELDS = RAW_FIELDS | {"canonical_id"}
INTERNAL_FIELDS = {"merge_old", "user_binding"}
ROOT = Path(__file__).resolve().parents[2]


def prepare(fixture, directory, runtime_directory):
    fixture = json.loads(Path(fixture).read_text())
    if fixture.get("schema_version") != 1:
        raise ValueError("unsupported identity fixture version")
    rows, boundaries = [], []
    for step in fixture["steps"]:
        if step["op"] == "event":
            payload = step["payload"]
            if set(payload) != RAW_FIELDS:
                raise ValueError("fixture raw fields differ from the production contract")
            rows.append(payload)
        elif step["op"] == "snapshot_restore":
            boundaries.append((len(rows), step["checkpoint_id"]))
        else:
            raise ValueError(f"unsupported fixture step {step['op']}")
    if boundaries != [(10, 41)] or len(rows) != 13:
        raise ValueError("capture runner requires checkpoint 41 after event 10 of 13")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    runtime_directory = Path(runtime_directory)
    if not runtime_directory.is_absolute():
        raise ValueError("runtime directory must be absolute")
    (directory / "input.jsonl").write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows))
    query = (Path(__file__).with_name("identity-capture.sql")).read_text()
    for token, name in (("$identity_input", "input.jsonl"),
                        ("$identity_output", "streamr-internal.jsonl")):
        query = query.replace(token, str(runtime_directory / name).replace("'", "''"))
    (directory / "query.sql").write_text(query)
    manifest = {"events": len(rows), "checkpoint_after_events": 10,
                "checkpoint_id": 41, "runtime_directory": str(runtime_directory)}
    (directory / "fixture-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def adapt(rows):
    result = []
    for row in rows:
        if not isinstance(row, dict) or set(row) != UNIFIED_FIELDS | INTERNAL_FIELDS:
            raise ValueError("SQL capture fields differ from the declared internal sink")
        for field in ("event_id", "tenant_id", "canonical_id", "event_time"):
            if not isinstance(row[field], str) or not row[field]:
                raise ValueError(f"capture requires nonempty {field}")
        if row["user_id"] not in (None, "") and row["user_binding"] != row["canonical_id"]:
            raise ValueError("SQL user binding does not match the chosen identity")
        result.append({"stream": "unified-events",
                       "payload": {key: row[key] for key in sorted(UNIFIED_FIELDS)}})
        if row["merge_old"] is not None:
            if not isinstance(row["merge_old"], str) or not row["merge_old"]:
                raise ValueError("merge_old must be a nonempty identity or null")
            if row["merge_old"] == row["canonical_id"]:
                raise ValueError("SQL emitted a self-merge")
            result.append({"stream": "identity-merges", "payload": {
                "old_canonical_id": row["merge_old"], "canonical_id": row["canonical_id"],
                "tenant_id": row["tenant_id"], "merged_at": row["event_time"],
            }})
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    preparation = subparsers.add_parser("prepare")
    preparation.add_argument("directory", type=Path)
    preparation.add_argument("--runtime-directory", type=Path, required=True)
    preparation.add_argument("--fixture", type=Path, default=ROOT /
        "flink/identity-resolution/src/test/resources/reference/identity-input.json")
    adaptation = subparsers.add_parser("adapt")
    adaptation.add_argument("input", type=Path)
    adaptation.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        print(json.dumps(prepare(args.fixture, args.directory, args.runtime_directory)))
    else:
        rows = [json.loads(line) for line in args.input.read_text().splitlines() if line.strip()]
        records = adapt(rows)
        args.output.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in records))
        print(json.dumps({"rows": len(rows), "records": len(records)}))


if __name__ == "__main__":
    main()
