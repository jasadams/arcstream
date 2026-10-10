#!/usr/bin/env python3
"""Prepare and adapt the native state-table identity evaluation fixture."""

import argparse
import json
from pathlib import Path

from streamr_capture import ROOT, adapt, prepare as prepare_reference


QUERY = Path(__file__).with_name("native-state-table-identity-capture.sql")
DEFAULT_FIXTURE = ROOT / "flink/identity-resolution/src/test/resources/reference/identity-input.json"


def prepare(fixture, directory, runtime_directory):
    """Reuse the reference's strict 13-event/checkpoint-41 input validation."""
    manifest = prepare_reference(fixture, directory, runtime_directory)
    directory = Path(directory)
    runtime_directory = Path(runtime_directory)
    query = QUERY.read_text()
    for token, name in (("$identity_input", "input.jsonl"),
                        ("$identity_output", "streamr-internal.jsonl")):
        query = query.replace(token, str(runtime_directory / name).replace("'", "''"))
    (directory / "query.sql").write_text(query)
    manifest["query_template"] = QUERY.name
    manifest["input_fields"] = 16
    manifest["capture_fields"] = 19
    manifest["candidate_policy"] = "native SQL uuid() once per event in identity_candidates"
    manifest["replay_policy"] = (
        "committed bindings retain their canonical IDs after recovery; "
        "uncheckpointed events may receive fresh UUID candidates on replay, "
        "matching the Flink identity policy"
    )
    manifest["comparison_oracle"] = str(ROOT /
        "flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl")
    (directory / "fixture-manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    preparation = commands.add_parser("prepare")
    preparation.add_argument("directory", type=Path)
    preparation.add_argument("--runtime-directory", type=Path, required=True)
    preparation.add_argument("--fixture", type=Path, default=DEFAULT_FIXTURE)
    adaptation = commands.add_parser("adapt")
    adaptation.add_argument("input", type=Path)
    adaptation.add_argument("output", type=Path)
    args = parser.parse_args()
    if args.command == "prepare":
        print(json.dumps(prepare(args.fixture, args.directory, args.runtime_directory)))
    else:
        rows = [json.loads(line) for line in args.input.read_text().splitlines() if line.strip()]
        records = adapt(rows)
        args.output.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in records))
        print(json.dumps({"rows": len(rows), "records": len(records)}))


if __name__ == "__main__":
    main()
