#!/usr/bin/env python3
"""Operate only the isolated arcstream-streamr-eval local Compose project."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[2]
HERE = Path(__file__).resolve().parent
EVIDENCE = ROOT / "target/streamr-local"
PROJECT = "arcstream-streamr-eval"
API = "http://127.0.0.1:15115/api/v1"
TOPICS = ["arc-eval-raw-events", "arc-eval-identity-capture",
          "arc-eval-unified-events", "arc-eval-identity-merges"]
PIPELINES = {"identity": "arc-eval-identity-owner", "unified": "arc-eval-unified", "merges": "arc-eval-merges"}
sys.path.insert(0, str(ROOT / "test/streamr-reference"))
from compare_identity import compare, read_records  # noqa: E402
from streamr_capture import adapt  # noqa: E402


class SetupError(RuntimeError):
    pass


def run(args, *, data=None, timeout=120, include_stderr=False):
    try:
        result = subprocess.run(args, input=data, text=True, capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        raise SetupError(f"Command timed out after {timeout}s: {' '.join(args)}") from error
    if result.returncode:
        raise SetupError(f"Command failed: {' '.join(args)}\n{result.stdout}\n{result.stderr}")
    return result.stdout + (result.stderr if include_stderr else "")


def compose(*args):
    lock = json.loads((EVIDENCE / "candidate.json").read_text())
    if args and args[0] == "up":
        existing = run([engine(), "ps", "-a", "--filter", f"label=com.docker.compose.project={PROJECT}",
                        "--filter", "label=com.docker.compose.service=streamr", "--format", "{{.ID}}"])
        for identifier in existing.splitlines():
            info = json.loads(run([engine(), "inspect", identifier]))[0]
            if info["Image"].removeprefix("sha256:") != lock["image_id"].removeprefix("sha256:"):
                raise SetupError("Existing evaluation container uses another image; refusing to replace it")
    environment = EVIDENCE / "compose.env"
    environment.write_text(f"STREAMR_IMAGE={lock['image_id']}\n")
    provider = ["podman-compose"] if os.environ.get("CONTAINER_ENGINE", "podman") == "podman" else ["docker", "compose"]
    return run(provider + ["--env-file", str(environment), "-p", PROJECT,
                           "-f", str(HERE / "compose.yml"), *args], timeout=180)


def engine():
    value = os.environ.get("CONTAINER_ENGINE", "podman")
    if value not in {"podman", "docker"}:
        raise SetupError("CONTAINER_ENGINE must be podman or docker")
    return value


def container(service):
    identifier = run([engine(), "ps", "-a", "--filter", f"label=com.docker.compose.project={PROJECT}",
                      "--filter", f"label=com.docker.compose.service={service}", "--format", "{{.ID}}"]).strip().splitlines()
    if len(identifier) != 1:
        raise SetupError(f"Expected one {service} container, got {identifier}")
    return identifier[0]


def broker(*args, data=None, timeout=120):
    return run([engine(), "exec", "-i", container("broker"), "rpk", *args,
                "-X", "brokers=localhost:9092"], data=data, timeout=timeout)


def api(path, method="GET", payload=None):
    data = None if payload is None else json.dumps(payload).encode()
    request = Request(API + path, data=data, method=method, headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=10) as response:
            return json.load(response)
    except HTTPError as error:
        raise SetupError(f"API {method} {path}: HTTP {error.code}: {error.read().decode()}") from error
    except (URLError, TimeoutError, ConnectionError, json.JSONDecodeError) as error:
        raise SetupError(f"API {method} {path}: {error}") from error


def wait_for(check, description, timeout=120):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except SetupError as error:
            last = error
        time.sleep(1)
    raise SetupError(f"Timed out waiting for {description}. Last error: {last}")


def pin(image):
    info = json.loads(run([engine(), "image", "inspect", image]))[0]
    labels = info.get("Config", {}).get("Labels") or {}
    revision = labels.get("org.opencontainers.image.revision")
    if not revision or not labels.get("app.streamr.source.sha256"):
        raise SetupError("Candidate must include Streamr source revision/digest provenance labels")
    record = {"image_id": info["Id"], "image_digest": info.get("Digest"), "labels": labels,
              "source_revision": revision, "selected_image": image}
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    lock = EVIDENCE / "candidate.json"
    if lock.exists() and json.loads(lock.read_text())["image_id"] != record["image_id"]:
        raise SetupError("Another image is pinned. This machine supports one evaluation project; keep its image fixed")
    lock.write_text(json.dumps(record, indent=2) + "\n")
    print(json.dumps(record, indent=2))


def ready():
    wait_for(lambda: api("/ping"), "local Streamr API")


def job(pipeline_id):
    jobs = api(f"/pipelines/{pipeline_id}/jobs")["data"]
    if not jobs:
        raise SetupError(f"No job for pipeline {pipeline_id}")
    return max(jobs, key=lambda value: value["created_at"])


def running(pipeline_id):
    state = job(pipeline_id)
    if state["state"] in {"Failed", "Error"}:
        raise SetupError(f"Pipeline failed: {state}")
    return state if state["state"] == "Running" else None


def submit():
    ready()
    collection = api("/pipelines")
    existing = collection["data"]
    if collection.get("hasMore") or collection.get("has_more"):
        raise SetupError("Unexpected paginated pipeline list in isolated setup")
    ids = {}
    for filename, name in PIPELINES.items():
        query = (HERE / f"{filename}.sql").read_text()
        validation = api("/pipelines/validate_query", "POST", {"query": query, "udfs": []})
        (EVIDENCE / f"{filename}-plan.json").write_text(json.dumps(validation, indent=2) + "\n")
        if validation.get("errors") or not validation.get("graph"):
            raise SetupError(f"SQL validation failed for {name}: {validation}")
        nodes = validation["graph"]["nodes"]
        owners = sum(node["description"].count("StatefulProcessor") for node in nodes)
        required_owners = 1 if filename == "identity" else 0
        if owners != required_owners or any(node["parallelism"] != 1 for node in nodes):
            raise SetupError(f"Unsupported evaluation graph for {name}: expected {required_owners} state owners at parallelism 1")
        matches = [value for value in existing if value["name"] == name]
        if len(matches) > 1:
            raise SetupError(f"Duplicate evaluation pipelines named {name}")
        if matches:
            pipeline = matches[0]
            if pipeline["query"] != query:
                raise SetupError(f"Existing {name} has a different query; refusing to overwrite it")
            api(f"/pipelines/{pipeline['id']}", "PATCH", {"stop": "none"})
        else:
            pipeline = api("/pipelines", "POST", {"name": name, "query": query, "udfs": [],
                           "parallelism": 1, "checkpoint_interval_micros": 2_000_000,
                           "tags": {"evaluation": PROJECT}})
        ids[filename] = pipeline["id"]
        wait_for(lambda: running(pipeline["id"]), f"{name} Running")
    (EVIDENCE / "pipelines.json").write_text(json.dumps(ids, indent=2) + "\n")
    print(json.dumps(ids, sort_keys=True))
    return ids


def topic_rows(topic, count=None):
    args = ["topic", "consume", topic, "--read-committed", "--format", "%v\n",
            "--offset", "start" if count else ":end"]
    if count:
        args += ["--num", str(count)]
    output = broker(*args)
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def write_json(name, value):
    (EVIDENCE / name).write_text(json.dumps(value, indent=2) + "\n")


def collect_evidence():
    paths = list(HERE.glob("*.sql")) + [HERE / name for name in ("compose.yml", "streamr.toml", "local.py")]
    paths += [ROOT / "test/streamr-reference/compare_identity.py",
              ROOT / "flink/identity-resolution/src/test/resources/reference/identity-input.json",
              ROOT / "flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl"]
    info = json.loads(run([engine(), "inspect", container("streamr")]))[0]
    write_json("provenance.json", {
        "arcstream_revision": run(["git", "-C", str(ROOT), "rev-parse", "HEAD"]).strip(),
        "arcstream_working_tree": run(["git", "-C", str(ROOT), "status", "--porcelain"]),
        "candidate": json.loads((EVIDENCE / "candidate.json").read_text()),
        "running_image": info["Image"], "mounts": info["Mounts"], "project": PROJECT,
        "source_sha256": {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths},
        "artifact_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                            for path in EVIDENCE.glob("*.jsonl")},
        "scope": "Local Kafka identity correctness/container recreation; capacity, commit-phase fault recovery and profiles/sessions pending",
    })
    (EVIDENCE / "streamr.log").write_text(run([engine(), "logs", container("streamr")], include_stderr=True))


def checkpoint(pipeline_id, started_after):
    current = job(pipeline_id)
    records = api(f"/pipelines/{pipeline_id}/jobs/{current['id']}/checkpoints")["data"]
    finished = [value for value in records if value.get("finish_time")
                and value["start_time"] >= started_after]
    return max(finished, key=lambda value: value["epoch"]) if finished else None


def smoke(recovery):
    ready()
    if topic_rows(TOPICS[0]):
        raise SetupError("Raw evaluation topic is not empty. Use fresh evaluation volumes for a new fixture run")
    ids = submit()
    fixture = json.loads((ROOT / "flink/identity-resolution/src/test/resources/reference/identity-input.json").read_text())
    events = [step["payload"] for step in fixture["steps"] if step["op"] == "event"]
    def publish(rows):
        broker("topic", "produce", TOPICS[0], data="".join(json.dumps(row) + "\n" for row in rows))
    start = time.time_ns() // 1000
    if recovery:
        publish(events[:10])
        write_json("prefix-capture.json", topic_rows(TOPICS[1], 10))
        write_json("prefix-unified.json", topic_rows(TOPICS[2], 10))
        before = {name: job(identifier) for name, identifier in ids.items()}
        # Checkpoint after committed prefix observation, rather than a stale empty snapshot.
        after_prefix = time.time_ns() // 1000
        checkpoints = {name: wait_for(lambda identifier=identifier: checkpoint(identifier, after_prefix),
                                     f"{name} published checkpoint after prefix")
                       for name, identifier in ids.items()}
        write_json("before-recreation.json", {"jobs": before, "checkpoints": checkpoints})
        print("Recreating only the evaluation Streamr container; broker and all volumes persist.", flush=True)
        print(compose("up", "-d", "--no-deps", "--force-recreate", "streamr"))
        ready()
        def restored(name, identifier):
            current = running(identifier)
            return current if current and current["run_id"] > before[name]["run_id"] else None
        after = {name: wait_for(lambda name=name, identifier=identifier: restored(name, identifier), f"restored {name}")
                 for name, identifier in ids.items()}
        write_json("after-recreation.json", after)
        restore_records = []
        for line in run([engine(), "logs", container("streamr")], include_stderr=True).splitlines():
            try:
                fields = json.loads(line).get("fields", {})
            except json.JSONDecodeError:
                continue
            if fields.get("message") == "restoring checkpoint":
                restore_records.append(fields)
        write_json("restored-checkpoints.json", restore_records)
        for name in ids:
            if not any(record.get("job_id") == after[name]["id"]
                       and record.get("epoch", -1) >= checkpoints[name]["epoch"] for record in restore_records):
                raise SetupError(f"Missing published-checkpoint restore evidence for {name}: {restore_records}")
        publish(events[10:])
    else:
        publish(events)
    topic_rows(TOPICS[2], len(events))
    topic_rows(TOPICS[3], 2)
    # Stop at completed checkpoints before finite reads, so no open transaction masks extras.
    for name, identifier in ids.items():
        api(f"/pipelines/{identifier}", "PATCH", {"stop": "checkpoint"})
        wait_for(lambda identifier=identifier: job(identifier)["state"] == "Stopped", f"{name} checkpoint stop")
    raw = topic_rows(TOPICS[0])
    intermediate = topic_rows(TOPICS[1])
    unified = topic_rows(TOPICS[2])
    merges = topic_rows(TOPICS[3])
    if raw != events:
        raise SetupError("Raw Kafka input differs from ordered fixture")
    adapted = adapt(intermediate)
    actual = ([{"stream": "unified-events", "payload": row} for row in unified]
              + [{"stream": "identity-merges", "payload": row} for row in merges])
    oracle = read_records(ROOT / "flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl")
    result = compare(oracle, actual)
    compare(oracle, adapted)
    for name, rows in [("raw.jsonl", raw), ("intermediate.jsonl", intermediate), ("outputs.jsonl", actual)]:
        (EVIDENCE / name).write_text("".join(json.dumps(row) + "\n" for row in rows))
    result.update(status="pass", container_recreation=recovery, elapsed_seconds=(time.time_ns() // 1000 - start) / 1e6)
    write_json("comparison.json", result)
    collect_evidence()
    print(json.dumps(result, sort_keys=True))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["pin", "up", "submit", "smoke", "recovery", "status", "down", "reset"])
    parser.add_argument("--image", help="provenance-labelled local candidate to pin")
    args = parser.parse_args()
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    try:
        engine()
        if args.command == "pin":
            if not args.image:
                raise SetupError("pin requires --image")
            pin(args.image)
        elif args.command == "up":
            print(compose("up", "-d"))
            ready()
            listed = broker("topic", "list", "--format", "json")
            existing = {value["name"] for value in json.loads(listed)}
            for topic in TOPICS:
                if topic not in existing:
                    print(broker("topic", "create", topic, "--partitions", "1", "--replicas", "1"))
        elif args.command == "submit":
            submit()
        elif args.command in {"smoke", "recovery"}:
            smoke(args.command == "recovery")
        elif args.command == "status":
            print(compose("ps"))
            print(json.dumps(api("/jobs"), indent=2))
        elif args.command == "down":
            print(compose("down"))
        elif args.command == "reset":
            if (EVIDENCE / "comparison.json").exists():
                archive = EVIDENCE / "runs" / str(time.time_ns())
                archive.mkdir(parents=True)
                for path in EVIDENCE.iterdir():
                    if path.is_file() and path.suffix in {".json", ".jsonl", ".log"}:
                        shutil.copy2(path, archive / path.name)
                print(f"Previous evidence archived to {archive}")
            print(compose("down", "--volumes"))
            # Clear generated run outputs after archiving; keep the image pin.
            for name in ("comparison.json", "provenance.json", "before-recreation.json", "after-recreation.json",
                         "restored-checkpoints.json", "prefix-capture.json", "prefix-unified.json", "pipelines.json",
                         "raw.jsonl", "intermediate.jsonl", "outputs.jsonl", "streamr.log"):
                (EVIDENCE / name).unlink(missing_ok=True)
    except (SetupError, OSError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
