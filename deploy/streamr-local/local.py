#!/usr/bin/env python3
"""Operate only the isolated arcstream-streamr-eval local Compose project."""

import argparse
import hashlib
import json
import os
import re
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
HTTP_PORT = 15115
BROKER_PORT = 29092
TOPICS = ["arc-eval-raw-events", "arc-eval-identity-capture",
          "arc-eval-unified-events", "arc-eval-identity-merges", "arc-eval-identity-commits",
          "arc-eval-unified-commits", "arc-eval-merges-commits"]
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
    environment.write_text(f"STREAMR_IMAGE={lock['image_id']}\nSTREAMR_HTTP_PORT={HTTP_PORT}\nSTREAMR_BROKER_PORT={BROKER_PORT}\n")
    provider = ["podman-compose"] if os.environ.get("CONTAINER_ENGINE", "podman") == "podman" else ["docker", "compose"]
    files = ["-f", str(HERE / "compose.yml")]
    override = EVIDENCE / "live-state.override.json"
    if override.exists():
        files += ["-f", str(override)]
    faults = EVIDENCE / "faults.override.json"
    if faults.exists():
        files += ["-f", str(faults)]
    return run(provider + ["--env-file", str(environment), "-p", PROJECT,
                           *files, *args], timeout=180)


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
        "scope": "Local Kafka identity correctness/checkpoint recovery; selected commit fault is recorded separately; capacity and profiles/sessions pending",
        "commit_fault": json.loads((EVIDENCE / "commit-fault.json").read_text())
                        if (EVIDENCE / "commit-fault.json").exists() else None,
    })
    (EVIDENCE / "streamr.log").write_text(run([engine(), "logs", container("streamr")], include_stderr=True))


def checkpoint(pipeline_id, started_after):
    current = job(pipeline_id)
    records = api(f"/pipelines/{pipeline_id}/jobs/{current['id']}/checkpoints")["data"]
    finished = [value for value in records if value.get("finish_time")
                and value["start_time"] >= started_after]
    return max(finished, key=lambda value: value["epoch"]) if finished else None


def fresh_live_state():
    """Detach and retain the old state volume; mount a verified-empty new one."""
    image = json.loads((EVIDENCE / "candidate.json").read_text())["image_id"]
    before = json.loads(run([engine(), "inspect", container("streamr")]))[0]
    old = next(mount for mount in before["Mounts"] if mount["Destination"] == "/live-state")
    print(compose("stop", "streamr"))
    volume = f"{PROJECT}-fresh-live-state-{time.time_ns()}"
    run([engine(), "volume", "create", "--label", f"com.docker.compose.project={PROJECT}", volume])
    run([engine(), "run", "--rm", "--entrypoint", "/bin/sh", "-v", f"{volume}:/proof:ro",
         image, "-c", 'set -eu; contents=$(find /proof -mindepth 1 -maxdepth 1 -print -quit); test -z "$contents"'])
    override = {"services": {"streamr": {"volumes": ["fresh-live-state:/live-state"]}},
                "volumes": {"fresh-live-state": {"external": True, "name": volume}}}
    write_json("live-state.override.json", override)
    write_json("fresh-live-state.json", {"old_mount": old, "new_volume": volume,
                                         "verified_empty_before_restart": True})
    return volume


def commit_pause(job_id, point):
    """Only a structured hook emitted by the selected identity job is evidence."""
    for line in run([engine(), "logs", container("streamr")], include_stderr=True).splitlines():
        try:
            fields = json.loads(line).get("fields", {})
        except json.JSONDecodeError:
            continue
        if (fields.get("message") == "Kafka commit fault pause" and fields.get("job_id") == job_id
                and fields.get("point") == point):
            return fields
    return None


def committing_checkpoint(pipeline_id, epoch):
    records = api(f"/pipelines/{pipeline_id}/jobs/{job(pipeline_id)['id']}/checkpoints")["data"]
    return next((record for record in records if record["epoch"] == epoch
                 and not record.get("finish_time")
                 and any(event["event"] == "Committing" for event in record.get("events", []))), None)


def recovery_action(job_id, epoch):
    for line in run([engine(), "logs", container("streamr")], include_stderr=True).splitlines():
        try:
            fields = json.loads(line).get("fields", {})
        except json.JSONDecodeError:
            continue
        if (fields.get("message") == "Kafka commit recovery" and fields.get("job_id") == job_id
                and fields.get("epoch") == epoch):
            return fields
    return None


def enable_commit_faults():
    directory = EVIDENCE / "faults"
    directory.mkdir(exist_ok=True)
    if any(directory.iterdir()):
        raise SetupError("Fault directory has an existing arm file; refusing to reuse it")
    write_json("faults.override.json", {"services": {"streamr": {
        "environment": {"STREAMR_TEST_KAFKA_COMMIT_FAULT_DIR": "/faults"},
        "volumes": [f"{directory}:/faults:ro,z"]}}})
    print(compose("up", "-d", "--no-deps", "--force-recreate", "streamr"))
    ready()


def smoke(recovery, fresh_state=False, commit_fault=None, broker_fault=False, fault_delay=0):
    ready()
    if topic_rows(TOPICS[0]):
        raise SetupError("Raw evaluation topic is not empty. Use fresh evaluation volumes for a new fixture run")
    if commit_fault:
        enable_commit_faults()
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
        # Stop every job at a completed checkpoint so restart cannot race a later
        # checkpoint's unfinished Kafka commit phase. That fault is a separate gate.
        checkpoints = {name: wait_for(lambda identifier=identifier: checkpoint(identifier, after_prefix),
                                     f"{name} published checkpoint after prefix")
                       for name, identifier in ids.items()}
        if commit_fault:
            arm = EVIDENCE / "faults" / f"{commit_fault}-{before['identity']['id']}"
            arm.touch()
            publish(events[10:])
            pause = wait_for(lambda: commit_pause(before['identity']['id'], commit_fault),
                             f"identity paused {commit_fault} Kafka commit")
            if not isinstance(pause.get("epoch"), int) or pause["epoch"] <= checkpoints["identity"]["epoch"]:
                raise SetupError(f"Fault pause is not after the committed prefix checkpoint: {pause}")
            interrupted = wait_for(lambda: committing_checkpoint(ids["identity"], pause["epoch"]),
                                   "persisted identity checkpoint in committing phase")
            pause["interrupted_checkpoint"] = interrupted
            write_json("commit-fault.json", pause)
            if fault_delay:
                pause["fault_delay_seconds"] = fault_delay
                write_json("commit-fault.json", pause)
                time.sleep(fault_delay)
            # Controller sends this commit only after persisting the snapshot.
            checkpoints["identity"] = {"epoch": pause["epoch"]}
            if broker_fault:
                run([engine(), "kill", "--signal", "KILL", container("broker")])
                pause["broker_interruption"] = True
                write_json("commit-fault.json", pause)
            run([engine(), "kill", "--signal", "KILL", container("streamr")])
            arm.unlink()
            if broker_fault:
                run([engine(), "start", container("broker")])
                wait_for(lambda: broker("cluster", "info"), "restarted isolated broker")
        else:
            for name, identifier in ids.items():
                api(f"/pipelines/{identifier}", "PATCH", {"stop": "checkpoint"})
                wait_for(lambda identifier=identifier: job(identifier)["state"] == "Stopped",
                         f"{name} prefix checkpoint stop")
            checkpoints = {name: checkpoint(identifier, after_prefix) for name, identifier in ids.items()}
        write_json("before-recreation.json", {"jobs": before, "checkpoints": checkpoints})
        volume = fresh_live_state() if fresh_state else None
        print("Recreating evaluation Streamr; broker and published checkpoints persist.", flush=True)
        print(compose("up", "-d", "--no-deps", "--force-recreate", "streamr"))
        if volume:
            info = json.loads(run([engine(), "inspect", container("streamr")]))[0]
            mount = next(item for item in info["Mounts"] if item["Destination"] == "/live-state")
            if mount.get("Name") != volume:
                raise SetupError(f"Expected fresh live-state volume {volume}, got {mount}")
            record = json.loads((EVIDENCE / "fresh-live-state.json").read_text())
            record["new_mount"] = mount
            if mount["Source"] == record["old_mount"]["Source"]:
                raise SetupError("Live-state source did not change")
            write_json("fresh-live-state.json", record)
        ready()
        for identifier in ids.values():
            api(f"/pipelines/{identifier}", "PATCH", {"stop": "none"})
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
        if commit_fault:
            action = wait_for(lambda: recovery_action(before["identity"]["id"], pause["epoch"]),
                              "identity transaction replay/skip decision")
            expected_action = "replay" if commit_fault == "before" else "skipped"
            if action.get("action") != expected_action or action.get("records", 0) <= 0:
                raise SetupError(f"Expected nonempty {expected_action} recovery, got {action}")
            write_json("recovery-action.json", action)
        if not commit_fault:
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
    result.update(status="pass", container_recreation=recovery, fresh_live_state=fresh_state,
                  commit_fault=commit_fault,
                  broker_interruption=broker_fault,
                  fault_delay_seconds=fault_delay,
                  elapsed_seconds=(time.time_ns() // 1000 - start) / 1e6)
    write_json("comparison.json", result)
    collect_evidence()
    print(json.dumps(result, sort_keys=True))


def marker_loss():
    """Negative test only after a passing, checkpoint-stopped named fixture."""
    if json.loads((EVIDENCE / "comparison.json").read_text()).get("status") != "pass":
        raise SetupError("Marker-loss test requires a passing fixture")
    ids = json.loads((EVIDENCE / "pipelines.json").read_text())
    before = {name: job(identifier) for name, identifier in ids.items()}
    if any(value["state"] != "Stopped" for value in before.values()):
        raise SetupError("Marker-loss test requires all evaluation jobs checkpoint-stopped")
    physical_before = {topic: topic_rows(topic) for topic in TOPICS[1:4]}
    write_json("marker-loss-before.json", {"jobs": before, "marker_records": topic_rows(TOPICS[4])})
    broker("topic", "delete", TOPICS[4])
    broker("topic", "create", TOPICS[4], "--partitions", "1", "--replicas", "1",
           "--topic-config", "cleanup.policy=compact")
    print(compose("up", "-d", "--no-deps", "--force-recreate", "streamr"))
    ready()
    api(f"/pipelines/{ids['identity']}", "PATCH", {"stop": "none"})
    def rejected():
        logs = run([engine(), "logs", container("streamr")], include_stderr=True)
        return logs if "Kafka recovery generation sentinel missing" in logs else None
    logs = wait_for(rejected, "explicit rejection of lost Kafka marker history")
    (EVIDENCE / "marker-loss.log").write_text(logs)
    current = wait_for(lambda: (value if (value := job(ids["identity"]))["state"] in {"Failed", "Error"} else None),
                       "identity failure reported by API after marker loss")
    if current["id"] != before["identity"]["id"] or current["run_id"] <= before["identity"]["run_id"]:
        raise SetupError("Marker-loss rejection did not fail a newer run of the checkpointed identity job")
    physical_after = {topic: topic_rows(topic) for topic in TOPICS[1:4]}
    if physical_before != physical_after:
        raise SetupError("Marker-loss rejection unexpectedly changed physical output")
    write_json("marker-loss-result.json", {"status": "pass", "job": current,
                                          "output_unchanged": True, "history_rejected": True,
                                          "physical_before": {topic: {"count": len(rows), "sha256": hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()}
                                                              for topic, rows in physical_before.items()},
                                          "physical_after": {topic: {"count": len(rows), "sha256": hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()}
                                                             for topic, rows in physical_after.items()}})
    print("Marker history loss rejected explicitly; API reports failure and output is unchanged.")


def main():
    global PROJECT, API, EVIDENCE, HTTP_PORT, BROKER_PORT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["pin", "up", "submit", "smoke", "recovery", "commit-recovery", "marker-loss", "status", "down", "reset"])
    parser.add_argument("--image", help="provenance-labelled local candidate to pin")
    parser.add_argument("--instance", help="separate named evaluation instance (lowercase letters, digits, hyphens)")
    parser.add_argument("--http-port", type=int, default=15115)
    parser.add_argument("--broker-port", type=int, default=29092)
    parser.add_argument("--fresh-live-state", action="store_true", help="recovery only: retain old RocksDB volume and restart with an empty one")
    parser.add_argument("--commit-fault", choices=["before", "after"], help="commit-recovery only: kill before broker commit or after commit before acknowledgement")
    parser.add_argument("--broker-interruption", action="store_true", help="commit-recovery only: also kill/restart the isolated broker")
    parser.add_argument("--fault-delay-seconds", type=float, default=0, help="delay while paused before killing the isolated worker (0-120 seconds)")
    args = parser.parse_args()
    if args.instance:
        if not re.fullmatch(r"[a-z][a-z0-9-]{0,39}", args.instance):
            parser.error("Invalid instance name")
        if args.http_port == 15115 or args.broker_port == 29092:
            parser.error("Named instances require distinct HTTP and broker ports")
        PROJECT = f"arcstream-streamr-eval-{args.instance}"
        EVIDENCE = ROOT / "target/streamr-local" / args.instance
    elif args.http_port != 15115 or args.broker_port != 29092:
        parser.error("Custom ports require --instance")
    if args.http_port == args.broker_port:
        parser.error("HTTP and broker ports must differ")
    if not all(1024 <= port <= 65535 for port in (args.http_port, args.broker_port)):
        parser.error("Ports must be between 1024 and 65535")
    if args.fresh_live_state and (args.command not in {"recovery", "commit-recovery"} or not args.instance):
        parser.error("Fresh live-state recovery requires a separate named instance")
    if (args.command == "commit-recovery") != bool(args.commit_fault) or (args.commit_fault and not args.instance):
        parser.error("commit-recovery requires --commit-fault and a separate named instance")
    if args.broker_interruption and not args.commit_fault:
        parser.error("Broker interruption requires commit-recovery with an explicit fault point")
    if not 0 <= args.fault_delay_seconds <= 120 or (args.fault_delay_seconds and not args.commit_fault):
        parser.error("Fault delay must be 0-120 seconds and requires commit-recovery")
    if args.command == "marker-loss" and not args.instance:
        parser.error("Marker-loss testing requires a separate named instance")
    HTTP_PORT, BROKER_PORT = args.http_port, args.broker_port
    API = f"http://127.0.0.1:{HTTP_PORT}/api/v1"
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    try:
        engine()
        settings = {"project": PROJECT, "http_port": HTTP_PORT, "broker_port": BROKER_PORT}
        settings_file = EVIDENCE / "instance.json"
        if settings_file.exists() and json.loads(settings_file.read_text()) != settings:
            raise SetupError("Instance settings differ from the recorded ports/project; refusing to operate")
        settings_file.write_text(json.dumps(settings, indent=2) + "\n")
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
                    config = ["--topic-config", "cleanup.policy=compact"] if topic in TOPICS[4:] else []
                    print(broker("topic", "create", topic, "--partitions", "1", "--replicas", "1", *config))
        elif args.command == "submit":
            submit()
        elif args.command in {"smoke", "recovery", "commit-recovery"}:
            smoke(args.command != "smoke", args.fresh_live_state, args.commit_fault, args.broker_interruption,
                  args.fault_delay_seconds)
        elif args.command == "marker-loss":
            marker_loss()
        elif args.command == "status":
            print(compose("ps"))
            print(json.dumps(api("/jobs"), indent=2))
        elif args.command == "down":
            print(compose("down"))
        elif args.command == "reset":
            if any((EVIDENCE / name).exists() for name in ("comparison.json", "before-recreation.json", "prefix-capture.json", "commit-fault.json")):
                archive = EVIDENCE / "runs" / str(time.time_ns())
                archive.mkdir(parents=True)
                for path in EVIDENCE.iterdir():
                    if path.is_file() and path.suffix in {".json", ".jsonl", ".log"}:
                        shutil.copy2(path, archive / path.name)
                if (EVIDENCE / "faults").exists():
                    shutil.copytree(EVIDENCE / "faults", archive / "faults")
                print(f"Previous evidence archived to {archive}")
            print(compose("down", "--volumes"))
            # Clear generated run outputs after archiving; keep the image pin.
            for name in ("comparison.json", "provenance.json", "before-recreation.json", "after-recreation.json",
                         "restored-checkpoints.json", "prefix-capture.json", "prefix-unified.json", "pipelines.json",
                         "raw.jsonl", "intermediate.jsonl", "outputs.jsonl", "streamr.log",
                         "live-state.override.json", "fresh-live-state.json", "commit-fault.json", "faults.override.json",
                         "recovery-action.json", "marker-loss-before.json", "marker-loss-result.json", "marker-loss.log"):
                (EVIDENCE / name).unlink(missing_ok=True)
            shutil.rmtree(EVIDENCE / "faults", ignore_errors=True)
    except (SetupError, OSError, ValueError, KeyError) as error:
        print(str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
