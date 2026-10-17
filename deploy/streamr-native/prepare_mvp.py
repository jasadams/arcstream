#!/usr/bin/env python3
"""Prepare isolated native MVP pipelines, or submit the prepared manifest.

The manifest uses dedicated topics and parallelism one. It never changes the
existing Flink deployment or assigns an existing application's producer topics.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import urllib.request
import urllib.parse

HERE = Path(__file__).resolve().parent
STREAMR = "ad0a4814d5b7c07d74dce54753f5e6e9aaa7a062"
UDF_PLUGIN = "a86297dc049c0c3a5a85f965c782cc571ae0d2d2"


def digest(data):
    return hashlib.sha256(data.encode()).hexdigest()


def literal(value):
    return "'" + value.replace("'", "''") + "'"


def connector(broker, topic, name, sink=False, raw=False, recovery_topic=None):
    options = {
        "connector": "kafka", "bootstrap_servers": broker, "topic": topic,
        "format": "raw_string" if raw else "json", "type": "sink" if sink else "source",
    }
    if sink:
        options.update({"sink.commit_mode": "exactly_once", "sink.recovery_topic": recovery_topic or topic + "-commits",
                        "sink.recovery_max_bytes": "8388608"})
    else:
        options.update({"source.offset": "earliest", "source.group_id": name,
                        "source.read_mode": "read_committed"})
    return ", ".join(literal(key) + "=" + literal(value) for key, value in options.items())


def replace_file(query, token, connection):
    expression = r"connector\s*=\s*'single_file'\s*,\s*path\s*=\s*'" + re.escape(token) + r"'\s*,\s*format\s*=\s*'(?:json|raw_string)'\s*,\s*type\s*=\s*'(?:source|sink)'(?:\s*,\s*wait_for_control\s*=\s*'true')?"
    query, count = re.subn(expression, lambda _: connection, query)
    if count != 1:
        raise ValueError(f"expected one native connector template for {token}, found {count}")
    return query


def prepare(directory, broker, prefix, clock_mode):
    if not re.fullmatch(r"arc-mvp-[a-z0-9][a-z0-9-]{0,45}", prefix):
        raise ValueError("topic prefix must be an isolated arc-mvp-<name> namespace")
    directory.mkdir(parents=True, exist_ok=False)
    pipelines = []
    topics = set()

    def topic(suffix):
        name = prefix + "-" + suffix
        topics.add(name)
        return name

    def connection(suffix, pipeline, sink=False, raw=False):
        name = topic(suffix)
        recovery = prefix + "-" + pipeline + "-" + suffix + "-commits"
        if sink:
            topics.add(recovery)
        return connector(broker, name, prefix + "-" + pipeline, sink, raw, recovery)

    def add(name, query, udf=None):
        definition = (HERE / udf).read_text() if udf else None
        body = {"name": prefix + "-" + name, "query": query, "parallelism": 1,
                "checkpoint_interval_micros": 2_000_000,
                "udfs": [{"definition": definition, "language": "rust"}] if definition else []}
        body["tags"] = {"arcstream_namespace": prefix, "query_sha256": digest(query),
                        "udf_sha256": digest(definition or "")}
        (directory / (name + ".sql")).write_text(query)
        (directory / (name + ".json")).write_text(json.dumps(body, indent=2) + "\n")
        pipelines.append({"name": name, "request": name + ".json", "sha256": digest(json.dumps(body, sort_keys=True))})

    for name in ["identity", "unified", "merges"]:
        query = (HERE / (name + ".sql")).read_text().replace("'broker:9092'", literal(broker))
        query = query.replace("arc-native-", prefix + "-")
        if name == "unified":
            fields = re.search(r"CREATE TABLE unified_events\s*(\(.*?\))\s*WITH", query, re.DOTALL)
            if fields is None:
                raise ValueError("unified event schema template not found")
            insertion = query.split("INSERT INTO unified_events", 1)[1]
            query += "\nCREATE TABLE execution_events " + fields[1] + " WITH(" + connection(
                "execution-events", "unified-execution", sink=True) + ");\nINSERT INTO execution_events" + insertion
        add(name, query)
    for suffix in ["raw-events", "identity-capture", "unified-events", "identity-merges",
                   "identity-commits", "unified-commits", "merges-commits"]:
        topic(suffix)

    query = (HERE / "profile-events.sql").read_text()
    query = query.replace("{{SOURCE}}", connection("execution-events", "profile-events", raw=True))
    query = query.replace("{{SINK}}", connection("profile-signals", "profile-events", sink=True, raw=True))
    query = query.replace("{{SESSION_SINK}}", connection("session-signals", "profile-events", sink=True, raw=True))
    query = query.replace("{{REPLAY}}", "true" if clock_mode == "replay" else "false")
    add("profile-events", query, "profile_event.rs")

    query = (HERE / "profile-mvp/owner.sql").read_text()
    query = replace_file(query, "{{INPUT}}", connection("profile-signals", "profiles", raw=True))
    query = replace_file(query, "{{OUTPUT}}", connection("profile-outbox", "profiles", sink=True, raw=True))
    add("profiles", query, "profile-mvp/profile_transition.rs")

    query = (HERE / "profile-mvp/project.sql").read_text()
    query = replace_file(query, "{{INPUT}}", connection("profile-outbox", "profile-project", raw=True))
    query = replace_file(query, "{{OUTPUT}}", connection("profile-changelog", "profile-project", sink=True, raw=True))
    query = replace_file(query, "{{TIMERS}}", connection("profile-timer-seeds", "profile-project", sink=True, raw=True))
    add("profile-project", query)

    query = (HERE / "profile-flat.sql").read_text()
    query = query.replace("{{SOURCE}}", connection("profile-changelog", "profile-flat", raw=True))
    query = query.replace("{{SINK}}", connection("profile-updates", "profile-flat", sink=True, raw=True))
    add("profile-flat", query)

    query = (HERE / "profile-mvp/timers.sql").read_text()
    query = replace_file(query, "{{INPUT}}", connection("profile-timer-seeds", "profile-timers"))
    query = replace_file(query, "{{OUTPUT}}", connection("profile-signals", "profile-timers", sink=True))
    add("profile-timers", query)

    for name, template, source, output, timer in [
        ("sessions", "owner", "session-signals", "session-outbox", None),
        ("session-project", "project", "session-outbox", "sessions", "session-timer-seeds"),
        ("session-timers", "timers", "session-timer-seeds", "session-signals", None),
    ]:
        query = (HERE / ("session-mvp/" + template + ".sql")).read_text()
        query = replace_file(query, "{{INPUT}}", connection(source, name, raw=template != "timers"))
        query = replace_file(query, "{{OUTPUT}}", connection(output, name, sink=True, raw=template != "timers"))
        if timer:
            query = replace_file(query, "{{TIMERS}}", connection(timer, name, sink=True, raw=True))
        add(name, query, "session-mvp/session_transition.rs" if template == "owner" else None)

    manifest = {"streamr_merge": STREAMR, "namespace": prefix, "broker": broker,
                "udf_plugin_source": {"commit": UDF_PLUGIN, "ticket": "STR-143",
                    "reason": "edition-2024 safe export annotation; existing scalar UDF interface"},
                "clock_control": {"topic": prefix + "-execution-events",
                    "event_type": "__arc_clock__", "tenant_id": "__arc_clock__",
                    "canonical_id": "__arc_clock__",
                    "contract": "explicit trusted progress marker; not a business event or wall clock"},
                "clock_mode": clock_mode, "pipelines": pipelines,
                "topics": [{"name": name, "partitions": 1,
                            "cleanup.policy": "compact" if name.endswith("-commits") else "delete"}
                           for name in sorted(topics)],
                "consumer_environment": {"KAFKA_BROKERS": broker,
                    "KAFKA_EVENTS_TOPIC": prefix + "-unified-events",
                    "KAFKA_EVENTS_GROUP_ID": prefix + "-api-events",
                    "KAFKA_PROFILES_TOPIC": prefix + "-profile-updates",
                    "KAFKA_PROFILES_GROUP_ID": prefix + "-api-profiles"}}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def api(base, path, payload=None):
    request = urllib.request.Request(base.rstrip("/") + "/" + path,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=1200) as response:
        return json.load(response)


def submit(directory, api_url):
    if not os.environ.get("RUST_BUILD_QUEUE_HELD"):
        raise RuntimeError("submit through scripts/dev-build to reserve native compilation/runtime")
    manifest = json.loads((directory / "manifest.json").read_text())
    result = {"status": "running", "pipelines": {}}
    try:
        existing, seen, cursor = [], set(), None
        while True:
            path = "pipelines?limit=100"
            if cursor:
                path += "&starting_after=" + urllib.parse.quote(cursor, safe="")
            page = api(api_url, path)
            for row in page["data"]:
                if row["id"] in seen:
                    raise ValueError("pipeline pagination repeated an existing identity")
                seen.add(row["id"])
                existing.append(row)
            if not page.get("has_more", False):
                break
            if not page["data"]:
                raise ValueError("pipeline pagination cannot advance an empty page")
            cursor = page["data"][-1]["id"]
        for item in manifest["pipelines"]:
            body = json.loads((directory / item["request"]).read_text())
            if digest(json.dumps(body, sort_keys=True)) != item["sha256"]:
                raise ValueError("prepared pipeline request changed: " + item["name"])
            matches = [row for row in existing if row["name"] == body["name"]]
            if matches:
                nodes = matches[0].get("graph", {}).get("nodes", [])
                if len(matches) != 1 or any(matches[0].get(key) != body[key]
                    for key in ["query", "udfs", "checkpoint_interval_micros"]) or not nodes or any(
                        node["parallelism"] != body["parallelism"] for node in nodes):
                    raise ValueError("existing pipeline differs from prepared request: " + body["name"])
                pipeline = matches[0]
            else:
                validation = api(api_url, "pipelines/validate_query", {"query": body["query"], "udfs": body["udfs"]})
                (directory / (item["name"] + "-validation.json")).write_text(json.dumps(validation, indent=2) + "\n")
                if validation.get("errors") or not validation.get("graph"):
                    raise ValueError("native pipeline validation failed: " + item["name"])
                pipeline = api(api_url, "pipelines", body)
            result["pipelines"][item["name"]] = pipeline
            (directory / "submission.json").write_text(json.dumps(result, indent=2) + "\n")
        result["status"] = "submitted"
    except BaseException as error:
        result.update(status="failed", error=str(error))
        raise
    finally:
        (directory / "submission.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    prepare_cmd = commands.add_parser("prepare")
    prepare_cmd.add_argument("directory", type=Path)
    prepare_cmd.add_argument("--broker", required=True)
    prepare_cmd.add_argument("--topic-prefix", required=True)
    prepare_cmd.add_argument("--clock-mode", choices=["live", "replay"], default="live")
    submit_cmd = commands.add_parser("submit")
    submit_cmd.add_argument("directory", type=Path)
    submit_cmd.add_argument("--api-url", required=True, help="isolated Streamr API /api/v1 URL")
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(args.directory.resolve(), args.broker, args.topic_prefix, args.clock_mode)
    else:
        result = submit(args.directory.resolve(), args.api_url)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
