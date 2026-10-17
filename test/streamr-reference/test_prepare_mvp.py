import importlib.util
import io
import urllib.error
import json
from pathlib import Path
import re
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("prepare_mvp", ROOT / "deploy/streamr-native/prepare_mvp.py")
prepare_mvp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(prepare_mvp)


class NativeManifestTests(unittest.TestCase):
    def test_external_raw_input_replays_history_without_creating_or_rewiring_other_topics(self):
        with tempfile.TemporaryDirectory() as temporary:
            default = Path(temporary) / "default"
            external = Path(temporary) / "external"
            original = prepare_mvp.prepare(default, "broker:9092", "arc-mvp-history", "replay")
            replay = prepare_mvp.prepare(external, "broker:9092", "arc-mvp-history", "replay", "raw-events")
            self.assertEqual(replay["external_input_topic"], "raw-events")
            self.assertEqual(replay["clock_mode"], "replay")
            self.assertEqual(replay["topics"], [topic for topic in original["topics"]
                if topic["name"] != "arc-mvp-history-raw-events"])
            self.assertNotIn("raw-events", {topic["name"] for topic in replay["topics"]})
            identity = json.loads((external / "identity.json").read_text())
            self.assertIn("topic = 'raw-events'", identity["query"])
            self.assertIn("'source.offset' = 'earliest'", identity["query"])
            self.assertIn("'source.group_id' = 'arc-mvp-history-identity-owner'", identity["query"])
            self.assertNotIn("arc-mvp-history-raw-events", identity["query"])
            for pipeline in replay["pipelines"]:
                if pipeline["name"] != "identity":
                    self.assertEqual((external / pipeline["request"]).read_bytes(),
                                     (default / pipeline["request"]).read_bytes())

    def test_external_input_rejects_invalid_names_and_managed_topic_cycles(self):
        with tempfile.TemporaryDirectory() as temporary:
            for index, name in enumerate([".", "..", "bad/name", "bad'topic", "a" * 250,
                                          "arc-mvp-history-unified-events"]):
                with self.subTest(name=name), self.assertRaises(ValueError):
                    prepare_mvp.prepare(Path(temporary) / str(index), "broker:9092",
                                        "arc-mvp-history", "replay", name)

    def test_feedback_sinks_have_independent_compact_recovery_topics(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "candidate"
            manifest = prepare_mvp.prepare(directory, "isolated:9092", "arc-mvp-test", "replay")
            markers = []
            requests = []
            for pipeline in manifest["pipelines"]:
                body = json.loads((directory / pipeline["request"]).read_text())
                self.assertEqual(body["parallelism"], 1)
                self.assertNotIn("arc-native-", body["query"])
                self.assertNotIn("{{", body["query"])
                self.assertNotIn("single_file", body["query"])
                requests.append(body)
                markers.extend(re.findall(r"'sink.recovery_topic'\s*=\s*'([^']+)'", body["query"]))
            self.assertEqual(len(markers), 15)
            self.assertEqual(len(set(markers)), 15)
            topics = {topic["name"]: topic for topic in manifest["topics"]}
            for marker in markers:
                self.assertEqual(topics[marker]["cleanup.policy"], "compact")
                self.assertEqual(topics[marker]["partitions"], 1)
            normalizer = next(body for body in requests if body["name"].endswith("-profile-events"))
            timers = next(body for body in requests if body["name"].endswith("-profile-timers"))
            self.assertIn("arc-mvp-test-profile-signals", normalizer["query"])
            self.assertIn("arc-mvp-test-profile-signals", timers["query"])
            self.assertNotIn("WATERMARK", normalizer["query"])
            self.assertIn("JSON_VALUE(value, '$.event_time')", normalizer["query"])
            self.assertIn("arc-mvp-test-execution-events", normalizer["query"])
            sessions = next(body for body in requests if body["name"].endswith("-sessions"))
            self.assertIn("arc-mvp-test-session-signals", sessions["query"])
            self.assertIn("session_progress", sessions["query"])

    def test_analytics_templates_use_isolated_topics_and_preserve_schema_limits(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "candidate"
            manifest = prepare_mvp.prepare(directory, "isolated:9092", "arc-mvp-storage", "replay")
            for backend in ["flaredb", "pinot"]:
                for table, suffix in {"events": "unified-events", "profiles": "profile-updates", "sessions": "sessions"}.items():
                    config = json.loads((directory / "analytics" / backend / (table + ".json")).read_text())
                    if backend == "flaredb":
                        self.assertEqual(config["ingestionConfig"], {"topic": "arc-mvp-storage-" + suffix, "brokers": "isolated:9092"})
                    else:
                        streams = config["ingestionConfig"]["streamIngestionConfig"]["streamConfigMaps"]
                        for stream in streams:
                            self.assertEqual(stream["stream.kafka.topic.name"], "arc-mvp-storage-" + suffix)
                            self.assertEqual(stream["stream.kafka.broker.list"], "isolated:9092")
                            self.assertEqual(stream["stream.kafka.isolation.level"], "read_committed")
            self.assertTrue(manifest["analytics_configuration"]["limitations"])
            patch = json.loads((directory / "worker-configuration-patch.json").read_text())["worker"]
            self.assertEqual(patch, manifest["worker_configuration_patch"])
            self.assertEqual(patch["typed-sql-state"]["max-working-event-bytes"], 8388608)
            self.assertEqual(patch["typed-sql-state"]["max-resident-bytes"], 16777216)

    def test_live_and_replay_are_explicit_separate_manifests(self):
        with tempfile.TemporaryDirectory() as temporary:
            for mode in ["live", "replay"]:
                directory = Path(temporary) / mode
                manifest = prepare_mvp.prepare(directory, "isolated:9092", "arc-mvp-" + mode, mode)
                self.assertEqual(manifest["clock_mode"], mode)
                query = (directory / "profile-events.sql").read_text()
                self.assertIn("'$.event_time'), '" + mode + "')", query)

    def test_submission_error_preserves_native_diagnostics(self):
        error = urllib.error.HTTPError("http://unit-test/api/v1/pipelines", 400,
                                       "Bad Request", {}, io.BytesIO(b'{"error":"native UDF failed"}'))
        with patch.object(prepare_mvp.urllib.request, "urlopen", side_effect=error):
            with self.assertRaisesRegex(RuntimeError, "HTTP 400: .*native UDF failed"):
                prepare_mvp.api("http://unit-test/api/v1", "pipelines", {"query": "SELECT 1"})

    def test_existing_application_topic_namespace_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(ValueError):
                prepare_mvp.prepare(Path(temporary) / "candidate", "existing:9092", "raw-events", "live")

    def test_repeated_submission_finds_existing_pipelines_on_later_pages(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "candidate"
            manifest = prepare_mvp.prepare(directory, "isolated:9092", "arc-mvp-repeat", "replay")
            existing = []
            for index, pipeline in enumerate(manifest["pipelines"]):
                row = json.loads((directory / pipeline["request"]).read_text())
                row.pop("tags")  # Public Pipeline responses do not expose tags.
                row["id"] = "existing-" + str(index)
                row["graph"] = {"nodes": [{"parallelism": 1}]}
                existing.append(row)

            def fake_api(base, path, payload=None):
                self.assertIsNone(payload, "idempotent submission must create no new pipelines")
                if "starting_after=" not in path:
                    return {"data": existing[:1], "has_more": True}
                return {"data": existing[1:], "has_more": False}

            with patch.object(prepare_mvp, "api", side_effect=fake_api), patch.dict(
                "os.environ", {"RUST_BUILD_QUEUE_HELD": "unit-test-mocked-api"}
            ):
                result = prepare_mvp.submit(directory, "http://unit-test/api/v1")
            self.assertEqual(result["status"], "submitted")
            self.assertEqual(len(result["pipelines"]), 11)

    def test_same_tags_cannot_hide_different_existing_sql(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary) / "candidate"
            manifest = prepare_mvp.prepare(directory, "isolated:9092", "arc-mvp-drift", "live")
            existing = json.loads((directory / manifest["pipelines"][0]["request"]).read_text())
            existing.update(id="changed", query="SELECT 'different application policy'", graph={"nodes": [{"parallelism": 1}]})
            with patch.object(prepare_mvp, "api", return_value={"data": [existing], "has_more": False}), patch.dict(
                "os.environ", {"RUST_BUILD_QUEUE_HELD": "unit-test-mocked-api"}
            ):
                with self.assertRaisesRegex(ValueError, "existing pipeline differs"):
                    prepare_mvp.submit(directory, "http://unit-test/api/v1")


if __name__ == "__main__":
    unittest.main()
