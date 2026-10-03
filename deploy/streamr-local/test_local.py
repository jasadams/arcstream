import copy
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

import local


class SubmissionTests(unittest.TestCase):
    def setUp(self):
        self.directory = TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.scope = patch.object(local, "EVIDENCE", Path(self.directory.name))
        self.scope.start()
        self.addCleanup(self.scope.stop)

    def api_stub(self, existing=None, errors=None):
        def api(path, method="GET", payload=None):
            if path == "/pipelines" and method == "GET":
                return {"data": existing or [], "has_more": False}
            if path == "/pipelines/validate_query":
                description = "StatefulProcessor" if "state_get(" in payload["query"] else "projection"
                return {"errors": errors or [], "graph": {"nodes": [{"description": description, "parallelism": 1}]}}
            if path == "/pipelines" and method == "POST":
                return {"id": payload["name"], **payload}
            return {}
        return api

    def test_repeat_submission_reuses_exact_pipelines(self):
        existing = [{"name": name, "id": name, "query": (local.HERE / f"{filename}.sql").read_text()}
                    for filename, name in local.PIPELINES.items()]
        with patch.object(local, "ready"), patch.object(local, "wait_for"), \
                patch.object(local, "api", side_effect=self.api_stub(existing)) as api:
            ids = local.submit()
        self.assertEqual(set(ids), set(local.PIPELINES))
        self.assertFalse(any(call.args[1:] and call.args[0] == "/pipelines"
                             and call.args[1] == "POST" for call in api.call_args_list))

    def test_validation_http_success_with_errors_fails(self):
        with patch.object(local, "ready"), patch.object(local, "api", side_effect=self.api_stub(errors=["unsupported"])):
            with self.assertRaisesRegex(local.SetupError, "SQL validation failed"):
                local.submit()

    def test_missing_validation_graph_fails(self):
        def api(path, method="GET", payload=None):
            return {"data": []} if path == "/pipelines" else {"errors": [], "graph": None}
        with patch.object(local, "ready"), patch.object(local, "api", side_effect=api):
            with self.assertRaisesRegex(local.SetupError, "SQL validation failed"):
                local.submit()

    def test_changed_existing_query_is_not_overwritten(self):
        existing = [{"name": next(iter(local.PIPELINES.values())), "id": "old", "query": "SELECT 1"}]
        with patch.object(local, "ready"), patch.object(local, "api", side_effect=self.api_stub(existing)):
            with self.assertRaisesRegex(local.SetupError, "different query"):
                local.submit()

    def test_duplicate_pipeline_names_fail(self):
        row = {"name": next(iter(local.PIPELINES.values())), "id": "old", "query": "SELECT 1"}
        with patch.object(local, "ready"), patch.object(local, "api", side_effect=self.api_stub([row, copy.deepcopy(row)])):
            with self.assertRaisesRegex(local.SetupError, "Duplicate evaluation"):
                local.submit()

    def test_paginated_api_list_cannot_hide_existing_pipelines(self):
        with patch.object(local, "ready"), patch.object(local, "api", return_value={"data": [], "hasMore": True}):
            with self.assertRaisesRegex(local.SetupError, "paginated pipeline list"):
                local.submit()

    def test_readiness_timeout_is_a_failure(self):
        with self.assertRaisesRegex(local.SetupError, "Timed out"):
            local.wait_for(lambda: False, "readiness", timeout=0)

    def test_connection_reset_is_reported_as_retryable_setup_error(self):
        with patch.object(local, "urlopen", side_effect=ConnectionResetError("startup reset")):
            with self.assertRaisesRegex(local.SetupError, "startup reset"):
                local.api("/ping")

    def test_two_state_owners_are_rejected_before_submission(self):
        def api(path, method="GET", payload=None):
            return ({"data": []} if path == "/pipelines" else
                    {"errors": [], "graph": {"nodes": [{"description": "StatefulProcessor -> StatefulProcessor", "parallelism": 1}]}})
        with patch.object(local, "ready"), patch.object(local, "api", side_effect=api):
            with self.assertRaisesRegex(local.SetupError, "expected 1 state owners"):
                local.submit()

    def test_command_failure_retains_error_body(self):
        with self.assertRaisesRegex(local.SetupError, "useful error"):
            local.run(["python3", "-c", "import sys; print('useful error', file=sys.stderr); sys.exit(2)"])

    def test_raw_topic_must_be_fresh_before_publishing(self):
        with patch.object(local, "ready"), patch.object(local, "topic_rows", return_value=[{"event_id": "prior"}]), \
                patch.object(local, "broker") as broker:
            with self.assertRaisesRegex(local.SetupError, "not empty"):
                local.smoke(False)
            broker.assert_not_called()

    def test_reset_archives_results_and_clears_stale_recovery_metadata(self):
        (local.EVIDENCE / "comparison.json").write_text('{"status":"pass"}')
        (local.EVIDENCE / "restored-checkpoints.json").write_text('[{"epoch":4}]')
        (local.EVIDENCE / "candidate.json").write_text('{"image_id":"pinned"}')
        with patch.object(local.sys, "argv", ["local.py", "reset"]), \
                patch.object(local, "compose", return_value="") as compose:
            self.assertEqual(0, local.main())
            compose.assert_called_once_with("down", "--volumes")
        archive = next((local.EVIDENCE / "runs").iterdir())
        self.assertEqual('[{"epoch":4}]', (archive / "restored-checkpoints.json").read_text())
        self.assertFalse((local.EVIDENCE / "restored-checkpoints.json").exists())
        self.assertTrue((local.EVIDENCE / "candidate.json").exists())


if __name__ == "__main__":
    unittest.main()
