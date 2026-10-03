import json
from pathlib import Path
import tempfile
import unittest

from streamr_capture import adapt, prepare, RAW_FIELDS


ROOT = Path(__file__).resolve().parents[2]
RESOURCES = ROOT / "flink/identity-resolution/src/test/resources/reference"


class StreamrCaptureTests(unittest.TestCase):
    def setUp(self):
        self.expected = [json.loads(line) for line in
                         (RESOURCES / "identity-expected.jsonl").read_text().splitlines()]
        merges = {(r["payload"]["tenant_id"], r["payload"]["merged_at"]): r["payload"]
                  for r in self.expected if r["stream"] == "identity-merges"}
        self.rows = []
        for record in self.expected:
            if record["stream"] != "unified-events":
                continue
            row = record["payload"].copy()
            merge = merges.get((row["tenant_id"], row["event_time"]))
            row["merge_old"] = merge["old_canonical_id"] if merge else None
            row["user_binding"] = row["canonical_id"] if row["user_id"] not in (None, "") else None
            self.rows.append(row)

    def test_adapter_only_envelopes_sql_outputs_and_merge_metadata(self):
        actual = adapt(self.rows)
        canonical = lambda records: sorted(json.dumps(r, sort_keys=True) for r in records)
        self.assertEqual(canonical(self.expected), canonical(actual))

    def test_missing_business_field_fails(self):
        del self.rows[0]["properties"]
        with self.assertRaisesRegex(ValueError, "fields differ"):
            adapt(self.rows)

    def test_unknown_internal_field_fails(self):
        self.rows[0]["unexpected"] = "ignored?"
        with self.assertRaisesRegex(ValueError, "fields differ"):
            adapt(self.rows)

    def test_wrong_user_binding_fails(self):
        row = next(r for r in self.rows if r["user_id"] not in (None, ""))
        row["user_binding"] = "wrong"
        with self.assertRaisesRegex(ValueError, "user binding"):
            adapt(self.rows)

    def test_self_merge_fails(self):
        self.rows[0]["merge_old"] = self.rows[0]["canonical_id"]
        with self.assertRaisesRegex(ValueError, "self-merge"):
            adapt(self.rows)

    def test_null_identity_fails(self):
        self.rows[0]["canonical_id"] = None
        with self.assertRaisesRegex(ValueError, "canonical_id"):
            adapt(self.rows)

    def test_preparation_preserves_inputs_and_encodes_runtime_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            manifest = prepare(RESOURCES / "identity-input.json", temp, "/arc/fixture's")
            rows = [json.loads(line) for line in (Path(temp) / "input.jsonl").read_text().splitlines()]
            fixture = json.loads((RESOURCES / "identity-input.json").read_text())
            self.assertEqual([s["payload"] for s in fixture["steps"] if s["op"] == "event"], rows)
            self.assertEqual(13, manifest["events"])
            self.assertEqual(41, manifest["checkpoint_id"])
            self.assertTrue(all(set(row) == RAW_FIELDS for row in rows))
            query = (Path(temp) / "query.sql").read_text()
            self.assertIn("/arc/fixture''s/input.jsonl", query)
            self.assertNotIn("$identity_input", query)

    def test_changed_checkpoint_boundary_is_rejected(self):
        fixture = json.loads((RESOURCES / "identity-input.json").read_text())
        next(s for s in fixture["steps"] if s["op"] == "snapshot_restore")["checkpoint_id"] = 42
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / "fixture.json"
            path.write_text(json.dumps(fixture))
            with self.assertRaisesRegex(ValueError, "checkpoint 41"):
                prepare(path, Path(temp) / "out", "/arc/out")


if __name__ == "__main__":
    unittest.main()
