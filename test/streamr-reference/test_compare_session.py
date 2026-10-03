import copy
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from compare_session import ComparisonError, compare, read_records


class SessionComparisonTests(unittest.TestCase):
    def setUp(self):
        payload = {"session_id": "session-a", "canonical_id": "canonical-a", "tenant_id": "acme",
                   "start_time": "2026-06-01 12:00:00.000", "end_time": "2026-06-01 12:02:00.000",
                   "duration_sec": 120, "event_count": 3, "pages": ["/home", "/search"],
                   "event_types": {"page_view": 2, "click": 1}, "device_type": "desktop",
                   "browser": "Firefox", "country": "AU"}
        second = dict(payload, session_id="session-b", tenant_id="beta", canonical_id="canonical-b")
        fresh = dict(payload, event_count=1, duration_sec=0, pages=["/new"],
                     event_types={"login": 1}, start_time="2026-06-01 13:00:00.000",
                     end_time="2026-06-01 13:00:00.000")
        self.expected = [{"stream": "session-summaries", "step": step, "payload": value}
                         for step, value in [("close", payload), ("close", second), ("reuse", fresh)]]
        self.actual = copy.deepcopy(self.expected)

    def test_exact_fields_and_closed_id_reuse_pass(self):
        self.assertEqual({"session_summaries": 3, "fields_per_summary": 12},
                         compare(self.expected, self.actual))

    def test_page_set_and_cross_key_order_within_step_pass(self):
        self.actual[0]["payload"]["pages"].reverse()
        self.actual[0], self.actual[1] = self.actual[1], self.actual[0]
        compare(self.expected, self.actual)

    def test_missing_idle_closure_fails(self):
        self.actual.pop()
        with self.assertRaisesRegex(ComparisonError, "steps/order"):
            compare(self.expected, self.actual)

    def test_missing_key_fails(self):
        self.actual.pop(0)
        with self.assertRaisesRegex(ComparisonError, "keys differ"):
            compare(self.expected, self.actual)

    def test_duplicate_closure_fails(self):
        self.actual.insert(1, copy.deepcopy(self.actual[0]))
        with self.assertRaisesRegex(ComparisonError, "duplicate session"):
            compare(self.expected, self.actual)

    def test_early_processing_clock_closure_fails(self):
        self.actual[0]["step"] = "processing-time-only"
        with self.assertRaisesRegex(ComparisonError, "steps/order"):
            compare(self.expected, self.actual)

    def test_noncontiguous_step_fails(self):
        self.actual.append(dict(copy.deepcopy(self.actual[0]), step="close"))
        with self.assertRaisesRegex(ComparisonError, "contiguous"):
            compare(self.expected, self.actual)

    def test_payload_contract_rejects_missing_and_extra_fields(self):
        for field in ("country", "extra"):
            with self.subTest(field=field):
                actual = copy.deepcopy(self.actual)
                if field == "extra":
                    actual[0]["payload"][field] = "unrecognized"
                else:
                    del actual[0]["payload"][field]
                with self.assertRaisesRegex(ComparisonError, "payload fields"):
                    compare(self.expected, actual)

    def test_native_pages_and_uniqueness_required(self):
        for pages in ('["/home"]', None, ["/home", "/home"], [1]):
            with self.subTest(pages=pages):
                actual = copy.deepcopy(self.actual)
                actual[0]["payload"]["pages"] = pages
                with self.assertRaisesRegex(ComparisonError, "pages"):
                    compare(self.expected, actual)

    def test_native_integer_event_type_map_required(self):
        for counts in ('{"click":1}', None, [], {"click": True}, {"click": "1"}):
            with self.subTest(counts=counts):
                actual = copy.deepcopy(self.actual)
                actual[0]["payload"]["event_types"] = counts
                with self.assertRaisesRegex(ComparisonError, "event_types"):
                    compare(self.expected, actual)

    def test_integer_counters_required(self):
        for value in (None, "3", 3.0, True):
            with self.subTest(value=value):
                actual = copy.deepcopy(self.actual)
                actual[0]["payload"]["event_count"] = value
                with self.assertRaisesRegex(ComparisonError, "integer"):
                    compare(self.expected, actual)

    def test_first_arrival_start_is_not_normalized(self):
        self.actual[0]["payload"]["start_time"] = "2026-06-01 11:59:00.000"
        with self.assertRaisesRegex(ComparisonError, "mismatch"):
            compare(self.expected, self.actual)

    def test_window_end_is_not_session_end(self):
        self.actual[0]["payload"]["end_time"] = "2026-06-01 12:32:00.000"
        with self.assertRaisesRegex(ComparisonError, "mismatch"):
            compare(self.expected, self.actual)

    def test_metadata_page_and_count_changes_fail(self):
        for field, value in [("canonical_id", "other"), ("browser", ""), ("duration_sec", 180),
                             ("event_count", 4), ("pages", ["/home"]), ("event_types", {"click": 3})]:
            with self.subTest(field=field):
                actual = copy.deepcopy(self.actual)
                actual[0]["payload"][field] = value
                with self.assertRaisesRegex(ComparisonError, "mismatch"):
                    compare(self.expected, actual)

    def test_wrong_tenant_fails(self):
        self.actual[0]["payload"]["tenant_id"] = "beta"
        with self.assertRaisesRegex(ComparisonError, "keys differ"):
            compare(self.expected, self.actual)

    def test_duplicate_nested_json_key_fails(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.jsonl"
            path.write_text('{"payload":{"event_types":{"click":1,"click":2}}}\n')
            with self.assertRaisesRegex(ComparisonError, "duplicate JSON key"):
                read_records(path)


if __name__ == "__main__":
    unittest.main()
