import copy
from datetime import datetime, timezone
import json
from pathlib import Path
import unittest
from tempfile import TemporaryDirectory

from compare_profile import ComparisonError, compare, read_records


ORACLE = (Path(__file__).resolve().parents[2] / "flink/identity-resolution/"
          "src/test/resources/reference/profile-expected.jsonl")


class ProfileComparisonTests(unittest.TestCase):
    def test_duplicate_json_keys_fail(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "duplicate.jsonl"
            path.write_text('{"payload":{"total_events":1,"total_events":2}}\n')
            with self.assertRaisesRegex(ComparisonError, "duplicate JSON key"):
                read_records(path)

    def setUp(self):
        self.expected = read_records(ORACLE)
        self.base = 1_700_000_000_000
        self.actual = []
        for index, oracle in enumerate(self.expected):
            lower = self.base + 86_400_000 + index * 1000
            upper = lower + 100
            payload = copy.deepcopy(oracle["payload"])
            for field, value in payload.items():
                if not isinstance(value, dict):
                    continue
                if "base_offset_ms" in value:
                    payload[field] = self.base + value["base_offset_ms"]
                elif value["clock"] == "emission_ms":
                    payload[field] = lower + 50
                elif value["clock"] == "emission_timestamp":
                    moment = datetime.fromtimestamp((lower + 51) / 1000, timezone.utc)
                    payload[field] = moment.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                elif value["clock"] == "session_duration_seconds":
                    payload[field] = (lower + 50 - self.base - value["session_start_offset_ms"]) // 1000
            self.actual.append({"stream": oracle["stream"], "step": oracle["step"],
                                "captured_from_ms": lower, "captured_to_ms": upper,
                                "payload": payload})

    def test_exact_business_fields_and_valid_clock_bounds_pass(self):
        self.assertEqual({"profile_updates": 6, "fields_per_update": 33},
                         compare(self.expected, self.actual, self.base))

    def test_changed_field_order_is_a_set_not_a_timing_divergence(self):
        for record in self.actual:
            record["payload"]["changed_fields"].reverse()
        compare(self.expected, self.actual, self.base)

    def test_missing_idle_decay_fails(self):
        self.actual.pop()
        with self.assertRaisesRegex(ComparisonError, "steps/order"):
            compare(self.expected, self.actual, self.base)

    def test_duplicate_emission_fails(self):
        self.actual.append(copy.deepcopy(self.actual[-1]))
        with self.assertRaisesRegex(ComparisonError, "duplicate"):
            compare(self.expected, self.actual, self.base)

    def test_reordered_emissions_fail(self):
        self.actual.reverse()
        with self.assertRaisesRegex(ComparisonError, "steps/order"):
            compare(self.expected, self.actual, self.base)

    def test_serialized_array_is_not_an_array(self):
        self.actual[0]["payload"]["top_pages"] = '["/home"]'
        with self.assertRaisesRegex(ComparisonError, "real array"):
            compare(self.expected, self.actual, self.base)

    def test_null_counter_fails(self):
        self.actual[0]["payload"]["total_events"] = None
        with self.assertRaisesRegex(ComparisonError, "integer"):
            compare(self.expected, self.actual, self.base)

    def test_string_counter_fails(self):
        self.actual[0]["payload"]["total_events"] = "1"
        with self.assertRaisesRegex(ComparisonError, "integer"):
            compare(self.expected, self.actual, self.base)

    def test_boolean_is_not_an_integer(self):
        self.actual[0]["payload"]["total_events"] = True
        with self.assertRaisesRegex(ComparisonError, "integer"):
            compare(self.expected, self.actual, self.base)

    def test_integer_is_not_a_boolean(self):
        self.actual[0]["payload"]["current_session_active"] = 1
        with self.assertRaisesRegex(ComparisonError, "boolean"):
            compare(self.expected, self.actual, self.base)

    def test_wrong_counter_is_not_normalized(self):
        self.actual[1]["payload"]["total_events"] += 1
        with self.assertRaisesRegex(ComparisonError, "mismatch"):
            compare(self.expected, self.actual, self.base)

    def test_wrong_tenant_fails(self):
        self.actual[0]["payload"]["tenant_id"] = "wrong-tenant"
        with self.assertRaisesRegex(ComparisonError, "mismatch"):
            compare(self.expected, self.actual, self.base)

    def test_wrong_changed_fields_fail(self):
        self.actual[-1]["payload"]["changed_fields"] = ["total_events"]
        with self.assertRaisesRegex(ComparisonError, "mismatch"):
            compare(self.expected, self.actual, self.base)

    def test_duplicate_changed_fields_fail(self):
        self.actual[0]["payload"]["changed_fields"].append("canonical_id")
        with self.assertRaisesRegex(ComparisonError, "duplicate changed_fields"):
            compare(self.expected, self.actual, self.base)

    def test_updated_at_outside_measured_emission_fails(self):
        self.actual[0]["payload"]["updated_at"] = self.actual[0]["captured_from_ms"] - 1
        with self.assertRaisesRegex(ComparisonError, "updated_at outside"):
            compare(self.expected, self.actual, self.base)

    def test_invalid_timestamp_fails(self):
        self.actual[0]["payload"]["timestamp"] = "not a timestamp"
        with self.assertRaisesRegex(ComparisonError, "valid UTC timestamp"):
            compare(self.expected, self.actual, self.base)

    def test_timestamp_outside_measured_emission_fails(self):
        self.actual[0]["payload"]["timestamp"] = "2020-01-01 00:00:00.000"
        with self.assertRaisesRegex(ComparisonError, "timestamp outside"):
            compare(self.expected, self.actual, self.base)

    def test_old_event_time_duration_divergence_fails(self):
        self.actual[1]["payload"]["current_session_duration_sec"] = 120
        with self.assertRaisesRegex(ComparisonError, "session duration outside"):
            compare(self.expected, self.actual, self.base)

    def test_closed_session_duration_must_be_zero(self):
        record = next(r for r in self.actual if not r["payload"]["current_session_active"])
        record["payload"]["current_session_duration_sec"] = 1
        with self.assertRaisesRegex(ComparisonError, "mismatch"):
            compare(self.expected, self.actual, self.base)

    def test_missing_timestamp_contract_fails(self):
        del self.actual[0]["payload"]["timestamp"]
        with self.assertRaisesRegex(ComparisonError, "payload fields"):
            compare(self.expected, self.actual, self.base)


if __name__ == "__main__":
    unittest.main()
