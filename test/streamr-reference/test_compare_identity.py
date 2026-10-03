import copy
from pathlib import Path
import unittest

from compare_identity import ComparisonError, compare, read_records


ORACLE = (Path(__file__).resolve().parents[2] / "flink/identity-resolution/"
          "src/test/resources/reference/identity-expected.jsonl")


class IdentityComparisonTests(unittest.TestCase):
    def setUp(self):
        self.expected = read_records(ORACLE)
        self.actual = copy.deepcopy(self.expected)
        for record in self.actual:
            for field in ("canonical_id", "old_canonical_id"):
                if field in record["payload"]:
                    record["payload"][field] = (
                        "uuid-" + record["payload"]["tenant_id"] + "-" + record["payload"][field])

    def test_canonical_reuse_across_tenants_fails(self):
        events = [r["payload"] for r in self.actual if r["stream"] == "unified-events"]
        other = next(p for p in events if p["tenant_id"] != events[0]["tenant_id"])
        other["canonical_id"] = events[0]["canonical_id"]
        with self.assertRaisesRegex(ComparisonError, "across tenants"):
            compare(self.expected, self.actual)

    def test_random_ids_and_output_interleaving_are_normalized(self):
        self.actual.reverse()
        result = compare(self.expected, self.actual)
        self.assertEqual(13, result["unified_events"])
        self.assertEqual(2, result["identity_merges"])

    def test_missing_event_fails(self):
        self.actual.pop(0)
        with self.assertRaises(ComparisonError):
            compare(self.expected, self.actual)

    def test_duplicate_event_fails(self):
        self.actual.append(copy.deepcopy(self.actual[0]))
        with self.assertRaisesRegex(ComparisonError, "duplicate"):
            compare(self.expected, self.actual)

    def test_dropped_nullable_field_is_not_normalized_away(self):
        del self.actual[0]["payload"]["user_id"]
        with self.assertRaisesRegex(ComparisonError, "output mismatch"):
            compare(self.expected, self.actual)

    def test_changed_business_field_fails(self):
        self.actual[0]["payload"]["properties"] = "{}"
        with self.assertRaises(ComparisonError):
            compare(self.expected, self.actual)

    def test_wrong_identity_link_fails(self):
        events = [r for r in self.actual if r["stream"] == "unified-events"]
        events[1]["payload"]["canonical_id"] = "wrong-link"
        with self.assertRaises(ComparisonError):
            compare(self.expected, self.actual)

    def test_missing_merge_fails(self):
        actual = [r for r in self.actual if r["stream"] != "identity-merges"]
        with self.assertRaises(ComparisonError):
            compare(self.expected, actual)

    def test_reversed_merge_direction_fails(self):
        merge = next(r["payload"] for r in self.actual if r["stream"] == "identity-merges")
        merge["canonical_id"], merge["old_canonical_id"] = (
            merge["old_canonical_id"], merge["canonical_id"])
        with self.assertRaises(ComparisonError):
            compare(self.expected, self.actual)

    def test_duplicate_merge_fails(self):
        self.actual.append(copy.deepcopy(next(
            r for r in self.actual if r["stream"] == "identity-merges")))
        with self.assertRaises(ComparisonError):
            compare(self.expected, self.actual)

    def test_unknown_merge_identity_fails(self):
        merge = next(r["payload"] for r in self.actual if r["stream"] == "identity-merges")
        merge["old_canonical_id"] = "unknown"
        with self.assertRaisesRegex(ComparisonError, "absent"):
            compare(self.expected, self.actual)


if __name__ == "__main__":
    unittest.main()
