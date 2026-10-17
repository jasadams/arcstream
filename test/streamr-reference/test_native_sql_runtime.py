import unittest
from native_sql_runtime import records_equal


class NativeOutputComparison(unittest.TestCase):
    def test_cross_key_order_is_explicit(self):
        first = {'tenant': 'a', 'count': 1}
        second = {'tenant': 'b', 'count': 2}
        self.assertFalse(records_equal([first, second], [second, first]))
        self.assertTrue(records_equal([first, second], [second, first], unordered=True))

    def test_unordered_comparison_rejects_duplicates_missing_and_changed_fields(self):
        expected = [{'tenant': 'a', 'count': 1}, {'tenant': 'b', 'count': 2}]
        self.assertFalse(records_equal(expected + [expected[0]], expected, unordered=True))
        self.assertFalse(records_equal(expected[:1], expected, unordered=True))
        self.assertFalse(records_equal([{'tenant': 'a', 'count': 9}, expected[1]], expected, unordered=True))
        self.assertFalse(records_equal([{'tenant': 'a'}, expected[1]], expected, unordered=True))


if __name__ == '__main__':
    unittest.main()
