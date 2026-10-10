import copy
import unittest
from compare_flaredb_events import FIELDS, compare, loads, normalized, response_rows

class FlareDBComparisonTests(unittest.TestCase):
    def event(self):
        row = {name: None for name in FIELDS}
        row.update(event_id='event-1', event_type='page_view', tenant_id='tenant-a',
                   event_time='2026-01-01T00:00:01.123Z', canonical_id='canonical-1',
                   anonymous_id='device-1', properties='{"nested":{"enabled":true}}')
        return row

    def test_timestamp_format_and_timezone_preserve_logical_milliseconds(self):
        expected = normalized(self.event())
        actual = self.event()
        actual['event_time'] = '2026-01-01 01:00:01.123+01:00'
        compare([expected], [normalized(actual)])

    def test_duplicate_selected_event_fails(self):
        expected = normalized(self.event())
        with self.assertRaises(ValueError):
            compare([expected], [expected, expected])

    def test_missing_selected_event_fails(self):
        with self.assertRaises(ValueError):
            compare([normalized(self.event())], [])

    def test_nullable_empty_value_is_not_normalized_away(self):
        expected = normalized(self.event())
        actual = copy.deepcopy(expected)
        actual['referrer'] = ''
        with self.assertRaises(ValueError):
            compare([expected], [actual])

    def test_properties_reserialization_does_not_hide_string_difference(self):
        expected = normalized(self.event())
        actual = copy.deepcopy(expected)
        actual['properties'] = '{"nested": {"enabled": true}}'
        with self.assertRaises(ValueError):
            compare([expected], [actual])

    def test_duplicate_json_keys_fail(self):
        with self.assertRaises(ValueError):
            loads('{"result_table":{},"result_table":{}}')

    def test_response_missing_field_fails(self):
        response = {'result_table': {'data_schema': {'column_names': list(FIELDS[:-1]),
                     'column_data_types': ['Utf8'] * 16}, 'rows': [[]]}}
        with self.assertRaises(ValueError):
            response_rows(response)

if __name__ == '__main__':
    unittest.main()
