from copy import deepcopy
import json
import unittest
from native_sql_runtime import records_equal, decode_kafka_offset, check_cdc, validate_stages


class TinyBincode:
    @staticmethod
    def bincode_uint(data):
        if not data or data[0] >= 251:
            raise ValueError('test only encodes tiny varints')
        return data[0], 1


class KafkaFixtureChecks(unittest.TestCase):
    def test_source_next_offset_decoder_checks_identity_and_complete_encoding(self):
        self.assertEqual(decode_kafka_offset(TinyBincode, b'\0', b'\0\x06'), 3)
        for key, value in [(b'\x02', b'\x02\x06'), (b'\0', b'\x02\x06'),
                           (b'\0\0', b'\0\x06'), (b'\0', b'\0\x06\0'),
                           (b'\0', b'\0\x01'), (b'\0', b'\0')]:
            with self.subTest(key=key, value=value), self.assertRaises((ValueError, IndexError)):
                decode_kafka_offset(TinyBincode, key, value)

    def test_cdc_chain_exact_images_and_cross_key_order(self):
        first, second, other = {'key':'a','n':1}, {'key':'a','n':2}, {'key':'b','n':4}
        rows = [dict(before=None,after=first,op='c'), dict(before=None,after=other,op='c'),
                dict(before=first,after=second,op='u')]
        data = lambda values: '\n'.join(json.dumps(row) for row in values).encode()
        self.assertEqual(check_cdc(data(rows), [first,second,other], ['key']), [first,other,second])
        bad_rows = []
        broken = deepcopy(rows);broken[-1]['before']={'key':'a','n':99};bad_rows.append(broken)
        delete = deepcopy(rows);delete[-1]['after']=None;delete[-1]['op']='d';bad_rows.append(delete)
        wrong_op = deepcopy(rows);wrong_op[-1]['op']='c';bad_rows.append(wrong_op)
        bad_rows.extend([rows[:-1], rows+[rows[-1]], rows+[dict(before=second,after={'key':'a','n':3},op='u')]])
        for bad in bad_rows:
            with self.assertRaises(ValueError):check_cdc(data(bad), [first,second,other], ['key'])
        with self.assertRaises(ValueError):check_cdc(data(rows), [second,first,other], ['key'])
        with self.assertRaises(ValueError):check_cdc(b'{"before":null,"before":{},"after":{},"op":"c"}', [], ['key'])

    def test_stage_boundaries_leave_recovery_suffix_and_cover_complete_input(self):
        stages = [dict(input_lines=3,value={'n':1}), dict(input_lines=6,value={'n':2})]
        validate_stages(stages, 6, 3)
        for invalid in [[], stages[::-1], [stages[0]], [stages[0],stages[0]],
                        [dict(input_lines=3,value=None),stages[1]]]:
            with self.assertRaises(ValueError):validate_stages(invalid, 6, 3)


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
