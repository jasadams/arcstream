import json
from pathlib import Path
import tempfile
import unittest

from prepare import DESCRIPTIONS, fixture, original, prepare


class SessionNativeFixtures(unittest.TestCase):
    def test_recovery_case_has_nonempty_prefix_and_independent_future_sessions(self):
        data = fixture('chronological-recovery')
        self.assertEqual(len(data['expected']), 3)
        self.assertEqual(len(data['inputs']), 17)
        self.assertTrue(data['inputs'][2]['is_clock'])
        self.assertTrue(all(row['is_clock'] for row in data['inputs'][3:7]))
        self.assertEqual([row['session_id'] for row in data['expected']], ['recovery-1', 'recovery-2', 'recovery-3'])
        self.assertEqual([row['event_id'] for row in data['inputs'] if not row['is_clock']],
                         ['r1-first', 'r1-second', 'r2-first', 'r2-second', 'r3-first', 'r3-second'])
        self.assertTrue(all(row['event_count'] == 2 and row['duration_sec'] == 120 for row in data['expected']))
        self.assertLess(data['inputs'][2]['event_ms'], data['inputs'][7]['event_ms'])

    def test_full_original_expectations_remain_unchanged(self):
        self.assertEqual(fixture('complete-original')['expected'], original()['expected'])

    def test_arrival_fields_and_late_reopen_are_explicit(self):
        late = fixture('late-first-arrival')
        self.assertLess(late['inputs'][1]['event_ms'], late['inputs'][0]['event_ms'])
        summary = late['expected'][0]
        self.assertEqual(summary['canonical_id'], 'first-a')
        self.assertEqual(summary['start_time'], '2020-01-31 23:59:00.000')
        self.assertEqual(summary['browser'], 'Chrome')
        reopened = fixture('reopened-late')
        self.assertEqual(reopened['expected'][1]['event_count'], 2)
        self.assertEqual(reopened['expected'][1]['pages'], ['/new', '/oldlater'])
        self.assertEqual(reopened['expected'][1]['start_time'], '2020-02-01 00:40:00.000')
        self.assertLess(reopened['inputs'][4]['event_ms'], reopened['inputs'][3]['event_ms'])

    def test_all_cases_have_real_progress_and_complete_twelve_fields(self):
        for name in DESCRIPTIONS:
            data = fixture(name)
            self.assertTrue(data['inputs'][-1]['is_clock'])
            self.assertEqual([r['arrival_seq'] for r in data['inputs']], list(range(len(data['inputs']))))
            for row in data['inputs']:
                self.assertEqual(row['clock_ts'], row['event_ts'])
            for row in data['expected']:
                self.assertEqual(len(row), 12)
                self.assertEqual(sum(row['event_types'].values()), row['event_count'])
        full = fixture('complete-original')
        self.assertEqual({r['tenant_id'] for r in full['expected']}, {'tenant-a', 'tenant-b', 'tenant-pre'})

    def test_preparation_is_deterministic_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            one, two = Path(temp)/'one', Path(temp)/'two'
            prepare(one, 'reopened-late')
            manifest = prepare(two, 'reopened-late')
            for name in ['input.jsonl', 'expected.jsonl', 'manifest.json']:
                self.assertEqual((one/name).read_bytes(), (two/name).read_bytes())
            self.assertEqual(manifest['expected_rows'], 2)
            with self.assertRaises(FileExistsError):
                prepare(one, 'chronological')
            self.assertEqual(len([json.loads(line) for line in (one/'expected.jsonl').read_text().splitlines()]), 2)


if __name__ == '__main__':
    unittest.main()
