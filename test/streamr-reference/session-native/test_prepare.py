import json
from pathlib import Path
import tempfile
import unittest

from prepare import DESCRIPTIONS, fixture, product_oracle, prepare


class SessionNativeFixtures(unittest.TestCase):
    def test_recovery_has_committed_prefix_and_future_sessions(self):
        data = fixture('chronological-recovery')
        self.assertEqual(len(data['inputs']), 17)
        self.assertEqual([row['session_id'] for row in data['expected']], ['recovery-1','recovery-2','recovery-3'])
        self.assertEqual(data['inputs'][2]['session_id'], '')
        self.assertTrue(all(row['session_id']=='' for row in data['inputs'][3:7]))
        self.assertTrue(all(row['event_count']==2 and row['duration_sec']==120 for row in data['expected']))
        self.assertLess(data['inputs'][2]['event_ms'], data['inputs'][7]['event_ms'])

    def test_native_product_oracle_rejects_late_events(self):
        data = fixture('tenant-reopen')
        self.assertEqual(len(data['expected']), 4)
        self.assertEqual({row['tenant_id'] for row in data['expected']}, {'tenant-a','tenant-b'})
        self.assertEqual([row['event_count'] for row in data['expected']], [2,1,1,1])
        self.assertEqual(fixture('late-first-arrival')['expected'][0]['browser'], 'Firefox')
        reopened = fixture('reopened-late')['expected'][1]
        self.assertEqual(reopened['event_count'], 1)
        self.assertEqual(reopened['pages'], ['/new'])
        self.assertEqual(reopened['start_time'], '2020-02-01 00:40:00.000')
        self.assertEqual(data['expected'], product_oracle()['expected'])

    def test_five_second_cutoff_preserves_accepted_arrival_semantics(self):
        data = fixture('five-second-cutoff')
        first, inside, outside, last, progress = data['inputs']
        self.assertEqual(first['event_ms']-inside['event_ms'], 5000)
        self.assertEqual(first['event_ms']-outside['event_ms'], 5002)
        expected = data['expected'][0]
        self.assertEqual(expected['event_count'],3)
        self.assertEqual(expected['pages'], ['/first','/inside'])
        self.assertEqual(expected['browser'],'Chrome')
        self.assertEqual(expected['canonical_id'],'first-a')
        self.assertEqual(expected['start_time'],first['event_ts'])
        self.assertEqual(expected['end_time'],last['event_ts'])

    def test_all_cases_have_real_progress_and_complete_twelve_fields(self):
        for name in DESCRIPTIONS:
            data = fixture(name)
            self.assertEqual(data['inputs'][-1]['session_id'],'')
            self.assertEqual([row['arrival_seq'] for row in data['inputs']],list(range(len(data['inputs']))))
            for row in data['inputs']:
                self.assertNotIn('clock_ts',row)
                self.assertNotIn('is_clock',row)
                self.assertNotIn('kind',row)
                self.assertNotIn('generation',row)
            for row in data['expected']:
                self.assertEqual(len(row),12)
                self.assertEqual(sum(row['event_types'].values()),row['event_count'])

    def test_preparation_deterministic_and_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            one,two=Path(temp)/'one',Path(temp)/'two'
            prepare(one,'reopened-late');manifest=prepare(two,'reopened-late')
            for name in ['input.jsonl','expected.jsonl','manifest.json']:
                self.assertEqual((one/name).read_bytes(),(two/name).read_bytes())
            self.assertEqual(manifest['expected_rows'],2)
            with self.assertRaises(FileExistsError):prepare(one,'chronological')
            self.assertEqual(len([json.loads(line) for line in (one/'expected.jsonl').read_text().splitlines()]),2)


if __name__=='__main__':unittest.main()
