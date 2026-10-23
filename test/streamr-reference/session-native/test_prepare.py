import json
from pathlib import Path
import tempfile
import unittest

from prepare import DESCRIPTIONS, fixture, product_oracle, prepare, milliseconds


class SessionNativeFixtures(unittest.TestCase):
    def test_large_pages_exceed_key_budget_with_literal_full_summary(self):
        data = fixture('large-pages')
        events = data['inputs'][:-1]
        self.assertEqual(len(events),10)
        self.assertEqual(sum(len(row['page_url'].encode('utf-8')) for row in events),1100)
        self.assertGreater(sum(len(row['page_url'].encode('utf-8')) for row in events),512)
        self.assertEqual(len({row['page_url'] for row in events}),3)
        self.assertEqual(data['inputs'][-1]['session_id'],'')
        self.assertEqual(events[0]['event_time'],'2020-02-01 00:00:00.000')
        self.assertEqual(events[-1]['event_time'],'2020-02-01 00:00:09.000')
        self.assertNotEqual(events[-1]['canonical_id'],events[0]['canonical_id'])
        self.assertEqual(data['expected'], [{
            'tenant_id':'tenant-long','session_id':'long-session','canonical_id':'long-first',
            'start_time':'2020-02-01 00:00:00.000','end_time':'2020-02-01 00:00:09.000',
            'duration_sec':9,'event_count':10,'pages':['/catalog/product/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', '/checkout/bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb', '/home/cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc'],
            'event_types':{'page_view':4,'click':3,'feature_used':3},
            'device_type':'tablet','browser':'Safari','country':'NZ',
        }])

    def test_recovery_has_committed_prefix_and_future_sessions(self):
        data = fixture('chronological-recovery')
        self.assertEqual(len(data['inputs']), 17)
        self.assertEqual([row['session_id'] for row in data['expected']], ['recovery-1','recovery-2','recovery-3'])
        self.assertEqual(data['inputs'][2]['session_id'], '')
        self.assertTrue(all(row['session_id']=='' for row in data['inputs'][3:7]))
        self.assertTrue(all(row['event_count']==2 and row['duration_sec']==120 for row in data['expected']))
        self.assertLess(data['inputs'][2]['event_time'], data['inputs'][7]['event_time'])

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
        self.assertIsNone(next(row for row in data['inputs'] if row['event_id']=='empty')['session_id'])

    def test_five_second_cutoff_uses_earliest_accepted_event(self):
        data = fixture('five-second-cutoff')
        first, inside, outside, last, progress = data['inputs']
        self.assertEqual(milliseconds(first['event_time'])-milliseconds(inside['event_time']), 5000)
        self.assertEqual(milliseconds(first['event_time'])-milliseconds(outside['event_time']), 5001)
        expected = data['expected'][0]
        self.assertEqual(expected['event_count'],3)
        self.assertEqual(expected['pages'], ['/first','/inside'])
        self.assertEqual(expected['browser'],'Firefox')
        self.assertEqual(expected['canonical_id'],'later-a')
        self.assertEqual(expected['start_time'],inside['event_time'])
        self.assertEqual(expected['end_time'],last['event_time'])

    def test_event_time_ordering_differs_from_arrival_order(self):
        data = fixture('event-time-ordering')
        expected = data['expected'][0]
        self.assertEqual(expected['canonical_id'], 'earliest')
        self.assertEqual(expected['start_time'], data['inputs'][1]['event_time'])
        self.assertEqual(expected['end_time'], data['inputs'][2]['event_time'])
        self.assertEqual(expected['duration_sec'], 4)
        self.assertEqual((expected['device_type'],expected['browser'],expected['country']), ('tablet','Latest','GB'))
        self.assertNotEqual(expected['device_type'],data['inputs'][3]['device_type'])
        self.assertEqual(expected['event_count'],4)

    def test_equal_event_time_ties_keep_first_and_last_write(self):
        data = fixture('equal-time-ties')
        self.assertEqual(data['inputs'][0]['event_time'],data['inputs'][1]['event_time'])
        self.assertEqual(data['expected'][0]['canonical_id'],data['inputs'][0]['canonical_id'])
        self.assertEqual(data['expected'][0]['browser'],data['inputs'][1]['browser'])

    def test_all_cases_have_real_progress_and_complete_twelve_fields(self):
        for name in DESCRIPTIONS:
            data = fixture(name)
            self.assertEqual(data['inputs'][-1]['session_id'],'')
            self.assertTrue(all('event_time' in row for row in data['inputs']))
            for row in data['inputs']:
                self.assertNotIn('event_ts',row)
                self.assertNotIn('event_ms',row)
                self.assertNotIn('arrival_seq',row)
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
