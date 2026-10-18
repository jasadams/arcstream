from pathlib import Path
import tempfile
import unittest

from prepare import fixture,prepare,milliseconds


class NativeProfileFixtures(unittest.TestCase):
    def test_exact_closed_metrics_and_tenant_isolation(self):
        data=fixture()
        self.assertEqual(len(data['inputs']),13)
        self.assertEqual([(r['tenant_id'],r['closed_session_count'],r['avg_session_duration_sec']) for r in data['expected']],
                         [('tenant-a',1,1),('tenant-a',2,2),('tenant-b',1,5),('tenant-a',3,2)])
        self.assertEqual([r['canonical_id'] for r in data['final']],['shared-person','shared-person'])
        self.assertEqual([r['closed_session_count'] for r in data['final']],[3,1])

    def test_fractional_duration_is_not_truncated_per_session(self):
        data=fixture()
        self.assertEqual(milliseconds(data['inputs'][1]['event_time'])-milliseconds(data['inputs'][0]['event_time']),1900)
        self.assertEqual(milliseconds(data['inputs'][4]['event_time'])-milliseconds(data['inputs'][3]['event_time']),2900)
        self.assertEqual(data['expected'][1]['avg_session_duration_sec'],2)
        self.assertNotEqual((1+2)//2,2)
        self.assertEqual([p['completed_duration_ms'] for p in data['prefixes']],[1900,4800,5000,8800])

    def test_reused_id_late_rejection_and_ordinary_progress(self):
        rows=fixture()['inputs']
        self.assertEqual({r['session_id'] for r in rows if r['tenant_id']=='tenant-a'},{'reused'})
        self.assertEqual(milliseconds(rows[10]['event_time'])-milliseconds(rows[11]['event_time']),34000)
        self.assertEqual([i+1 for i,r in enumerate(rows) if r['session_id'] in ('',None)],[3,6,9,13])
        for row in rows:
            self.assertNotIn('is_clock',row);self.assertNotIn('clock_ts',row);self.assertNotIn('kind',row)
            self.assertNotIn('event_ts',row);self.assertNotIn('event_ms',row);self.assertNotIn('arrival_seq',row)
            self.assertIsInstance(row['event_time'],str)
            self.assertEqual(row['event_type'],'page_view')
        self.assertGreater(milliseconds(rows[9]['event_time'])-milliseconds(rows[8]['event_time']),24*60*60*1000)
        self.assertEqual(fixture()['expected'][-1]['closed_session_count'],3)
        self.assertEqual(rows[2]['session_id'],'')
        self.assertIsNone(rows[8]['session_id'])
        prefixes=fixture()['prefixes']
        self.assertEqual([p['input_lines'] for p in prefixes],[3,6,9,13])
        for prefix in prefixes:
            self.assertEqual(prefix['source_event_time'],rows[prefix['input_lines']-1]['event_time'])

    def test_preparation_deterministic_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as temp:
            a,b=Path(temp)/'a',Path(temp)/'b';prepare(a);prepare(b)
            for path in a.iterdir():self.assertEqual(path.read_bytes(),(b/path.name).read_bytes())
            with self.assertRaises(FileExistsError):prepare(a)


if __name__=='__main__':unittest.main()
