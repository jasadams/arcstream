import copy
import json
from pathlib import Path
import unittest
import native_graph_runtime as g
import native_graph_fixture as fixture


class GraphOracleTests(unittest.TestCase):
    def setUp(self):
        directory=Path(__file__).resolve().parent/'graph-release-fixtures'
        self.raw=[json.loads(x) for x in (directory/'input.jsonl').read_text().splitlines()]
        self.stages=json.loads((directory/'expected-prefixes.json').read_text())
        self.ids={'tenant-a':'00000000-0000-4000-8000-000000000001','tenant-b':'00000000-0000-4000-8000-000000000002'}
        self.actual=[dict(x,canonical_id=self.ids[x['tenant_id']]) for x in self.raw if x['event_id'] in self.stages[-1]['accepted_event_ids']]

    def test_maintained_resources_merge_preserves_base_paths_and_receipt(self):
        resource_path=Path(__file__).resolve().parents[2]/'deploy/streamr-native/runtime-resources.json'
        base={'checkpoint-url':'/owned/checkpoints','database':{'sqlite':{'path':'/owned/db'}},'worker':{'disk-sql-state':{'directory':'/owned/live'},'live-state-resources':{'max-open-databases':1},'sql-state-backend':'memory'},'pipeline':{'source-batch-size':1}}
        before=copy.deepcopy(base)
        merged,receipt=fixture.resource_config(base,resource_path)
        self.assertEqual(base,before)
        self.assertEqual(merged['checkpoint-url'],'/owned/checkpoints')
        self.assertEqual(merged['worker']['disk-sql-state']['directory'],'/owned/live')
        self.assertEqual(merged['worker']['sql-state-backend'],'rocksdb')
        self.assertEqual(merged['worker']['live-state-resources']['max-open-databases'],8)
        self.assertEqual(merged['worker']['join-state']['max-retained-rows'],100)
        self.assertEqual(receipt['sha256'],g.n.digest(resource_path))
        self.assertEqual(receipt['allocations'],json.loads(resource_path.read_text()))

    def test_exact_payload_and_binding_equivalence(self):
        aliases=g.identity_oracle(self.raw,self.stages[-1]['accepted_event_ids'],self.actual)
        self.assertEqual(len(aliases),2)
        self.assertEqual(self.raw[7]['anonymous_id'],self.raw[8]['anonymous_id'])
        self.assertEqual(self.raw[7]['user_id'],'logged-user')
        self.assertIsNone(self.raw[8]['user_id'])

    def test_identity_failures_are_not_normalized_away(self):
        for mutate in (lambda xs:xs.append(xs[-1]),lambda xs:xs[1].update(canonical_id='00000000-0000-4000-8000-000000000003'),lambda xs:xs[-1].update(canonical_id=self.ids['tenant-a']),lambda xs:xs[0].update(page_url='/wrong')):
            rows=copy.deepcopy(self.actual);mutate(rows)
            with self.assertRaises(ValueError):g.identity_oracle(self.raw,self.stages[-1]['accepted_event_ids'],rows)

    def test_unknown_profile_identity_fails(self):
        with self.assertRaises(ValueError):g.normalized([dict(tenant_id='tenant-a',canonical_id='unknown')],{})

    def test_native_materialized_snapshots_preserve_actions_and_reject_other_formats(self):
        first=dict(self.stages[0]['values'][0])
        second=dict(self.stages[1]['values'][-1])
        rows=[first,second]
        self.assertEqual(g.decode_profiles(rows),rows)
        for bad in ([first,first], [second], [dict(before=None,after=first,op='c')], [dict(first,extra=1)], [dict(first,action='delete')]):
            with self.assertRaises(ValueError):g.decode_profiles(bad)
        with self.assertRaises(ValueError):
            g.p.check_snapshots(g.decode_profiles(rows+[second]), rows, ['tenant_id','canonical_id'])

    def test_literal_profiles_and_fractional_closed_average(self):
        g.p.validate_stages(self.stages,len(self.raw),1)
        final=self.stages[-1]['values']
        self.assertTrue(any(x['avg_session_duration_sec']==3.75 for x in final))
        self.assertTrue(all(len(x)==22 for x in final))
        self.assertEqual(len(self.stages[5]['values']),len(self.stages[4]['values']))
        self.assertEqual(len(self.stages[-1]['sessions']),2)

if __name__=='__main__':unittest.main()
