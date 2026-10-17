#!/usr/bin/env python3
"""Adversarial caller-oracle checks only; never starts the SQL engine."""
import copy, json, pathlib, tempfile, unittest
import run
ROOT=pathlib.Path(__file__).resolve().parent
class OracleTests(unittest.TestCase):
    def setUp(self):
        self.rows=run.read_json(ROOT/'expected-prefixes.json');self.tmp=tempfile.TemporaryDirectory();self.path=pathlib.Path(self.tmp.name)/'output'
    def tearDown(self):self.tmp.cleanup()
    def write(self,events):self.path.write_text('\n'.join(json.dumps(x) for x in events))
    def event(self,old,new):return {'before':old,'after':new,'op':'c' if old is None else 'd' if new is None else 'u'}
    def valid(self):
        a=self.rows[0];f=self.rows[-1]
        return [self.event(None,a),self.event(a,a),self.event(a,None),self.event(None,a),self.event(a,f)]
    def test_delete_recreate_noop_and_checkpoint(self):
        self.write(self.valid());r=run.compare(self.path,self.rows[-1],self.rows[0],1);self.assertEqual(r['deletes'],1);self.assertEqual(r['noops'],1);self.assertFalse(r['accepted_application_emission_parity'])
    def test_typed_intermediate_not_whole_prefix_claim(self):
        a=copy.deepcopy(self.rows[0]);a['top_pages']=[]
        self.write([self.event(None,a),self.event(a,self.rows[-1])]);r=run.compare(self.path,self.rows[-1]);self.assertFalse(r['intermediate_whole_event_prefix_checked'])
    def test_wrong_before_value_rejected(self):
        events=self.valid();events[-1]['before']=dict(events[-1]['before'],clicks=100);self.write(events)
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1])
    def test_wrong_before_type_rejected(self):
        events=self.valid();events[-1]['before']=dict(events[-1]['before'],total_events=True);self.write(events)
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1])
    def test_final_values_rejected(self):
        events=self.valid();events[-1]['after']=dict(self.rows[-1],events_90d=0);self.write(events)
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1])
    def test_extra_final_multiplicity_rejected(self):
        self.write([self.event(None,self.rows[-1]),self.event(None,self.rows[-1])])
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1])
    def test_checkpoint_value_rejected(self):
        self.write(self.valid())
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1],self.rows[1],1)
    def test_duplicate_keys_rejected(self):
        self.path.write_text('{"op":"c","op":"d","before":null,"after":null}')
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1])
    def test_invalid_envelope_rejected(self):
        self.write([{'before':None,'after':self.rows[-1],'op':'u'}])
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1])
    def test_missing_column_rejected(self):
        row=dict(self.rows[-1]);row.pop('browser',None);row.pop('last_browser');self.write([self.event(None,row)])
        with self.assertRaises(AssertionError):run.compare(self.path,self.rows[-1])
    def test_receipt_binding_rejects_commit_and_binary(self):
        b=pathlib.Path(self.tmp.name)/'binary';b.write_bytes(b'not-executed');c='a'*40;r={'source_commit':c,'binary_sha256':run.digest(b),'build_exit':0}
        self.assertEqual(run.provenance(r,c,b),run.digest(b))
        with self.assertRaises(AssertionError):run.provenance(r,'b'*40,b)
        b.write_bytes(b'different')
        with self.assertRaises(AssertionError):run.provenance(r,c,b)
    def test_build_exit_boolean_rejected(self):
        b=pathlib.Path(self.tmp.name)/'binary';b.write_bytes(b'not-executed');c='a'*40
        for value in (False,True,'0'):
            with self.assertRaises(AssertionError):run.provenance({'source_commit':c,'binary_sha256':run.digest(b),'build_exit':value},c,b)
    def test_checkpoint_bytes_physical_count_and_suffix(self):
        cp=pathlib.Path(self.tmp.name)/'checkpoint';prefix=json.dumps(self.event(None,self.rows[0]))+'\n'
        cp.write_text(prefix);self.path.write_text(prefix+json.dumps(self.event(self.rows[0],self.rows[-1]))+'\n')
        self.assertTrue(run.checkpoint_proof(cp,self.path,self.rows[0],1)['recovered_byte_prefix_identical'])
        with self.assertRaises(AssertionError):run.checkpoint_proof(cp,self.path,self.rows[0],2)
        self.path.write_text(prefix.replace('"op": "c"','"op":"c"')+json.dumps(self.event(self.rows[0],self.rows[-1]))+'\n')
        with self.assertRaises(AssertionError):run.checkpoint_proof(cp,self.path,self.rows[0],1)
        self.path.write_text(prefix)
        with self.assertRaises(AssertionError):run.checkpoint_proof(cp,self.path,self.rows[0],1)
    def test_checkpoint_bag_value_rejected(self):
        cp=pathlib.Path(self.tmp.name)/'checkpoint';prefix=json.dumps(self.event(None,self.rows[1]))+'\n'
        cp.write_text(prefix);self.path.write_text(prefix+json.dumps(self.event(self.rows[1],self.rows[-1]))+'\n')
        with self.assertRaises(AssertionError):run.checkpoint_proof(cp,self.path,self.rows[0],1)
    def test_clean_config_native_selection_and_resource_bounds(self):
        e=run.environment(pathlib.Path('/absolute'), 'rocksdb','leader',8)
        self.assertNotIn('STREAMR_TEST_NATIVE_AGGREGATES',e);self.assertEqual(e['ARROYO__WORKER__AGGREGATE_STATE__OVERLAY_BYTES'],'1572864');self.assertEqual(e['STREAMR_TEST_QUEUED_WRITE_BYTES'],'100663296');self.assertEqual(e['STREAMR_TEST_MAX_SNAPSHOTS'],'8')
if __name__=='__main__':unittest.main()
