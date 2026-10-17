from copy import deepcopy
import unittest
from compare import compare,pairs
from fixture import fixture

class SessionOracleTests(unittest.TestCase):
 def setUp(self):self.expected=fixture()['expected']
 def test_complete_source_bound_fixture_passes(self):
  self.assertEqual(compare(self.expected,deepcopy(self.expected))['rows'],5)
 def test_missing_extra_and_duplicate_records_fail(self):
  for actual in [self.expected[:-1],self.expected+[self.expected[0]],list(reversed(self.expected))]:
   with self.assertRaises(ValueError):compare(self.expected,actual)
 def test_malformed_schema_types_and_counts_fail(self):
  for field,value in [('pages',['/same','/same']),('event_count',True),('event_types',{'custom_event':True}),('duration_sec',1.0),('start_time','2020-01-31 23:59:00'),('country',None)]:
   actual=deepcopy(self.expected);actual[0][field]=value
   with self.assertRaises(ValueError):compare(self.expected,actual)
  for key in ['canonical_id','event_types']:
   actual=deepcopy(self.expected);del actual[0][key]
   with self.assertRaises(ValueError):compare(self.expected,actual)
  actual=deepcopy(self.expected);actual[0]['engine_internal']='unexpected'
  with self.assertRaises(ValueError):compare(self.expected,actual)
 def test_first_identity_and_raw_late_boundaries_cannot_drift(self):
  for field,value in [('canonical_id','later-a'),('start_time','2020-01-31 23:58:30.000'),('end_time','2020-02-01 00:02:00.000'),('event_types',{'page_view':3}),('country','AU')]:
   actual=deepcopy(self.expected);actual[0][field]=value
   with self.assertRaises(ValueError):compare(self.expected,actual)
 def test_json_duplicate_fields_are_rejected(self):
  with self.assertRaises(ValueError):pairs([('event_count',1),('event_count',2)])
if __name__=='__main__':unittest.main()
