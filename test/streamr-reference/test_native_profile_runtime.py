"""Strict generic staged append-oracle checks; no engine execution is mocked."""
import unittest
import native_profile_runtime as r


class ProfileOracleTest(unittest.TestCase):
    keys = ['tenant_id','canonical_id']

    def row(self, value, tenant='a'):
        return dict(tenant_id=tenant,canonical_id='person',total_events=value)

    def test_stage_plan_keeps_late_no_output_and_quiet_two_key_updates(self):
        a,b,c=self.row(1),self.row(2),self.row(1,'b')
        stages=[dict(input_lines=1,values=[a]),dict(input_lines=2,values=[a]),
                dict(input_lines=3,values=[a,b,c])]
        r.validate_stages(stages,3,1)
        r.check_snapshots([a,c,b],[a,b,c],self.keys)

    def test_diagnostic_empty_prefixes_do_not_claim_recovery(self):
        stages=[dict(input_lines=1,values=[]),dict(input_lines=2,values=[]),
                dict(input_lines=3,values=[self.row(1)])]
        r.validate_stages(stages,3,1,recovery=False)
        with self.assertRaises(ValueError):
            r.validate_stages(stages,3,1)

    def test_every_wrong_extra_duplicate_missing_snapshot_is_rejected(self):
        a,b=self.row(1),self.row(2)
        for actual in ([a,a,b],[b,a],[a], [dict(a,extra=1),b], [a,dict(b,total_events=3)]):
            with self.subTest(actual=actual),self.assertRaises(ValueError):
                r.check_snapshots(actual,[a,b],self.keys)

    def test_partial_observation_may_only_be_valid_per_key_prefix(self):
        a,b,c=self.row(1),self.row(2),self.row(1,'b')
        self.assertFalse(r.check_snapshots([c,a],[a,b,c],self.keys,partial=True))
        self.assertTrue(r.check_snapshots([a,c,b],[a,b,c],self.keys,partial=True))
        with self.assertRaises(ValueError):
            r.check_snapshots([b],[a,b],self.keys,partial=True)

    def test_invalid_stages_cannot_rewrite_or_omit_recovery_suffix(self):
        a,b=self.row(1),self.row(2)
        for stages in ([dict(input_lines=1,values=[a]),dict(input_lines=2,values=[b])],
                       [dict(input_lines=1,values=[a]),dict(input_lines=2,values=[a])],
                       [dict(input_lines=1,rows=[a]),dict(input_lines=2,rows=[b])]):
            with self.subTest(stages=stages),self.assertRaises(ValueError):
                r.validate_stages(stages,2,1)


class FiniteCaptureTest(unittest.TestCase):
    def test_transaction_control_offsets_are_retained_but_not_app_rows(self):
        payload = b'{"tenant_id":"a","canonical_id":"p","value":1}'.hex()
        rows, records = r.parse_capture('0 0 0 '+payload+'\n0 1 1 00000001\n', 2)
        self.assertEqual(rows, [dict(tenant_id='a',canonical_id='p',value=1)])
        self.assertEqual([x['offset'] for x in records], [0,1])
        self.assertTrue(records[-1]['control'])

    def test_empty_broker_boundary_and_truncated_nonempty_capture(self):
        self.assertEqual(r.parse_capture('',0),([],[]))
        with self.assertRaises(ValueError):
            r.parse_capture('',1)
        with self.assertRaises(ValueError):
            r.parse_capture('0 1 1 00\n',3)

    def test_invalid_transport_offsets_and_payloads_rejected(self):
        for data in ('1 0 1 00\n','0 0 1 00\n0 0 1 00\n','0 0 2 00\n','0 0 0 ff\n'):
            with self.subTest(data=data), self.assertRaises((ValueError,UnicodeError)):
                r.parse_capture(data,1)


if __name__=='__main__': unittest.main()
