import importlib.util
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import tempfile
import unittest

spec = importlib.util.spec_from_file_location('profile_fixture_prepare', Path(__file__).with_name('prepare.py'))
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class ProfileFixtureTests(unittest.TestCase):
    def test_raw_payload_and_exact_partial_schema(self):
        fields = json.loads((Path(__file__).resolve().parents[3] /
            'deploy/streamr-native/profile-native/fields.json').read_text())
        expected = {field['name'] for field in fields['output_fields']}
        for event in p.INPUT:
            self.assertTrue({'event_ts', 'event_ms', 'arrival_seq'}.isdisjoint(event))
            datetime.fromisoformat(event['event_time'])
        for case in p.CASES:
            for row in case['rows']:
                self.assertEqual(set(row), expected)
                self.assertEqual(len(row), 20)

    def test_midnight_numeric_calendar_oracle(self):
        # Independent arithmetic for the admitted earlier trigger, including
        # excluding the already observed future-day contribution.
        times = [datetime.fromisoformat(e['event_time']) for e in p.INPUT]
        self.assertEqual(times[0] - times[1], timedelta(seconds=3))
        self.assertGreater(times[1], times[0] - timedelta(seconds=5))
        self.assertEqual(sum(t.date() == times[1].date() for t in times[:2]), 1)
        self.assertEqual(p.A2['events_1d'], 1)
        self.assertEqual(p.A2['events_7d'], 1)
        # First event's AS watermark is still Jan31, but trigger is Feb1.
        self.assertNotEqual(times[0].date(), (times[0] - timedelta(seconds=5)).date())
        self.assertEqual(p.A1['events_1d'], 1)
        self.assertEqual(p.A2['first_seen'], int(times[1].replace(tzinfo=timezone.utc).timestamp() * 1000))

    def test_selector_order_ranking_and_late_rejection(self):
        self.assertEqual(p.A2['last_device'], 'desktop')
        self.assertEqual(p.A3['last_device'], 'desktop')
        self.assertEqual(p.A4['last_device'], 'desktop')
        self.assertEqual(p.A5['last_device'], 'tablet')
        self.assertEqual(p.A4['top_pages'], ['/a', '/b'])
        self.assertEqual(p.A5['top_pages'], ['/b', '/a'])
        self.assertEqual(p.CASES[4]['rows'], p.CASES[5]['rows'])
        late = datetime.fromisoformat(p.INPUT[5]['event_time'])
        maximum = max(datetime.fromisoformat(e['event_time']) for e in p.INPUT[:5])
        self.assertLess(late, maximum - timedelta(seconds=5))

    def test_quiet_calendar_zero_retains_lifetime_and_tenant(self):
        for name in p.A5:
            if name != 'events_1d':
                self.assertEqual(p.A5[name], p.A_QUIET[name])
        self.assertEqual(p.A_QUIET['events_1d'], 0)
        self.assertEqual(p.A_QUIET['total_events'], 5)
        self.assertEqual(p.B1['total_events'], 1)
        self.assertEqual(p.A_QUIET['canonical_id'], p.B1['canonical_id'])
        self.assertNotEqual(p.A_QUIET['tenant_id'], p.B1['tenant_id'])

    def test_deterministic_artifacts_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.assertEqual(p.prepare(root / 'one'), p.prepare(root / 'two'))
            self.assertEqual((root / 'one/input.jsonl').read_bytes(),
                             (root / 'two/input.jsonl').read_bytes())
            with self.assertRaises(FileExistsError):
                p.prepare(root / 'one')


if __name__ == '__main__':
    unittest.main()
