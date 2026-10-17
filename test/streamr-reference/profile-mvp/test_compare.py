import unittest
from copy import deepcopy
from compare import compare, chains
from fixture import fixture, live_fixture, boundary_fixture, feedback_fixture


class ProfileOracleTests(unittest.TestCase):
    def test_hand_counted_fixtures_have_complete_cdc_chains(self):
        for build, count in [(fixture,15),(live_fixture,4),(boundary_fixture,4),(feedback_fixture,5)]:
            data = build()
            self.assertEqual(compare(data['expected'],deepcopy(data['expected']))['rows'],count)
            self.assertEqual(len(data['timer_seeds']),4*sum(s['kind']=='event' for s in data['inputs']))

    def test_rejects_boolean_for_integer_and_broken_previous(self):
        expected = fixture()['expected']
        actual = deepcopy(expected)
        actual[0]['after']['total_events'] = True
        with self.assertRaises(AssertionError):
            compare(expected,actual)
        actual = deepcopy(expected)
        actual[1]['before'] = None
        with self.assertRaises(AssertionError):
            chains(actual)

    def test_rejects_missing_or_duplicate_output(self):
        expected = fixture()['expected']
        with self.assertRaises(AssertionError):
            compare(expected,expected[:-1])
        with self.assertRaises(AssertionError):
            compare(expected,expected+[expected[-1]])

    def test_rejects_observation_clock_as_replay_business_time(self):
        expected = fixture()['expected']
        actual = deepcopy(expected)
        actual[0]['after']['first_seen'] = actual[0]['after']['updated_at']
        with self.assertRaises(AssertionError):
            compare(expected,actual)


if __name__ == '__main__':
    unittest.main()
