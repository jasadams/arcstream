import importlib.util
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location('kafka_fixture', Path(__file__).with_name('native_kafka_fixture.py'))
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ConnectorTemplateTests(unittest.TestCase):
    def test_actual_closed_metric_query_binds_owned_topic_and_keeps_output_for_runner(self):
        query = (ROOT/'deploy/streamr-native/profile-native/closed-metrics.sql').read_text()
        result = module.render_connector_template(query, 'broker:9092', 'owned-topic', 'owned-reader')
        self.assertNotIn('{{SOURCE_OPTIONS}}', result)
        self.assertNotIn('{{SINK_OPTIONS}}', result)
        self.assertIn("topic='owned-topic'", result)
        self.assertIn("'source.group_id'='owned-reader'", result)
        self.assertIn("path='{{OUTPUT}}',format='debezium_json'", result)
        self.assertIn("INTERVAL '5 seconds'", result)
        self.assertIn('FIRST_VALUE(canonical_id)', result)

    def test_partial_template_is_rejected(self):
        with self.assertRaises(ValueError):
            module.render_connector_template('WITH({{SOURCE_OPTIONS}})', 'broker', 'topic', 'reader')


if __name__ == '__main__':
    unittest.main()
