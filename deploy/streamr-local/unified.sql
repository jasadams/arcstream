-- Stateless local fan-out. Submit with API parallelism = 1.
-- Its independent consumer group reads every committed identity-owner row.
CREATE TABLE identity_capture (
  event_id TEXT NOT NULL, event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL, event_time TEXT NOT NULL,
  canonical_id TEXT NOT NULL, anonymous_id TEXT NOT NULL,
  user_id TEXT, session_id TEXT, page_url TEXT, referrer TEXT,
  element_id TEXT, feature_name TEXT, device_type TEXT, browser TEXT,
  os TEXT, country TEXT, properties TEXT, merge_old TEXT, user_binding TEXT
) WITH (
  connector = 'kafka', bootstrap_servers = 'broker:9092',
  topic = 'arc-eval-identity-capture', format = 'json', type = 'source',
  'source.offset' = 'earliest', 'source.group_id' = 'arc-eval-identity-unified',
  'source.read_mode' = 'read_committed'
);

CREATE TABLE unified_events (
  event_id TEXT NOT NULL, event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL, event_time TEXT NOT NULL,
  canonical_id TEXT NOT NULL, anonymous_id TEXT NOT NULL,
  user_id TEXT, session_id TEXT, page_url TEXT, referrer TEXT,
  element_id TEXT, feature_name TEXT, device_type TEXT, browser TEXT,
  os TEXT, country TEXT, properties TEXT
) WITH (
  connector = 'kafka', bootstrap_servers = 'broker:9092',
  topic = 'arc-eval-unified-events', format = 'json', type = 'sink',
  'sink.commit_mode' = 'exactly_once'
);

INSERT INTO unified_events
SELECT event_id, event_type, tenant_id, event_time, canonical_id,
  anonymous_id, user_id, session_id, page_url, referrer, element_id,
  feature_name, device_type, browser, os, country, properties
FROM identity_capture;
