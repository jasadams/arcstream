-- Stateless local fan-out. Submit with API parallelism = 1.
-- Directed merges match the current Flink IdentityMerge output exactly.
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
  'source.offset' = 'earliest', 'source.group_id' = 'arc-eval-identity-merges',
  'source.read_mode' = 'read_committed'
);

CREATE TABLE identity_merges (
  old_canonical_id TEXT NOT NULL, canonical_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL, merged_at TEXT NOT NULL
) WITH (
  connector = 'kafka', bootstrap_servers = 'broker:9092',
  topic = 'arc-eval-identity-merges', format = 'json', type = 'sink',
  'sink.commit_mode' = 'exactly_once'
);

INSERT INTO identity_merges
SELECT merge_old AS old_canonical_id, canonical_id, tenant_id,
  event_time AS merged_at
FROM identity_capture
WHERE merge_old IS NOT NULL;
