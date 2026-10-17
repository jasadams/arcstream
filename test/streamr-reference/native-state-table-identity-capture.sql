-- Application-owned identity evaluation through Streamr's native state tables.
-- Both state tables are partitioned by tenant, so their accesses share an owner.
CREATE TABLE raw_events (
  event_id TEXT NOT NULL, event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL, event_time TEXT NOT NULL,
  anonymous_id TEXT NOT NULL, user_id TEXT, session_id TEXT,
  page_url TEXT, referrer TEXT, element_id TEXT, feature_name TEXT,
  device_type TEXT, browser TEXT, os TEXT, country TEXT, properties TEXT
) WITH (
  connector = 'single_file', path = '$identity_input',
  format = 'json', type = 'source', wait_for_control = 'true'
);

CREATE TABLE identity_capture (
  event_id TEXT NOT NULL, event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL, event_time TEXT NOT NULL,
  canonical_id TEXT NOT NULL, anonymous_id TEXT NOT NULL,
  user_id TEXT, session_id TEXT, page_url TEXT, referrer TEXT,
  element_id TEXT, feature_name TEXT, device_type TEXT, browser TEXT,
  os TEXT, country TEXT, properties TEXT, merge_old TEXT, user_binding TEXT
) WITH (
  connector = 'single_file', path = '$identity_output',
  format = 'json', type = 'sink'
);

CREATE STATE TABLE anonymous_bindings (
  tenant_id TEXT, anonymous_id TEXT, canonical_id TEXT NOT NULL,
  PRIMARY KEY (tenant_id, anonymous_id)
) PARTITION BY tenant_id;
CREATE STATE TABLE user_bindings (
  tenant_id TEXT, user_id TEXT, canonical_id TEXT NOT NULL,
  PRIMARY KEY (tenant_id, user_id)
) PARTITION BY tenant_id;

-- Generate one candidate per input event in the projected producer. The fused
-- owner evaluates it once and carries the value through both dependent MERGEs.
-- A committed binding survives recovery; an uncommitted replay may generate a
-- fresh candidate, matching the existing Flink identity policy.
CREATE VIEW identity_candidates AS
SELECT e.event_id, e.event_type, e.tenant_id, e.event_time,
  e.anonymous_id, e.user_id, e.session_id, e.page_url, e.referrer,
  e.element_id, e.feature_name, e.device_type, e.browser, e.os,
  e.country, e.properties, uuid() AS candidate_id
FROM raw_events e;

-- Capture the anonymous value before either state-table write.
CREATE VIEW anonymous_before AS
SELECT e.event_id, e.event_type, e.tenant_id, e.event_time,
  e.anonymous_id, e.user_id, e.session_id, e.page_url, e.referrer,
  e.element_id, e.feature_name, e.device_type, e.browser, e.os,
  e.country, e.properties,
  e.candidate_id AS candidate_id,
  a.canonical_id AS prior_anonymous_id
FROM identity_candidates e LEFT JOIN anonymous_bindings a
ON a.tenant_id = e.tenant_id AND a.anonymous_id = e.anonymous_id;

-- A missing or empty user ID takes the no-action path and still returns source.
-- For an established user, MERGE's new row is the pre-existing binding.
CREATE VIEW user_applied AS MERGE INTO user_bindings AS target
USING anonymous_before AS source
ON target.tenant_id = source.tenant_id AND target.user_id = source.user_id
WHEN NOT MATCHED AND source.user_id IS NOT NULL AND source.user_id <> ''
THEN INSERT (tenant_id, user_id, canonical_id)
VALUES (source.tenant_id, source.user_id,
  COALESCE(source.prior_anonymous_id, source.candidate_id))
RETURNING source AS source, old AS old, new AS new, action AS action;

-- Keep the user binding from the first MERGE and the anonymous pre-write value.
-- User identity wins over a different anonymous identity, producing a directed
-- merge from the old anonymous ID to the established user ID.
CREATE VIEW anonymous_decision AS
SELECT r.source.event_id AS event_id, r.source.event_type AS event_type,
  r.source.tenant_id AS tenant_id, r.source.event_time AS event_time,
  r.source.anonymous_id AS anonymous_id, r.source.user_id AS user_id,
  r.source.session_id AS session_id, r.source.page_url AS page_url,
  r.source.referrer AS referrer, r.source.element_id AS element_id,
  r.source.feature_name AS feature_name, r.source.device_type AS device_type,
  r.source.browser AS browser, r.source.os AS os,
  r.source.country AS country, r.source.properties AS properties,
  r.source.prior_anonymous_id AS prior_anonymous_id,
  r.new.canonical_id AS user_binding,
  COALESCE(r.new.canonical_id, r.source.prior_anonymous_id,
    r.source.candidate_id) AS chosen_id
FROM user_applied r;

CREATE VIEW anonymous_applied AS MERGE INTO anonymous_bindings AS target
USING anonymous_decision AS source
ON target.tenant_id = source.tenant_id
  AND target.anonymous_id = source.anonymous_id
WHEN MATCHED AND target.canonical_id <> source.chosen_id
THEN UPDATE SET canonical_id = source.chosen_id
WHEN NOT MATCHED THEN INSERT (tenant_id, anonymous_id, canonical_id)
VALUES (source.tenant_id, source.anonymous_id, source.chosen_id)
RETURNING source AS source, old AS old, new AS new, action AS action;

INSERT INTO identity_capture
SELECT r.source.event_id, r.source.event_type, r.source.tenant_id,
  r.source.event_time, r.new.canonical_id AS canonical_id,
  r.source.anonymous_id, r.source.user_id, r.source.session_id,
  r.source.page_url, r.source.referrer, r.source.element_id,
  r.source.feature_name, r.source.device_type, r.source.browser,
  r.source.os, r.source.country, r.source.properties,
  CASE WHEN r.source.prior_anonymous_id IS NOT NULL
      AND r.source.user_binding IS NOT NULL
      AND r.source.prior_anonymous_id <> r.source.user_binding
    THEN r.source.prior_anonymous_id ELSE CAST(NULL AS TEXT) END AS merge_old,
  r.source.user_binding
FROM anonymous_applied r;
