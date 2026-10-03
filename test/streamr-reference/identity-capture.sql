-- Evaluation-only file capture. No canonical Kafka topics.
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

INSERT INTO identity_capture
WITH keyed AS (
  SELECT *, uuid() AS generated_id,
    NULLIF(user_id, '') AS normalized_user,
    concat(CAST(character_length(tenant_id) AS TEXT), ':', tenant_id, anonymous_id) AS anon_key,
    concat(CAST(character_length(tenant_id) AS TEXT), ':', tenant_id, user_id) AS user_key
  FROM raw_events
), looked_up AS (
  SELECT *, state_get('anon_map', anon_key) AS anon_before,
    CASE WHEN normalized_user IS NOT NULL THEN state_get('user_map', user_key)
      ELSE CAST(NULL AS TEXT) END AS user_before
  FROM keyed
), decided AS (
  SELECT *, COALESCE(user_before, anon_before, generated_id) AS chosen,
    CASE WHEN anon_before IS NOT NULL AND user_before IS NOT NULL
        AND anon_before <> user_before THEN anon_before
      ELSE CAST(NULL AS TEXT) END AS merge_old
  FROM looked_up
), written AS (
  SELECT *, state_put('anon_map', anon_key, chosen) AS canonical_id,
    CASE WHEN normalized_user IS NOT NULL AND user_before IS NULL
      THEN state_put('user_map', user_key, chosen)
      ELSE user_before END AS user_binding
  FROM decided
)
SELECT event_id, event_type, tenant_id, event_time, canonical_id,
  anonymous_id, user_id, session_id, page_url, referrer, element_id,
  feature_name, device_type, browser, os, country, properties,
  merge_old, user_binding
FROM written;
