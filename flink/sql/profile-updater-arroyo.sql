-- ARC-3: Profile Updater pipeline in Arroyo using stateful SQL primitives.
-- Replaces the Flink ProfileUpdaterJob / ProfileFunction.java read-modify-write.
--
-- The fork's JSON SQL functions are READ-ONLY, so all state mutation happens in
-- the runtime-compiled Rust UDF `profile_step` (flink/sql/udfs/profile_step.rs).
-- This file mirrors the CTE/state shape of identity-resolution-arroyo.sql:
--   cte1: state_get the prior profile blob
--   cte2: profile_step the blob (read-modify-write) AND state_put it back, so
--         the persist is a side effect of the SAME row that flows downstream
--   final SELECT: mechanically extract the ~33 output columns from the
--         persisted blob.
--
-- Output topic: profile-updates (consumed by the Pinot `profiles` FULL-upsert
-- table whose time/comparison column is updated_at).
-- NOTE: no column is aliased `_timestamp` (reserved internal column, ARC-1).

CREATE TABLE unified_events (
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  event_time TEXT NOT NULL,
  canonical_id TEXT NOT NULL,
  anonymous_id TEXT,
  user_id TEXT,
  session_id TEXT,
  page_url TEXT,
  referrer TEXT,
  device_type TEXT,
  browser TEXT,
  os TEXT,
  country TEXT,
  feature_name TEXT,
  element_id TEXT,
  properties TEXT
) WITH (
  connector = 'kafka',
  topic = 'unified-events',
  bootstrap_servers = 'redpanda:9092',
  format = 'json',
  type = 'source',
  'source.offset' = 'earliest'
);

CREATE TABLE profile_updates (
  canonical_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  user_id TEXT,
  first_seen BIGINT,
  last_seen BIGINT,
  updated_at BIGINT,
  total_events BIGINT,
  total_sessions BIGINT,
  events_1d BIGINT,
  events_7d BIGINT,
  events_30d BIGINT,
  events_90d BIGINT,
  sessions_1d BIGINT,
  sessions_7d BIGINT,
  sessions_30d BIGINT,
  sessions_90d BIGINT,
  avg_session_duration_sec BIGINT,
  current_session_active BOOLEAN,
  current_session_duration_sec BIGINT,
  page_views BIGINT,
  clicks BIGINT,
  logins BIGINT,
  feature_uses BIGINT,
  last_page TEXT,
  last_country TEXT,
  last_device TEXT,
  last_browser TEXT,
  top_pages TEXT,
  top_features TEXT,
  action TEXT,
  changed_fields TEXT,
  event_time TEXT,
  trigger TEXT
) WITH (
  connector = 'kafka',
  topic = 'profile-updates',
  bootstrap_servers = 'redpanda:9092',
  format = 'json',
  type = 'sink'
);

INSERT INTO profile_updates
WITH cte1 AS (
  SELECT
    *,
    state_get('profiles', canonical_id) AS old_blob
  FROM unified_events
),
cte2 AS (
  SELECT
    canonical_id,
    tenant_id,
    event_time,
    -- state_put returns the stored value, so wrapping profile_step's output in
    -- state_put both PERSISTS the new blob (side effect, every row) and yields
    -- the persisted blob for downstream extraction. Mirrors how
    -- identity-resolution-arroyo.sql aliases state_put as a flowing column.
    state_put(
      'profiles',
      canonical_id,
      profile_step(
        old_blob,
        event_type,
        event_time,
        user_id,
        session_id,
        page_url,
        device_type,
        browser,
        country,
        feature_name
      )
    ) AS stored_blob
  FROM cte1
)
SELECT
  canonical_id,
  tenant_id,
  extract_json_string(stored_blob, '$.user_id') AS user_id,
  CAST(extract_json_string(stored_blob, '$.first_seen') AS BIGINT) AS first_seen,
  CAST(extract_json_string(stored_blob, '$.last_seen') AS BIGINT) AS last_seen,
  CAST(extract_json_string(stored_blob, '$.updated_at') AS BIGINT) AS updated_at,
  CAST(extract_json_string(stored_blob, '$.total_events') AS BIGINT) AS total_events,
  CAST(extract_json_string(stored_blob, '$.total_sessions') AS BIGINT) AS total_sessions,
  CAST(extract_json_string(stored_blob, '$.events_1d') AS BIGINT) AS events_1d,
  CAST(extract_json_string(stored_blob, '$.events_7d') AS BIGINT) AS events_7d,
  CAST(extract_json_string(stored_blob, '$.events_30d') AS BIGINT) AS events_30d,
  CAST(extract_json_string(stored_blob, '$.events_90d') AS BIGINT) AS events_90d,
  CAST(extract_json_string(stored_blob, '$.sessions_1d') AS BIGINT) AS sessions_1d,
  CAST(extract_json_string(stored_blob, '$.sessions_7d') AS BIGINT) AS sessions_7d,
  CAST(extract_json_string(stored_blob, '$.sessions_30d') AS BIGINT) AS sessions_30d,
  CAST(extract_json_string(stored_blob, '$.sessions_90d') AS BIGINT) AS sessions_90d,
  CAST(extract_json_string(stored_blob, '$.avg_session_duration_sec') AS BIGINT) AS avg_session_duration_sec,
  CAST(extract_json_string(stored_blob, '$.current_session_active') AS BOOLEAN) AS current_session_active,
  CAST(extract_json_string(stored_blob, '$.current_session_duration_sec') AS BIGINT) AS current_session_duration_sec,
  CAST(extract_json_string(stored_blob, '$.page_views') AS BIGINT) AS page_views,
  CAST(extract_json_string(stored_blob, '$.clicks') AS BIGINT) AS clicks,
  CAST(extract_json_string(stored_blob, '$.logins') AS BIGINT) AS logins,
  CAST(extract_json_string(stored_blob, '$.feature_uses') AS BIGINT) AS feature_uses,
  extract_json_string(stored_blob, '$.last_page') AS last_page,
  extract_json_string(stored_blob, '$.last_country') AS last_country,
  extract_json_string(stored_blob, '$.last_device') AS last_device,
  extract_json_string(stored_blob, '$.last_browser') AS last_browser,
  extract_json_string(stored_blob, '$.top_pages') AS top_pages,
  extract_json_string(stored_blob, '$.top_features') AS top_features,
  extract_json_string(stored_blob, '$.action') AS action,
  extract_json_string(stored_blob, '$.changed_fields') AS changed_fields,
  event_time,
  extract_json_string(stored_blob, '$.trigger') AS trigger
FROM cte2;
