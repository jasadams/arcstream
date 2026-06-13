-- ARC-2: Identity Resolution pipeline in Arroyo using stateful SQL primitives
-- Mirrors the logic in IdentityResolutionFunction.java (Flink DataStream API)
-- Output topic: unified-events-arroyo (separate from Flink's unified-events for comparison)

CREATE TABLE raw_events (
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  event_time TEXT NOT NULL,
  anonymous_id TEXT NOT NULL,
  user_id TEXT,
  session_id TEXT,
  page_url TEXT,
  referrer TEXT,
  device_type TEXT,
  browser TEXT,
  os TEXT,
  country TEXT,
  element_id TEXT,
  feature_name TEXT,
  properties TEXT
) WITH (
  connector = 'kafka',
  topic = 'raw-events',
  bootstrap_servers = 'redpanda:9092',
  format = 'json',
  type = 'source',
  'source.offset' = 'latest'
);

CREATE TABLE unified_events_arroyo (
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  event_time TEXT NOT NULL,
  canonical_id TEXT NOT NULL,
  session_id TEXT,
  page_url TEXT,
  referrer TEXT,
  device_type TEXT,
  browser TEXT,
  os TEXT,
  country TEXT,
  event_action TEXT
) WITH (
  connector = 'kafka',
  topic = 'unified-events-arroyo',
  bootstrap_servers = 'redpanda:9092',
  format = 'json',
  type = 'sink'
);

INSERT INTO unified_events_arroyo
WITH anon_resolved AS (
  SELECT
    event_id,
    event_type,
    tenant_id,
    event_time,
    anonymous_id,
    user_id,
    session_id,
    page_url,
    referrer,
    device_type,
    browser,
    os,
    country,
    state_upsert('anon_map', concat(tenant_id, ':', anonymous_id), uuid()) AS anon_canonical
  FROM raw_events
),
user_resolved AS (
  SELECT
    event_id,
    event_type,
    tenant_id,
    event_time,
    anonymous_id,
    user_id,
    session_id,
    page_url,
    referrer,
    device_type,
    browser,
    os,
    country,
    anon_canonical,
    CASE
      WHEN user_id IS NOT NULL AND user_id != ''
      THEN state_upsert('user_map', concat(tenant_id, ':', user_id), anon_canonical)
      ELSE NULL
    END AS user_canonical
  FROM anon_resolved
)
SELECT
  event_id,
  event_type,
  tenant_id,
  event_time,
  state_put('anon_map', concat(tenant_id, ':', anonymous_id),
    COALESCE(NULLIF(user_canonical, anon_canonical), anon_canonical)) AS canonical_id,
  session_id,
  page_url,
  referrer,
  device_type,
  browser,
  os,
  country,
  CASE WHEN user_canonical IS NOT NULL AND user_canonical != anon_canonical THEN 'merge' END AS event_action
FROM user_resolved;
