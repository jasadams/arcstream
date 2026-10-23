-- First-release native core relation LEFT JOIN completed-session average.
-- CDC carries native create/update/delete metadata; no application clock/UDF.
SET updating_ttl = NULL;
CREATE TABLE profile_events (
  event_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL, canonical_id TEXT NOT NULL,
  event_time TEXT NOT NULL,
  event_type TEXT NOT NULL, user_id TEXT, session_id TEXT,
  page_url TEXT, country TEXT, device_type TEXT, browser TEXT,
  feature_name TEXT,
  event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
  event_ms BIGINT GENERATED ALWAYS AS
    (CAST(CAST(event_time AS TIMESTAMP(3)) AS BIGINT)) STORED,
  WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH ({{SOURCE_OPTIONS}});

CREATE VIEW profile_lifetime AS
SELECT tenant_id, canonical_id,
  FIRST_VALUE(event_ms) AS first_seen,
  MAX(event_ms) AS last_seen,
  COUNT(*) AS total_events,
  COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_views,
  COUNT(*) FILTER (WHERE event_type = 'click') AS clicks,
  COUNT(*) FILTER (WHERE event_type = 'login') AS logins,
  COUNT(*) FILTER (WHERE event_type = 'feature_used') AS feature_uses,
  COALESCE(LAST_VALUE(user_id)
    FILTER (WHERE user_id IS NOT NULL AND user_id <> ''), '') AS user_id,
  COALESCE(LAST_VALUE(page_url)
    FILTER (WHERE page_url IS NOT NULL AND page_url <> ''), '') AS last_page,
  COALESCE(LAST_VALUE(country)
    FILTER (WHERE country IS NOT NULL AND country <> ''), '') AS last_country,
  COALESCE(LAST_VALUE(device_type)
    FILTER (WHERE device_type IS NOT NULL AND device_type <> ''), '') AS last_device,
  COALESCE(LAST_VALUE(browser)
    FILTER (WHERE browser IS NOT NULL AND browser <> ''), '') AS last_browser
,
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) = EVENT_DATE()) AS events_1d,
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN EVENT_DATE() - INTERVAL '6' DAY AND EVENT_DATE()) AS events_7d,
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN EVENT_DATE() - INTERVAL '29' DAY AND EVENT_DATE()) AS events_30d,
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN EVENT_DATE() - INTERVAL '89' DAY AND EVENT_DATE()) AS events_90d
FROM profile_events GROUP BY tenant_id, canonical_id;


-- SESSION membership and closing are owned by the native window operator.
CREATE VIEW profile_completed_sessions AS
SELECT tenant_id, session_id, SESSION(INTERVAL '30 minutes') AS window,
  FIRST_VALUE(canonical_id) AS canonical_id,
  FIRST_VALUE(event_ms) AS start_ms, MAX(event_ms) AS end_ms
FROM profile_events WHERE session_id IS NOT NULL AND session_id <> ''
GROUP BY tenant_id, session_id, window;
CREATE VIEW profile_closed_durations AS
SELECT tenant_id, canonical_id, end_ms-start_ms AS duration_ms
FROM profile_completed_sessions;
CREATE VIEW profile_closed_average AS
SELECT tenant_id, canonical_id,
  CAST(SUM(duration_ms) AS DOUBLE)/CAST(COUNT(*) AS DOUBLE)/1000.0
    AS avg_session_duration_sec
FROM profile_closed_durations GROUP BY tenant_id, canonical_id;

CREATE VIEW page_counts AS SELECT tenant_id,canonical_id,page_url,COUNT(*) AS uses FROM profile_events WHERE page_url IS NOT NULL AND page_url <> '' GROUP BY tenant_id,canonical_id,page_url;
CREATE VIEW feature_counts AS SELECT tenant_id,canonical_id,feature_name,COUNT(*) AS uses FROM profile_events WHERE feature_name IS NOT NULL AND feature_name <> '' GROUP BY tenant_id,canonical_id,feature_name;
CREATE VIEW tagged AS
SELECT tenant_id, canonical_id, CAST('core' AS TEXT) AS kind, first_seen, last_seen, total_events, page_views, clicks, logins, feature_uses, user_id, last_page, last_country, last_device, last_browser, events_1d, events_7d, events_30d, events_90d, CAST(NULL AS TEXT) AS item, CAST(NULL AS BIGINT) AS uses FROM profile_lifetime
UNION ALL
SELECT tenant_id, canonical_id, CAST('page' AS TEXT) AS kind, CAST(NULL AS BIGINT) AS first_seen, CAST(NULL AS BIGINT) AS last_seen, CAST(NULL AS BIGINT) AS total_events, CAST(NULL AS BIGINT) AS page_views, CAST(NULL AS BIGINT) AS clicks, CAST(NULL AS BIGINT) AS logins, CAST(NULL AS BIGINT) AS feature_uses, CAST(NULL AS TEXT) AS user_id, CAST(NULL AS TEXT) AS last_page, CAST(NULL AS TEXT) AS last_country, CAST(NULL AS TEXT) AS last_device, CAST(NULL AS TEXT) AS last_browser, CAST(NULL AS BIGINT) AS events_1d, CAST(NULL AS BIGINT) AS events_7d, CAST(NULL AS BIGINT) AS events_30d, CAST(NULL AS BIGINT) AS events_90d, page_url AS item, uses FROM page_counts
UNION ALL
SELECT tenant_id, canonical_id, CAST('feature' AS TEXT) AS kind, CAST(NULL AS BIGINT) AS first_seen, CAST(NULL AS BIGINT) AS last_seen, CAST(NULL AS BIGINT) AS total_events, CAST(NULL AS BIGINT) AS page_views, CAST(NULL AS BIGINT) AS clicks, CAST(NULL AS BIGINT) AS logins, CAST(NULL AS BIGINT) AS feature_uses, CAST(NULL AS TEXT) AS user_id, CAST(NULL AS TEXT) AS last_page, CAST(NULL AS TEXT) AS last_country, CAST(NULL AS TEXT) AS last_device, CAST(NULL AS TEXT) AS last_browser, CAST(NULL AS BIGINT) AS events_1d, CAST(NULL AS BIGINT) AS events_7d, CAST(NULL AS BIGINT) AS events_30d, CAST(NULL AS BIGINT) AS events_90d, feature_name AS item, uses FROM feature_counts;
CREATE VIEW profile_current AS
SELECT tenant_id, canonical_id,
  MAX(first_seen) FILTER (WHERE kind = 'core') AS first_seen,
  MAX(last_seen) FILTER (WHERE kind = 'core') AS last_seen,
  MAX(total_events) FILTER (WHERE kind = 'core') AS total_events,
  MAX(page_views) FILTER (WHERE kind = 'core') AS page_views,
  MAX(clicks) FILTER (WHERE kind = 'core') AS clicks,
  MAX(logins) FILTER (WHERE kind = 'core') AS logins,
  MAX(feature_uses) FILTER (WHERE kind = 'core') AS feature_uses,
  MAX(user_id) FILTER (WHERE kind = 'core') AS user_id,
  MAX(last_page) FILTER (WHERE kind = 'core') AS last_page,
  MAX(last_country) FILTER (WHERE kind = 'core') AS last_country,
  MAX(last_device) FILTER (WHERE kind = 'core') AS last_device,
  MAX(last_browser) FILTER (WHERE kind = 'core') AS last_browser,
  MAX(events_1d) FILTER (WHERE kind = 'core') AS events_1d,
  MAX(events_7d) FILTER (WHERE kind = 'core') AS events_7d,
  MAX(events_30d) FILTER (WHERE kind = 'core') AS events_30d,
  MAX(events_90d) FILTER (WHERE kind = 'core') AS events_90d,
  COALESCE(array_slice(ARRAY_AGG(item ORDER BY uses DESC, item ASC)
    FILTER (WHERE kind = 'page'), 1, 5), CAST(ARRAY[] AS TEXT[])) AS top_pages,
  COALESCE(array_slice(ARRAY_AGG(item ORDER BY uses DESC, item ASC)
    FILTER (WHERE kind = 'feature'), 1, 3), CAST(ARRAY[] AS TEXT[])) AS top_features
FROM tagged GROUP BY tenant_id, canonical_id;

-- Native updating LEFT JOIN preserves the core before any SESSION completes.
-- The right relation emits only completed-session averages; NULL means absent.
CREATE VIEW profile_release_current AS
SELECT core.*, closed.avg_session_duration_sec
FROM profile_current AS core
LEFT JOIN profile_closed_average AS closed
ON core.tenant_id = closed.tenant_id
 AND core.canonical_id = closed.canonical_id;

-- The sink is debezium_json. Standard CDC c/u/d describes relation changes;
-- the application transport adapter maps those operations to action labels.
CREATE TABLE profile_release_output (
  tenant_id TEXT,
  canonical_id TEXT,
  first_seen BIGINT,
  last_seen BIGINT,
  total_events BIGINT,
  page_views BIGINT,
  clicks BIGINT,
  logins BIGINT,
  feature_uses BIGINT,
  user_id TEXT,
  last_page TEXT,
  last_country TEXT,
  last_device TEXT,
  last_browser TEXT,
  events_1d BIGINT,
  events_7d BIGINT,
  events_30d BIGINT,
  events_90d BIGINT,
  top_pages TEXT[],
  top_features TEXT[],
  avg_session_duration_sec DOUBLE
) WITH ({{SINK_OPTIONS}});
INSERT INTO profile_release_output SELECT * FROM profile_release_current;
