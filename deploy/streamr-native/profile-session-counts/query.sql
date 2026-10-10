--25-field native candidate only; profile timeout/reset not implemented or qualified.
-- Existing-SQL candidate only: updating UNION metadata and outer scalar retractions unverified.
-- Proposal until planner/runtime execution; no full33-field profile claim.
SET updating_ttl = NULL;
CREATE TABLE profile_events (
  event_ts TIMESTAMP NOT NULL,
  tenant_id TEXT NOT NULL, canonical_id TEXT NOT NULL,
  event_ms BIGINT NOT NULL,
  event_type TEXT NOT NULL, user_id TEXT, session_id TEXT,
  page_url TEXT, country TEXT, device_type TEXT, browser TEXT,
  feature_name TEXT, arrival_seq BIGINT NOT NULL,
  WATERMARK FOR event_ts AS event_ts
) WITH (connector = 'single_file', path = '{{INPUT}}', format = 'json',
        type = 'source', wait_for_control = 'true');

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
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) = WATERMARK_DATE()) AS events_1d,
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '6' DAY AND WATERMARK_DATE()) AS events_7d,
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '29' DAY AND WATERMARK_DATE()) AS events_30d,
  COUNT(*) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '89' DAY AND WATERMARK_DATE()) AS events_90d
FROM profile_events GROUP BY tenant_id, canonical_id;


CREATE VIEW page_counts AS SELECT tenant_id,canonical_id,page_url,COUNT(*) AS uses FROM profile_events WHERE page_url IS NOT NULL AND page_url <> '' GROUP BY tenant_id,canonical_id,page_url;
CREATE VIEW feature_counts AS SELECT tenant_id,canonical_id,feature_name,COUNT(*) AS uses FROM profile_events WHERE feature_name IS NOT NULL AND feature_name <> '' GROUP BY tenant_id,canonical_id,feature_name;
CREATE STATE TABLE last_session (
  tenant_id TEXT, canonical_id TEXT, last_nonempty_session_id TEXT NOT NULL,
  PRIMARY KEY (tenant_id, canonical_id)
) PARTITION BY tenant_id;

CREATE VIEW session_applied AS MERGE INTO last_session AS target
USING profile_events AS source
ON target.tenant_id = source.tenant_id
 AND target.canonical_id = source.canonical_id
WHEN MATCHED AND source.session_id IS NOT NULL AND source.session_id <> ''
 AND target.last_nonempty_session_id <> source.session_id
THEN UPDATE SET last_nonempty_session_id = source.session_id
WHEN NOT MATCHED AND source.session_id IS NOT NULL AND source.session_id <> ''
THEN INSERT (tenant_id, canonical_id, last_nonempty_session_id)
VALUES (source.tenant_id, source.canonical_id, source.session_id)
RETURNING source AS source, old AS old, new AS new, action AS action;

CREATE VIEW session_flags AS
SELECT r.source.tenant_id AS tenant_id,
       r.source.canonical_id AS canonical_id,
       r.source.arrival_seq AS arrival_seq, r.source.event_ts AS event_ts,
       r.source.session_id AS session_id,
       r.old.last_nonempty_session_id AS previous_session_id,
       CASE WHEN r.action = 'insert' OR r.action = 'update'
            THEN CAST(1 AS BIGINT) ELSE CAST(0 AS BIGINT) END
            AS session_started
FROM session_applied AS r;


CREATE VIEW session_counts AS SELECT tenant_id,canonical_id, SUM(session_started) AS total_sessions,
SUM(session_started) FILTER (WHERE CAST(event_ts AS DATE) = WATERMARK_DATE()) AS sessions_1d,
SUM(session_started) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '6' DAY AND WATERMARK_DATE()) AS sessions_7d,
SUM(session_started) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '29' DAY AND WATERMARK_DATE()) AS sessions_30d,
SUM(session_started) FILTER (WHERE CAST(event_ts AS DATE) BETWEEN WATERMARK_DATE() - INTERVAL '89' DAY AND WATERMARK_DATE()) AS sessions_90d FROM session_flags GROUP BY tenant_id,canonical_id;
CREATE VIEW tagged AS
SELECT tenant_id,canonical_id,CAST('core' AS TEXT) AS kind, first_seen, last_seen, total_events, page_views, clicks, logins, feature_uses, user_id, last_page, last_country, last_device, last_browser, events_1d, events_7d, events_30d, events_90d, CAST(NULL AS BIGINT) AS total_sessions, CAST(NULL AS BIGINT) AS sessions_1d, CAST(NULL AS BIGINT) AS sessions_7d, CAST(NULL AS BIGINT) AS sessions_30d, CAST(NULL AS BIGINT) AS sessions_90d,CAST(NULL AS TEXT) AS item,CAST(NULL AS BIGINT) AS uses FROM profile_lifetime
UNION ALL
SELECT tenant_id,canonical_id,CAST('session' AS TEXT) AS kind, CAST(NULL AS BIGINT) AS first_seen, CAST(NULL AS BIGINT) AS last_seen, CAST(NULL AS BIGINT) AS total_events, CAST(NULL AS BIGINT) AS page_views, CAST(NULL AS BIGINT) AS clicks, CAST(NULL AS BIGINT) AS logins, CAST(NULL AS BIGINT) AS feature_uses, CAST(NULL AS TEXT) AS user_id, CAST(NULL AS TEXT) AS last_page, CAST(NULL AS TEXT) AS last_country, CAST(NULL AS TEXT) AS last_device, CAST(NULL AS TEXT) AS last_browser, CAST(NULL AS BIGINT) AS events_1d, CAST(NULL AS BIGINT) AS events_7d, CAST(NULL AS BIGINT) AS events_30d, CAST(NULL AS BIGINT) AS events_90d, total_sessions, sessions_1d, sessions_7d, sessions_30d, sessions_90d,CAST(NULL AS TEXT) AS item,CAST(NULL AS BIGINT) AS uses FROM session_counts
UNION ALL
SELECT tenant_id,canonical_id,CAST('page' AS TEXT) AS kind, CAST(NULL AS BIGINT) AS first_seen, CAST(NULL AS BIGINT) AS last_seen, CAST(NULL AS BIGINT) AS total_events, CAST(NULL AS BIGINT) AS page_views, CAST(NULL AS BIGINT) AS clicks, CAST(NULL AS BIGINT) AS logins, CAST(NULL AS BIGINT) AS feature_uses, CAST(NULL AS TEXT) AS user_id, CAST(NULL AS TEXT) AS last_page, CAST(NULL AS TEXT) AS last_country, CAST(NULL AS TEXT) AS last_device, CAST(NULL AS TEXT) AS last_browser, CAST(NULL AS BIGINT) AS events_1d, CAST(NULL AS BIGINT) AS events_7d, CAST(NULL AS BIGINT) AS events_30d, CAST(NULL AS BIGINT) AS events_90d, CAST(NULL AS BIGINT) AS total_sessions, CAST(NULL AS BIGINT) AS sessions_1d, CAST(NULL AS BIGINT) AS sessions_7d, CAST(NULL AS BIGINT) AS sessions_30d, CAST(NULL AS BIGINT) AS sessions_90d,page_url AS item,uses FROM page_counts
UNION ALL
SELECT tenant_id,canonical_id,CAST('feature' AS TEXT) AS kind, CAST(NULL AS BIGINT) AS first_seen, CAST(NULL AS BIGINT) AS last_seen, CAST(NULL AS BIGINT) AS total_events, CAST(NULL AS BIGINT) AS page_views, CAST(NULL AS BIGINT) AS clicks, CAST(NULL AS BIGINT) AS logins, CAST(NULL AS BIGINT) AS feature_uses, CAST(NULL AS TEXT) AS user_id, CAST(NULL AS TEXT) AS last_page, CAST(NULL AS TEXT) AS last_country, CAST(NULL AS TEXT) AS last_device, CAST(NULL AS TEXT) AS last_browser, CAST(NULL AS BIGINT) AS events_1d, CAST(NULL AS BIGINT) AS events_7d, CAST(NULL AS BIGINT) AS events_30d, CAST(NULL AS BIGINT) AS events_90d, CAST(NULL AS BIGINT) AS total_sessions, CAST(NULL AS BIGINT) AS sessions_1d, CAST(NULL AS BIGINT) AS sessions_7d, CAST(NULL AS BIGINT) AS sessions_30d, CAST(NULL AS BIGINT) AS sessions_90d,feature_name AS item,uses FROM feature_counts;
CREATE TABLE profile_output(tenant_id TEXT,canonical_id TEXT,first_seen BIGINT, last_seen BIGINT, total_events BIGINT, page_views BIGINT, clicks BIGINT, logins BIGINT, feature_uses BIGINT, user_id TEXT, last_page TEXT, last_country TEXT, last_device TEXT, last_browser TEXT, events_1d BIGINT, events_7d BIGINT, events_30d BIGINT, events_90d BIGINT, total_sessions BIGINT, sessions_1d BIGINT, sessions_7d BIGINT, sessions_30d BIGINT, sessions_90d BIGINT,top_pages TEXT[],top_features TEXT[]) WITH(connector='single_file',path='{{OUTPUT}}',format='debezium_json',type='sink');
INSERT INTO profile_output SELECT tenant_id,canonical_id,
FIRST_VALUE(first_seen ORDER BY total_events ASC) FILTER(WHERE kind='core') AS first_seen,
FIRST_VALUE(last_seen ORDER BY total_events ASC) FILTER(WHERE kind='core') AS last_seen,
FIRST_VALUE(total_events ORDER BY total_events ASC) FILTER(WHERE kind='core') AS total_events,
FIRST_VALUE(page_views ORDER BY total_events ASC) FILTER(WHERE kind='core') AS page_views,
FIRST_VALUE(clicks ORDER BY total_events ASC) FILTER(WHERE kind='core') AS clicks,
FIRST_VALUE(logins ORDER BY total_events ASC) FILTER(WHERE kind='core') AS logins,
FIRST_VALUE(feature_uses ORDER BY total_events ASC) FILTER(WHERE kind='core') AS feature_uses,
FIRST_VALUE(user_id ORDER BY total_events ASC) FILTER(WHERE kind='core') AS user_id,
FIRST_VALUE(last_page ORDER BY total_events ASC) FILTER(WHERE kind='core') AS last_page,
FIRST_VALUE(last_country ORDER BY total_events ASC) FILTER(WHERE kind='core') AS last_country,
FIRST_VALUE(last_device ORDER BY total_events ASC) FILTER(WHERE kind='core') AS last_device,
FIRST_VALUE(last_browser ORDER BY total_events ASC) FILTER(WHERE kind='core') AS last_browser,
FIRST_VALUE(events_1d ORDER BY total_events ASC) FILTER(WHERE kind='core') AS events_1d,
FIRST_VALUE(events_7d ORDER BY total_events ASC) FILTER(WHERE kind='core') AS events_7d,
FIRST_VALUE(events_30d ORDER BY total_events ASC) FILTER(WHERE kind='core') AS events_30d,
FIRST_VALUE(events_90d ORDER BY total_events ASC) FILTER(WHERE kind='core') AS events_90d,
COALESCE(FIRST_VALUE(total_sessions ORDER BY total_sessions ASC) FILTER(WHERE kind='session'),CAST(0 AS BIGINT)) AS total_sessions,
COALESCE(FIRST_VALUE(sessions_1d ORDER BY total_sessions ASC) FILTER(WHERE kind='session'),CAST(0 AS BIGINT)) AS sessions_1d,
COALESCE(FIRST_VALUE(sessions_7d ORDER BY total_sessions ASC) FILTER(WHERE kind='session'),CAST(0 AS BIGINT)) AS sessions_7d,
COALESCE(FIRST_VALUE(sessions_30d ORDER BY total_sessions ASC) FILTER(WHERE kind='session'),CAST(0 AS BIGINT)) AS sessions_30d,
COALESCE(FIRST_VALUE(sessions_90d ORDER BY total_sessions ASC) FILTER(WHERE kind='session'),CAST(0 AS BIGINT)) AS sessions_90d,
COALESCE(array_slice(ARRAY_AGG(item ORDER BY uses DESC,item ASC) FILTER(WHERE kind='page'),1,5),CAST(ARRAY[] AS TEXT[])) AS top_pages,
COALESCE(array_slice(ARRAY_AGG(item ORDER BY uses DESC,item ASC) FILTER(WHERE kind='feature'),1,3),CAST(ARRAY[] AS TEXT[])) AS top_features FROM tagged GROUP BY tenant_id,canonical_id;
