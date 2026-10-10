-- Proposal until planner/runtime execution; no full33-field profile claim.
SET updating_ttl = NULL;
CREATE TABLE profile_events (
  event_ts TIMESTAMP NOT NULL,
  tenant_id TEXT NOT NULL, canonical_id TEXT NOT NULL,
  event_ms BIGINT NOT NULL,
  event_type TEXT NOT NULL, user_id TEXT, session_id TEXT,
  page_url TEXT, country TEXT, device_type TEXT, browser TEXT,
  feature_name TEXT,
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


CREATE VIEW page_counts AS SELECT tenant_id,canonical_id,page_url,COUNT(*) FILTER (WHERE page_url IS NOT NULL AND page_url <> '') AS uses
FROM profile_events GROUP BY tenant_id,canonical_id,page_url;
CREATE VIEW page_tops AS SELECT tenant_id,canonical_id,
array_slice(ARRAY_AGG(page_url ORDER BY uses DESC,page_url ASC) FILTER (WHERE page_url IS NOT NULL AND page_url <> '' AND uses > 0),1,5) AS top_pages
FROM page_counts GROUP BY tenant_id,canonical_id;
CREATE VIEW feature_counts AS SELECT tenant_id,canonical_id,feature_name,COUNT(*) FILTER (WHERE feature_name IS NOT NULL AND feature_name <> '') AS uses
FROM profile_events GROUP BY tenant_id,canonical_id,feature_name;
CREATE VIEW feature_tops AS SELECT tenant_id,canonical_id,
array_slice(ARRAY_AGG(feature_name ORDER BY uses DESC,feature_name ASC) FILTER (WHERE feature_name IS NOT NULL AND feature_name <> '' AND uses > 0),1,3) AS top_features
FROM feature_counts GROUP BY tenant_id,canonical_id;
CREATE TABLE profile_output (tenant_id TEXT, canonical_id TEXT, first_seen BIGINT, last_seen BIGINT, total_events BIGINT, page_views BIGINT, clicks BIGINT, logins BIGINT, feature_uses BIGINT, user_id TEXT, last_page TEXT, last_country TEXT, last_device TEXT, last_browser TEXT, events_1d BIGINT, events_7d BIGINT, events_30d BIGINT, events_90d BIGINT, top_pages TEXT[], top_features TEXT[]) WITH (connector='single_file',path='{{OUTPUT}}',format='debezium_json',type='sink');
INSERT INTO profile_output SELECT p.tenant_id, p.canonical_id, p.first_seen, p.last_seen, p.total_events, p.page_views, p.clicks, p.logins, p.feature_uses, p.user_id, p.last_page, p.last_country, p.last_device, p.last_browser, p.events_1d, p.events_7d, p.events_30d, p.events_90d, COALESCE(t.top_pages, CAST(ARRAY[] AS TEXT[])) AS top_pages, COALESCE(f.top_features, CAST(ARRAY[] AS TEXT[])) AS top_features
FROM profile_lifetime p INNER JOIN page_tops t ON p.tenant_id=t.tenant_id AND p.canonical_id=t.canonical_id INNER JOIN feature_tops f ON p.tenant_id=f.tenant_id AND p.canonical_id=f.canonical_id;
