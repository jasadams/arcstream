-- FlareDB's published managed ingestion admits scalar fields. Encode only the
-- three logical string arrays as JSON text for this storage wire format.
-- JSON_OBJECT quotes/escapes JSON_QUERY text unless FORMAT JSON is requested.
-- The authoritative profile-updates stream keeps its original arrays.
CREATE TABLE application_profiles(
 value TEXT,
 admission_progress TIMESTAMP GENERATED ALWAYS AS (CAST(NULL AS TIMESTAMP)) STORED
) WITH({{SOURCE}}, watermark_field='admission_progress');
CREATE TABLE storage_profiles(value TEXT) WITH({{SINK}});
INSERT INTO storage_profiles
SELECT JSON_OBJECT(
  'canonical_id' VALUE JSON_VALUE(value, '$.canonical_id'),
  'tenant_id' VALUE JSON_VALUE(value, '$.tenant_id'),
  'user_id' VALUE JSON_VALUE(value, '$.user_id'),
  'first_seen' VALUE CAST(JSON_VALUE(value, '$.first_seen') AS BIGINT),
  'last_seen' VALUE CAST(JSON_VALUE(value, '$.last_seen') AS BIGINT),
  'updated_at' VALUE CAST(JSON_VALUE(value, '$.updated_at') AS BIGINT),
  'total_events' VALUE CAST(JSON_VALUE(value, '$.total_events') AS BIGINT),
  'total_sessions' VALUE CAST(JSON_VALUE(value, '$.total_sessions') AS BIGINT),
  'events_1d' VALUE CAST(JSON_VALUE(value, '$.events_1d') AS BIGINT),
  'events_7d' VALUE CAST(JSON_VALUE(value, '$.events_7d') AS BIGINT),
  'events_30d' VALUE CAST(JSON_VALUE(value, '$.events_30d') AS BIGINT),
  'events_90d' VALUE CAST(JSON_VALUE(value, '$.events_90d') AS BIGINT),
  'sessions_1d' VALUE CAST(JSON_VALUE(value, '$.sessions_1d') AS BIGINT),
  'sessions_7d' VALUE CAST(JSON_VALUE(value, '$.sessions_7d') AS BIGINT),
  'sessions_30d' VALUE CAST(JSON_VALUE(value, '$.sessions_30d') AS BIGINT),
  'sessions_90d' VALUE CAST(JSON_VALUE(value, '$.sessions_90d') AS BIGINT),
  'avg_session_duration_sec' VALUE CAST(JSON_VALUE(value, '$.avg_session_duration_sec') AS BIGINT),
  'current_session_active' VALUE CAST(JSON_VALUE(value, '$.current_session_active') AS BOOLEAN),
  'current_session_duration_sec' VALUE CAST(JSON_VALUE(value, '$.current_session_duration_sec') AS BIGINT),
  'page_views' VALUE CAST(JSON_VALUE(value, '$.page_views') AS BIGINT),
  'clicks' VALUE CAST(JSON_VALUE(value, '$.clicks') AS BIGINT),
  'logins' VALUE CAST(JSON_VALUE(value, '$.logins') AS BIGINT),
  'feature_uses' VALUE CAST(JSON_VALUE(value, '$.feature_uses') AS BIGINT),
  'last_page' VALUE JSON_VALUE(value, '$.last_page'),
  'last_country' VALUE JSON_VALUE(value, '$.last_country'),
  'last_device' VALUE JSON_VALUE(value, '$.last_device'),
  'last_browser' VALUE JSON_VALUE(value, '$.last_browser'),
  'top_pages' VALUE JSON_QUERY(value, '$.top_pages'),
  'top_features' VALUE JSON_QUERY(value, '$.top_features'),
  'changed_fields' VALUE JSON_QUERY(value, '$.changed_fields'),
  'action' VALUE JSON_VALUE(value, '$.action'),
  'timestamp' VALUE JSON_VALUE(value, '$.timestamp'),
  'trigger' VALUE JSON_VALUE(value, '$.trigger')
) AS value FROM application_profiles;
