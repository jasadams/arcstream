-- Diagnostic of finalized SESSION -> explicit append projection -> calendar
-- aggregate -> MERGE. Original rejected closed-calendar.sql is preserved.
-- Native window scope ends when a projection drops the complete window struct;
-- retaining its start/end scalar data does not alter contribution timestamps.
-- closed_session_count is NOT the consumer total_sessions (starts immediately).
SET updating_ttl = NULL;
CREATE TABLE events (
  event_id TEXT NOT NULL, tenant_id TEXT NOT NULL, canonical_id TEXT NOT NULL,
  session_id TEXT, event_time TEXT NOT NULL, page_url TEXT,
  event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
  event_ms BIGINT GENERATED ALWAYS AS
    (CAST(CAST(event_time AS TIMESTAMP(3)) AS BIGINT)) STORED,
  WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH ({{SOURCE_OPTIONS}});
CREATE VIEW completed AS
SELECT tenant_id,session_id,SESSION(INTERVAL '30 minutes') AS window,
  FIRST_VALUE(canonical_id) AS canonical_id,
  FIRST_VALUE(event_ts) AS start_ts,
  FIRST_VALUE(event_ms) AS start_ms, MAX(event_ms) AS end_ms,
  COUNT(*) AS event_count
FROM events WHERE session_id IS NOT NULL AND session_id <> ''
GROUP BY tenant_id,session_id,window;
CREATE VIEW closed_contributions AS
SELECT tenant_id,canonical_id,start_ts,start_ms,end_ms FROM completed;
CREATE VIEW summaries AS
SELECT tenant_id,canonical_id,COUNT(*) AS closed_session_count,
  COUNT(*) FILTER (WHERE CAST(start_ts AS DATE) = EVENT_DATE()) AS closed_sessions_1d,
  COUNT(*) FILTER (WHERE CAST(start_ts AS DATE) BETWEEN EVENT_DATE() - INTERVAL '6' DAY AND EVENT_DATE()) AS closed_sessions_7d,
  COUNT(*) FILTER (WHERE CAST(start_ts AS DATE) BETWEEN EVENT_DATE() - INTERVAL '29' DAY AND EVENT_DATE()) AS closed_sessions_30d,
  COUNT(*) FILTER (WHERE CAST(start_ts AS DATE) BETWEEN EVENT_DATE() - INTERVAL '89' DAY AND EVENT_DATE()) AS closed_sessions_90d,
  CAST(SUM(end_ms-start_ms)/COUNT(*)/CAST(1000 AS BIGINT) AS BIGINT) AS avg_session_duration_sec
FROM closed_contributions GROUP BY tenant_id,canonical_id;
CREATE STATE TABLE retained (
  tenant_id TEXT,canonical_id TEXT,closed_session_count BIGINT,
  closed_sessions_1d BIGINT,closed_sessions_7d BIGINT,closed_sessions_30d BIGINT,
  closed_sessions_90d BIGINT,avg_session_duration_sec BIGINT,
  PRIMARY KEY(tenant_id,canonical_id)
) PARTITION BY tenant_id;
CREATE VIEW applied AS MERGE INTO retained AS previous USING summaries AS next_row
ON previous.tenant_id=next_row.tenant_id AND previous.canonical_id=next_row.canonical_id
WHEN MATCHED THEN UPDATE SET closed_session_count=next_row.closed_session_count,
  closed_sessions_1d=next_row.closed_sessions_1d,closed_sessions_7d=next_row.closed_sessions_7d,
  closed_sessions_30d=next_row.closed_sessions_30d,closed_sessions_90d=next_row.closed_sessions_90d,
  avg_session_duration_sec=next_row.avg_session_duration_sec
WHEN NOT MATCHED THEN INSERT (tenant_id,canonical_id,closed_session_count,closed_sessions_1d,
  closed_sessions_7d,closed_sessions_30d,closed_sessions_90d,avg_session_duration_sec)
VALUES (next_row.tenant_id,next_row.canonical_id,next_row.closed_session_count,
  next_row.closed_sessions_1d,next_row.closed_sessions_7d,next_row.closed_sessions_30d,
  next_row.closed_sessions_90d,next_row.avg_session_duration_sec)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE output WITH ({{SINK_OPTIONS}});
INSERT INTO output SELECT r.new.tenant_id AS tenant_id,r.new.canonical_id AS canonical_id,
  r.new.closed_session_count AS closed_session_count,
  r.new.closed_sessions_1d AS closed_sessions_1d,r.new.closed_sessions_7d AS closed_sessions_7d,
  r.new.closed_sessions_30d AS closed_sessions_30d,r.new.closed_sessions_90d AS closed_sessions_90d,
  r.new.avg_session_duration_sec AS avg_session_duration_sec FROM applied r;
