-- Observe WHEN the native session becomes publicly available. No active flag
-- is fabricated from a finalized row and no timer/feedback is introduced.
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
CREATE TABLE output WITH ({{SINK_OPTIONS}});
INSERT INTO output SELECT tenant_id,canonical_id,session_id,start_ms,end_ms,event_count
FROM completed;
