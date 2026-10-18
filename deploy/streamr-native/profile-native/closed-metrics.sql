-- Native completed-session metrics; requires STR-60 implicit event-time order.
-- Source payload event_time is cast in SQL; no arrival-order substitute.
-- Lifetime profile metrics retain their native aggregate state.
SET updating_ttl = NULL;
CREATE TABLE events (
  event_id TEXT NOT NULL,
  tenant_id TEXT NOT NULL,
  canonical_id TEXT NOT NULL,
  session_id TEXT,
  event_time TEXT NOT NULL,
  event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
  event_ms BIGINT GENERATED ALWAYS AS
    (CAST(CAST(event_time AS TIMESTAMP(3)) AS BIGINT)) STORED,
  WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH ({{SOURCE_OPTIONS}});

CREATE VIEW completed_sessions AS
SELECT tenant_id,session_id,SESSION(INTERVAL '30 minutes') AS window,
       FIRST_VALUE(canonical_id) AS canonical_id,
       FIRST_VALUE(event_ms) AS start_ms,
       MAX(event_ms) AS end_ms
FROM events
WHERE session_id IS NOT NULL AND session_id <> ''
GROUP BY tenant_id,session_id,window;

CREATE VIEW closed_durations AS
SELECT tenant_id,canonical_id,end_ms-start_ms AS duration_ms
FROM completed_sessions;

CREATE TABLE metrics (
  tenant_id TEXT,canonical_id TEXT,
  closed_session_count BIGINT,avg_session_duration_sec BIGINT
) WITH ({{SINK_OPTIONS}});

INSERT INTO metrics
SELECT tenant_id,canonical_id,COUNT(*) AS closed_session_count,
       CAST(SUM(duration_ms)/COUNT(*)/CAST(1000 AS BIGINT) AS BIGINT)
         AS avg_session_duration_sec
FROM closed_durations
GROUP BY tenant_id,canonical_id;
