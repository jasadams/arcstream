-- ARC-4: Sessionization pipeline in Arroyo using a native event-time SESSION window.
-- Replaces the Flink SessionizationJob.java / SessionFunction.java KeyedProcessFunction.
-- Pure Arroyo SQL: no UDF, no engine work (unlike ARC-3, which needed a Rust UDF).
--
-- Accepted divergence: Flink closes a session on a PROCESSING-time timer
-- (SessionFunction.onTimer uses System.currentTimeMillis), whereas Arroyo's
-- session() window is EVENT-time / watermark-driven. Event-time is more correct
-- and replay-safe (a backfill produces the same sessions as the live stream),
-- so the divergence is accepted -- same posture as ARC-3.
--
-- Group key is session_id: Flink keys the process function by event.sessionId,
-- and the Pinot `sessions` REALTIME table dedups on session_id (its primary key).
-- tenant_id / canonical_id are functionally dependent on session_id; they are in
-- the GROUP BY only so they can be projected (one tenant/canonical per session).
--
-- Output topic: session-events (consumed only by the Pinot `sessions` REALTIME
-- table). Pinot derives session_date / session_hour itself via transformConfigs,
-- so this pipeline does NOT emit them; it also does not emit `pages` / `event_types`
-- (present on SessionSummary.java but not ingested by the sessions schema).

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
  -- session() REQUIRES a declared event-time column + watermark. event_time is
  -- TEXT (yyyy-MM-dd HH:mm:ss.SSS); generate a TIMESTAMP from it. The 5s slack
  -- mirrors Flink's forBoundedOutOfOrderness(Duration.ofSeconds(5)).
  event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)),
  watermark FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH (
  connector = 'kafka',
  topic = 'unified-events',
  bootstrap_servers = 'redpanda:9092',
  format = 'json',
  type = 'source',
  'source.offset' = 'latest'
);

CREATE TABLE session_events (
  session_id TEXT,
  canonical_id TEXT,
  tenant_id TEXT,
  start_time TEXT,
  end_time TEXT,
  duration_sec BIGINT,
  event_count INT,
  device_type TEXT,
  browser TEXT,
  country TEXT
) WITH (
  connector = 'kafka',
  topic = 'session-events',
  bootstrap_servers = 'redpanda:9092',
  format = 'json',
  type = 'sink'
);

INSERT INTO session_events
SELECT
  session_id,
  canonical_id,
  tenant_id,
  first_value(event_time ORDER BY event_ts) AS start_time,
  last_value(event_time  ORDER BY event_ts) AS end_time,
  -- duration is first-event -> last-event, NOT the window bounds. The session()
  -- operator sets window.end = last_event + gap (30 min), which would inflate
  -- every session's duration by the gap. EXTRACT(EPOCH FROM ...) is the assumed
  -- epoch-seconds function; if Arroyo's planner rejects it at submit time, the
  -- alternative is date_part('epoch', ...).
  CAST((EXTRACT(EPOCH FROM max(event_ts)) - EXTRACT(EPOCH FROM min(event_ts))) AS BIGINT) AS duration_sec,
  CAST(count(*) AS INT) AS event_count,
  -- device_type / browser / country = last NON-EMPTY value (SessionFunction.java
  -- lines 52-60 only overwrite when the incoming field is non-empty).
  last_value(device_type ORDER BY event_ts) FILTER (WHERE device_type <> '') AS device_type,
  last_value(browser     ORDER BY event_ts) FILTER (WHERE browser     <> '') AS browser,
  last_value(country     ORDER BY event_ts) FILTER (WHERE country     <> '') AS country
FROM unified_events
WHERE session_id IS NOT NULL AND session_id <> ''
GROUP BY session(INTERVAL '30 minutes'), session_id, canonical_id, tenant_id;
