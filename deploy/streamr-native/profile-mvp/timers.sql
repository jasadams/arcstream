-- Exact millisecond deadlines on existing native TUMBLE. Calendar/business
-- reference time remains in the signal, distinct from source progress metadata.
-- A deadline already at/before declared input progress emits an immediate append
-- control; feeding such a row to a native window would silently drop it as late.
CREATE TABLE timer_seeds (
  tenant_id TEXT NOT NULL,canonical_id TEXT NOT NULL,generation TEXT NOT NULL,
  kind TEXT NOT NULL,deadline_time TEXT NOT NULL,progress_time TEXT NOT NULL,
  observed_ms BIGINT,replay_mode BOOLEAN NOT NULL,dispatch_time TEXT,
  deadline_ts TIMESTAMP GENERATED ALWAYS AS (CAST(deadline_time AS TIMESTAMP)) STORED,
  event_ts TIMESTAMP GENERATED ALWAYS AS
    (CASE WHEN kind='clock' AND dispatch_time IS NOT NULL THEN CAST(dispatch_time AS TIMESTAMP)
      ELSE GREATEST(CAST(deadline_time AS TIMESTAMP)-INTERVAL '1 nanosecond',
              CAST(progress_time AS TIMESTAMP)+INTERVAL '1 nanosecond') END) STORED,
  progress_ts TIMESTAMP GENERATED ALWAYS AS (CAST(progress_time AS TIMESTAMP)) STORED,
  WATERMARK FOR event_ts AS CAST(progress_time AS TIMESTAMP)
) WITH(connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE VIEW future_due AS
SELECT tenant_id,canonical_id,generation,kind,replay_mode,observed_ms,deadline_time,
       TUMBLE(INTERVAL '1 millisecond') AS window
FROM timer_seeds
WHERE kind IN ('session_timeout','window_decay_1d','window_decay_7d','window_decay_30d')
  AND deadline_ts>progress_ts
GROUP BY tenant_id,canonical_id,generation,kind,replay_mode,observed_ms,deadline_time,window;
CREATE VIEW controls_flat AS
SELECT tenant_id,canonical_id,generation,kind,deadline_time AS logical_time,observed_ms,replay_mode
FROM future_due
UNION ALL
SELECT tenant_id,canonical_id,generation,kind,deadline_time AS logical_time,observed_ms,replay_mode
FROM timer_seeds
WHERE kind IN ('session_timeout','window_decay_1d','window_decay_7d','window_decay_30d')
  AND deadline_ts<=progress_ts;
CREATE TABLE controls WITH(connector='single_file',path='{{OUTPUT}}',format='json',type='sink');
INSERT INTO controls SELECT tenant_id,canonical_id,generation,kind,logical_time,observed_ms,replay_mode FROM controls_flat;
