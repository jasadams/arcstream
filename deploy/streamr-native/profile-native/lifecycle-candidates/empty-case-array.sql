-- Minimal metadata failure: unused CASE branch evaluates on a zero-row batch.
CREATE TABLE events (
  event_id TEXT NOT NULL, tenant_id TEXT NOT NULL, canonical_id TEXT NOT NULL,
  event_time TEXT NOT NULL,
  event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
  WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH ({{SOURCE_OPTIONS}});
CREATE TABLE output WITH ({{SINK_OPTIONS}});
INSERT INTO output SELECT tenant_id,canonical_id,
  CASE WHEN event_id='insert' THEN ARRAY['created']
    ELSE array_concat(ARRAY[event_id],ARRAY[event_id]) END AS changed_fields
FROM events;
