-- Diagnostic: native OLD/NEW-derived action and exact per-field differences.
-- User confirmed stream event time for application timestamps on 2026-10-23.
-- This probes preservation of original trigger time. Do not feed the partial
-- diagnostic schema to the 33-field consumer.
SET updating_ttl = NULL;
CREATE TABLE events (
  event_id TEXT NOT NULL, tenant_id TEXT NOT NULL, canonical_id TEXT NOT NULL,
  session_id TEXT, event_time TEXT NOT NULL, page_url TEXT,
  event_ts TIMESTAMP GENERATED ALWAYS AS (CAST(event_time AS TIMESTAMP)) STORED,
  event_ms BIGINT GENERATED ALWAYS AS
    (CAST(CAST(event_time AS TIMESTAMP(3)) AS BIGINT)) STORED,
  WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH ({{SOURCE_OPTIONS}});
CREATE VIEW snapshots AS SELECT tenant_id,canonical_id,COUNT(*) AS total_events,
  MAX(event_ms) AS last_seen FROM events GROUP BY tenant_id,canonical_id;
CREATE STATE TABLE retained (tenant_id TEXT,canonical_id TEXT,total_events BIGINT,last_seen BIGINT,
  PRIMARY KEY(tenant_id,canonical_id)) PARTITION BY tenant_id;
CREATE VIEW applied AS MERGE INTO retained AS previous USING snapshots AS next_row
ON previous.tenant_id=next_row.tenant_id AND previous.canonical_id=next_row.canonical_id
WHEN MATCHED THEN UPDATE SET total_events=next_row.total_events,last_seen=next_row.last_seen
WHEN NOT MATCHED THEN INSERT(tenant_id,canonical_id,total_events,last_seen)
VALUES(next_row.tenant_id,next_row.canonical_id,next_row.total_events,next_row.last_seen)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE output WITH ({{SINK_OPTIONS}});
INSERT INTO output SELECT r.new.tenant_id AS tenant_id,r.new.canonical_id AS canonical_id,
  r.new.total_events AS total_events,r.new.last_seen AS last_seen,
  CASE WHEN r.action='insert' THEN 'create' ELSE 'update' END AS action,
  CASE WHEN r.action='insert' THEN ARRAY['tenant_id','canonical_id','total_events','last_seen']
    ELSE array_concat(
      CASE WHEN r.old.total_events IS DISTINCT FROM r.new.total_events THEN ARRAY['total_events'] ELSE CAST(ARRAY[] AS TEXT[]) END,
      CASE WHEN r.old.last_seen IS DISTINCT FROM r.new.last_seen THEN ARRAY['last_seen'] ELSE CAST(ARRAY[] AS TEXT[]) END)
    END AS changed_fields,
  CAST(CAST(EVENT_TIMESTAMP() AS TIMESTAMP(3)) AS BIGINT) AS candidate_event_updated_at,
  CAST(EVENT_TIMESTAMP() AS TEXT) AS candidate_event_timestamp
FROM applied r;
