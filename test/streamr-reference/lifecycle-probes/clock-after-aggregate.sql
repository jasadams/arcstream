CREATE TABLE events(tenant_id TEXT,canonical_id TEXT,session_id TEXT,event_ts TIMESTAMP NOT NULL,total_events BIGINT,last_page TEXT,WATERMARK FOR event_ts AS event_ts - INTERVAL '5001 milliseconds') WITH(connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE VIEW counts AS SELECT tenant_id,canonical_id,COUNT(*) AS total_events FROM events WHERE session_id IS NOT NULL AND session_id <> '' GROUP BY tenant_id,canonical_id;
CREATE TABLE result WITH(connector='single_file',path='{{OUTPUT}}',format='json',type='sink');
INSERT INTO result SELECT tenant_id,canonical_id,total_events,EVENT_TIMESTAMP() AS updated_at FROM counts;
