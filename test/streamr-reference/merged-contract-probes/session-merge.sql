SET updating_ttl = NULL;
CREATE TABLE events (
 event_id TEXT NOT NULL,tenant_id TEXT NOT NULL,canonical_id TEXT NOT NULL,
 event_time TEXT NOT NULL,page_url TEXT,
 event_ts TIMESTAMP GENERATED ALWAYS AS(CAST(event_time AS TIMESTAMP)) STORED,
 WATERMARK FOR event_ts AS CAST(event_time AS TIMESTAMP) - INTERVAL '5 seconds'
) WITH(connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE VIEW computed AS SELECT tenant_id,canonical_id,SESSION(INTERVAL '30 minutes') AS window,COUNT(*) AS total_events FROM events GROUP BY tenant_id,canonical_id,window;
CREATE STATE TABLE snapshots(tenant_id TEXT,canonical_id TEXT,total_events BIGINT,
PRIMARY KEY(tenant_id,canonical_id)) PARTITION BY tenant_id;
CREATE VIEW applied AS MERGE INTO snapshots AS previous USING computed AS next_value
ON previous.tenant_id=next_value.tenant_id AND previous.canonical_id=next_value.canonical_id
WHEN MATCHED THEN UPDATE SET total_events=next_value.total_events
WHEN NOT MATCHED THEN INSERT(tenant_id,canonical_id,total_events)
VALUES(next_value.tenant_id,next_value.canonical_id,next_value.total_events)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE result WITH(connector='single_file',path='{{OUTPUT}}',format='json',type='sink');
INSERT INTO result SELECT r.source.tenant_id AS tenant_id,r.source.canonical_id AS canonical_id,
r.old.total_events AS previous_total,r.new.total_events AS total_events,r.action AS action
FROM applied r;
