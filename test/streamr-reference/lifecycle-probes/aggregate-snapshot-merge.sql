CREATE TABLE events(tenant_id TEXT,canonical_id TEXT,session_id TEXT,event_ts TIMESTAMP NOT NULL,total_events BIGINT,last_page TEXT,WATERMARK FOR event_ts AS event_ts - INTERVAL '5 seconds') WITH(connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE STATE TABLE emitted(tenant_id TEXT,canonical_id TEXT,total_events BIGINT,last_page TEXT,PRIMARY KEY(tenant_id,canonical_id)) PARTITION BY tenant_id;
CREATE VIEW snapshots AS SELECT tenant_id,canonical_id,COUNT(*) AS total_events,LAST_VALUE(last_page) AS last_page FROM events WHERE session_id IS NOT NULL AND session_id <> '' GROUP BY tenant_id,canonical_id;
CREATE VIEW applied AS MERGE INTO emitted AS target USING snapshots AS source
ON target.tenant_id=source.tenant_id AND target.canonical_id=source.canonical_id
WHEN MATCHED THEN UPDATE SET total_events=source.total_events,last_page=source.last_page
WHEN NOT MATCHED THEN INSERT(tenant_id,canonical_id,total_events,last_page) VALUES(source.tenant_id,source.canonical_id,source.total_events,source.last_page)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE result WITH(connector='single_file',path='{{OUTPUT}}',format='json',type='sink');
INSERT INTO result SELECT r.source.tenant_id AS tenant_id,r.source.canonical_id AS canonical_id,r.old.total_events AS previous_total_events,r.new.total_events AS total_events,
CASE WHEN r.action='insert' THEN 'create' ELSE 'update' END AS action,
CASE WHEN r.old.total_events IS NULL THEN ARRAY['canonical_id','tenant_id','total_events','first_seen','last_seen']
WHEN r.old.last_page <> r.new.last_page THEN ARRAY['total_events','last_page'] ELSE ARRAY['total_events'] END AS changed_fields FROM applied r;
