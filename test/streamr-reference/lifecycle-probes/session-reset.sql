CREATE TABLE events(tenant_id TEXT,canonical_id TEXT,session_id TEXT,event_ts TIMESTAMP NOT NULL,total_events BIGINT,last_page TEXT,WATERMARK FOR event_ts AS event_ts - INTERVAL '5001 milliseconds') WITH(connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE VIEW closed AS SELECT tenant_id,canonical_id,session_id,SESSION(INTERVAL '30 minutes') AS window,COUNT(*) AS event_count FROM events WHERE session_id IS NOT NULL AND session_id <> '' GROUP BY tenant_id,canonical_id,session_id,window;
CREATE STATE TABLE cursor(tenant_id TEXT,canonical_id TEXT,session_id TEXT,current_session_active BOOLEAN,PRIMARY KEY(tenant_id,canonical_id)) PARTITION BY tenant_id;
CREATE VIEW reset AS MERGE INTO cursor AS target USING closed AS source
ON target.tenant_id=source.tenant_id AND target.canonical_id=source.canonical_id
WHEN MATCHED AND target.session_id=source.session_id THEN UPDATE SET current_session_active=false
WHEN NOT MATCHED THEN INSERT(tenant_id,canonical_id,session_id,current_session_active) VALUES(source.tenant_id,source.canonical_id,source.session_id,false)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE result WITH(connector='single_file',path='{{OUTPUT}}',format='json',type='sink');
INSERT INTO result SELECT r.source.tenant_id AS tenant_id,r.source.canonical_id AS canonical_id,r.source.session_id AS session_id,r.source.event_count AS event_count,r.new.current_session_active AS current_session_active FROM reset r;
