CREATE TABLE closed_sessions(session_id TEXT,canonical_id TEXT,tenant_id TEXT,start_time TIMESTAMP,end_time TIMESTAMP,duration_sec BIGINT,event_count BIGINT,pages TEXT[],event_types TEXT[],device_type TEXT,browser TEXT,country TEXT,window_start TIMESTAMP,window_end TIMESTAMP) WITH(connector='single_file',path='@INPUT@',format='json',type='source',wait_for_control='true');
CREATE TABLE result WITH(connector='single_file',path='@OUTPUT@',format='json',type='sink');
INSERT INTO result SELECT session_id,map(ARRAY['diagnostic'],ARRAY[1]) AS diagnostic_map FROM closed_sessions;
