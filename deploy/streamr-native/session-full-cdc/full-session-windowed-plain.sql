-- Preserve complete existing SESSION window lineage through both downstream aggregates.
CREATE TABLE events(event_id TEXT NOT NULL,session_id TEXT NOT NULL,tenant_id TEXT NOT NULL,canonical_id TEXT NOT NULL,event_type TEXT NOT NULL,page_url TEXT,device_type TEXT,browser TEXT,country TEXT,event_ts TIMESTAMP NOT NULL,event_ms BIGINT NOT NULL,clock_ts TIMESTAMP NOT NULL,arrival_seq BIGINT NOT NULL,is_clock BOOLEAN NOT NULL,WATERMARK FOR clock_ts AS clock_ts - INTERVAL '30 minutes') WITH(connector='single_file',path='@INPUT@',format='json',type='source',wait_for_control='true');
CREATE VIEW selected AS SELECT *,page_url IS NOT NULL AND page_url <> '' AS page_selected,device_type IS NOT NULL AND device_type <> '' AS device_selected,browser IS NOT NULL AND browser <> '' AS browser_selected,country IS NOT NULL AND country <> '' AS country_selected FROM events WHERE NOT is_clock;
CREATE VIEW closed_sessions AS SELECT session_id,canonical_id,tenant_id,start_time,end_time,CAST((end_ms-start_ms)/1000 AS BIGINT) AS duration_sec,event_count,pages,event_types,device_type,browser,country,window,window.start AS window_start,window.end AS window_end FROM (
SELECT tenant_id,session_id,SESSION(INTERVAL '30 minutes') AS window,
FIRST_VALUE(canonical_id ORDER BY arrival_seq) AS canonical_id,
FIRST_VALUE(event_ts ORDER BY arrival_seq) AS start_time,MAX(event_ts) AS end_time,
FIRST_VALUE(event_ms ORDER BY arrival_seq) AS start_ms,MAX(event_ms) AS end_ms,
COUNT(*) AS event_count,
ARRAY_AGG(page_url ORDER BY arrival_seq) FILTER(WHERE page_selected) AS pages,
ARRAY_AGG(event_type ORDER BY arrival_seq) AS event_types,
COALESCE(LAST_VALUE(device_type ORDER BY arrival_seq) FILTER(WHERE device_selected),'') AS device_type,
COALESCE(LAST_VALUE(browser ORDER BY arrival_seq) FILTER(WHERE browser_selected),'') AS browser,
COALESCE(LAST_VALUE(country ORDER BY arrival_seq) FILTER(WHERE country_selected),'') AS country
FROM selected GROUP BY tenant_id,session_id,window);
CREATE TABLE result WITH(connector='single_file',path='@OUTPUT@',format='json',type='sink');
CREATE VIEW expanded AS SELECT session_id,canonical_id,tenant_id,start_time,end_time,duration_sec,event_count,pages,device_type,browser,country,window,window_start,window_end,unnest(event_types) AS event_type FROM closed_sessions;
CREATE VIEW counts AS SELECT session_id,canonical_id,tenant_id,start_time,end_time,duration_sec,event_count,pages,device_type,browser,country,window,window_start,window_end,event_type,COUNT(*) AS n FROM expanded GROUP BY session_id,canonical_id,tenant_id,start_time,end_time,duration_sec,event_count,pages,device_type,browser,country,window,window_start,window_end,event_type;
INSERT INTO result SELECT session_id,canonical_id,tenant_id,to_char(start_time,'%Y-%m-%d %H:%M:%S%.3f') AS start_time,to_char(end_time,'%Y-%m-%d %H:%M:%S%.3f') AS end_time,duration_sec,event_count,array_sort(array_distinct(pages)) AS pages,map(ARRAY_AGG(event_type ORDER BY event_type),ARRAY_AGG(n ORDER BY event_type)) AS event_types,device_type,browser,country FROM counts GROUP BY session_id,canonical_id,tenant_id,start_time,end_time,duration_sec,event_count,pages,device_type,browser,country,window,window_start,window_end;
