CREATE TABLE seeds(
 tenant_id TEXT NOT NULL,session_id TEXT NOT NULL,generation TEXT NOT NULL,
 kind TEXT NOT NULL,deadline_time TEXT NOT NULL,progress_time TEXT NOT NULL,dispatch_time TEXT,
 deadline_ts TIMESTAMP GENERATED ALWAYS AS (CAST(deadline_time AS TIMESTAMP)) STORED,
 dispatch_ts TIMESTAMP GENERATED ALWAYS AS
 (CASE WHEN kind='clock' AND dispatch_time IS NOT NULL THEN CAST(dispatch_time AS TIMESTAMP)
 ELSE GREATEST(CAST(deadline_time AS TIMESTAMP)-INTERVAL '1 nanosecond',CAST(progress_time AS TIMESTAMP)+INTERVAL '1 nanosecond') END) STORED,
 progress_ts TIMESTAMP GENERATED ALWAYS AS (CAST(progress_time AS TIMESTAMP)) STORED,
 WATERMARK FOR dispatch_ts AS CAST(progress_time AS TIMESTAMP)
) WITH(connector='single_file',path='{{INPUT}}',format='json',type='source',wait_for_control='true');
CREATE VIEW future AS SELECT tenant_id,session_id,generation,kind,deadline_time,
 TUMBLE(INTERVAL '1 millisecond') AS window FROM seeds
WHERE kind='session_summary_timeout' AND deadline_ts>progress_ts
GROUP BY tenant_id,session_id,generation,kind,deadline_time,window;
CREATE VIEW due AS
SELECT tenant_id,session_id,generation,kind,deadline_time AS logical_time FROM future
UNION ALL
SELECT tenant_id,session_id,generation,kind,deadline_time AS logical_time FROM seeds
WHERE kind='session_summary_timeout' AND deadline_ts<=progress_ts;
CREATE TABLE controls WITH(connector='single_file',path='{{OUTPUT}}',format='json',type='sink');
INSERT INTO controls SELECT tenant_id,session_id,generation,kind,logical_time FROM due;
