-- Stateless native append projection; all durable application state and
-- previous-emitted snapshots are owned by the upstream STATE TABLE pipeline.
CREATE TABLE outbox(value TEXT, admission_progress TIMESTAMP GENERATED ALWAYS AS (CAST(NULL AS TIMESTAMP)) STORED) WITH(connector='single_file',path='{{INPUT}}',format='raw_string',type='source',wait_for_control='true',watermark_field='admission_progress');
CREATE VIEW frames AS SELECT arc_timer_outbox(JSON_VALUE(value,'$.payload'),
 CAST(JSON_VALUE(value,'$.dispatch_ms') AS BIGINT),
 CAST(JSON_VALUE(value,'$.progress_ms') AS BIGINT)) AS value FROM outbox;
CREATE TABLE profile_updates(value TEXT) WITH(connector='single_file',path='{{OUTPUT}}',format='raw_string',type='sink');
CREATE TABLE profile_timer_seeds(value TEXT) WITH(connector='single_file',path='{{TIMERS}}',format='raw_string',type='sink');
INSERT INTO profile_updates
SELECT unnest(extract_json(value,'$.outputs[*]')) AS value FROM frames;
INSERT INTO profile_timer_seeds
SELECT unnest(array_append(
 COALESCE(extract_json(value,'$.timer_seeds[*]'),CAST(ARRAY[] AS TEXT[])),
 JSON_QUERY(value,'$.technical_clock'))) AS value FROM frames;
