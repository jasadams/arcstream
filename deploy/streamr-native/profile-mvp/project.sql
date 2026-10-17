-- Stateless native append projection; all durable application state and
-- previous-emitted snapshots are owned by the upstream STATE TABLE pipeline.
CREATE TABLE outbox(value TEXT) WITH(connector='single_file',path='{{INPUT}}',format='raw_string',type='source',wait_for_control='true');
CREATE TABLE profile_updates(value TEXT) WITH(connector='single_file',path='{{OUTPUT}}',format='raw_string',type='sink');
CREATE TABLE profile_timer_seeds(value TEXT) WITH(connector='single_file',path='{{TIMERS}}',format='raw_string',type='sink');
INSERT INTO profile_updates
SELECT unnest(extract_json(value,'$.outputs[*]')) AS value FROM outbox;
INSERT INTO profile_timer_seeds
SELECT unnest(CASE WHEN JSON_VALUE(value,'$.kind')='clock' THEN ARRAY[value]
                  ELSE extract_json(value,'$.timer_seeds[*]') END) AS value FROM outbox;
