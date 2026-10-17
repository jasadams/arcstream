-- Append-only projection, beyond the scalar native state-owner boundary.
CREATE TABLE outbox(value TEXT) WITH(connector='single_file',path='{{INPUT}}',format='raw_string',type='source',wait_for_control='true');
CREATE TABLE summaries(value TEXT) WITH(connector='single_file',path='{{OUTPUT}}',format='raw_string',type='sink');
CREATE TABLE timer_seeds(value TEXT) WITH(connector='single_file',path='{{TIMERS}}',format='raw_string',type='sink');
INSERT INTO summaries SELECT unnest(extract_json(value,'$.outputs[*]')) AS value FROM outbox;
INSERT INTO timer_seeds SELECT unnest(extract_json(value,'$.timer_seeds[*]')) AS value FROM outbox;
