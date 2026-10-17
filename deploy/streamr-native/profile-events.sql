-- Stateless normalization must admit every arrival, including late events.
-- Arcstream carries the original source time explicitly into its state owner.
CREATE TABLE unified_input(value TEXT) WITH({{SOURCE}});
CREATE TABLE signals(value TEXT) WITH({{SINK}});
INSERT INTO signals
SELECT arc_profile_signal(value, JSON_VALUE(value, '$.event_time'), {{REPLAY}})
FROM unified_input;

CREATE TABLE session_signals(value TEXT) WITH({{SESSION_SINK}});
INSERT INTO session_signals
SELECT arc_profile_signal(value, JSON_VALUE(value, '$.event_time'), {{REPLAY}})
FROM unified_input;
