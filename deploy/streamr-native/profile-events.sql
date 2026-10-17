-- Stateless normalization must admit every arrival, including late events.
-- Arcstream carries the original source time explicitly into its state owner.
-- Explicitly suppress implicit transport-time admission on this append bridge.
-- Application event progress lives in its native state owner, not this bridge.
CREATE TABLE unified_input(
 value TEXT,
 admission_progress TIMESTAMP GENERATED ALWAYS AS (CAST(NULL AS TIMESTAMP)) STORED
) WITH({{SOURCE}}, watermark_field='admission_progress');
CREATE TABLE signals(value TEXT) WITH({{SINK}});
INSERT INTO signals
SELECT arc_profile_signal(value, JSON_VALUE(value, '$.event_time'), {{CLOCK_MODE}})
FROM unified_input;

CREATE TABLE session_signals(value TEXT) WITH({{SESSION_SINK}});
INSERT INTO session_signals
SELECT arc_profile_signal(value, JSON_VALUE(value, '$.event_time'), {{CLOCK_MODE}})
FROM unified_input;
