-- Keep the native CDC audit stream; publish its complete after-images for the
-- application's existing FULL-upsert analytics schema and profile subscription.
-- Explicitly suppress implicit transport-time admission on this append bridge.
-- Application event progress lives in its native state owner, not this bridge.
CREATE TABLE changes(
 value TEXT,
 admission_progress TIMESTAMP GENERATED ALWAYS AS (CAST(NULL AS TIMESTAMP)) STORED
) WITH({{SOURCE}}, watermark_field='admission_progress');
CREATE TABLE profiles(value TEXT) WITH({{SINK}});
INSERT INTO profiles
SELECT get_first_json_object(value,'$.after') AS value FROM changes
WHERE extract_json_string(value,'$.op') IN ('c','u','r');
