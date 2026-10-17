-- Keep the native CDC audit stream; publish its complete after-images for the
-- application's existing FULL-upsert analytics schema and profile subscription.
CREATE TABLE changes(value TEXT) WITH({{SOURCE}});
CREATE TABLE profiles(value TEXT) WITH({{SINK}});
INSERT INTO profiles
SELECT get_first_json_object(value,'$.after') AS value FROM changes
WHERE extract_json_string(value,'$.op') IN ('c','u','r');
