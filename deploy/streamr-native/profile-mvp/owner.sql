-- Register profile_transition.rs as a Rust UDF through PipelinePost.udfs.
-- A raw append connector is the durable boundary between native timer windows
-- and this native state-table owner. No application state lives in the driver.
CREATE TABLE profile_signals (
  value TEXT,
  tenant_id TEXT GENERATED ALWAYS AS (JSON_VALUE(value, '$.tenant_id')) STORED,
  canonical_id TEXT GENERATED ALWAYS AS (JSON_VALUE(value, '$.canonical_id')) STORED,
  kind TEXT GENERATED ALWAYS AS (JSON_VALUE(value, '$.kind')) STORED,
  effective_ms BIGINT GENERATED ALWAYS AS (CAST(JSON_VALUE(value,'$.effective_ms') AS BIGINT)) STORED,
  source_ms BIGINT GENERATED ALWAYS AS (CAST(JSON_VALUE(value,'$.source_ms') AS BIGINT)) STORED,
  scope TEXT GENERATED ALWAYS AS ('mvp') STORED
) WITH(connector='single_file',path='{{INPUT}}',format='raw_string',type='source',wait_for_control='true');

-- Fixed parallelism one: the constant scope makes stream progress and all
-- profiles share the same fused owner. This is application input progress, not
-- EVENT_TIMESTAMP and not a substituted calendar reference date.
CREATE STATE TABLE input_progress(scope TEXT,progress_ms BIGINT NOT NULL,PRIMARY KEY(scope)) PARTITION BY scope;
CREATE VIEW progressed AS MERGE INTO input_progress AS target USING profile_signals AS source
ON target.scope=source.scope
WHEN MATCHED AND source.kind IN ('event','clock')
THEN UPDATE SET progress_ms=GREATEST(target.progress_ms,source.source_ms)
WHEN NOT MATCHED AND source.kind IN ('event','clock')
THEN INSERT(scope,progress_ms) VALUES(source.scope,source.source_ms)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE VIEW clocked AS SELECT r.source.value AS value,r.source.tenant_id AS tenant_id,
  r.source.canonical_id AS canonical_id,r.source.kind AS kind,r.source.scope AS scope,
  r.new.progress_ms-CAST(5001 AS BIGINT) AS progress_ms,
  COALESCE(r.old.progress_ms,r.new.progress_ms)-CAST(5001 AS BIGINT) AS previous_progress_ms FROM progressed r;

CREATE STATE TABLE profiles (
  scope TEXT,tenant_id TEXT, canonical_id TEXT, payload TEXT NOT NULL,
  PRIMARY KEY(scope,tenant_id, canonical_id)
) PARTITION BY scope;

CREATE VIEW applied AS MERGE INTO profiles AS target
USING clocked AS source
ON target.scope=source.scope AND target.tenant_id=source.tenant_id AND target.canonical_id=source.canonical_id
WHEN MATCHED THEN UPDATE SET payload=arc_profile_transition(target.payload,source.value,source.progress_ms,source.previous_progress_ms)
WHEN NOT MATCHED AND source.kind='event'
THEN INSERT(scope,tenant_id,canonical_id,payload)
VALUES(source.scope,source.tenant_id,source.canonical_id,arc_profile_transition('{}',source.value,source.progress_ms,source.previous_progress_ms))
RETURNING source AS source,old AS old,new AS new,action AS action;

-- Native fusion deliberately ends at a scalar append sink. Row-expanding
-- projection runs in project.sql after an ordinary append-source boundary.
CREATE TABLE profile_outbox(value TEXT) WITH(connector='single_file',path='{{OUTPUT}}',format='raw_string',type='sink');
INSERT INTO profile_outbox
SELECT CASE WHEN r.source.kind='clock' THEN r.source.value ELSE r.new.payload END AS value
FROM applied r WHERE r.new.payload IS NOT NULL OR r.source.kind='clock';
