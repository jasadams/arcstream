-- App source time stays raw. Profile live display clamps do not affect sessions.
CREATE TABLE session_signals(
 value TEXT,
 tenant_id TEXT GENERATED ALWAYS AS (JSON_VALUE(value,'$.tenant_id')) STORED,
 session_id TEXT GENERATED ALWAYS AS (COALESCE(JSON_VALUE(value,'$.session_id'),'')) STORED,
 kind TEXT GENERATED ALWAYS AS (JSON_VALUE(value,'$.kind')) STORED,
 raw_ms BIGINT GENERATED ALWAYS AS
  (CAST(JSON_VALUE(value,'$.session_source_ms') AS BIGINT)) STORED,
 scope TEXT GENERATED ALWAYS AS ('mvp') STORED
) WITH(connector='single_file',path='{{INPUT}}',format='raw_string',type='source',wait_for_control='true');
CREATE STATE TABLE session_progress(scope TEXT,progress_ms BIGINT NOT NULL,PRIMARY KEY(scope)) PARTITION BY scope;
CREATE VIEW progressed AS MERGE INTO session_progress AS target USING session_signals AS source
ON target.scope=source.scope
WHEN MATCHED AND source.kind IN ('event','clock') THEN UPDATE SET progress_ms=GREATEST(target.progress_ms,source.raw_ms)
WHEN NOT MATCHED AND source.kind IN ('event','clock') THEN INSERT(scope,progress_ms) VALUES(source.scope,source.raw_ms)
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE VIEW clocked AS SELECT r.source.value AS value,r.source.tenant_id AS tenant_id,
 r.source.session_id AS session_id,r.source.kind AS kind,r.source.scope AS scope,
 r.new.progress_ms-CAST(5001 AS BIGINT) AS progress_ms,
 COALESCE(r.old.progress_ms,r.new.progress_ms)-CAST(5001 AS BIGINT) AS previous_progress_ms FROM progressed r;
CREATE STATE TABLE active_sessions(scope TEXT,tenant_id TEXT,session_id TEXT,payload TEXT NOT NULL,
 PRIMARY KEY(scope,tenant_id,session_id)) PARTITION BY scope;
-- The application scalar runs once inside the native mutation contract. A
-- dependent second MERGE deletes closed/ignored rows in the same fused event;
-- its source decision survives DELETE for the append outbox.
CREATE VIEW transitioned AS MERGE INTO active_sessions AS target USING clocked AS source
ON target.scope=source.scope AND target.tenant_id=source.tenant_id AND target.session_id=source.session_id
WHEN MATCHED THEN UPDATE SET payload=arc_session_transition(target.payload,source.value,source.progress_ms,source.previous_progress_ms)
WHEN NOT MATCHED THEN INSERT(scope,tenant_id,session_id,payload)
VALUES(source.scope,source.tenant_id,source.session_id,arc_session_transition('{}',source.value,source.progress_ms,source.previous_progress_ms))
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE VIEW decided AS SELECT r.source.scope AS scope,r.source.tenant_id AS tenant_id,
 r.source.session_id AS session_id,r.new.payload AS decision FROM transitioned r;
CREATE VIEW applied AS MERGE INTO active_sessions AS target USING decided AS source
ON target.scope=source.scope AND target.tenant_id=source.tenant_id AND target.session_id=source.session_id
WHEN MATCHED AND JSON_VALUE(source.decision,'$.mode')='delete' THEN DELETE
RETURNING source AS source,old AS old,new AS new,action AS action;
CREATE TABLE session_outbox(value TEXT) WITH(connector='single_file',path='{{OUTPUT}}',format='raw_string',type='sink');
INSERT INTO session_outbox SELECT r.source.decision AS value FROM applied r;
