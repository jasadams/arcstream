# Native SESSION

This query uses native SESSION and SQL aggregates. It registers no UDF and has
no timer-seed sink, feedback controls, synthetic dispatch timestamps or transport
cursor. The source event timestamp drives session closure with a 30-minute gap.
The product allows five seconds of disorder: its watermark is event time minus
exactly five seconds. Older events are dropped; they do not reopen closed sessions
or mutate an unrelated newly opened session. No legacy maximum-zero timestamp
initialization is reproduced.

Identity and start use the earliest accepted event time. Device, browser and
country use the latest nonempty value by event time. FIRST_VALUE/LAST_VALUE
intentionally omit ORDER BY: STR-60 must prepend the WATERMARK FOR timestamp
as the primary key. Equal-key FIRST retains the first arrival; LAST prefers
the later write. Kafka offsets and client-generated sequence fields are not
substitutes for this contract. Pages are sorted and deduplicated; event
types are counted through native UNNEST, grouping and map aggregation. Application
metadata projections remain SQL in Arcstream.

Finite fixtures use ordinary page-view events with empty session IDs to advance
source time. Normal session-ID filtering excludes them from aggregates. There is
no special clock column or clock-message protocol and no production heartbeat
requirement.
EVENT_DATE()/EVENT_TIMESTAMP() are not needed or replaced with a watermark.

Prepare fixtures with test/streamr-reference/session-native/prepare.py. Use the
public pipeline API with udfs=[], RocksDB and native_sql_runtime.py. Select
--typed-owners 0 --window-owners 3. For chronological-recovery select
--fault-prefix 3 --require-committed-output. A complete recovery result requires
actual worker SIGKILL, same-job fresh generation, selected checkpoint restoration,
unchanged nonempty committed bytes, fresh output and exact application results.

Prepared fixtures alone establish neither integration readiness nor production
cutover. Record actual current-source runtime evidence separately.

The source accepts application `event_time` text. SQL generates `event_ts` and
millisecond `event_ms`; neither is added to the payload by the fixture producer.
The watermark expression repeats the cast of the physical field because the
current planner cannot resolve generated aliases inside that expression. This
preserves event time minus five seconds and documents the planner limitation.
`session_id` is nullable; null and empty IDs are excluded by normal SQL filtering.

This candidate assumes STR-60, as explicitly requested. Its implicit FIRST/LAST
ordering is not claimed to work on the older qualified binary. The out-of-order
and equal-time fixtures must pass through the actual engine and Kafka/RocksDB
recovery after STR-60 merges. The file source is for finite fixtures; the maintained
integration renderer selects Kafka source options without rewriting the SQL body.
