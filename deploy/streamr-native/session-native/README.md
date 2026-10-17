# Native SESSION

This query uses native SESSION and SQL aggregates. It registers no UDF and has
no timer-seed sink, feedback controls, synthetic dispatch timestamps or transport
cursor. The source event timestamp drives session closure with a 30-minute gap.
The product allows five seconds of disorder: its watermark is event time minus
5,001 milliseconds. Older events are dropped; they do not reopen closed sessions
or mutate an unrelated newly opened session. No legacy maximum-zero timestamp
initialization is reproduced.

Identity and start use the first accepted arrival. Device, browser and country
use the last nonempty accepted arrival. Pages are sorted and deduplicated; event
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
