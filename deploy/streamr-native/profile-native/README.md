# Native profile components

`closed-session-metrics.sql` is a native composition candidate: Kafka source ->
SESSION -> lifetime COUNT/SUM -> updating CDC. It contributes completed-session
count and average duration; it is not a full profile or a deployment graph.
Planning, exact application results and recovery require current runtime evidence.

The source uses the actual application `event_time` text. SQL casts it to event
time and millisecond precision; no preprocessing stage or timestamp correction is
used. Exactly five seconds of disorder governs admission and native SESSION
closes on a thirty-minute inactivity gap. Null/empty session IDs are ordinary
excluded events which still participate in source watermark progress.

Ordered FIRST_VALUE uses Kafka's native `offset_id` metadata. The qualified MVP
input must have one partition: offsets then define source arrival order. Offsets
across partitions do not define global arrival order. Do not invent an ordering
field in application payloads or imply a multi-partition ordering guarantee.
Source metadata is an existing native interface documented by
[Arroyo Kafka source metadata](https://doc.arroyo.dev/connectors/kafka/#source-metadata).

Lifetime counts use the existing `SET updating_ttl = NULL` SQL setting with
configured native RocksDB/aggregate resource bounds. The default updating-table
retention is 24 hours and would not implement a lifetime count. This does not
change window expiry or add an application timer.

Retain millisecond duration until after summing and dividing by completed count,
then project integer seconds. The independent fixture includes 1.9s and 2.9s
sessions to distinguish that calculation from averaging individually truncated
seconds. COUNT(*) is over completed SESSION rows, not COUNT(DISTINCT session_id):
a reused ID after closure contributes another native session.

The output is a normal updating CDC stream. Native aggregate before/after images
must remain intact; the oracle checks per-key chains and exact metrics. Neither
state-table MERGE nor another topic/feedback boundary is introduced to accept
those changes. If this composition fails, preserve the native failure before
proposing an engine change.

Placeholders: `{{BROKER}}`, `{{INPUT_TOPIC}}`, `{{GROUP_ID}}`, `{{OUTPUT}}`.
Use an isolated broker/topic for application tests. The independent input and
expected metrics are under `test/streamr-reference/profile-native/`.
