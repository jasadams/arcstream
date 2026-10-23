# Native Streamr application integration

The approved native graph is implemented and has scoped Kafka/RocksDB
correctness/recovery evidence. Start with the
[current delivery snapshot](../../docs/arc-18-native-delivery-20261023.md) for exact
sources, pins, passed cases and the current FLA-183 query-availability blocker.
Older composition rejection reports remain historical source-bound evidence;
do not restart that already completed integration work from those reports.

The five zero-UDF jobs are identity, unified events, directed merges, closed
sessions and profiles. Schemas, identity/lifecycle policy and SQL remain here in
Arcstream. Streamr supplies native SQL, SESSION, arrays, state tables and RocksDB.
Use original payload event time and the configured five-second watermark.
`EVENT_DATE()`/`EVENT_TIMESTAMP()` are stream-trigger-relative, not wall-clock or
watermark substitutes. No external timer, opaque state owner or pacing is part
of this graph.

The approved release is 22 native profile fields (21 GraphQL business fields plus
native action), including core metrics and nullable fractional closed-session
average. Seven live/start fields and four publication metadata fields are
explicitly deferred/omitted; see
[`profile-native/release-fields.json`](profile-native/release-fields.json).
SESSION emits 12 native fields; the analytics store keeps ten plus derived
date/hour, omitting `pages`/`event_types`. This is not full Flink parity.

Render the release SQL and public API requests without contacting an engine:

```sh
python3 deploy/streamr-native/prepare.py --profile-release \
  --output /absolute/new/native-bundle \
  --brokers owned-broker:9092 --namespace arc-evaluation \
  --raw-events-topic raw-events
```

Retain the SQL, literal expectations, hashes and actual results. Rendering or
planning is not a correctness/recovery pass. Runtime fixtures use
`test/streamr-reference/native_sql_runtime.py` and the maintained shared queue;
require actual SIGKILL, selected-checkpoint restoration, committed-output
continuity and fresh results. Follow [bounded resource guidance](runtime-resources.md),
not memory-only execution or disabled guards. The delivery snapshot distinguishes
empty versus nonempty committed-prefix cases.

Operational launch, qualified FlareDB-only update and production routing use the
private maintained CI workflows linked in the delivery snapshot. Image publication
is not deployment, readiness is not cutover, and replay progress is not replay
completion. Preserve failed evidence and surface any new native gap before changing
semantics or proposing an engine capability.
