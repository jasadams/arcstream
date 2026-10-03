# Session readiness handoff: ARC-16 / STR-20 / STR-28

The strict session oracle provides three complete, 12-field summaries from the current
production Flink function, with fixed historical event times and three fresh-operator
restores. Run `bash test/streamr-reference/run.sh`. This audit targets Streamr
`b18dd98f3120a641887322dfb56c3574206f4469`, stacked on milestone 2 `fb01f5e9`; it is
source evidence, not a passing Streamr session execution.

## Freeze the selected business contract

| Behavior | Current Flink `SessionFunction` | Current generic SQL session operator |
| --- | --- | --- |
| Start/identity | First arriving event owns start, canonical/session/tenant metadata | Sorts by event time and uses minimum start; identity requires explicit aggregation |
| End | Maximum accepted event time | Window end includes inactivity gap |
| Deadline | Close at watermark >= last event + 30 minutes | Closes only when watermark > deadline |
| Long gap before watermark advances | Same keyed session remains open | Splits into sessions by event-time gaps |
| Older records | Function accepts older records | Drops records older than current watermark |
| Attributes | Last nonempty arrival wins | Must define order/FILTER semantics explicitly |
| Pages/type counts | Unique page set, frequency map | Requires explicit native collection aggregates |

Flink references: `SessionFunction.java:33` (ownership/counts), `:52` (metadata), `:69`
(deadline replacement), `:87` (closure/clear), `:97` (summary) and `:114` (timestamp
parsing). Valid historical timestamps are not clamped; malformed timestamps fall back to
wall clock and need a separate acceptance decision. `SessionizationJob.java:48` assigns
payload event timestamps, five-second bounded disorder and 30-second partition idleness,
then keys by session ID alone. Either prove global session-ID uniqueness or adopt
`(tenant_id, session_id)` ownership and document the baseline correction.

Streamr references are in `crates/arroyo-worker/src/arrow/session_aggregating_window.rs`:
`:439`/`:455` (minimum/splitting), `:517` (gap added to end), `:65`/`:566` (strict deadline
comparison), `:858` (old-event filtering). Generic SQL `SESSION` cannot be assumed to
implement the Flink contract merely because it has a 30-minute gap. Freeze compatible
behavior or explicitly approve differences before selecting the replacement query/path.

## Retained state and execution still need bounds

- `session_aggregating_window.rs:50`, `:532`, `:589` retain per-key holders, deadline/start
  indexes, raw Arrow batches and an unbounded channel. Partial aggregation checkpointing
  is explicitly unfinished at `:385`; the candidate uses a final aggregation plan.
- Checkpoint/restore uses legacy expiring time tables (`:802`, `:877`, `:927`).
  `crates/arroyo-state/src/tables/table_manager.rs:563` routes RocksDB live state only for
  `DiskKeyedMap`; this timestamp table is not converted by milestone 2. Flushed batches
  remain resident until retention cleanup (`tables/expiring_time_key_map.rs:826`, `:851`).
  Checkpoint files alone do not supply disk-backed working state.
- A SQL-map alternative needs explicit durable timer/index support and qualified native
  lists/maps. Disk input/output validation currently rejects collection types
  (`stateful_processor.rs:140`, `:244`), rejects unqualified functions (`:527`), and reads
  complete values (`:668`). A growing whole-session JSON value does not bound decoding.
- Persist scalar metadata, deadline replacement and output/clear relationships alongside
  separately addressable page membership/type counts. Use bounded scans/cursors and
  account for output construction. An unlimited `pages` array itself needs a declared
  output-size contract; silently truncating it is not acceptable.

## Watermark and delivery qualification

`watermark_generator.rs:211` emits an idle marker without advancing event time by wall
clock. Other active partitions can advance the shared watermark; if all live producers
stop, idleness alone does not establish timer closure. Finite EOF sends a final watermark
(`:137`). Test and document both scenarios separately; do not substitute activity events
or claim processing time closes event-time sessions.

Kafka source offsets are checkpointed (`crates/arroyo-connectors/src/kafka/source/mod.rs:249`),
while the Kafka sink has an unimplemented commit-phase recovery path
(`crates/arroyo-connectors/src/kafka/sink/mod.rs:359`).
These are separate delivery acceptance questions. The Flink harness and existing Streamr
identity file capture do not prove session Kafka offset/commit consistency.

## First runnable STR-20 acceptance slice

Run the resolved session fixture through the actual selected plan in memory/controller,
RocksDB/controller and RocksDB/leader modes. Recover before canceled/extended deadlines,
between closures and after cleared state. Check no processing-time-only output, exact
watermark boundary closure, old-arrival metadata/counters and fresh reuse of a closed ID.
Compare all 12 fields using `compare_session.py`; record operator ownership/retained-state
inventory, candidate revision and checkpoint evidence.

Follow with gap semantics, records arriving behind an advanced watermark, tenant-ID
collisions, malformed/future timestamps, mixed historical/current streams, many simultaneous
sessions, one hot session, stopped producers, slow outputs, process/remote recovery and
Kafka commit failures. STR-20 and ARC-16 remain open; STR-32 owns full replay, capacity and
24-hour fault qualification.
