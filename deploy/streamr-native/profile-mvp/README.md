# Full profile candidate using existing native interfaces

Application code only: register `profile_transition.rs` as a Rust scalar UDF via
the public pipeline API's `udfs` array. `owner.sql` keeps all durable state in a
native STATE TABLE partitioned by a fixed MVP scope; the scalar receives its previous row and
one append trigger, then returns its replacement. It reads/writes no external
state. Native memory/RocksDB backends and normal pipeline checkpoints apply.

The owner writes one scalar raw JSON state outbox record per input. `project.sql`
reads that append connector and expands its full33 CDC envelopes (`before`,
`after`, `op`) and timer-seed records through two native sinks. No omitted
profile fields are invented as constants. Prior emitted snapshots live in the
same native state row, so `before` and `changed_fields` survive recovery.

`timers.sql` consumes append timer seeds, placing each exact millisecond
deadline into a native one-millisecond TUMBLE at deadline minus one nanosecond.
The resulting window ends at the requested deadline. A deadline already at or
before the seed's declared progress takes a direct append-control branch; sending
it into a native window would drop it as late. The source transport timestamp is
the greater of deadline-minus-1ns and declared-progress-plus-1ns, ensuring overdue
rows reach this immediate branch without changing their retained deadline.
Each event ID is a generation,
and the application owner ignores cancelled generations. Native progress is
independent of requested deadlines, so a future seed cannot advance the clock.

An application-owned native singleton `input_progress` table stores maximum
source timestamp; both this table and profiles share the same fixed scope at
parallelism one. The original source timestamp policy clamps only excessively
future live values, while profile business time also clamps values older than
91 days. The signal's `source_ms` and `effective_ms` remain separate and are
validated by the application scalar function. Timer progress is maximum
source_ms minus 5,001 milliseconds, preserving Flink 1.20.1's five-second
bounded-out-of-orderness rule and its extra millisecond boundary. Calendar
reference dates and logical replay duration never use that delayed progress.

The incoming event schedules its own 30-minute timeout even if late. Day
deadlines use maximum last_seen, as in Java. Switching session IDs increments
starts and replaces session start; it does not count a closed duration.

There are connector boundaries between owner and native timer windows, because
actual attempts at the current engine rejected aggregate CDC -> MERGE and
SESSION -> MERGE in the same graph; post-MERGE UNNEST is also excluded by current
fusion bounds, so a native append projector owns expansion. These pipelines commit independently; no
cross-pipeline transaction or globally ordered timer/raw-event arrival is claimed.
Before applying a new event, the scalar catches up through the PRIOR persisted
global progress, so asynchronous old timer feedback cannot lose an already-due
closure/reset. The current event's newly advanced watermark is not used for
that catchup. Later duplicate/cancelled controls are idempotent. End-to-end
evidence must exercise this ordering through real connectors, not merely feed
a handwritten closure to the owner.

## Clock policy

Each event/control has `replay_mode`; missing means live. Persisted state rejects
changing this mode for an existing profile. Use an isolated state namespace when
changing modes. Both modes reject invalid timestamps instead of substituting a
clock. The raw event's original timestamp remains on the source event topic.

Live mode preserves the original valid-timestamp clamp: more than 91 days past
or more than 60 seconds future becomes the invocation observation time. Daily
buckets are pruned at observation UTC date minus 91 days. Current-session duration
uses observation time and can be negative for an allowed future session start,
matching the signed Java value.

Replay mode preserves valid historical event time and retains historical daily
contributions so subsequent replay calendar calculations remain correct. Session
duration uses the logical reference time, not elapsed wall time. Event snapshot
calendar reference is maximum last_seen, as in existing-profile debounced Java
emissions; timer snapshots use their exact logical deadline. A late event can
therefore preserve last_seen while changing its scheduled session deadline.

`updated_at` and `timestamp` are observation metadata in both modes. The normalizer supplies
`observed_ms` once per event; fixtures additionally set `freeze_observation=true`
so timer controls inherit their explicit observation clock. Normal timer seeds
carry a null observation clock and the owner samples UTC at control invocation. The source
clock, watermark completeness and observation clock are separate. This candidate
does not replace EVENT_DATE/EVENT_TIMESTAMP with a watermark.

Current candidate emits a snapshot for each event and due lifecycle trigger.
It does not claim the old exact five-second processing-time debounce cadence.
Changes are compared with the previous actual emitted snapshot. Existing
consumer periodic flushing can coalesce display updates without altering the
authoritative CDC stream; emission cadence requires explicit acceptance separate
from profile state correctness.

## Running and evidence

Prepare the finite owner fixtures:

```sh
python3 test/streamr-reference/profile-mvp/prepare.py /absolute/new/replay-case
python3 test/streamr-reference/profile-mvp/prepare.py /absolute/new/live-case --case live
python3 -m unittest discover -s test/streamr-reference/profile-mvp -p 'test_*.py' -v
```

The replay oracle explicitly specifies fifteen complete snapshots over two
tenants sharing a canonical ID: UTC midnight, A/A/B/A/null/empty transitions,
out-of-order incoming deadlines, duplicate/stale controls, timeout/reset and
reused session ID, average duration, top-count ties, 1/7/30-day decay. Live oracle
covers accepted source time, old/future clamping and signed future-within-60s
duration. Both pin observation time to 2030 while replay input is from 2020.
The comparator checks exact field values/types, all prior/current CDC snapshots,
per-profile order and complete finite multiplicity. A passing self-test validates
the comparator, not the application UDF or pipeline.

`owner.sql` and `timers.sql` placeholders are `{{INPUT}}`/`{{OUTPUT}}`.
`project.sql` adds `{{TIMERS}}` for the timer-seed sink. A public API supervisor must register
the UDF definition and capture source/executable/config hashes, native checkpoint
proof, real worker process loss, unchanged committed prefix and fresh outputs.
Replace single-file connectors with application-owned Kafka connector settings
for the real cyclic composition; source fixtures alone are not deployment.

The first official runtime attempt exposed a generic Rust UDF compiler/macro
edition mismatch: generated crate edition 2024, plugin `#[udf]` expands
`#[no_mangle]` instead of `#[unsafe(no_mangle)]`. Exact runtime logs are held by
the coordinator. The separate STR-143 compatibility repair at
`a86297dc049c0c3a5a85f965c782cc571ae0d2d2` corrects that export annotation;
production engine execution code is unchanged. Source pins must distinguish the
qualified engine binary from the plugin used by the public compiler. Do not
describe unexecuted application SQL as passing because the fixture exists.

Raw append stages explicitly use a nullable source watermark expression through
Streamr's current `watermark_field` option. This preserves record transport time
without admitting a default wall-clock-derived lateness floor: historical native
timer controls must reach the application policy. Source/business progress still
comes exclusively from the native singleton input-progress table.

That table also owns a technical dispatch cursor, advanced only by raw events or
explicit clock markers: `max(previous + 2000ms, effective + 31d, source + 31d)`.
The shared `timer_outbox.rs` scalar adds a technical clock outside persisted
business state. The native projector emits it after each frame's business seeds
in the same ordered sink. Timer `FOR` uses this separate dispatch timestamp for
technical clocks; `AS` remains the unchanged declared application progress.
Clock rows cannot become windows or application controls. This avoids the
existing source generator's `FOR` interval throttle suppressing a nearer logical
watermark after a far-future deadline. It changes no business date or deadline.

The dispatch cursor is finite and must fit Streamr's nanosecond timestamp range;
the scalar rejects exhaustion explicitly. It grows two seconds per raw arrival
and can exceed business time substantially. This is an MVP transport bound, not
an unlimited production clock contract. No engine capability was added.
