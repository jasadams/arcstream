# Streamr reference preparation

Run the current production Flink functions against independent reference assertions before
using them to qualify Streamr. This is preparatory coverage for ARC-14/15/16 and STR-28/32.
It does not submit jobs, connect to Kafka, or change canonical output topics.

## Run

From the repository root, with Python 3 and Podman:

```sh
bash test/streamr-reference/run.sh
```

The runner uses a pinned Maven/JDK 17 image, the existing Flink 1.20.1 test dependencies,
and a public-dependency cache under ignored `target/reference/maven-cache`. Set
`CONTAINER_ENGINE=docker` to use Docker, or `REFERENCE_MAVEN_IMAGE` to choose another
JDK 17 Maven builder. The selected image, source file hashes, Git revision/status, captured
output hash and logs are recorded under `target/reference/`. Network access is used for
public Maven dependencies; these tests exercise local keyed operators.

## Identity fixture and comparison

`flink/identity-resolution/src/test/resources/reference/identity-input.json` contains
ordered raw-event steps and a checkpoint/reconstructed-operator step. The test runs the
production tenant-keyed `IdentityResolutionFunction` and captures its main and merge side
outputs. The independent `identity-expected.jsonl` oracle covers 13 unified events and
two directed merges, including anonymous repeats, null/empty user IDs, first login,
known-user/new-device lookup, conflict merge, tenant separation and linking after recovery.

Actual UUID-bearing records are exported to
`flink/identity-resolution/target/reference/identity.jsonl`. Each line has this envelope:

```json
{"stream":"unified-events","payload":{"event_id":"...","tenant_id":"...","canonical_id":"..."}}
```

Merge records use `stream: identity-merges` and the production merge payload. The Streamr
capture adapter emits these envelopes after executing the same input steps; it preserves
fields and separate merge outputs, including checkpoint/replay boundaries.
For a Kafka-only replay, publish the `payload` of event steps, retaining order, and implement
the snapshot/restart step in the evaluation harness rather than publishing it as an event.

Compare a candidate capture:

```sh
python3 test/streamr-reference/compare_identity.py \
  flink/identity-resolution/src/test/resources/reference/identity-expected.jsonl \
  /path/to/streamr-identity.jsonl
```

Canonical IDs are normalized by first appearance in the oracle's event order, separately
for each tenant. Candidate events are matched by `(tenant_id, event_id)` so output
interleaving does not change identity labels. Raw candidate IDs must remain distinct
across tenants, consistent with the current global UUID identity contract. The comparator
checks every payload field exactly, including explicit nulls and properties, and checks
merge direction and multiplicity. It rejects missing/duplicate events, lost fields,
wrong identity links and missing/reversed/duplicate merges. It does not silently coerce
types, remove timestamps or ignore extra business fields. Eleven comparator tests inject
these failures; comparison against only another engine is insufficient.
This comparison proves record contents and identity equivalence; it intentionally ignores
emission order, including merge-versus-unified ordering. Delivery/order guarantees require
separate replay and sink tests.

## Profile and session reference coverage

Production-function tests cover counter updates and last-nonempty metadata, event-time
session deadlines, checkpoint/recreated-operator recovery, the profile's five-second
processing-time debounce, and restored 1/7/30-day decay timers. Session tests distinguish
processing time from watermark advancement, ensure extended deadlines replace earlier
ones, and verify closed state does not emit again. Assertions check business results
rather than reproduce a separate implementation. Profile times are anchored to yesterday
at noon UTC to remain inside the production 91-day wall-clock clamp; volatile output
fields are checked against measured emission clock bounds by the profile comparator below.
The session comparator below checks fixed event-time outputs without clock normalization.

### Profile capture and strict comparison

`profile-input.json` drives the production `ProfileFunction` through four events, a
five-second debounce, a 30-minute session timeout, and 1/7/30-day idle decay. It restores
three snapshots into fresh operators before the pending debounce, session and decay
timers. No event arrives after the four initial events. The independent
`profile-expected.jsonl` asserts six emissions and every one of the 33 payload fields.
Steps immediately before debounce and timeout must emit nothing.

The runner exports `flink/identity-resolution/target/reference/profile.jsonl` and the
resolved `profile-fixture.json`, then runs:

```sh
python3 test/streamr-reference/compare_profile.py \
  flink/identity-resolution/src/test/resources/reference/profile-expected.jsonl \
  /path/to/profile-capture.jsonl \
  /path/to/profile-fixture.json
```

Candidate captures must use `stream: profile-updates`, the fixture's emission `step`,
`payload`, and `captured_from_ms`/`captured_to_ms` measured by the harness around each
emitting action. Replay the resolved fixture to share the event-time anchor. Advance
processing time independently of watermarks and restore at every snapshot step; do not
inject synthetic activity to make idle timers fire.

Numeric fields must be integers, arrays must be native arrays of strings, and the active
session flag must be a boolean. Only `changed_fields` order is treated as a set; its
membership and uniqueness are exact. Top-K lists have no ties in this fixture and retain
their order. Emission steps, triggers, counters and metadata must match exactly. Relative
event timestamps resolve against the exported anchor. `updated_at`, formatted UTC
`timestamp` and active session duration must fall within harness-measured wall-clock
bounds; none is dropped. The active duration intentionally characterizes the current
Flink wall-clock calculation. An event-time duration change needs explicit acceptance.

This small fixture does not qualify high-cardinality top-K bounds, tenant collisions,
late events, historical replay, Kafka delivery or a Streamr timer implementation.
[Profile capability handoff](profile-capabilities.md) identifies the remaining STR-29
work. All 16 Flink tests and 40 Python tests pass, including the six captured profile
updates compared against the independent oracle.

### Session capture and strict comparison

`session-input.json` uses fixed historical event times (2026-06-01 noon UTC) and two
session IDs across two tenants. It extends both deadlines, restores into a fresh operator,
checks silence at the canceled deadlines and immediately before each valid deadline, then
closes sessions with watermarks. Advancing processing time alone must emit nothing. Two
more restores cover one closed/one pending session and all closed state. Reusing a closed
session ID with a new canonical ID must start fresh state and produce a third summary.

The runner exports `flink/identity-resolution/target/reference/session.jsonl` and resolved
`session-fixture.json`, and compares the actual output with the independent
`session-expected.jsonl` oracle. Compare a candidate using:

```sh
python3 test/streamr-reference/compare_session.py \
  flink/identity-resolution/src/test/resources/reference/session-expected.jsonl \
  /path/to/session-capture.jsonl
```

Each line has `stream: session-summaries`, the emitting fixture `step`, and `payload`.
Replay the exported resolved fixture, including separate processing-clock/watermark
controls and snapshot steps. All 12 payload fields and their types must match. `pages`
is an unordered distinct string set, serialized as a native array; `event_types` is a
native object of exact integer counts. Only page order and cross-key ordering within the
same emitting step are ignored. Emission steps, duplicate/missing closures, first-arrival
start, maximum event-time end, duration, ownership and last-nonempty arrival metadata stay
exact. No output timestamps are dropped or normalized.

The older arriving event deliberately preserves the first-arrival start and changes
last-nonempty metadata; it is not a post-watermark late-event test. Distinct tenant/session
IDs avoid the production cross-tenant key collision. This is operator-state recovery,
not source offsets, sink commits, process crash or all-idle Kafka qualification.
[Session capability handoff](session-capabilities.md) records the SQL session semantic
differences and remaining STR-20 work. The complete runner passes 17 Flink tests and 56
Python tests, plus all three independent output comparisons.

Some tests deliberately characterize existing behavior that needs a contract decision:

- Profile keys are `canonical_id` alone and session keys are `session_id` alone. Reusing
  either across tenants collides in the current jobs. The normal identity path generates
  unique canonical UUIDs; externally supplied/replayed keys still need explicit guarantees.
- Session start is the first arriving event, while end is the maximum event time. An older
  event arriving afterward can leave duration zero despite earlier historical activity.
- A late profile event preserves maximum `last_seen` but currently rearms the timeout from
  its incoming timestamp, moving the deadline earlier.

These assertions preserve evidence of the current reference; they do not approve those
behaviors as Streamr acceptance. STR-28 and ARC-15/16 must document fixes or supported
input assumptions explicitly. Set-like page outputs and tied top-K ordering need defined
comparison semantics before broader profile/session capture comparison.

## Remaining qualification

The snapshots here are Flink keyed-operator harness snapshots restored into new operator
instances. They do not establish Kafka offset/sink consistency, remote checkpoint durability,
RocksDB memory bounds, process crash recovery, partition idleness or a full replay.
The identity SQL capture below executes the same fixture through actual Streamr operators.
Next, execute the profile and session fixtures through Streamr once STR-29/20 supply
their required capabilities, then run the STR-32 backfill, hot-key,
larger-than-RAM and 24-hour live/fault qualification in isolated outputs. Milestone 1/2
review and state-semantics acceptance remain prerequisites for migration.

## Actual Streamr identity SQL capture

`identity-capture.sql` implements the full current identity contract through a linear
projection/CTE chain. It reads both maps, chooses the existing user identity before the
anonymous identity, writes the anonymous binding, and conditionally creates a missing user
binding. It forwards all 17 unified-event fields and captures the old anonymous identity
when a real merge occurs. Tenant keys include a length-prefixed tenant component rather
than a delimiter-only concatenation. UUID generation occurs before stateful execution.

The milestone 2 candidate includes one ordered owner for chained state projections and
guarded conditional calls. This capture requires that implementation and the opt-in
`arcstream_identity_capture` hook in `arroyo-sql-testing`; the older STR-1-only branch is
insufficient. The hook is ignored during normal tests and requires explicit file paths.
It asserts one source, one singleton state owner, and actual resolved backend/mode.

Run from this Arcstream worktree, using an isolated Streamr worktree containing the hook:

```sh
export STREAMR_BUILD_WRAPPER=/home/jason/repos/streamr/scripts/rust-build
export STREAMR_DEV_IMAGE=9bbf20dad97b162f3c1d82fdfae020d344436b69cb66c8e06b804c6001241f1a
# Optional warm target; builds sharing this target must use the same cooperative queue.
export STREAMR_CAPTURE_TARGET=/path/to/warm/streamr/target
bash test/streamr-reference/run-streamr.sh /path/to/streamr controller
bash test/streamr-reference/run-streamr.sh /path/to/streamr leader
bash test/streamr-reference/run-streamr.sh /path/to/streamr memory
```

The image ID above is the verified local Bookworm/Rust 1.96 builder. On another machine,
build the repository's prescribed Bookworm image and set `STREAMR_DEV_IMAGE` accordingly;
the runner explicitly invokes `cargo +1.96.0` and records the immutable image ID. Set
`STREAMR_BUILD_WRAPPER` to your cooperative `scripts/rust-build` wrapper. Public Cargo
dependencies are cached under ignored `target/capture-build`; no application database,
Kafka broker, or production service is required.

For each mode the hook captures an initial 13-row run, then starts another execution,
checkpoints after event 10 at epoch 41, verifies published local checkpoint metadata,
cancels all worker tasks, constructs a fresh restored program, and processes the remaining
three events. The test file sink restores its checkpoint byte offset. Each run uses fresh
RocksDB attempt directories; local working-state reuse is not the recovery source.
`streamr_capture.py` only envelopes actual sink fields and converts SQL-produced merge
metadata into directed merge records; it does not simulate identity resolution.

Both initial and recovered captures must match the independent oracle exactly after UUID
normalization. Raw sink rows, normalized envelopes, comparison results, query/input,
operator/checkpoint logs and provenance stay in `target/streamr-identity/<mode>/`.
This is a small worker-task cancellation/local durable restore fixture. It does not prove
process-crash recovery, remote object storage, post-checkpoint speculative output rollback,
Kafka delivery guarantees or larger-than-RAM throughput. It provides executable Arcstream
evidence for the shared-map/conditional path without closing the broader STR-3/4 acceptance.

Local verification on 2026-10-03 passed in all three modes:

| Backend / checkpoint protocol | Initial capture | Recovered capture at epoch 41 |
| --- | --- | --- |
| Memory / controller | 13 unified events + 2 merges | 13 unified events + 2 merges |
| RocksDB / controller | 13 unified events + 2 merges | 13 unified events + 2 merges |
| RocksDB / leader | 13 unified events + 2 merges | 13 unified events + 2 merges |

Every capture matched the independent oracle, preserving all fields and merge direction.
The planned graph contains a file source, ordinary projections/watermark generation,
one singleton stateful owner, and a file sink; it uses no join or aggregate/window state.
The capture hook is stacked on milestone 2 revision `fb01f5e9`; exact tested revisions and
artifact hashes are recorded by the runner. The 15 Flink tests and all 19 Python
comparator/adapter tests also pass. This table is correctness evidence for the small
identity fixture, not a capacity or full migration claim.
