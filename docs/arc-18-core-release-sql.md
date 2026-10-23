# Native first-release producer

The user approved core profile metrics and completed-session average, deferring
seven immediate session-start/live fields. They also approved omitting four
unused publication fields (`updated_at`, `changed_fields`, `timestamp`,
`trigger`) from this release. The producer/consumer contract therefore contains
22 fields: the already exercised 20-field core, nullable fractional
`avg_session_duration_sec`, and native `action` (`create`/`update`).

`deploy/streamr-native/profile-native/release-fields.json` records the exact
active and deferred fields. No absent session or metadata value is fabricated.
The average is NULL before any completed native SESSION exists; afterwards it
is total completed duration in milliseconds divided by completed count and
1000.0, computed as DOUBLE. It must not truncate fractional seconds or include
an open session as a completed zero-duration sample.

## Native query and graph

`profile-native/release.sql` retains the existing core's native lifetime,
calendar, page-count and feature-count relations. A native SESSION branch
computes completed duration, then native COUNT/SUM computes the current average.
The active query composes four native branches through the existing accepted
MERGE result boundary: core, page counts, feature counts and closed-session
average. The finalized branch acknowledges raw events without emitting a
session; actual native watermark closures independently update its average.
`MERGE RETURNING source` supplies all 21 business fields, including native
array values, and the SQL projection maps native insert/update action to
create/update. The Kafka sink uses plain JSON with exactly 22 fields. The
transport adapter validates that snapshot without computing business values or
inferring an action. There is no CDC fallback or suppression of delete records.
The earlier JOIN/CDC query is retained as `lifecycle-candidates/join-cdc-release.sql`.
The repaired engine composition and this query pass the scoped Kafka/RocksDB
fixture described in [publication recovery evidence](arc-18-publication-recovery-20261023.md).
A closure must publish the entire profile including the new average without
waiting for another event belonging to that person.

The declared event-time admission remains the payload timestamp minus exactly
five seconds. FIRST/LAST ordering uses the implicit raw WATERMARK FOR column.
Calendar FILTERs remain on the original-event core relation, before the SESSION
union. No calendar function is called on a finalized SESSION result. No original
trigger timestamp is invented from SESSION's closure timestamp, wall clock,
watermark progress or maximum observed time. `first_seen`/`last_seen` retain
their actual business meanings as earliest/latest accepted profile events.

Render the production-shaped five-query graph without submission:

```sh
python3 deploy/streamr-native/prepare.py --output /tmp/arc-native-release \
  --brokers owned-broker:9092 --namespace owned-arc --profile-release
```

This emits identity, unified events, identity merges, completed sessions and
22-field profiles. The profiles topic is `owned-arc-profiles`; it is not fed by
the partial 20-field diagnostic producer. Default preparation keeps the prior
diagnostic graph for focused qualification. Every Kafka source is read-committed
and every sink uses Streamr's public exactly-once recovery-marker contract.
Recovery-marker topics must be preprovisioned with one partition and compact-only
cleanup. Preparation never creates topics, submits jobs or deploys software.

## Qualification fixtures

`test/streamr-reference/profile-native/release/` contains eleven inputs and
literal cumulative, complete 22-field outputs. It covers all four event types,
ranking changes, out-of-order admission, nonblank event-time metadata selection,
late rejection, cross-tenant isolation, two native session closures and quiet
calendar expiry. The first completed span is 5 seconds; the second is 2.5,
so the second average is exactly 3.75. The oracle was prepared independently of
engine output, reusing the existing literal mixed-core oracle for its first
five snapshots. Its README states the exact per-prefix expectations.

Use the maintained shared run queue and the owned Kafka fixture wrapper with
these explicit input/expectation files. The intended recovery point is after
input 1: a nonempty committed create plus open SESSION state. A real worker
SIGKILL must restore both and subsequently produce the fresh 5.0/3.75 closure
results, with the old committed Kafka prefix unchanged. Require RocksDB for all
native aggregate/window/typed owners. The coordinator records actual owner
counts, pinned source/executable hashes, queue command, output and restoration
receipts; no runtime pass is claimed merely by preparing these files.

## Scope of remaining Streamr work

Open SESSION publication and original-trigger clocks after finalized windows
remain separate Streamr follow-ups. They are not dependencies of the explicitly
reduced application contract above. The array-concat empty-CASE repair remains
a generic engine correction; this release does not conceal it with another
expression, and its profiles query has no changed-fields expression requiring
that capability. Production cutover remains separate from integration readiness.

## Preserved four-branch composition rejection

The first native attempt placed the completed-average relation alongside core,
page and feature branches in the tagged UNION, with the existing outer aggregate
and typed MERGE. Actual current-source planning on Streamr a5d28ad8 rejected it:

```
composed result supports one to three registered native aggregate branches
```

The full original query is retained as
`profile-native/lifecycle-candidates/four-branch-release.sql`; exact rendered SQL
and planner receipt are in
`/home/jason/qa-evidence/arc18-core-release-20261023/profile-release-plan/`.
This is a concrete composition limitation, not an application semantic decision.
The current LEFT JOIN candidate uses the existing native relational operator to
combine independently maintained current relations. It does not regroup unrelated
categories merely to bypass the branch limit. Its acceptance, complete emitted
CDC images, application decoder compatibility and recovery still require runtime
validation. In particular, a correct final relation alone does not establish that
intermediate published images satisfy the application contract. No intermediate
CDC record may be discarded to disguise incorrect or incomplete outputs.

The first JOIN candidate used `core.*` and a final `SELECT *`. Actual planning
rejected its INSERT with `Column count doesn't match insert query!`; receipt:
`/home/jason/qa-evidence/arc18-core-release-20261023/profile-release-join-plan/`.
That exact candidate is preserved as `lifecycle-candidates/join-wildcard-release.sql`.
The active query now explicitly selects the 21 declared business fields at both
boundaries, avoiding accidental selection of internal relation fields. This is
an ordinary SQL projection; it does not discard CDC records or change a metric.
The public application adapter derives action from the standard CDC operation,
so the complete API object still contains 22 fields. Actual acceptance remains
bound to the subsequent coordinator receipt.


The explicit-field JOIN candidate successfully compiled on the pinned Streamr
`a5d28ad868f8e64cd3d2daa158b12e11b52e3c03` executable. Its exact validation
receipt is
`/home/jason/qa-evidence/arc18-core-release-20261023/profile-release-join-explicit-plan/query-validation.json`.
This establishes planner acceptance only. Runtime image completeness, exact
metrics, all intermediate CDC operations, Kafka transactional continuity and
actual process-loss recovery remain required. The generic four-branch limitation
is tracked separately as STR-150 and is not itself a release blocker for this
accepted native JOIN expression.

## Initial full-graph configuration failure

The first full-graph launch (`graph-1/runtime/`) stopped during worker admission,
before input events, with `[join_29] RocksDB SQL does not bound retained state
for Join`. Its effective config omitted `worker.join-state`. This is an
application configuration omission, not proof of an unsupported JOIN or STR-65
being required. The RocksDB guard accepts the shared `OperatorName::Join` only
when the serialized operator is updating and valid bounded join limits exist
(`crates/arroyo-worker/src/engine.rs`, `native_updating_join_enabled`). The
accepted non-windowed LEFT JOIN already follows the planner's updating route;
non-updating unwindowed LEFT is rejected by that planner.

The same missing configuration was investigated on STR-65 on 18 October. Its
successful startup used the existing eleven-key `JoinStateConfig`: retained
rows 100, probe rows 10, key bytes 256, value bytes 8192, page bytes 32768,
page entries 1, write bytes 65536, write operations 8, overlay bytes 65536,
pending output bytes 65536 and resident bytes 8388608. The current source keeps
that public contract. Supplying these ordinary bounded allocations preserves
RocksDB, watermark semantics and admission checks; it adds no engine feature.
Actual row-size and shared-pool admission must still pass the subsequent run.

The accepted profiles plan contains five UpdatingAggregate owners (lifetime,
page counts, feature counts, combined core, completed average), one native
SESSION owner and one native UpdatingJoin owner: seven disk owners and zero
typed-table owners. Its configured eight database slots cover that inventory.
The existing aggregate store checks encoded-key/value page and write limits,
queued-write reservation, scan workspace and decoded scope headroom before
execution. A configured join scope requires 65536 + 3 * 8192 = 90112 decoded
bytes, below the existing 32 MiB shared decoded allocation; its page and write
reservations likewise fit existing shared pools. This arithmetic is an admission
precheck, not evidence of successful application execution or a capacity claim.

The [upstream Arroyo 0.9 release description](https://www.arroyo.dev/blog/arroyo-0-9-0/#joining-update-tables)
also documents joining updating tables using ordinary SQL LEFT JOIN. Current
Streamr `docs/updating-joins.md` narrows its bounded native implementation and
explicitly warns that independent branch replacement/retraction transitions can
be published separately. Configuring its existing limits does not establish
application publication coherence; the runtime oracle must still check it.

## Shared write allocation for seven native owners

The correctly join-configured `graph-2` attempt then failed on
`queued_write_bytes` (33554432 bytes). Its exact effective configuration and
runtime error remain under
`/home/jason/qa-evidence/arc18-core-release-20261023/graph-2/`.
This is a second distinct application allocation failure, not evidence of a SQL
feature gap. No output is treated as a passing application result.

`crates/arroyo-state/src/live/write.rs` defines the declared write reservation:

```
reservation(write_bytes, operations)
  = 5 * write_bytes + operations * (sizeof(WriteOperation) + 32)
```

`crates/arroyo-worker/src/arrow/aggregate_store.rs::begin_scope` reserves the
configured maximum with `try_reserve`, not just the small event's encoded bytes.
Its constructor checks admission of one owner's scope; that does not promise
that all seven owners fit concurrently. The five aggregate scopes each declare
2 MiB and 128 operations, SESSION declares 512 KiB and 64 operations, and JOIN
64 KiB and eight operations. Their complete simultaneous write bound is:

```
5 * reservation(2097152, 128)
  + reservation(524288, 64)
  + reservation(65536, 8)
= 55377920 + 712 * (sizeof(WriteOperation) + 32) bytes
```

The previous 32 MiB pool is smaller than the byte term alone. The next owned
run allocates a finite 96 MiB (`100663296`) shared queued-write pool, preserving
all per-owner limits, RocksDB and existing admission failure behavior. It does
not change source pacing, watermarks, SQL or retention. Backend admitted-write
commit transfers the scope's reservation rather than introducing another full
copy reservation (`AggregateScope::commit` and `RocksLiveState::write_admitted`).
This is resource planning for the actual graph, not a throughput regression or
production capacity claim.

Other retained shared allocations are decoded values 32 MiB, scans 4 MiB,
RocksDB shared block cache 8 MiB, memtable 4 MiB and eight open databases.
The checkpoint merge-page calculation at eight database slots yields
`4194304 // (8 * (8 + 1)) - 32768 = 25486` bytes, enough for two configured
8192-byte rows plus 1024 bytes overhead. No row bound or admission rule is
removed. Decoder/runtime checks must still prove the actual fixture's rows and
publication semantics fit.

The bounded allocations are tracked in
`deploy/streamr-native/runtime-resources.json`, copied from the effective
`graph-3/fixture-config.json` resource sections. The maintained graph fixture
recursively merges them over its base configuration and records the source hash,
allocation content and effective configuration. Owned disk-state and catalog
paths remain isolated. The resource file contains no connections or credentials.
The legacy Docker Compose graph is not a native production deployment; these
qualification allocations and image publication do not authorize cutover.

These are finite qualification-fixture allocations. In particular, JOIN admits
at most 100 retained row identities and ten matches per probe; this does not
qualify 100 customers or unrestricted production cardinality. Its 8 MiB
resident allocation is separate from the shared cache/write/decoded pools.
Increasing these limits for production requires resource accounting and
application evidence, not reusing a small-fixture pass as capacity approval.
Typed state limits remain for the identity graph; profiles itself owns no typed
table after choosing native JOIN. No production cutover is performed here.

## Actual first-event publication failure

With the bounded JOIN configuration and 96 MiB write allocation, `graph-3`
started the native graph and processed the first event. The committed profiles
capture (`graph-3/runtime/capture-5-profiles.json`) contains create with
`top_features=[]`, delete of that image, then create with
`top_features=['search']`. The application correctly rejects the delete; the
first incomplete create also fails the independent complete-event oracle.
No record was filtered or treated as an invisible implementation detail.

Current `UpdatingJoin::process_batch_index` applies each input retract/add
separately. `ToDebeziumStream::as_debezium_batch` in planner `physical.rs` groups
same-identity changes only within one RecordBatch. Because the JOIN publishes
those replacement changes in separate batches, the sink emits `d`/`c`, not
one `u`. Adding a Kafka key or changing the JSON formatter does not establish
whole-event coherence across independently maintained branches.

`lifecycle-candidates/join-merge-release.sql` is the explicit native keyed-MERGE
materialization attempt. It preserves the JOIN and applies the existing primitive
snapshot schema, passing arrays through `RETURNING source`; it does not store
serialized arrays or discard retractions. The baseline accepted-JOIN MERGE route
is narrow: two simple COUNT/MAX branches sharing one original admitted source;
its planner also rejects calendars and unsupported nested lineage. That baseline probe failed as recorded below; it is superseded by the native
four-branch materialization repair. Neither this preparation nor an eventual planner-only pass
would establish successful runtime publication.

## Keyed materialization attempt

The runnable `lifecycle-candidates/join-merge-release.sql` attempts to materialize
the native joined relation through existing keyed MERGE. On a5d28ad8,
`run-plan.py lifecycle-join-merge-release` rejects it with:

```
per-event aggregate MERGE does not support joins, unions or nested maintained dependencies
```

Exact request, validation and source receipt are preserved under
`/home/jason/qa-evidence/arc18-core-release-20261023/lifecycle-join-merge-release-plan/`.
STR-150 records this composed-result publication gap and proposed native
materialization contract. No engine extension, record filtering or consumer
semantic workaround has been implemented. Application integration and recovery
remain unqualified for this release query.

## Publication repair candidate

The current release query is the four-branch native materialization candidate,
not the failing ordinary JOIN query. The first 22-field literal fixture and
complete-prefix assertions are unchanged. The graph oracle validates every
native create/update snapshot and compares all resulting images, without
filtering duplicates or intermediate rows. Profiles now owns five native
aggregate backends, one native SESSION backend and one typed state-table owner;
the existing eight database slots still cover this seven-owner graph.

Repair worktree: Streamr `str-150-composed-publication-20261023`, fetched base
a5d28ad8. Application repair worktree starts from fetched cbcb901b with existing
reviewed PR head 6a13f63 incorporated by fast-forward. Runtime results and exact
new source pin will be recorded after execution; neither compilation nor source
review alone establishes successful application/recovery behavior.
