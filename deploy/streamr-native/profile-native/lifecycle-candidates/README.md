# Remaining profile lifecycle: concrete native attempts

Prepared against Streamr `a5d28ad8` in ARC-18 on 23 October 2026. These are
standalone planner/runtime candidates, not validated production components. Render
`{{SOURCE_OPTIONS}}` and `{{SINK_OPTIONS}}` exactly as the existing core renderer
does. No engine build/run was performed by this investigation; coordinator receipts
must record the full engine commit, executable digest, rendered query, command,
plan/error, and outputs. Do not publish these partial schemas to the 33-field
consumer.

## Session publication boundary

`session-output-boundary.sql` contains the ordinary native SESSION query with
FIRST_VALUE ordered implicitly by event time. It exposes the earliest/latest
observed timestamps and count without inventing an active flag. Feed
`session-input.jsonl` in three committed source prefixes. The independent
`session-prefixes.json` describes expected finalized output and the distinct live
application expectations. The final complete row is also in
`session-finalized-expected.jsonl`.

1. The 10:00:00 event opens session-a. Application total_sessions must already be
   1 and current_session_active true. No finalized SESSION row is available.
2. The 10:00:02 event updates that open session. It must not count a second
   session. A finalized SESSION query still has no output.
3. Another person's ordinary 10:30:08 event advances the source watermark past
   10:30:02. SESSION emits the two-event summary. The person's active flag must
   now become false and closed-session average must become 2 seconds.

Do not finish the finite source before inspecting prefixes 1 and 2: EOF would
close the session and conceal the missing live publication. Do not add a clock
source, fabricated source timestamp or cursor/timer state machine.

Source inspection: `crates/arroyo-worker/src/arrow/session_native.rs`
`process_batch` (line 469) admits/updates retained state without a Collector;
`handle_watermark` (line 765) closes due sessions, calling the output helper
which collects at line 751. No early/updating SESSION or EMIT CHANGES path was
found in the current planner/worker/RPC search. Native closure -> MERGE is now
supported; it supplies the **third** step, not the first two.

[Upstream SESSION documentation](https://doc.arroyo.dev/sql/windows/) describes
inactivity windows; [updating-table documentation](https://doc.arroyo.dev/sql/updating-tables/)
distinguishes watermark-completed window results from continuously updated
unwindowed aggregates. Neither supplies documented live SESSION output. An
unwindowed grouping by session_id could update immediately, but would merge
reused IDs forever, whereas SESSION separates them after inactivity. That is
not an equivalent implementation. Likewise deriving active from a timestamp
comparison in a per-person aggregate does not by itself revisit quiet people
when another person's events advance time.

This is a concrete missing **expression of required behavior** in the current
integration; actual prefix receipts are still required before declaring a newly
proven engine deficiency. If no existing native route can expose an open SESSION
relation, the smallest generic contract to discuss is an updating SESSION view
(open membership, earliest/latest timestamps, and closure transitions), including
late bridges/retractions and checkpoint semantics. This is a proposal for user
discussion, not authorization to add syntax or engine behavior.

## Closed metrics and calendar composition

`closed-calendar.sql` tests finalized SESSION -> COUNT/SUM and calendar FILTER ->
MERGE. It deliberately calls these **closed_sessions**, never total_sessions or
sessions_1d: the application fields count starts before closure. It groups
session starts by their original start date; it does not use closure date as the
contribution date. The two-event input closes with count 1, all four closed
calendar counts 1, and average duration 2 seconds.

EVENT_DATE must retain a valid current-trigger meaning through this producer.
A planner rejection is evidence of this exact composition only. A passing
same-day fixture does not establish replay/midnight behavior. Further required
case: start 23:59:59, end 00:00:01, ordinary progress after 00:30:06; the contribution
belongs to the prior day, while the reference day is the relevant stream clock.
Do not replace EVENT_DATE with the session's start/end date, MAX event time,
watermark, or wall clock merely to make planning succeed. A two-day quiet-key
case must show native calendar expiry and lifetime closed count retention.

## Publication metadata

`publication-metadata.sql` isolates COUNT -> MERGE -> RETURNING old/new/action.
It computes exact differences for the two stored diagnostic fields, including
NULL-safe comparison. It also probes original trigger EVENT_TIMESTAMP after the
maintained aggregate/RETURNING boundary; there is no MAX-time surrogate.

For person-a's first two events, expected diagnostic changes are:

- first: action=create; changed_fields=[tenant_id, canonical_id, total_events,
  last_seen]; total_events=1; last_seen=1580551200000;
- second: action=update; changed_fields=[total_events, last_seen]; total_events=2;
  last_seen=1580551202000.

The candidate event updated_at values are those same source milliseconds, but
only if original trigger context actually survives. This diagnostic create list
covers its four fields; it is not the full consumer create-list contract.

No full metadata policy is silently chosen here. Historical Flink used wall
clock for updated_at/timestamp and current-session duration; current native
replay requirements forbid assuming that policy remains correct. Existing
profile-native guidance explicitly calls publication semantics unfinished.
Questions still requiring an explicit contract:

- Does live duration mean event span, stream-reference time minus start, or
  publication observation time? Those differ during replay and quiet intervals.
- Should updated_at/timestamp describe original event trigger, closure boundary,
  calendar expiry boundary, or observation? A mixture must have explicit rules.
- What trigger label describes a recomputation that changes several branches?
  Do not infer a timer cause from changed values or fill every row with 'event'.
- Is changed_fields relative to previous native committed output, or an older
  debounced publication? Native SQL can compare OLD/NEW; restoring the removed
  custom debounce algorithm is not authorized. Full-field differences still need
  native array comparisons without storing arrays in primitive state tables.

Thus action/changed-fields have a concrete native composition attempt. The
remaining time/trigger policy is application-contract work, not evidence that
SQL cannot implement a settled requirement.

## Application time decision, 23 October 2026

The user confirmed that wall-clock time is incorrect during replay. Session
duration and application timestamps must use stream event time in both replay
and live processing; wall-clock time is reserved for operational logs and
processing metrics. This decision supersedes the clock-policy uncertainty above.
The runtime probes still must establish original-trigger context through native
aggregates and MERGE. Quiet-key calendar/closure publications must carry a defined
stream reference; a window boundary or watermark is not an automatic substitute.

## Explicit finalized-window projection attempt

The original `closed-calendar.sql` was actually rejected on a5d28ad8 with
`unsupported clock-dependent window aggregate: calendar FILTER requires a
maintained non-windowed aggregate`. Preserve that failed source and receipt.

`closed-calendar-projected.sql` adds a native relational projection dropping the
complete `window` struct before the outer calendar aggregate. Its scalar
`start_ts`, `start_ms`, `end_ms` values and every expected result are unchanged.
Current source `crates/arroyo-planner/src/plan/mod.rs` lines 176–189 explicitly
ends window scope at this projection and documents finalized results as ordinary
append relations; `plan/aggregate.rs` lines 580–591 otherwise rejects calendar
FILTER under inherited window scope. This is the existing native relational
boundary, not recomputing session membership, stripping update metadata or
changing event-clock meaning. It still requires actual planner/runtime validation
of original-trigger EVENT_DATE lineage and quiet-key calendar expiry.

The projected attempt was also actually rejected on a5d28ad8, now with:

```
EVENT_TIMESTAMP()/EVENT_DATE() requires current-trigger clock metadata across maintained aggregates (STR-62); aggregate output time is not an event clock
```

The exact receipt is
`/home/jason/qa-evidence/arc18-readiness-20261023/lifecycle-closed-calendar-projected-plan/query-validation.json`.
This proves the first window-scope restriction was removed by the ordinary
projection, but the subsequent calendar aggregate still cannot bind an original
trigger clock through finalized SESSION. `rewriters.rs` lines 852–922 recognizes
special current-trigger routes for accepted updating aggregates and response
joins, but a finalized AggregateExtension still marks aggregate lineage and is
rejected by the clock rewriter. Native SESSION's emitted internal timestamp
(`session_native.rs` lines 719–726) is session end plus gap minus one nanosecond;
it is not the source event that advanced progress. Do not use it as a substitute.

Capturing EVENT_DATE before SESSION and retaining FIRST/LAST would preserve a
contribution's own date, not the later stream reference needed for quiet-key
expiry. Grouping completed results by that captured date does not solve the
same requirement. Upstream's documented window/updating split does not establish
original-trigger metadata propagation through finalized windows either.

This is a demonstrated limitation of this specific native calendar-after-SESSION
query. It does not alone prove that every possible MVP session-start counting
composition requires the same route. No new engine feature or time semantics
have been implemented. A generic contract proposal, if this route is required,
is to preserve and checkpoint explicit original stream-progress trigger context
across finalized-window emission and make it available to downstream maintained
calendar aggregates, with exact behavior for several windows closed by one
progress event. This must be agreed before implementation; using a window
boundary or maximum contribution time instead would change the reference clock.

## Reduced native array failure

`empty-case-array.sql` reduces the publication failure to a single source event
with `event_id='insert'`. Expected output is exactly
`{"tenant_id":"tenant-a","canonical_id":"person-a","changed_fields":["created"]}`.
The unused ELSE contains ordinary `array_concat`. Actual current-source runtime
case `empty-case-array-1` fails with `Compute error: concat requires input of at
least one array`. No aggregate, state table, SESSION or UDF is involved.

The pinned dependency is Arroyo DataFusion commit
`916b45f5c28d94765ae4a6393c5e126b2ea55e1c`.
Its [concat implementation](https://github.com/ArroyoSystems/arrow-datafusion/blob/916b45f5c28d94765ae4a6393c5e126b2ea55e1c/datafusion/functions-nested/src/concat.rs#L354-L434)
accepts typed List input with zero rows, then calls Arrow concat with no arrays.
CASE evaluates the unused branch over an empty selected batch. This is a generic
native expression failure, not a reason to replace changed-fields SQL with an
application UDF or another expression chosen to conceal the defect.

Smallest proposed generic repair: preserve the List/LargeList datatype when
returning an empty result for zero selected rows. Regression coverage must include
an unused CASE branch, an empty batch and typed null lists. This is a proposal;
no dependency or engine modification is included in Arcstream.
