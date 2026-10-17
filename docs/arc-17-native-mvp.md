> Historical ARC-17 record. The UDF-backed graph and its deployment tools were
> removed under ARC-18. Commands and acceptance claims below describe the old
> implementation and must not be used as current run/deployment instructions.
> See [current native status](arc-18-native-status.md). Source-bound evidence is
> retained; legacy Flink behavior does not override native product semantics.

# ARC-17 native MVP integration contract

## Provenance and status

Application base: `c7f477799ef7fb716a4b8d3917a671db210deeba`, fetched default
branch, isolated worktree `/home/jason/repos/arcstream-worktrees/arc-17-native-mvp-20261017`.
Guidance read: `CLAUDE.md`, `.claude/build-test.md`, `.claude/team.md` in that
worktree. No `AGENTS.md` was present. Current user instructions control delegation,
retired review signing, source isolation and production authorization.

Streamr integration target: merged PR 69, `ad0a4814d5b7c07d74dce54753f5e6e9aaa7a062`.
Read source-bound `docs/str-126-validation.md` and
`docs/str-126-delivery-receipt.json` at that commit, and evidence under
`/home/jason/qa-evidence/str126-qualification-20261017/`. The receipt identifies
production source `e2ff41e7c991b3e0f0c68bd7d2f874631a00501b` and SQL-test source
`0e63174498ef3adb9848a669c4ea12e3c21110f3`; merged-source identity and executable
identity must be recorded separately. Historical publication-pending wording in
those documents predates the user's confirmation that PR 69 is merged.

Engine qualification is accepted for MVP: 400,000 keys, 1.2 million events,
819,200,000 retained payload bytes, 62 MiB backend budget, approximately 335 MiB
peak RSS below 768 MiB, real worker SIGKILL/recovery, exact outputs and fourteen
additional correctness/recovery cases. Do not repeat that campaign or interpret
incidental throughput under machine load as a regression result. These are engine
results, not Arcstream application acceptance.

## Current application runtime evidence

The isolated eleven-pipeline Kafka graph passed its representative application
and worker-loss gates at Arcstream source `b2fb06e` (identical frozen file hashes
recorded by the run). Evidence:
`/home/jason/qa-evidence/arc17-native-mvp-20261017/kafka-graph-attempt8/`.
The exact maintained-queue invocation is retained in
`kafka-graph-attempt8-invocation.json`, queue job `ad06cb291ec8`.

The run checked thirteen complete unified events, two identity merges, five
active and five closed profiles with all33 fields, four complete12-field session
summaries, and exact CDC before/after chains. Both native profile and session
owners restored checkpoint6 after actual SIGKILL; all fourteen committed topic
prefixes remained unchanged. A fresh fourteenth event retained identity and
produced the exact nineteenth profile update; native flat publication equalled
all nineteen CDC after-images. Actual repeated public submission preserved the
eleven pipeline identities. Engine and isolated broker/network cleanup passed.
This graph used RocksDB. The identity Kafka owner was not killed: separate
native CLI identity recovery passed both backends, and the whole graph checked
canonical continuity through downstream owner recovery.

This runtime pass is bounded application evidence, not production cutover or
universal Java parity. Independent artifact review and required CI are recorded
separately in [the validation ledger](arc-17-native-validation.md). The graph
used the qualified engine executable and the separately pinned repaired generic
Rust UDF plugin source; their identities remain distinct. Prior twelve policy
owner/projector cases used the unchanged business scalars before this transport
revision. The focused current transport feedback owner/projector proof and the
complete Kafka graph establish the new transport path; do not relabel old runs
as executions of the new envelope.

## Reusable application source

| Branch and exact source | Reusable coverage | Limit |
| --- | --- | --- |
| `agent/arcstream-streamr-integration-20261010`, `97fe1b03ecc2eb3ec3d071c68009d7dc23662e39` | Tenant-partitioned anonymous/user state tables; dependent MERGE identity SQL; Kafka capture, unified-event and merge queries; 13-event/two-merge identity oracle | Initial fixture check alone does not exclude extra records or prove recovery; output pipelines commit independently |
| `agent/arcstream-native-profile-session-20261010`, `0077b7acfb67fcb79ccb7dc1c6cd5e1ed55c7c5c` | `profile-session-counts/query-guarded.sql`, 25-field profile fixture; `session-full-cdc/full-session-windowed-plain.sql`, 12-field SESSION output and recovery runners | Historical source qualification; remaining profile lifecycle fields and live Kafka integration unqualified |
| `agent/arcstream-native-consumers-20261010`, `1e394e362e8800d3ca2456db5a3725c78c092e4b` | Configurable event/profile Kafka topics and groups, optional-text normalization, isolated gateway and stored-event comparator | UI feed samples/buffers and is not an authoritative lossless output audit |

These branches were inspected without checkout or mutation. Their historical
receipts are inputs to current-source validation, not replacement evidence.

## Application behavior mapped to native interfaces

| Requirement | Application-owned native pattern | Explicit expected behavior |
| --- | --- | --- |
| Identity | Two `CREATE STATE TABLE` owners partitioned by tenant, source lookup and dependent `MERGE ... RETURNING` | Anonymous first event allocates canonical ID; first login reuses anonymous identity; known user on new device reuses user identity; established user wins conflict and emits directed loser-to-winner merge |
| Input/output schema | SQL source/sink definitions in Arcstream | Preserve all unified event fields, null versus empty text and original properties JSON text; null/empty user ID means anonymous |
| Profile lifetime and calendar | Arcstream scalar transition stored by native STATE TABLE/MERGE | Arrival-first first_seen, maximum last_seen, event-type counters and UTC 1/7/30/90-day daily contributions; live/replay clock policies below |
| Session starts and lifecycle | Arcstream scalar cursor policy, native keyed state and exact-deadline TUMBLE feedback | A,A,B,A,null,empty,A counts three starts; incoming-event timeout, same-ID reuse after closure, generation guards and prior-watermark catchup |
| Ranked collections | Arcstream scalar item-count policy persisted in native state | Top five pages/top three features with explicit deterministic count/key tie ordering |
| Profile updates | Native append outbox and stateless UNNEST projector | All33 fields, exact CDC before/after chains, action/changed_fields, observation metadata and lifecycle triggers |
| Complete session summaries | Separate Arcstream session scalar owner, native STATE TABLE/MERGE, outbox/projector and TUMBLE feedback | Preserve Java late-arrival processing and thirty-minute inactivity, first-arrival canonical/start, maximum end, deduplicated pages, event-type counts and last nonempty attributes |
| State lookups/recovery | Native state-table lookup and checkpoint interfaces | Restored anonymous/user/profile/session bindings must influence fresh events after restart |

The former COUNT/FILTER/calendar/ARRAY_AGG profile queries and complete native
SESSION query are historical diagnostic fixtures, not the selected production
execution path. They remain useful source-bound qualification evidence. Actual
native SESSION probes dropped a late-base event before application processing;
Java processes that arrival and updates its session timer. The selected separate
session owner therefore receives unwatermarked append signals and implements
Arcstream policy with native durable state and existing TUMBLE scheduling. No
application-specific behavior is added to Streamr.

The generated graph currently has eleven pipelines: identity, unified event
publication, profile/session event normalization, profile owner, profile
projector, profile timers, profile flat publication, session owner, session
projector, session timers, and identity-merge publication. Unified publication
also writes an internal execution topic; reserved clock controls enter only that
internal topic. The normalizer writes separate profile and session signal topics.
Both owners emit scalar outboxes; stateless native projectors expand business
outputs and timer seeds, and timer pipelines feed controls back into owner signal
topics. Nonwindow append sources use a nullable watermark expression to avoid a
transport-time admission floor: late arrivals and historical timer feedback must
reach Arcstream policy. The normalizer assigns application timestamps before
any timer-window admission. It parses the original schema event_time explicitly; no
wall-clock or watermark value substitutes for that source field. Exact job names
and sink-marker inventory come from the generated manifest, not this descriptive
list. Connector commits are independent.

Identity merges do not automatically rewrite historical profiles or all other
anonymous aliases in the existing Java policy. Do not invent transitive merging.
Existing Java profile/session keying omits tenant while the native candidates
include tenant. Treat tenant isolation as an explicit application acceptance case,
not an unrecorded parity assertion.

## Event clock and replay policy

At target source, `crates/arroyo-planner/src/rewriters.rs` binds EVENT_TIMESTAMP()
to the current triggering source row timestamp. EVENT_DATE() casts that timezone-free
UTC timestamp to Date32, using floor division even before the epoch. These are
neither wall clock nor lateness-adjusted watermark. Source lineage must remain
unambiguous: the planner rejects absent/multiple triggering sources, overwritten
timestamp metadata, and direct clock projection across a maintained aggregate.

`plan/aggregate.rs` has a specific supported calendar lowering: non-distinct
COUNT/SUM with one clock-independent argument, static conjuncts and one calendar
clause, `DATE = EVENT_DATE()` or
`DATE BETWEEN EVENT_DATE() - INTERVAL 'N' DAY AND EVENT_DATE()`.
The contribution DATE must be independent of the trigger clock. Therefore update
the old WATERMARK_DATE expressions at the base calendar aggregate and retain
their contribution-date expressions; do not add EVENT_DATE to the outer composed
profile projection. Source inspection establishes this contract; application
fixture execution must still establish results and recovery.

Historical replay conflicts, resolved by the explicit live/replay contract below:

- Java profile clamps timestamps more than 91 days old or 60 seconds future to
  wall clock, prunes buckets by today's UTC date, and falls back to wall clock
  after timestamp parse errors. This changes historical replay results. A native
  replay contract should preserve valid input event time and reject invalid time;
  the selected replay mode preserves valid time and rejects invalid time; live
  mode retains the documented clamps.
- Java debounce is processing-time five seconds, while session close/decay uses
  event-time timers. Do not claim identical intermediate emission cadence from a
  maintained SQL snapshot. Specify accepted emission behavior separately.
- Java current-session duration, updated_at and output timestamp read wall clock.
  Deriving them from EVENT_TIMESTAMP would change semantics. Record whether they
  are observational metadata or replay-deterministic application values before
  implementing a replacement.
- Calendar aggregate trigger date and session completion watermark have distinct
  jobs. A thirty-minute delayed watermark must not silently shift a midnight
  profile reference date. Out-of-order and midnight fixtures must make this visible.

## Historical investigation of the remaining eight profile fields

The reused query emits 25 of the Java ProfileUpdate's 33 fields. The remainder is
`updated_at`, `avg_session_duration_sec`, `current_session_active`,
`current_session_duration_sec`, `action`, `changed_fields`, `timestamp`, `trigger`.
Do not populate them with plausible constants to claim parity.

The initial investigation considered these native candidates without engine
changes. The implemented scalar/state/outbox path below supersedes them:

1. Feed closed-session native SQL results into SUM(duration)/COUNT for average
   completed duration; compare integer seconds/truncation with Java.
2. Extend the existing application-owned cursor MERGE to retain session start,
   last event, prior emitted snapshot and closure status. Use RETURNING old/new
   to derive create/update and exact changed-field arrays where supported.
3. Evaluate whether existing native SESSION output and state-table composition
   can reset the profile cursor at timeout and supply session_timeout triggers.
   A closure for a former session must not clear a newer active session.
4. Investigate native SQL and upstream Arroyo patterns for deadline emissions
   and day-decay triggers before proposing any interface extension. The Java
   policy schedules 1/7/30-day decay from last_seen after timeout, but no 90-day
   timer; fixtures must distinguish calendar values from emission scheduling.

These are source-informed candidates, not runnable capability failures. No
remaining capability is declared missing here. For any unsuccessful attempt,
retain exact SQL, source/binary versions, command, planner/runtime error and
expected result; investigate upstream support; then report the precise gap and
smallest proposed contract change to the user before changing engine capability
or application semantics.

## Application acceptance and production separation

Integration readiness requires exact expected representative fixtures for tenant
isolation, identity merge direction, all output fields, UTC midnight, historical
replay, out-of-order input, reused session IDs after closure, null/empty values,
updates/retractions, calendar decay and post-recovery state lookups. CDC checks
must validate complete before/after chains and materialized results, not only
the last row. Scan complete finite outputs to reject extras and duplicates.

Recovery must run through the implemented integration: capture a committed
prefix, interrupt the real worker, restore its checkpoint, append fresh events,
and verify literal identity continuity, unchanged committed output prefix,
expected fresh suffix and final application state. A comparator unit test or
engine-only recovery fixture does not satisfy this gate. Kafka multi-pipeline
commit independence must remain visible in evidence and conclusions.

Record on ARC-17 exact application and engine revisions, executable hashes/build
receipts, queue reservation, commands/configuration, fixture/oracle hashes,
expected and observed outputs, recovery evidence, independent findings/written
verdict and required CI. Run maintained build scripts through the shared queue;
no concurrent heavy engine runs. Historical passing evidence must be labeled by
its actual source. Review signing is retired.

Production acceptance is separate: consumer/storage compatibility, full agreed
profile lifecycle semantics, deployment configuration, operational recovery and
explicit cutover authorization remain gates. Do not deploy, switch traffic, or
claim application parity based on engine qualification or partial fixtures.

## Full33 native-owner implementation candidate

`deploy/streamr-native/profile-mvp/` now contains application Rust scalar policy,
native state-table owner SQL, and native exact-deadline TUMBLE SQL. The scalar
keeps no external durable state: the native state row owns all profile counters,
daily contributions, sessions, ranked item counts, timer generations and previous
emitted snapshots. Full33 raw JSON CDC output preserves exact `before`/`after`
state and create/update actions. This candidate implements the formerly remaining
eight fields; preparation is not current runtime qualification.

The owner now emits a scalar native outbox frame per input. `project.sql` reads
that append connector and performs actual native UNNEST expansion into CDC and
timer-seed sinks. This boundary follows an actual planner rejection of
`UnnestExec` in the state-table fusion region; Python projection is not counted
as native projector evidence. Single CASE/UNNEST paths preserve seed/clock order
and one writer per output sink. Generated keyed lookups use qualified ANSI
JSON_VALUE, after the older extraction scalar was rejected by fusion bounds.

Actual current-engine composition attempts in evidence
`application-fixtures/retry1/` rejected updating aggregate -> MERGE (`native
MERGE source must be an append event stream, not a changelog`) and SESSION ->
MERGE (`unsupported SessionWindowAggregate between related state accesses`).
The candidate uses an append connector boundary instead of adding capabilities.
The normalizer's `source_ms` is distinct from profile `effective_ms`: live source
timestamp assignment clamps only values over 60 seconds future, while live
business state also clamps values older than 91 days. Replay preserves valid
historical values for both. Existing profile state rejects mode changes.

Native singleton input-progress state tracks maximum source time at fixed MVP
parallelism one; timers receive that maximum minus 5,001 ms. The extra millisecond
comes from the pinned Flink 1.20.1 implementation of
[BoundedOutOfOrdernessWatermarks](https://raw.githubusercontent.com/apache/flink/release-1.20.1/flink-core/src/main/java/org/apache/flink/api/common/eventtime/BoundedOutOfOrdernessWatermarks.java).
This delayed progress is used only to schedule timers, not EVENT_DATE or replay
business duration. The incoming timestamp still sets session deadline while
maximum last_seen sets decay deadlines. Exact future deadlines enter native
one-millisecond TUMBLE; overdue deadlines take an immediate append branch to
avoid native-window late-row suppression. Stale generations cannot reset a newer
session, duplicates cannot close it twice, and switched session IDs do not
prematurely contribute closed duration.

Runnable timer attempts retained in local `timer-review/` first showed native
SESSION dropping the late-base requested deadline. The first TUMBLE attempt
preserved the two future deadlines but dropped an overdue input at source
watermark admission. Its corrected transport timestamp is the greater of
deadline-minus-1ns and progress-plus-1ns; the logical deadline remains unchanged.
The third TUMBLE attempt passed all three exact future/late/overdue controls and
checkpoint recovery through the existing native engine. No timer engine feature
was added for these cases.

Before a raw event mutates a profile, the UDF catches up through the PRIOR
persisted stream watermark, not the current event's newly advanced progress.
This prevents delayed connector feedback from cancelling an already-due old
timeout/reset. The independent feedback fixture specifies A at 00:00, B at
01:00, then A at 01:01 with the same session ID before delayed old controls:
old A closes once, A starts its second session and duplicate stale controls do
not change it. The boundary fixture separately keeps a deadline-plus-two-second
other-key event behind the original five-second watermark, allowing a late
same-session event to cancel the old timer first.

The live policy preserves valid-time clamping, observation-time duration,
observation-date pruning and observation metadata. Replay bypasses wall-clock
clamping/pruning, computes duration using logical reference time, and preserves
historical daily contributions. `timestamp` and `updated_at` remain observation
metadata in both modes. Invalid timestamps reject explicitly. Current candidate
emits per-event snapshots plus due lifecycle snapshots; exact five-second
processing-time debounce cadence is not claimed.

The independent expected fixture in `test/streamr-reference/profile-mvp/` contains
fifteen full33 replay snapshots and four live snapshots, including signed future
duration, late incoming timer, same-ID reuse after closure, duplicate/stale
controls, top-count ties, two tenants sharing canonical ID, UTC midnight and
1/7/30-day decay. Comparator tests reject type drift, broken before-state chains,
missing/duplicate outputs and wall-clock substitution. Four comparator tests
pass; this establishes the oracle tooling only. Public API UDF compilation,
real owner/recovery, native timer stage and complete connector-cycle execution
are separately required. The first official UDF runtime attempt exposed an
edition-2024 export-annotation error in the generic compiler/plugin; source and
precise runtime evidence are tracked by the coordinator before any engine fix.

Whole-graph evaluation uses an isolated broker and private container network.
`native_mvp_kafka.py` validates every generated public query before submission,
uses complete read-committed finite offset scans, captures native source/sink
checkpoint positions and typed artifacts, kills the actual profile and session
owner workers, requires new generations/restored epochs and audits committed
topic prefixes before fresh inputs. An independently authored oracle specifies
all five profile roles and four complete session summaries from the thirteen
identity events. Reserved clock controls enter internal execution topics only;
the public unified stream remains business events. The harness and oracle need
actual passing runtime evidence before any whole-graph readiness statement.

The whole-graph preflight also invokes the official Rust `udfs/validate` endpoint
for every inline UDF before creating any pipeline: SQL `validate_query` alone did
not compile Rust. The first boolean normalizer mode argument exposed the existing
plugin's `PrimitiveArray<BooleanType>` compilation error (E0277/E0308/E0599),
retained in `kafka-graph-attempt3/pipelines/profile-events-udf-0-validation.json`.
The smallest application adaptation uses the already supported string input
contract (`live`/`replay`) while retaining boolean mode in the application payload
and persisted policy. This avoids an engine capability change. Failed isolated
attempts are retained separately from any passing graph receipt.


## Source admission and technical timer transport

Actual continuous Kafka attempt6 exposed two issues that terminal file fixtures
did not establish. Profile timer source checkpoints had advanced maximum `AS`
progress to January1 00:30:54.999 but retained emitted progress at 00:00:07.999:
the native generator throttles emission using `FOR`, previously advanced by a
thirty-day deadline. Session timer windows drained correctly, but their
historical controls encountered the owner's October17 default source admission
floor before application policy. Raw checkpoint audits and failed outputs remain
under `kafka-graph-attempt6/`; they are not passing receipts.

The selected existing-interface pattern disables finite source admission
watermarks on nonwindow append stages using a generated nullable timestamp and
supported `watermark_field` option. It preserves record transport timestamps and
all explicit business time. Only timer pipelines emit application progress.
Native singleton progress state also retains a technical dispatch cursor:
`max(previous + 2000ms, effective + 31d, source + 31d)` for profiles, raw source
plus31days for sessions. Returned controls do not advance that cursor.

A native qualified `JSON_OBJECT` owner envelope carries the unchanged business
payload string plus dispatch and progress. The application scalar
`timer_outbox.rs` runs in the separate stateless projector and appends a technical
clock after each frame's business seeds in the same native writer. Clock `FOR`
uses dispatch; `AS` remains exact declared application progress. Technical clocks
are excluded from windows and application outputs. An actual attempt7 rejected
the scalar in the fused owner's sink projection; moving it after the native
append boundary resolved that restriction without an engine capability change.

The technical cursor is finite, grows two seconds per raw arrival and must fit
the native nanosecond timestamp range. The scalar rejects exhaustion explicitly;
unbounded production operation is not established. Event dates, session
deadlines, replay duration and observation metadata never derive from this
technical cursor. The unchanged business scalars retain their independent exact
oracles. The public compiler, native projector, continuous Kafka timers,
checkpoint recovery and fresh output all passed on the selected path.

Production acceptance still includes the agreed storage/consumer contract and
operational configuration. The newly published, pinned FlareDB image passed its
separate startup/SELECT1/restart smoke receipt; that does not establish Kafka
aborted-transaction ingestion or application materialization. Full-graph memory
backend execution and identity Kafka-owner loss are not claimed by the RocksDB
graph receipt. No production traffic was switched.
