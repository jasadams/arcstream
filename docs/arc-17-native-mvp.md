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
| Profile lifetime | Native keyed COUNT, filtered COUNT, FIRST/LAST/MAX | Arrival-first first_seen, maximum last_seen, total and event-type counters, last nonempty descriptive fields |
| Profile calendar | Native COUNT/SUM calendar FILTER with EVENT_DATE | UTC current date plus previous N-1 dates for 1/7/30/90-day totals; retained contribution date remains separate from triggering date |
| Session starts | Cursor state-table MERGE plus SUM | A,A,B,A,null,empty,A produces three starts before timeout; null/empty does not reset current session |
| Ranked collections | Native per-item COUNT and ordered ARRAY_AGG | Top five pages/top three features; explicit deterministic tie rule requires fixture coverage |
| Closed sessions | Native SESSION plus ARRAY_AGG/UNNEST/count/map composition | Thirty-minute inactivity; arrival-first canonical/start, maximum end, deduplicated pages, arbitrary event-type counts, last nonempty device/browser/country |
| State lookups/recovery | Native state-table lookup and checkpoint interfaces | Restored anonymous/user/session cursor bindings must influence fresh events after restart |

Keep complete SESSION window lineage through both downstream aggregate stages;
flattening to start/end alone was a historical rejected query. Existing session
fixture has seventeen input rows and three expected closures. Validate real
source batching rather than borrowing its batch-specific committed-row count.

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

Replay conflicts requiring explicit application resolution:

- Java profile clamps timestamps more than 91 days old or 60 seconds future to
  wall clock, prunes buckets by today's UTC date, and falls back to wall clock
  after timestamp parse errors. This changes historical replay results. A native
  replay contract should preserve valid input event time and reject invalid time;
  this is a proposed policy until explicitly accepted and tested.
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

## Remaining eight profile fields: investigation contract

The reused query emits 25 of the Java ProfileUpdate's 33 fields. The remainder is
`updated_at`, `avg_session_duration_sec`, `current_session_active`,
`current_session_duration_sec`, `action`, `changed_fields`, `timestamp`, `trigger`.
Do not populate them with plausible constants to claim parity.

Smallest native candidates to attempt, without engine changes:

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
