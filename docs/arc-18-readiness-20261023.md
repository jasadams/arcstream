> Historical source-bound report. Preserve the results below for their recorded
> versions; use the [23 October delivery snapshot](arc-18-native-delivery-20261023.md)
> for current integration and deployment status.

# ARC-18 current native readiness, 23 October 2026

The native core20 passes real Kafka/RocksDB worker-loss recovery. Full33
application readiness remains blocked by the SESSION publication and native SQL
findings below. No production cutover is included.

## Source and instructions

- Arcstream fetched base: `cbcb901be8482c01acce3471b4b6ba3a4242c3d0`.
- Isolated worktree: `/home/jason/repos/arcstream-worktrees/arc-18-readiness-20261023`.
- Applicable instructions: `AGENTS.md`, `CLAUDE.md`, `.claude/build-test.md`,
  `.claude/team.md`, ARC-18 and `docs/arc-18-native-status.md`.
- Streamr: `a5d28ad868f8e64cd3d2daa158b12e11b52e3c03` (merged PR78/79).
- Evidence: `/home/jason/qa-evidence/arc18-readiness-20261023/`.
- CLI SHA256: `0d435684decc9a6dbf7973452d503dbe0020e3d536eccf4330f1580a6d180e1d`.
- Maintained `scripts/build-pin`, dev profile, matching precompiled RocksDB;
  full command and immutable images in `dev-pin/` receipts. All runtime work
  uses `scripts/rust-build`. No throughput claim or repeated engine campaign.

## Application time

The user confirmed stream event time for session duration and application
publication timestamps, including replay. Wall-clock time belongs only in
operational logs/processing metrics. Preserve the five-second watermark.
`EVENT_TIMESTAMP()` binds original triggering event time, not wall-clock,
maximum-observed time, or lateness-adjusted watermark. Quiet-key closure and
calendar publications still require validation of their stream reference.

## Current findings

Public API planning accepts the complete prepared core20 query, the native
SESSION publication diagnostic and COUNT/MERGE/OLD/NEW/EVENT_TIMESTAMP metadata
probe. See each `*-plan/query-validation.json` and command receipt. Planner
acceptance is not application correctness evidence.

`session-boundary-1` passes actual Kafka input, exactly-once Kafka output,
RocksDB and staged committed-prefix checks. Source prefix1/checkpoint1 and
prefix2/checkpoint2 retain native SESSION artifacts but emit zero output.
An ordinary other-person event at 10:30:08 advances stream time; prefix3 emits
exactly start10:00:00/end10:00:02/count2. The source remains live until all
prefixes pass. This is **diagnostic only**, with no recovery claim.

Independent review verified executable digest, checkpoint identities/native
artifact hashes, finite complete Kafka captures, staged results and owned
resource cleanup. This demonstrates closure-only publication of this query;
it does not prove that every possible SQL route fails. Open active-session
and immediate session-start profile fields are not supplied by this query.
Source inspection and upstream SESSION/updating-table docs revealed no early
SESSION publication route. The smallest generic contract to discuss, if no
existing native route is available, is an updating SESSION relation exposing
open changes and closure transitions, with late bridges/retractions and
checkpoint semantics. No engine feature or application workaround was added.

The original closed-calendar composition is rejected with:
`unsupported clock-dependent window aggregate: calendar FILTER requires a maintained non-windowed aggregate`.
The rejected query and response remain preserved. A native projection that
removes the complete window field is being investigated before declaring a
capability gap.

## Test configuration failures retained

- `core-kafka-recovery-1`: wrapper mistakenly placed database admission under
  `disk-sql-state`; corrected to `live-state-resources`.
- `core-kafka-recovery-2`: eight database slots required a compatible shared
  scan pool. Increased scan allocation from2MiB to4MiB; native admission stays
  enabled (derived checkpoint page25486bytes exceeds17408 required).
- `core-kafka-recovery-3`: first source event rejected by typed row encoding
  limit4096bytes. This limit includes Arrow IPC schema/record encoding; it is
  distinct from the disk backend row limit. Ordinary typed allocation was subsequently validated by attempt8, without
  changing SQL semantics or bypassing native limits.

All attempts retain rendered SQL, configurations, literal oracles, image IDs,
commands, logs, outcomes and cleanup receipts. Failed runs are not recovery
passes. The current output is a **20-field diagnostic**, never the strict
33-field consumer topic. Full identity/unified/profile/consumer correctness,
remaining lifecycle fields and real integration recovery remain unestablished.

## Further current-source attempts

The projected closed-calendar query removes the complete window struct before
its outer aggregate, following the native planner's documented append-result
projection path. It then rejects the missing **original trigger clock metadata**
(STR-62). The SESSION closure timestamp is last-event+gap-1ns and is not the
original triggering event time. Neither window time, MAX nor watermark was
substituted. Both rejected SQL forms and responses are retained. A generic
clock-context contract through finalized windows would need explicit discussion;
this is not authorization to implement one.

`publication-metadata-1` reaches the first event but fails with
`Compute error: concat requires input of at least one array`. The reduced
`empty-case-array-1` reproduces the same error without aggregates/state tables:
a CASE's unused ELSE calls array_concat on a zero-row selected batch. See
`lifecycle-candidates/empty-case-array.sql` and its README for pinned upstream
source and smallest proposed kernel repair. No SQL workaround was substituted.

`core-kafka-recovery-4` exposed an incorrect harness inventory assumption:
checkpoint kind3 is DiskKeyedMap, which includes aggregates, not only windows.
Raw checkpoint metadata contains four native aggregate owners and one typed
state owner. The harness now checks these exact counts. Attempt5 caught a
wrapper/inner flag mismatch during that rename; neither is an engine failure.

Attempt6 proved a nonempty committed first snapshot, actual worker kill and
selected checkpoint1 restoration, then failed because the restored Arrow row
requires65896 decoded bytes against the stale16384 allocation. It is a failed
recovery test, not a successful continuity/fresh-output claim. An explicit
131072-byte typed decoded-row allocation subsequently passed attempt8, retaining
encoded-row8192 and backend-row8192 limits, with shareddecoded32MiB as described below. No native guard
or memory/RocksDB semantics changed. Both repeatedly failing diagnostic jobs
were stopped through their owned pipeline APIs to release the shared queue;
stop commands and original error logs remain preserved.

Attempt7 and `mixed-events-1` rejected typed-page admission before processing:
64 page entries ×131072 decoded bytes ×3 =25165824 bytes exceeded the inherited
16777216-byte shared decoded pool. The explicit bounded pool is now33554432
bytes; all page, encoded-row, backend and native admission rules remain enabled.
This is ordinary application resource configuration, independently reviewed.
No performance or production capacity claim follows from these finite fixtures.

## Passed core recovery

`core-kafka-recovery-8` passed the exact seven source-prefix oracles over real
Kafka input and exactly-once Kafka output. Checkpoint1/source offset1 contains
four native aggregate disk owners and one typed state owner, with one committed
complete profile snapshot. The harness verifies the actual worker PID identity,
SIGSTOP state and SIGKILL; worker generation1/PID54 is replaced by generation2/
PID229 for the same job/pipeline. The controller logs restoration of epoch1.

All seven final twenty-field snapshots match the independent literal oracle,
including the original committed snapshot and **six fresh recovered snapshots**.
Input6 is late and produces no extra snapshot; input7 produces quiet-person
calendar expiry and a separate tenant's profile. Event-time FIRST/LAST, nonblank
metadata, page ranking changes, raw EVENT_DATE calendar behavior and native typed
old-row reads are exercised. Every committed capture reaches its broker boundary
with transaction controls accounted for; no duplicate application output is
filtered out. Worker process and owned broker/network cleanup pass.

Final bounded fixture overrides are recorded in `outer-result.json`: database
slots8, scan pool4MiB, shareddecoded32MiB, typedencoded row8192 and typeddecoded
row131072. Other inherited limits remain recorded in the exact config. This is
a finite correctness/recovery result, not a new engine capacity qualification.

Reproduce through Streamr's maintained queue:

```sh
scripts/rust-build python3 /home/jason/qa-evidence/arc18-readiness-20261023/run-core.py 8
```

The original evidence case is deliberately non-overwritable; choose a new case
suffix to repeat. `invocation.json`, `command.json`, `query.sql`, config, literal
expected prefixes, `fault.json`, selected checkpoint metadata/objects, finite
captures, per-stage receipts, final `result.json` and both cleanup receipts are
retained. The evidence driver records the full wrapper arguments; the wrapper is
maintained in `test/streamr-reference/native_profile_fixture.py`.

## Mixed event correctness and verification

`mixed-events-2` passes five complete cumulative twenty-field literal snapshots
through actual Kafka and RocksDB, including all four event-type counters with
nonzero results (page1/click1/login1/feature2), admitted earlier metadata and
first_seen, blank/null LAST preservation, and search/export ranking reversals.
This is explicitly staged correctness with no worker fault/recovery claim.
Input/expected files are tracked under
`test/streamr-reference/profile-native/mixed-events/`; exact command and config
are retained in the case. Both owned container/network and worker cleanup pass.

Local Python checks:45 reference/harness +7 SESSION +5 profile +4 closed-metric
tests =61 passing. Independent source review approves the native product boundary,
corrected bounded allocations, strict exact-output harness, original defect
probes, real core recovery evidence and scoped SESSION diagnostic. Required
PR CI must pass on the final pushed head; historical green CI is not the new
head's result.

Complete33 readiness remains blocked. The product investigations to discuss are
open/updating SESSION output, original stream-trigger context through finalized
SESSION into calendar maintenance, and zero-row array_concat correctness. Exact
queries/fixtures, expected results, source versions and errors are preserved.
No application UDF, timer, clock source, pacing, default-field filler or native
semantic substitution was introduced. No engine implementation change, release,
production deployment, traffic switch or production data change is included.
