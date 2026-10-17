> Historical ARC-17 record. The UDF-backed graph and its deployment tools were
> removed under ARC-18. Commands and acceptance claims below describe the old
> implementation and must not be used as current run/deployment instructions.
> See [current native status](arc-18-native-status.md). Source-bound evidence is
> retained; legacy Flink behavior does not override native product semantics.

# Native MVP validation receipt

The finite isolated native MVP integration passed application and recovery
validation, independent review and required code CI. Complete Kafka
connector-cycle evidence is recorded below. Production cutover,
full backend parity and the broader ARC-17 live acceptance are separate gates.

## Sources and interfaces

Arcstream fetched base: `c7f477799ef7fb716a4b8d3917a671db210deeba`.
Application implementation and initial reviewed runner/config fixes:
`7f4ed57b974eecbe75cdbac92afde59196df9947`.
Corrected transport and final runtime configuration:
`b2fb06e6887e2a8563aaf821a670be5ebe692c97`.
Instruction sources: `CLAUDE.md`, `.claude/build-test.md`, `.claude/team.md` and
the user's current AGENTS instructions; no repository AGENTS file was present.
Original checkout changes were preserved in the separate task worktree.

Streamr public contract: PR69 merge
`ad0a4814d5b7c07d74dce54753f5e6e9aaa7a062`. Reused qualified production ELF
SHA256 `0e6078d2fe6f2a2c4279b4de3d7a8d6b5486b369b47a23a5c26dcd2bee363a36`,
actual build source `e2ff41e7c991b3e0f0c68bd7d2f874631a00501b`.
The diff to the merge contains documentation and two SQL smoke-test lines;
production engine code is unchanged. The reused SQL test executable is separately
pinned to source `0e63174498ef3adb9848a669c4ea12e3c21110f3`, SHA256
`b2e7d0df968fbe69babbfa0d13614d57e017b9bc211c5657a9cb476621750cc8`.
Its diff to the merge is documentation only. No qualification executable was
relabeled as a new build.

Official Rust UDF compilation uses the existing-interface safe-export repair in
[Streamr PR70](https://github.com/jasadams/streamr/pull/70), implementation
`a86297dc049c0c3a5a85f965c782cc571ae0d2d2`, executed clean source checkout
`852488b38ad1d3e28501df2443fc23edc15f43fe` (additional delivery documentation).
This dependency remains separate from the qualified engine ELF. Its independent
review, maintained checks and configured CI pass. No Arcstream behavior was
added to Streamr.

Runtime uses public `POST /api/v1/udfs/validate`,
`POST /api/v1/pipelines/validate_query` and `POST /api/v1/pipelines`;
checkpoint/job observations use the corresponding public pipeline routes.
SQL validation alone does not compile Rust UDF bodies. Actual compilation is
explicitly checked before creating the graph. Required state lookups use native
complete-primary-key SQL accesses; no nonexistent HTTP state-table API is assumed.

The normalizer uses the supported String UDF ABI for exact `live`/`replay` modes.
A retained official attempt proved Boolean argument lowering fails with E0277,
E0308 and E0599 (`PrimitiveArray<BooleanType>`). Current Streamr and pinned upstream
Arroyo `18c203dcdd5515ab36237b2f02721570d6f20eef` share that implementation.
The application-only string adaptation keeps its JSON replay policy Boolean and
requires no engine extension. Original source event dates, assigned timer
progress and observation metadata remain separate.

## Accepted native application matrix

| Fixture | Input signals | Exact full outputs | Timer seeds | Native backend |
| --- | ---: | ---: | ---: | --- |
| Profile feedback ordering | 6 | 5 full33 CDC | 12 | RocksDB |
| Historical replay/UTC/decay | 19 | 15 full33 CDC | 36 | RocksDB |
| Live clamp/signed duration | 4 | 4 full33 CDC | 16 | RocksDB |
| Source-progress boundary | 5 | 4 full33 CDC | 12 | RocksDB |
| Session lifecycle | 17 | 5 full12 summaries | 11 | RocksDB and memory |

Every row was exercised through both the actual native state owner and a
separate actual native projector: twelve successful runtime stages. Each stage
lost its real worker to SIGKILL, restored the selected checkpoint into a newer
worker generation, preserved committed bytes, matched every expected result and
seed without extras, and produced physically absent-before-loss fresh results.
The session case also proves persisted native DELETE and subsequent ID reuse.
These receipts predate the transport-clock correction below. They establish the
unchanged application policies; changed transport SQL requires its own current
runtime and complete Kafka graph receipts.

Identity additionally passed actual native worker-loss recovery on both backends:
checkpoint10 with two typed owners, thirteen exact events, two directed merges,
byte-identical committed prefix and two genuinely fresh recovered events.
Earlier qualified scalar/calendar25/SESSION12 cases and source-clock probes remain
supplementary diagnostics, separately source-bound; they are not substituted for
the selected application state-owner graph.

Evidence root: `/home/jason/qa-evidence/arc17-native-mvp-20261017/`.
`profile-runtime/current-application-results.json` binds twelve accepted result
SHA256s; `profile-runtime/README.md` contains exact queued commands, frozen
source/query/UDF/config/fixture hashes and rejected attempts.
`identity/source-and-command.json` binds the separate identity matrix.
The compatible engine image is pinned to
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`;
broker image identities are inspected, pinned and retained per graph attempt.
Build/run work uses the shared queue. Regenerable Cargo cache cleanup restored
its disk reserve without bypassing admission or removing qualification evidence.

## Complete Kafka graph

Attempt8 **passed** on the source/configuration committed as `b2fb06e`.
Result SHA256: `acc665202cadc160dff492ce85d06dbbfab649be2f440bc6f5caac920ee24feb`.
The graph runner uses a private broker/network/catalog,
fresh single-partition topics and distinct compacted recovery topics per sink.
It scans complete finite read-committed outputs, validates the independent
identity/profile/session oracle, repeats pipeline submission, kills/restores both
profile and session owners, checks every committed topic prefix, and supplies a
fresh real event after recovery. Exact receipts identify successful and rejected
attempts separately. Backend materialization is outside this finite Kafka gate.

All eleven native pipelines compiled, validated and ran through the public APIs;
repeated submission reused the same pipelines. Complete committed scans matched
thirteen initial unified events, two directed merges, five exact full33 profiles
in active and closed states, and four full12 session summaries. Profile CDC
contains eighteen emissions through closure and nineteen after the fresh
recovered event, with every literal before-image matching the preceding
after-image. The flat adapter publishes the same nineteen complete after-images.
Event `identity-14` retains the established canonical ID
and produces the expected fresh profile counters and attributes.

Both profile and session owners lost their real worker to SIGKILL, restored
checkpoint6 with Kafka source offset11, sink recovery epoch6 and two typed state
owners, then ran as generation2. All fourteen committed data-topic prefixes
remained byte-identical. The identity owner was not killed in this Kafka run;
its separate memory/RocksDB native CLI recovery evidence is described above.
Both native-process and host broker/network/container cleanup receipts pass.
The complete result, raw topic scans, checkpoint descriptors and restore logs are
under `kafka-graph-attempt8/`; the exact queued invocation is retained alongside it.

Attempt6 exposed two real connector-cycle failures: far-future deadline FOR
timestamps suppressed subsequent AS emissions, and implicit ingestion-time
admission dropped historical control feedback. Attempt7 rejected an arbitrary
helper in the fused post-MERGE projection. The application repair uses native
JSON_OBJECT envelopes across the existing append boundary, with the helper in
the stateless projector. Nonwindow sources explicitly suppress transport-time
admission through a nullable generated watermark field; original source FOR
metadata and business event-time fields remain unchanged.

Persisted dispatch counters produce technical clock pulses after business timer
seeds. Their FOR metadata advances the native source emission gate; AS remains
the declared application source progress. Clocks never enter business windows
or outputs. Business deadlines, UTC dates and replay durations are unchanged.
The counter is bounded by the native nanosecond timestamp domain and fails
explicitly on exhaustion; this finite MVP does not claim unbounded operation.

The corrected profile feedback fixture passed both current native stages:
five exact full33 CDC outputs, twelve business timer seeds, and eighteen
projected seeds including six technical pulses. Both stages survived real
SIGKILL/checkpoint restoration and produced fresh results. Receipts are
`profile-runtime/profile-feedback-rocksdb-technical3-{owner,project}/result.json`.
The accepted finite configuration separates 8 MiB working-event allowance,
1 MiB write scope and 16 MiB typed resident budget; the shared decoded pool
remains 16 MiB. Earlier rejected guard configurations remain recorded.

Two further current-transport fixture pairs passed to check the different clock
domains: live profile clamp/signed duration (four full33 CDC outputs, sixteen
business seeds/twenty projected seeds) and full session edges including pre-epoch
time, late arrivals, tenant isolation and ID reuse (five full12 summaries,
eleven business seeds/twenty-eight projected seeds). All four stages include
real checkpoint/SIGKILL restoration, committed-prefix preservation, fresh outputs
and cleanup. The coordinator independently recomputed complete typed outputs,
timer oracles, source hashes, prefixes and recovery identities in
`review/current-time-domains-root-audit.json`: **APPROVE**. Exact per-case commands
and frozen fixtures are under `profile-runtime/*-rocksdb-technical3-*`.

The full graph is run from the task worktree through the maintained queue:

```sh
scripts/dev-build python3 test/streamr-reference/native_mvp_kafka.py \
  --case /home/jason/qa-evidence/arc17-native-mvp-20261017/kafka-graph-attempt8 \
  --namespace arc-mvp-20261017-graph8 \
  --binary /home/jason/qa-evidence/str126-qualification-20261017/release-pin/package/bin/arroyo \
  --config-template /home/jason/qa-evidence/str126-qualification-20261017/runner-retry1/cases/pilot/config.yaml \
  --engine-source /home/jason/repos/streamr-worktrees/arc17-udf-unsafe-export-20261017 \
  --plugin-source-commit 852488b38ad1d3e28501df2443fc23edc15f43fe \
  --binary-receipt /home/jason/qa-evidence/str126-qualification-20261017/delivery-receipt.json \
  --oracle test/streamr-reference/native_mvp_oracle.py
```

Use a new case path and namespace for a repeat. The invocation receipt also
records the exact absolute argv, working directory and native recovery helper
SHA256. Each attempt freezes its executable, queries, UDFs, fixtures, image
identities, configuration and complete commands before testing.

## Independent review and CI

Written findings and verdicts under the evidence `review/` directory cover
consumer/GraphQL/dashboard transport, candidate generation, application policy,
normalizer String ABI, test-runner resource ownership, all native profile runtime
cases and the session runtime/DELETE evidence. The coordinator independently
executed the session artifact/output audit. Review signing is retired.

`review/native-graph8-source-review.md` approves the final source/configuration.
`review/native-graph8-independent-review.md` independently recomputes the raw
application oracles, literal CDC/flat outputs, both worker identities and exact
restore epochs, checkpoint Parquet source/sink metadata, artifact hashes,
fourteen committed prefixes and cleanup: **APPROVE**, no blocking findings.
`review/profile-runtime-review.md` covers the explicit payload adapter and
separate resource settings. `review/native-validation-receipt-review.md` approves
the factual scope of this receipt; the final CI and runtime verdicts are added
here after completion. FlareDB image/config/runtime reviews are separate.

Forty-six Python fixture/manifest/ownership checks pass, along with seventy local
query API tests. Required CI at runtime implementation `b2fb06e` passes all four jobs:
[CI run39024969412](https://github.com/jasadams/arcstream/actions/runs/39024969412)
(identity/profile/session checks, query API tests, dashboard SSR and hydrate).

## Published FlareDB image

User-requested FlareDB update: source
`95b58cb1fccc650c3b484e8b5c05e440508ae6ce`, successful
[publish run39020567423](https://github.com/jasadams/flaredb/actions/runs/39020567423).
Compose and the native analytics manifest now pin
`ghcr.io/jasadams/flaredb:arcstream-2026-10-17-95b58cb@sha256:3c8ea585571f4e12f24761356febc071ee018600bc0a58a1848c44312f913a91`.
The digest was independently read from the registry and matches the publication.
Compose/readiness and the smoke supervisor were committed as `ff74a31` on
the isolated task branch and pushed to [Arcstream PR23](https://github.com/jasadams/arcstream/pull/23).
The scratch image runs UID/GID10001 and contains no shell/curl. Compose therefore
uses a separately pinned curl container for actual SQL readiness; the query API
waits for that probe. Resolved Compose validation passes.

Actual queued `flaredb-image-smoke1` passed nonroot startup, exact HTTP image_tag,
`SELECT 1 AS smoke` result, same-container/same-fresh-volume restart and both
resource cleanups. Source, inspected image, command list, HTTP outputs and logs
are retained under the evidence root; independent runtime review approves this
narrow scope. The restart used the container runtime's eventual kill after its
stop timeout. This exposed the published source's SIGINT-only orderly shutdown
handler (`src/main.rs:667`). Compose now sets `stop_signal: SIGINT`.
Current queued `flaredb-image-smoke2` passed explicit SIGINT stop with exited
status, exit code0 and no OOM kill, then same-container/same-volume start,
exact image_tag, SQL result and both cleanups. Result SHA256:
`86c668bb4c9029cda46df82b704eeffb4b21b229043df1d6daf84e56e97b3fd7`.
Independent review: `review/flaredb-sigint-review.md`; exact maintained command:
`review/flaredb-smoke2-command.json`. This supersedes smoke1 for orderly lifecycle
acceptance. There were zero tables: this is not ingestion, data persistence,
transaction-abort visibility or backend parity evidence. Existing deployment
services and volume ownership were not changed.

The actual command was `scripts/dev-build bash
/home/jason/qa-evidence/arc17-native-mvp-20261017/run-flaredb-smoke1.sh`.
To repeat the maintained smoke with a fresh evidence directory after pulling the
pinned image:

```sh
scripts/dev-build env PYTHONDONTWRITEBYTECODE=1 python3 \
  test/streamr-reference/flaredb_image_smoke.py --case /path/to/new-flaredb-smoke
```

## Remaining qualification

The candidate fixes parallelism at one and carries explicit row/decode/scope/
working-event/scan bounds. JSON rows exceeding those declared limits fail rather
than bypass admission. It emits per-event/lifecycle snapshots; exact old five-second
processing-time debounce cadence is not asserted. Invalid timestamps reject
explicitly instead of falling back to observation time. Replay mode is explicit
and immutable for persisted profile state. Idle event-time timers require source
progress, and retained lifetime/top-item state has no general TTL claim.

Generated analytics configurations are isolated review templates. Actual backend
image binding, committed/aborted-transaction controls and materialized application
queries remain open. Existing session storage schemas retain ten scalar fields
of the full twelve-field Kafka contract, and existing Pinot session-ID-only dedup
is not certified for tenant isolation or legitimate ID reuse. The selected FlareDB image is now source/publication-bound and startup-tested;
actual transactional ingestion and complete materialized results remain open.
No backend parity is claimed.

Broader ARC-17 acceptance remains open: full historical backfill/live handoff,
broker/container faults, backend/dashboard results in supported configurations,
24-hour sustained application run and evaluation switch/rollback rehearsal.
The [cutover/rollback runbook](arc-17-cutover-runbook.md) is reviewable;
production traffic, data and release publishing have not been changed.
Accepted engine capacity qualification was reused. Incidental shared-machine
execution times are not throughput regression evidence.
