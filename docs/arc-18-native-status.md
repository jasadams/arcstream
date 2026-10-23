# Native SQL integration status

The approved five-job zero-UDF graph is implemented and has scoped native
Kafka/RocksDB correctness/recovery evidence. The actual cluster launch passes
readiness, while FLA-184 generation starvation at the larger live store still blocks production
routing. Start with the [current delivery snapshot](arc-18-native-delivery-20261023.md)
for exact versions, immutable pins, approved 22-field scope and remaining gaps.
Production is not cut over; full parity and completed 90-day replay are not claimed.

Application SQL/policy stays in Arcstream. Native SQL, original stream event time,
the five-second watermark, state tables and RocksDB govern execution; historical
Flink equivalence never authorizes a UDF, synthetic timer or semantic workaround.

## Historical source-bound record

The dated statements below are preserved verbatim. References to rejected
composition, missing native graph or unresolved native List ingestion describe
those older sources; the current snapshot supersedes them as agent instructions.

> Current result (23 October): [publication and recovery evidence](arc-18-publication-recovery-20261023.md).
> The approved 22-field native SQL graph passes all 11 real Kafka/RocksDB stages,
> 13 complete literal profile snapshots, two actual worker SIGKILL/checkpoint
> restores, committed-prefix continuity and fresh recovered outputs on Streamr
> 1314e702. All 30 native Python and 10 API tests pass. Independent evidence audit
> approves this scope. The ordinary JOIN/CDC failure remains preserved.
> Native production-host test configuration renders five jobs and three schemas;
> actual FlareDB native List ingestion/HTTP failures are reproduced as FLA-178.
> There is no application serialization workaround, deployed-service result or
> production parity/cutover claim.
> [Native test deployment configuration](../deploy/native-test/README.md).
>
> Older statements below describe their dated source versions.

> Current-source follow-up: [23 October readiness ledger](arc-18-readiness-20261023.md).
> Historical results below remain bound to their recorded engine revisions.

# Native SQL integration status

Current merged-source findings (22 October):
[validation results](arc-18-merged-validation.md). Streamr c5e06665 contains all
four dependency deliveries. COUNT-to-state-table recovery, full native SESSION
summaries and Kafka-input completed-session metrics pass their scoped fixtures.
The prepared full profile composition remains rejected: calendar, array/ranking
and finalized-window MERGE boundaries exceed the delivered route. No workaround
was added. Older pending-dependency statements below describe preparation history;
the linked report supplies current exact SQL, errors, results and limits.

Arcstream validates Streamr as a product. Native SQL, configured event-time
watermarks, state-table semantics and RocksDB recovery govern execution.
Legacy Flink output equivalence does not override those contracts.

The UDF-backed MVP execution path has been removed. There is currently no
complete, deployable profile/session replacement graph. Do not use historical
ARC-17 receipts as evidence that the remaining SQL is a complete integration.

## Removed execution path

- All four application Rust UDFs and their registrations/compiler setup.
- Opaque profile/session JSON transition owners and manual lifecycle catch-up.
- Separate timer request, projection and feedback pipelines.
- Synthetic dispatch clocks, two-second cursor increments and 31-day offsets.
- Overdue-deadline bypass branches and adjusted timer transport timestamps.
- Nullable source-watermark admission bypasses, including identity bridges.
- Full-MVP generation/submission, its custom graph/oracles and active CI callers.

Historical code remains in Git and source-bound evidence remains in
`/home/jason/qa-evidence/arc17-native-mvp-20261017/`. Those are historical
observations, not current implementation instructions or acceptance requirements.
No deployed jobs, production topics or retained application data are changed by
this source removal.

## Remaining native paths

`deploy/streamr-native/identity.sql`, `unified.sql` and `merges.sql` retain native
identity state-table logic. Their sources now cast the application event_time
and declare an explicit five-second disorder watermark,
instead of suppressing admission or using an implicit wall-clock source floor.
Their changed Kafka event-time behavior requires application validation before
cutover. Existing sink exactly-once configuration and identity policies remain.

`deploy/streamr-native/session-native/query.sql` uses native SESSION and native
ordered aggregates/arrays/maps. The standalone fixtures test accepted events,
late rejection, tenant isolation and session reuse. A finite fixture advances
source time using ordinary events with excluded session IDs; this does not add
a production clock/control protocol. The configured watermark governs lateness.
An event arriving 30 seconds behind progress must be dropped with a five-second
watermark. This is correct product behavior, not a product gap.

Other retained SQL directories are native capability candidates with their own
source-bound qualification limits. They are not wired into a full application
graph. EVENT_DATE()/EVENT_TIMESTAMP() are source-trigger-relative functions;
calendar logic must not substitute wall clock or delayed watermark time.

## Outstanding integration and product work

Preparation against STR-29, STR-60, STR-64 and STR-146 is now available through
`deploy/streamr-native/prepare.py`. It renders native SQL and zero-UDF public
API requests only; it does not submit or deploy. The graph includes the native
identity paths, full 12-field completed-session summaries, a 20-field partial
profile component and separate completed-session count/average metrics. Partial
profile output is confined to diagnostic topics, never the 33-field consumer.

SESSION fixtures now use actual payload event_time and require implicit
event-time FIRST/LAST, including admitted out-of-order events and full ties.
They no longer supply arrival_seq or typed timestamps fabricated by the fixture.
Profile fixtures explicitly distinguish raw event date from delayed watermark
and maximum observed time, include ranking changes, late rejection, tenant
isolation and quiet-key calendar zero. Prepared expectations are not runtime
passes. Remaining 13 profile fields are listed in profile-native/fields.json;
their session lifecycle/publication semantics are unfinished application work,
not proof of new engine deficiencies or permission to fabricate values.

Current preparation evidence is under
`/home/jason/qa-evidence/arc18-native-integration-contracts-20261018/`.
Public API planner checks on qualified main14353046 accepted the corrected
identity/unified/merges queries. The exact partial profile query was rejected
with `native MERGE source must be an append event stream, not a changelog`,
the expected pending STR-64 route. No engine changes or application runtime
success are claimed. Dependency completion must be followed by actual composed
Kafka/RocksDB correctness, state lookup and committed-output recovery evidence.

- Compose the full profile schema and lifecycle using native SQL; no fabricated
  fields, serialized state-machine substitute or custom deadline dispatcher.
- Validate native session output with the application consumers and real Kafka
  input/recovery after removing the old feedback graph.
- Revalidate identity sources with payload event time and the configured cutoff.
- Historical runnable attempts rejected aggregate changelog -> MERGE and SESSION
  between related state accesses. Preserve those attempts and recheck the
  smallest required composition on current source before declaring an engine
  deficiency. A missing integration is not by itself proof of a product gap.
- If a required native query cannot execute, record exact SQL, source revision,
  expected/actual results and the smallest generic proposed engine change on
  ARC-18 before adding a capability. No demo workaround is authorized.

Runtime receipts and exact commands for this removal belong to ARC-18 and
`/home/jason/qa-evidence/arc18-remove-workarounds-20261018/`.
The previous SESSION diagnostic and recovery evidence remains at
`/home/jason/qa-evidence/arc18-native-session-20261018/`; its late-acceptance
expectation was superseded by the user's explicit native-watermark contract.
