# Native SQL integration status

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

The candidate `deploy/streamr-native/profile-native/closed-session-metrics.sql`
composes native SESSION with lifetime COUNT/SUM and updating CDC. It contributes
only completed-session count and average seconds. Independent 13-event fixtures
cover session reuse, tenant isolation, fractional-second arithmetic, five-second
late rejection and a two-day inactivity interval. Fixture preparation and runner
unit tests do not establish application runtime correctness.

Current investigation evidence is under
`/home/jason/qa-evidence/arc18-native-profile-20261018/`. The native composition
planned successfully before adding the existing `SET updating_ttl = NULL`
setting. The current Kafka/RocksDB checkpoint/SIGKILL experiment remains pending:
the shared queue disk guard rejected launch while another build held its lock.
No Kafka application result or recovery success is claimed for this component.
The same planner investigation corrected watermark expressions in the three
identity queries to cast the physical payload field directly; those queries
still require revalidation.

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
