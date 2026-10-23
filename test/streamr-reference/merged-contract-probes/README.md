# Merged native contract probes

These are small runnable application-owned SQL cases, not assertions that an
unsupported query is a passing feature test. They preserve precise failures for
future repair. No engine behavior, business rule or admission policy is changed.

On Streamr `c5e066652755c76a2ac6cba46f190109a9fa421d`:

| Query | Actual public API result |
| --- | --- |
| count-merge.sql | Compiles; native RocksDB worker-loss recovery passes with the four-row fixture |
| calendar-merge.sql | Rejects: per-event aggregate MERGE requires SET updating_ttl = NULL and no calendar aggregates |
| array-merge.sql | Rejects: per-event aggregate MERGE supports native non-distinct COUNT/MAX and grouped selectors only |
| rank-merge.sql | Same allowed-aggregate rejection; ranked array is consumed in RETURNING projection, so cannot be pruned as unused |
| session-merge.sql | Rejects: state-table fusion: node 4 uses unsupported SessionWindowAggregate between related state accesses |

The complete prepared `profile-native/core.sql`, rendered with file connectors,
also rejects at the allowed-aggregate guard. That single error does not prove
each of its nested UNION, calendar and ranking steps is independently defective.

COUNT input has one tenant/profile. The two-row planner fixture remains intact.
The separate four-row recovery fixture expects old/new values null/1, 1/2,
2/3, 3/4 with native actions insert, update, update, update. Expected values are
literal and independently reviewed. A first two-row runtime attempt reached all
physical outputs before the supervisor could inject a fault, so it correctly
failed the fresh-suffix gate. Its receipt is preserved; no gate was relaxed.

With four inputs, checkpoint 1 contains source prefix 1, 108 committed output
bytes and native aggregate plus typed-table state. Actual SIGSTOP/SIGKILL of
worker generation 1 is followed by generation 2 restoring that same job and
checkpoint. The replayed second result reads old=1/new=2, proving restored target
state, and rows 3/4 are fresh beyond the pre-loss physical output. All four exact
rows and the committed byte prefix match. This case uses file input/output,
not Kafka.

## Provenance and reproduction

Evidence root: `/home/jason/qa-evidence/arc18-integrated-validation-20261022/`.
Each `*-command.json` contains the exact immutable-image/container invocation;
the corresponding `*-plan/` or `*-runtime/` directory contains the rendered SQL,
configuration, API response, supervisor copy, source/sink receipts and logs.
`run-plan.py`, `run-plans.py` and `run-runtime.py` retain the outer commands.
Use the maintained shared run queue when repeating a case; supply fresh evidence
directories to preserve prior attempts. The existing application runner is
`test/streamr-reference/native_sql_runtime.py`; it substitutes `{{INPUT}}` and
`{{OUTPUT}}` and uses the real public pipeline API and production worker binary.

Current CLI was built with maintained scripts/build-pin, dev profile and matching
precompiled RocksDB using immutable Bookworm image
`bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e`.
ELF SHA256: `d8f179232d0fcc0f3df12658ee383caf166840c43d817d2f65d760c523a10e4d`.
All 1,331 compiled source hashes independently match the merged commit. The old
CLI pin had 49 source mismatches and was not used. Initial source-pin argument
and default-image native-pin preflight failures remain in the shared queue
history/evidence; the existing matching image was selected through --image.
No RocksDB source compilation, custom builder or performance campaign was used.

Independent review verified query/oracle/output hashes, checkpoint artifacts,
physical and committed prefixes, same-job fresh worker identity, restore logs
and complete cleanup. Planner rejection is not a runtime pass. The accepted
COUNT slice is not full profile readiness or production cutover.

## Boundary requiring discussion

The new engine route supports direct COUNT/MAX/selectors, and a bounded
two-branch COUNT/MAX LEFT JOIN composition. Calendar maintenance, ranked arrays
and finalized window producers are outside that accepted route. Supporting the
queries above would require preserving coherent result/maintenance context at
those additional native MERGE boundaries. No expansion is implemented here.

Before declaring those expansions MVP requirements, establish exactly which
application metadata/state lookups require them. Ordinary native profile CDC can
supply before/after images; it does not by itself compute changed_fields or
settle previous-calculated versus previous-published semantics. Removing MERGE
without proving those requirements would not validate this application path.

Upstream investigation: [Arroyo updating tables](https://doc.arroyo.dev/sql/updating-tables/)
documents native updating aggregates, CDC sinks and periodic flushing;
[updating joins](https://doc.arroyo.dev/sql/joins/) describes maintained relations.
Those interfaces alone do not establish Streamr's event-scoped state-table
MERGE or finalized-window ownership boundary. Preserve this distinction instead
of proposing new syntax or an application processor.
