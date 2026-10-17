# Native lifecycle composition probes

Prepared 2026-10-17 against Streamr target `ad0a4814d5b7c07d74dce54753f5e6e9aaa7a062`.
These files are runnable attempts awaiting coordinator execution, not passing
results or proof that a capability is missing. Substitute absolute `{{INPUT}}`
and `{{OUTPUT}}` paths, use the existing typed SQL harness through the shared
queue, retain query/input/executable hashes, plan or exact error, complete output
and command. No engine changes are proposed here.

`snapshot-merge.sql` isolates source snapshot -> previous snapshot MERGE and
RETURNING-derived action/changed_fields. Expected rows in order:

```json
{"tenant_id":"tenant-a","canonical_id":"person-a","previous_total_events":null,"total_events":1,"action":"create","changed_fields":["canonical_id","tenant_id","total_events","first_seen","last_seen"]}
{"tenant_id":"tenant-a","canonical_id":"person-a","previous_total_events":1,"total_events":2,"action":"update","changed_fields":["total_events","last_page"]}
```

This is a minimal known-input delta, not all thirteen Java changed-field tests.
`aggregate-snapshot-merge.sql` is the paired attempt replacing source snapshots
with maintained COUNT/LAST_VALUE snapshots. Execute it separately; producer CDC
and batch coalescing may change emitted rows, so retain its plan/error and exact
outputs rather than assuming the two-row source oracle applies. Success would
still require source-prefix and recovery assertions before full composition.

`session-reset.sql` isolates SESSION completion -> state-table MERGE; expect
one tenant-a/person-a/session-a row with event_count 2 and
current_session_active false. It intentionally does not initialize cursor from
active events: passing only proves a closure can write state. Full lifecycle
must combine ordered starts and closures at the same owner and prevent an old
closure from resetting a newer active session. Retain any planner restriction
on windowed producer input as evidence of this attempt only.

`clock-after-aggregate.sql` tests direct EVENT_TIMESTAMP projection after a
maintained COUNT. Source inspection predicts explicit current-trigger-metadata
rejection. Do not replace it with aggregate output time or watermark merely to
make planning succeed. Capturing EVENT_TIMESTAMP before aggregation is a
separate valid candidate for last-trigger metadata, requiring replay policy.

SESSION schedules closure on advancing event time; proving that pattern does
not prove the separate arbitrary 1/7/30-day decay schedule or five-second
processing-time debounce. Calendar aggregate correctness and periodic emission
cadence require separate contracts and fixtures.

Upstream investigation: [Arroyo streaming windows](https://doc.arroyo.dev/sql/windows/)
documents SESSION closure after inactivity; [window functions](https://doc.arroyo.dev/sql/window-functions/)
requires a streaming window in PARTITION BY, so LAG is not automatically an
unbounded prior-profile substitute. [UDF overview](https://doc.arroyo.dev/udfs/)
documents scalar extensions; it does not establish arbitrary timer registration.
These sources motivate native attempts, not an impossibility claim. Streamr
source is authoritative for the exact target behavior.
