# Native session application owner

These Arcstream files preserve session business policy through existing Streamr
state-table lookups, dependent MERGEs, native DELETE, append connectors and native
TUMBLE deadline dispatch. No session policy or application state is added to the
engine. The stateless normalizer supplies exact raw `session_source_ms`; profile
live-clamping does not change session timestamps.

The active key is `(tenant_id, session_id)` at MVP parallelism one. Tenant isolation
is explicit; the historical Java job keyed only by session ID. Empty/null session
IDs advance source progress but do not create a business session. First arrival
sets canonical identity and start; end is MAX with Java's initial epoch-zero
boundary, including pre-epoch input. Pages are unique and sorted; event-type maps
accept arbitrary nonempty names. Last nonempty device/browser/country wins.

Input progress retains raw source MAX minus 5,001 ms separately from business
timestamps. Before an event touches an existing session, the application catches
up only through the PRIOR persisted watermark, preserving record-before-own-
watermark ordering when native feedback is delayed. A matching returned control
closes at maximum event time plus thirty minutes. The first MERGE evaluates one
scalar decision; a dependent MERGE deletes closed/ignored rows in the same event.
Its source decision remains available to the append outbox after deletion.
`project.sql` expands summaries and timer seeds beyond that scalar owner boundary.

`timers.sql` uses the independently tested one-millisecond TUMBLE pattern:
future dispatch timestamps are deadline-minus-one-nanosecond; overdue timestamps
are raised above declared progress for transport admission while their logical
deadline stays unchanged. Explicit clock markers advance progress without
becoming session events. Passage of wall time alone does not close sessions.

Prepare the independent hand-counted fixture:

```sh
python3 test/streamr-reference/session-mvp/prepare.py /path/to/new-case
python3 -m unittest discover -s test/streamr-reference/session-mvp -p 'test_*.py' -v
```

The fixture contains seventeen signals, five full12 summaries and eleven timer
seeds. It covers late input, identity changes, tenant isolation, last nonempty
fields, same-ID reuse after closure, prior-watermark catchup, stale/duplicate
controls, null sessions and the Java pre-epoch end boundary. The selected source
prefix is six signals. These are expectations, not a runtime receipt. Actual
public compiler/owner/project/recovery and whole Kafka graph results must be
recorded separately on ARC-17. Invalid source timestamps reject explicitly;
wall-clock parse fallback and production cutover are not claimed.
