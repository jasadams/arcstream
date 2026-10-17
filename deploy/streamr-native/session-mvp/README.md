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

Technical transport clocks are separate from logical session time: append owner
and projector sources declare a nullable admission watermark, and a native
progress column advances dispatch by at least two seconds per raw event/clock,
with a 31-day lead. The owner emits a native JSON envelope holding the untouched decision string
and transport metadata. The stateless projector calls pure shared `arc_timer_outbox`
to add one bounded technical clock per frame; the projector appends it after that frame's business timer seeds
in the same ordered sink. Timer source admission uses its dispatch time, while
the declared watermark remains raw source progress minus 5,001 milliseconds.
Technical clocks are filtered from session windows and output controls. They do
not advance business progress, create sessions or change the five summary oracle.
The shared helper rejects timestamps outside the native nanosecond horizon; this
finite transport scheme requires an operational horizon check before cutover.
Fixture preparation also writes `expected-project-timers.jsonl`: eleven business
seeds plus seventeen technical clocks in exact order. New graph evidence must
validate this transport; earlier file-owner recovery receipts alone do not.
