# Native SESSION compatibility candidate

This query exercises upstream SESSION directly. It is a diagnostic candidate,
not a replacement deployment and not a parity claim. The existing integration
is unchanged. There are no registered UDFs, timer-seed sinks, feedback controls,
synthetic dispatch timestamps or +31-day transport cursor in this query.

Source event time is the unchanged event_ts. The watermark is event_ts minus
5,001 milliseconds, retaining the selected five-second disorder boundary.
Business clocks EVENT_DATE()/EVENT_TIMESTAMP() are not replaced by watermarks;
neither function is needed by this query. Explicit progress records in finite
fixtures advance source event time and are excluded from session aggregates.
They do not establish a production heartbeat requirement.

The query retains SESSION window lineage through the existing native UNNEST,
count and map aggregation pattern. Identity/start use first arrival; device,
browser and country use last nonempty arrival; pages are sorted/deduplicated.
The native source/window late-row policy may differ from the application keyed
process contract. Test expectations must not be edited to hide that difference.
Pre-epoch maximum-end initialization also needs an explicit SQL projection
before full parity; the original application initializes its maximum to zero.

Prepare independent cases with test/streamr-reference/session-native/prepare.py.
Run them through the public pipeline API with udfs=[], RocksDB, and the existing
profile_runtime.py supervisor. Set --typed-owners 0 --window-owners 3: SESSION
and the two downstream instant windows own native state, not typed tables.
For chronological-recovery use --fault-prefix 3 --require-committed-output.
The supervisor requires actual worker SIGKILL, same-job fresh generation,
selected checkpoint restoration, identical committed bytes, fresh suffix and
exact application results. A generation/compilation-only pass is insufficient.

Exact commands, source binding and initial failed harness attempts are retained
under /home/jason/qa-evidence/arc18-native-session-20261018/ and on ARC-18.
