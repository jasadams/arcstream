# Native SESSION compatibility inputs

These fixtures preserve the application contract from `../session-mvp/fixture.py`.
They prepare input and explicit expected JSONL only. They neither run Streamr nor
claim that a native SESSION query produces the expected results. No application
transition UDF is imported. The existing integration and its oracle are unchanged.

Run `python3 prepare.py /absolute/new/directory --case chronological`.
Run generator checks with `python3 -m unittest discover -s test/streamr-reference/session-native -p 'test_*.py'`.

| Case | Requirement tested |
| --- | --- |
| `chronological` | Two ordered events, first identity, last nonempty metadata and closure after source progress |
| `late-first-arrival` | Earlier timestamp arrives second; start and canonical identity remain first-arrival values, browser follows arrival order |
| `reopened-late` | Close old session, reopen at 00:40, then accept a5 at timestamp00:20 into the reopened session, yielding two events |
| `complete-original` | Original five exact summaries, shared session ID across tenants, reopened identity, null session exclusion and pre-epoch boundary |
| `chronological-recovery` | Three independent sequential sessions, each with two activity rows and a raw-time progress closure; nonempty committed prefix plus fresh suffix |

For `chronological-recovery`, use the recommended source fault prefix of 3:
two events and the first closure-progress row. Four additional raw progress rows
separate that closure from the next session. The expected output is three
explicit 12-field summaries at 00:00–00:02, 02:00–02:02 and 04:00–04:02 UTC.
The runner must observe at least one committed summary and at least one still
unwritten summary at the fault; the fixture does not guarantee scheduling.
Keep exact committed-prefix and fresh-output gates. This separate case does not
change any existing compatibility oracle.

Each source record matches the fields of the existing native SESSION candidate:
`event_ts`, `event_ms`, `clock_ts`, `arrival_seq`, `is_clock`, identity and metadata.
`clock_ts` is exactly raw source event time. Final explicit progress rows advance
event time; no synthetic dispatch cursor, wall clock or lateness subtraction is
introduced. The SQL candidate must declare and record its own watermark policy.
Clock rows must advance source watermarks while being excluded from aggregation.
An empty session ID is a business-excluded input, not an implicit progress row.

The original fixture's timeout feedback records are omitted from native input.
Native windows are expected to perform closure. Their presence in the original
oracle does not prescribe timer topics or technical clock transport. Cross-key
output ordering must be recorded; do not silently sort away duplicates or
intermediate records when evaluating application compatibility.

`reopened-late` is a deliberate compatibility challenge: timestamp SESSION might
drop a5 as late or attach it to an earlier window. Neither outcome is the expected
application result. Do not revise the expected two-event reopened summary to make
a candidate pass. The complete case also preserves Java's `last_ms` initial zero:
the pre-epoch event ends at epoch zero, rather than at its negative timestamp.
Native `MAX(event_ms)` alone will differ there. A chronological pass establishes
only that case; full compatibility requires the other cases and actual recovery.

The manifest hashes the generator, original oracle source and exact prepared
files. Runtime/source-version/checkpoint/recovery evidence belongs to the runner;
generation success is not runtime validation.
