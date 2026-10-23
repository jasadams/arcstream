# Native SESSION product fixtures

The fixtures and full 12-field expected summaries are standalone application
oracles. They import no UDF or legacy transition implementation. The product
allows five seconds of event-time disorder; older records are rejected. The
previous workaround's acceptance of thirty-second-late, twenty-minute-late and
ancient records is intentionally removed under the user's explicit instruction.

| Case | Product requirement |
| --- | --- |
| chronological | Ordered activity, first identity, last nonempty metadata and native closure |
| late-first-arrival | Thirty-second-late activity is dropped |
| reopened-late | Reopened session excludes activity twenty minutes behind current progress |
| tenant-reopen | Four explicit summaries, tenant isolation, reopening, null-session exclusion and ancient-event rejection |
| five-second-cutoff | Accept 5,000ms disorder, drop 5,001ms disorder; choose identity/start and metadata by event time among admitted rows |
| event-time-ordering | Four admitted events arrive out of timestamp order; earlier arrival must not choose first identity/start or final metadata |
| equal-time-ties | FIRST keeps the first equal-time arrival; LAST uses the later write |
| chronological-recovery | Three independently specified summaries; first committed before worker loss, two fresh afterward |

The tenant-reopen product oracle explicitly expects four summaries. Historical
failed diagnostics remain historical evidence; new source and expectation hashes
require new runs.

Run python3 prepare.py /absolute/new/directory --case chronological.
Run python3 -m unittest discover -s test/streamr-reference/session-native -p 'test_*.py'.
Preparation refuses overwrite and hashes its source and output files.

Finite progress inputs are ordinary page-view events with empty session IDs.
They advance source event time; the normal business session-ID filter excludes
them from SESSION aggregates. Inputs contain no special clock flag or timestamp.
No wall-clock advance, synthetic transport cursor, feedback control or lateness
offset is applied. The SQL declares its watermark separately.

There is no cross-key output ordering contract. Compare complete expected records
with multiplicity; never discard duplicates or extra results. Preserve physical
committed output bytes exactly when validating recovery.

For chronological-recovery use --fault-prefix 3 --require-committed-output.
Four additional raw progress rows separate the first closure from the next
session. The supervisor must observe nonempty committed output and unwritten
application results at the fault, verify actual SIGKILL and selected checkpoint
restoration, preserve committed bytes and compare all three exact summaries.

The source payload carries only the application `event_time` text. Typed timestamp
and millisecond columns are generated in SQL, with no fabricated `arrival_seq`.
The event-time-ordering fixture deliberately makes event-time and arrival answers
different: arrivals at seconds 4, 2, 6, 3 yield start 2, end 6, identity from second
2 and last metadata from second 6. All four are admitted by five-second disorder.
The cutoff fixture's accepted second-5 event changes the first identity/start;
the second-4.999 event is dropped. Equal-event-time expectations follow STR-60's
first-arrival FIRST and later-write LAST rule. These are independent expectations,
not observed engine results. Runtime qualification remains pending STR-60.
