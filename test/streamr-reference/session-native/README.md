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
| large-pages |10 admitted mixed-type events; long repeated pages exceed512 bytes before encoding, preserving the complete summary in native aggregate values |
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


The `large-pages` regression contains10 ordinary ordered activity records over
9seconds and one ordinary closing progress record. Repeated page strings alone
occupy1100 UTF8 bytes before encoding: the previous grouping of the full raw
pages array could not fit a512-byte key. The expected12 fields are independent
literals: first identity `long-first`, count10, duration9, three110-byte literal URLs rooted at
`/catalog/product`, `/checkout`, `/home`, event counts4page_view/3click/
3feature_used, final metadata tablet/Safari/NZ. Metadata/page aggregates remain
native values; no limit is increased, event is removed or result is inferred
from an engine run. Existing fixture expectations remain unchanged.

For bounded runtime qualification, run all nine cases on RocksDB with the
existing512-byte key limits. Use `native_sql_runtime.py`, `--typed-owners 0`,
`--window-owners 3`, `--unordered-output` and the existing normal resource
configuration. Use `--fault-prefix 2` for ordinary cases, `--fault-prefix 10`
for `large-pages` (all10 open-session rows checkpointed before closure), and
`--fault-prefix 3 --require-committed-output` for `chronological-recovery`.
The latter must retain the first committed summary across actual SIGKILL and
produce both fresh later summaries. These are commands to execute, not passes.
Coordinate one shared run-queue reservation with the deployment owner; do not
start a second overlapping campaign.

Published Streamr image `ghcr.io/jasadams/streamr@sha256:c341eca8e6f88bd4f7d97bde7462e337faf5cfd604db50c7cb1840ff264e5a20`
is source `ac15b4e9dec7bfa5a8ea080a712c71897c520ca5`. The already qualified
`1314e702a955ad3f19c390b3f10c9d3ee1958d40` executable is an alternate runtime:
these sources have no diff in `crates/`, `Cargo.toml` or `Cargo.lock`. Its binary
SHA256 is `3e2594d62f6103501a84d17ff133032866d1929a0e84c42949ad3d7976b9d255`.
Record this source equivalence and actual executable/config/query/fixture hashes
when using that alternate; do not describe it as executing the published image.

The ten-row case avoids the single-file harness's checkpoint-per-input overhead;
each of its three explicit page URLs is110 UTF8 bytes. This remains a regression
for full-array grouping, without a larger event campaign.
