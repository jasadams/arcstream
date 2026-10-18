# Native application integration preparation

Prepare against STR-29 (quiet calendar zero), STR-60 (event-time selectors),
STR-64 (maintained results into state tables) and STR-146 (Kafka checkpoint
journal flow control). This prepares Arcstream SQL against their agreed
contracts; ticket completion is not an application runtime pass.

```sh
python3 deploy/streamr-native/prepare.py \
  --output /absolute/new/native-bundle \
  --brokers owned-broker:9092 --namespace arc-evaluation
```

The command only writes reviewable SQL, public API request JSON and a hashed
manifest. It never contacts an engine, creates topics, submits pipelines or
deploys. Requests use zero UDFs, parallelism 1 and the existing two-second
checkpoint cadence. Use configured RocksDB and ordinary bounded native resource
settings; no memory-only execution or client-side pacing is a replacement.
All sources use actual payload event_time and exactly five seconds of disorder.

| Prepared component | Input → output | Status |
| --- | --- | --- |
| identity | Raw events → identity capture | Existing native two-table owner; watermark alias corrected; revalidation required |
| unified | Identity capture → unified events | Native projection; revalidation required |
| merges | Identity capture → directed identity merges | Native filtered projection; revalidation required |
| sessions | Unified events → 12-field closed session summaries | Native SESSION, implicit event-time FIRST/LAST; awaits STR-60 runtime |
| profile-core | Unified events → 20-field diagnostic snapshots | Native aggregates, arrays, calendar filters, UNION and scalar snapshot MERGE; awaits STR-29/60/64 composed validation |
| closed-metrics | Unified events → closed-session count and average seconds | Native SESSION and COUNT/SUM, millisecond precision; awaits STR-60 runtime |

The last two topics are explicitly diagnostics. They cannot be connected to the
existing profile consumer, which requires all 33 fields. Core MERGE RETURNING
is an append snapshot diagnostic (`format=json`); it is not mislabelled as the
final profile CDC publication. Closed-metrics is an updating aggregate CDC
stream. Sessions are append summaries. No incomplete full-profile pipeline is
submitted or exposed by this preparation command.

For the existing file-source runtime harness, render a single component with
`--fixture-component sessions` (or profile-core/closed-metrics),
`--fixture-input /absolute/input.jsonl` and
`--fixture-output /absolute/output.jsonl`, plus the arguments above. Paths must
be visible inside the qualified runtime. Preparation remains unexecuted evidence.

## Acceptance after engine integration

Use `test/streamr-reference/native_sql_runtime.py` in the qualified runtime image
through the maintained shared queue. Record binary/source/image hashes and
configurations. Require RocksDB, actual SIGKILL, selected checkpoint restore,
nonempty committed output continuity and fresh recovered results. Never label
compiler/planner checks as application correctness or recovery passes.

SESSION fixtures include accepted out-of-order activity, equal-time FIRST/LAST
ties, cutoff rejection, tenant isolation and reuse. Profile fixtures include
raw-event-date midnight behavior, retained future contributions, top ranking
changes, nonblank metadata and quiet-key zero. Closed metric fixtures distinguish
millisecond averaging from truncating each duration and retain lifetime values
across two days. They are independent expected results, not engine-generated
oracles. The isolated Kafka helper retains exact per-key CDC chains for updating
metric fixtures; append snapshots/summaries need the appropriate output oracle,
not a falsely applied CDC assertion.

Do not infer that STR-64's first COUNT/MAX increment supports this whole graph:
multi-branch retractions, named MERGE source/old/new, calendar maintenance with
no ordinary new event for the affected key, array source pass-through and
checkpoint/output continuity must be demonstrated on its delivered source.

## Still unfinished application SQL

`profile-native/fields.json` maps 20 prepared fields and the remaining 13. The
closed-metrics component supplies a separate concrete average candidate, not a
complete combined profile. Remaining session start/count timing, current active
session/duration, lifecycle metadata and final publication require explicit SQL
and results. A completion-only SESSION row cannot be relabelled a live session
start; prior calculated snapshots cannot be relabelled prior published snapshots.
EVENT_TIMESTAMP() is source-trigger event time, not wall time, MAX time or the
watermark expression. Native closure/quiet-maintenance context requires evidence.

The older profile-session-counts cursor and historical profile-composition
instructions retain source-bound evidence only. They are not used by this
renderer. No arbitrary cursor, timer feedback, scalar transition code or
fabricated false/zero fields are authorized. If another native capability is
required, preserve and report its runnable failed SQL before adding one.

Production cutover remains separately authorized work. This bundle deliberately
has complete_mvp=false and production_ready=false.
