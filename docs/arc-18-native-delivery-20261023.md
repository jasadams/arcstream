# ARC-18 native delivery snapshot — 23 October 2026

Status snapshot: **2026-10-23 06:04 UTC**. Pending statuses below are time-bound;
consult the current ARC-17/18 ledger before acting.

The approved native graph and scoped recovery fixtures pass. The actual cluster
launch also passed readiness, but **production is not cut over**: FLA-183 blocks
routing because valid FlareDB queries fail during concurrent ingestion. Pending
candidate builds are not qualified artifacts. ARC-17/18 retain the delivery ledger.

## Current application contract

Five zero-UDF jobs implement identity state lookups, unified events, directed
identity merges, closed sessions and profiles. Application SQL, schemas and
identity/lifecycle policy remain in Arcstream; execution uses native SQL/state
tables and RocksDB. Sources cast original payload `event_time` into `event_ts`
and declare exactly five seconds of event-time disorder. `EVENT_DATE()` and
`EVENT_TIMESTAMP()` use original stream-trigger time, never wall clock or the
lateness-adjusted watermark.

Profiles publish 22 native fields: 21 business fields exposed by GraphQL plus
native create/update action. Core metrics, native arrays and nullable fractional
closed-session average are included. Seven live/start fields and four unused
publication fields are explicitly deferred/omitted as recorded in
[`release-fields.json`](../deploy/streamr-native/profile-native/release-fields.json).
SESSION publishes 12 native fields. The existing analytics schema stores ten of
those fields plus derived date/hour; it does not store `pages` or `event_types`.
This is the approved scope, not full historical Flink parity.

Profile calendar SQL follows processed stream date. Existing API chart filters in
`query-api/src/schema/stats.rs` (`TimeRange::cutoff_timestamp`) still use
`Utc::now()`; historical replay can therefore leave wall-clock Day charts empty.
Neither contract is silently changed to populate the demo, and UI parity is not
claimed. WebSocket consumers read committed output without that chart cutoff.

## Exact sources and deployed test pins

| Component | Source / immutable artifact |
| --- | --- |
| SQL/configuration | Arcstream `b173ca1bf84d19877d38021f64231789dcefad90` (PR38 merge) |
| Streamr | `ac15b4e9dec7bfa5a8ea080a712c71897c520ca5`; `ghcr.io/jasadams/streamr@sha256:c341eca8e6f88bd4f7d97bde7462e337faf5cfd604db50c7cb1840ff264e5a20` |
| Application images | `ce4962fb2965783446649f34472d964ba076ff35` |
| Query API | `ghcr.io/jasadams/arcstream/query-api@sha256:b423e8da8ecfdbd068574eaa37332487cc2c948bf427a031b5c94984e8c73528` |
| Dashboard | `ghcr.io/jasadams/arcstream/dashboard@sha256:ebbd4b133410faadd457a43134794eae2699651a453391fc858afa67d31f80c3` |
| Existing test FlareDB | `9e7b82b68789c7c543171d84ef0b83f8e3da27d3`; `ghcr.io/jasadams/flaredb@sha256:d70db8acf6a71864e045ef0fa69972218a79a05ddece56a87c45f427ce9ef4d8` |
| Deployment source | Private launch `fa85228f156193f04baa6ee5e9296e8d629b7ff4`; guarded update merged as `ba53bb68870a4c9e5383774e41097d1310a9cb29` (PR21) |

The graph/SESSION fixture binary is source `1314e702a955ad3f19c390b3f10c9d3ee1958d40`,
SHA256 `3e2594d62f6103501a84d17ff133032866d1929a0e84c42949ad3d7976b9d255`.
Independent compiled-input audits establish identical Rust crate/Cargo inputs in
the published ac15 build; packaging differs. Those fixture runs are not described
as executions of the published image. The actual cluster launch uses that image.

## Passed evidence and its limits

- The [composed publication fixture](arc-18-publication-recovery-20261023.md)
  passes one composed Kafka/RocksDB history with 11 stages and 13 literal
  22-field profile snapshots, including two actual worker SIGKILLs, selected-
  checkpoint restores, committed-output prefixes, fresh suffixes and recovered
  identity lookups. This fixture does not establish full directed identity-merge
  parity.
- PR38 passes nine full 12-field SESSION correctness/recovery cases, including
  late admission/cutoff, ordering/ties, tenant reuse and larger repeated pages.
  The chronological recovery has a nonempty committed prefix; the larger-pages
  case restores an open session with an empty prefix and emits a fresh summary.
- Existing native `array_sort(array_distinct(pages))` normalizes the final pages
  set before histogram grouping. The coherent bounded configuration is 16KiB
  window keys, 32KiB partials and an 8MiB shared scan pool; all guards remain on.
  A 13,000-byte key reproduces old checkpoint export failure (required27013,
  limit25486), then passes actual restore with the coherent configuration.
  A separate full SESSION oracle preserves native `pages:null` for no pages.
  See [resource bounds](../deploy/streamr-native/runtime-resources.md).
- Published FlareDB/strict API three-table qualification passes exact literal
  relations, typed arrays, durable offsets, actual SIGKILL137, restored prefix
  and fresh suffix. This replays captured native wire payloads; it does not
  recompute Streamr during that phase.
- [Launch6 CI39903686741](https://github.com/jasadams/arcstream-cloud/actions/runs/39903686741)
  succeeds in `arc-native-test-20261023d`: five native jobs, three ingestion
  sources, stored native arrays/closed average, direct stored-profile/GraphQL
  agreement and dashboard response. Its intermittent readiness success does
  **not** clear the independently observed query-availability failure.

Independent written audits and required CI accompany the scoped source changes;
PR38 CI39902308405 and private update CI39907256811 pass. No throughput/RSS
regression claim follows from these fixtures or loaded-host observations. Reuse
Streamr's completed STR-126 qualification rather than rerunning its full campaign.

## Current blocker and pending correction

FLA-183 reproduces valid `SELECT COUNT(*) AS row_count FROM events` returning
HTTP400 during ingestion despite ample memory credit. The finite old-image case
records 3,988 successful probes and 12 actual contention errors, with zero literal
prefix violations. Exact three-table relations/offsets and SIGKILL recovery still
pass; query availability fails. Typed ledger contention bypasses pre-execution
capture retries; current-generation validation can also race continuous writes.
No SQL rewrite, pacing, raised budget or stale snapshot hides either issue.

The reviewed correction is FlareDB PR126 source
`caa0cfe5e2446126e0048adb905f06dc6cdc0607`. Its maintained local full suite passes
1,745 tests with zero failures and 26 ignored cases across 43 result groups;
the corrected native-admission regression passes 1/1. Ignored cases are not
claimed executed. Exact command, output and audit are retained in
`/home/jason/qa-evidence/fla183-query-admission-20261023/full-validation-caa0cfe.json`.
At this snapshot,
[required CI39908124229](https://github.com/jasadams/flaredb/actions/runs/39908124229)
and [publisher39908160611](https://github.com/jasadams/flaredb/actions/runs/39908160611)
are pending. Candidate concurrent-query/recovery qualification and the actual
retained-store update must pass before routing. Retry classification alone is
not a guarantee of snapshot progress under arbitrary continuous mutation.

## Maintained deployment path and remaining work

Use the private [`native-kubernetes-test` guidance](https://github.com/jasadams/arcstream-cloud/blob/ba53bb68870a4c9e5383774e41097d1310a9cb29/scripts/native-kubernetes-test/README.md).
Its explicit `update-flaredb` operation selects a successful main launch artifact,
changes only the qualified FlareDB image/source and preserves replay/PVCs/SQL.
It checks a current operator-observed namespace UID and the original CI creation
window; it does not invent a historical UID capture. Fresh readiness and 100
unchanged concurrent queries are required. Production routing remains a separate
[workflow](https://github.com/jasadams/arcstream-cloud/blob/ba53bb68870a4c9e5383774e41097d1310a9cb29/.github/workflows/native-production-cutover.yml),
followed by external browser, GraphQL and actual WebSocket message checks.

FLA-181 tracks idle restored partition health visibility; durable-offset and
relation recovery are verified separately. FLA-182 tracks rootless host-network
memory discovery; ordinary private/container/pod networking passes. STR-152 tracks
native collection aggregates with variable-width selector companions. STR-153
tracks inconsistent live-entry versus checkpoint-page admission. The current
supported SQL/coherent resource configuration avoids those unsupported shapes;
these are disclosed product gaps, not silently implemented capabilities.

Historical replay has produced July25 data. Neither 90-day coverage nor replay
completion is established. Raw retention, timestamp policies and broker limits
are unchanged; no retention hold is introduced. Production traffic remains on the
existing route until actual correction/update/cutover evidence is recorded.

## Commands and retained receipts

Read-only workflow inspection:

```sh
gh run view 39903686741 --repo jasadams/arcstream-cloud
gh run view 39908124229 --repo jasadams/flaredb
gh run view 39908160611 --repo jasadams/flaredb
```

Exact executable/container arguments, outputs and independent audits are retained
under these local evidence roots; existing cases are non-overwritable. Any repeat
uses the maintained shared build/run queue and a new owned case directory.

| Evidence root under `/home/jason/qa-evidence/` | Key receipts |
| --- | --- |
| `str150-composed-publication-20261023/attempt-2/` | `integration.json`, `graph-2/` |
| `arc18-session-group-keys-20261023/attempt-3/` | `run-cases.py`, `*-command.json`, `runtime-results.json`, `independent-audit.json` |
| `arc18-session-key-envelope-20261023/` | `run-probes.py`, `coherent-command.json`, `runtime-results.json`, `independent-runtime-review.json` |
| `arc18-flaredb-three-table-20261023/` | `image-3/receipt.json`, `independent-image-3-audit.json` |
| `arc18-flaredb-query-contention-20261023/` | `baseline-summary.json`, `baseline-256/commands.json`, complete HTTP outcomes and recovery receipts |
| `arc18-image-pins-20261023/` | source/provenance captures, `launch6-dispatch-inputs.json`, `launch6-artifact/`, `launch6-audit/` |
