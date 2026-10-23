# ARC-18 native delivery snapshot — 23 October 2026

Status snapshot: **2026-10-23 08:18 UTC**. Statuses below are time-bound;
consult the current ARC-17/18 ledger before acting.

The approved native graph and scoped recovery fixtures pass. The actual cluster
launch also passed readiness. FLA-183 is repaired and its published image passes
the finite concurrent-query/API/recovery gate, but **production is not cut over**:
the larger live store exposes generation starvation tracked as FLA-184. Its
scoped repair is underway; no replacement image is qualified yet. ARC-17/18 retain
the delivery ledger.

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
| Current test FlareDB | `caa0cfe5e2446126e0048adb905f06dc6cdc0607`; `ghcr.io/jasadams/flaredb@sha256:efaa877e987ef0452bcc1e13a23ab0a6613674691a7842276252f80c0643d653` |
| Deployment source | Private launch `fa85228f156193f04baa6ee5e9296e8d629b7ff4`; current proof/recovery path `7b03f9cb7fb5c2b8092c1529ae4d77732670ca22` (PR23) |

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

## Availability correction and remaining blocker

FLA-183 reproduced valid `SELECT COUNT(*) AS row_count FROM events` returning
HTTP400 during ingestion despite ample memory credit. The old `9e7b82b`/`d70db8` image
records 3,988 successful probes and 12 contention errors, with zero literal prefix
violations; exact three-table relations/offsets and SIGKILL recovery still pass.
Typed ledger contention incorrectly bypassed pre-execution capture retries.
That original failed availability evidence remains unchanged.

The correction is FlareDB PR126 source
`caa0cfe5e2446126e0048adb905f06dc6cdc0607`, merged as
`1dd82c6f02c0ceaa8988d1ff9651c1b2d5579164`.
[Required CI39908124229](https://github.com/jasadams/flaredb/actions/runs/39908124229)
and [publisher39908160611](https://github.com/jasadams/flaredb/actions/runs/39908160611)
pass on that exact source; OCI/SLSA source/digest verification is retained. Local
full validation passes 1,745 tests, zero failures and 26 ignored cases across 43
result groups, including the corrected native-admission regression 1/1. Ignored
cases are not claimed executed.

The actual published efaa image passes 4,000 concurrent unchanged COUNT probes
with zero probe errors, nine exact SQL relation comparisons, durable-offset
restore, actual SIGKILL137/fresh suffix and all 21 GraphQL profile fields for two
tenants (nullable/fractional averages `3.75`/`null`). One separate recovery-startup
HTTP400 is retained; it is not hidden or counted as a probe success. This finite
small-store gate does not establish progress at the larger live store.

FLA-184 is the current release blocker. After efaa was applied in the original
namespace, its actual 180-second readiness proof failed with two events COUNT
HTTP400s: `mutable generation changed during admission`. It never reached the
100-query stage. A deterministic old-source failure now reproduces this gap.
The scoped repair targets an append-only, one-source local full-schema retained
immutable Arrow prefix; implementation is underway and candidate qualification is pending.
No SQL rewrite, pacing,
raised budget, stale snapshot or extended deadline substitutes for that repair.
FLA-185 tracks broader upsert/validity/multi-source/hybrid/owned snapshot progress
as backlog; it is not automatically a release prerequisite for this scoped route.

The operational attempts are distinct from that product failure:

| Attempt / delivery | Actual result |
| --- | --- |
| [Update39913613800](https://github.com/jasadams/arcstream-cloud/actions/runs/39913613800) | Failed before any patch: Kubernetes omitted optional empty PodSpec volumes for query-api/dashboard. PR22 fixes only that equivalence; CI39914475139 passes 39 tests. |
| [Retry39914604592](https://github.com/jasadams/arcstream-cloud/actions/runs/39914604592) | Applied efaa and completed rollout; only FlareDB changed. Combined resource creation emitted two JSON objects and the receipt parser failed. The actual proof also independently failed as described above. |
| [Private PR23](https://github.com/jasadams/arcstream-cloud/pull/23) / [CI39923538819](https://github.com/jasadams/arcstream-cloud/actions/runs/39923538819) | Merged with 47 tests passing: create proof resources separately and support explicit, source-bound recovery from the proven-applied failed update. No recovery execution or readiness credit yet. |

The other four application pods, all five native job/run identities, state PVCs
and ConfigMap data remain preserved by the recorded update. Profiles/sessions
were replaying their derived topics at restart; this observation is not an exact
live payload-continuity proof. The separate fixture qualification supplies exact
relation/offset recovery evidence. No failed CI run is relabelled successful.

## Maintained deployment path and remaining work

Use the private [`native-kubernetes-test` guidance](https://github.com/jasadams/arcstream-cloud/blob/7b03f9cb7fb5c2b8092c1529ae4d77732670ca22/scripts/native-kubernetes-test/README.md).
Its explicit `recover-flaredb` operation binds the original successful launch
39903686741 and failed applied update 39914604592, verifies the currently applied
efaa UID/full-spec/CM/PVC baseline, and permits only a newly qualified image
replacement or explicit current-image verification. Original failures remain
unchanged. It checks a current operator-observed namespace UID and the original
CI creation window; it does not invent historical UID capture or blindly rerun
the old d70 image update.

The repaired engine must first pass source/image-bound large-store query and
separate three-table/API/SIGKILL gates. The retained-store recovery then requires
actual strict readiness and 100 unchanged concurrent queries with zero errors
and ingestion advance. Production routing remains a separate
[workflow](https://github.com/jasadams/arcstream-cloud/blob/7b03f9cb7fb5c2b8092c1529ae4d77732670ca22/.github/workflows/native-production-cutover.yml),
followed by external browser, GraphQL and actual WebSocket message checks. No
recovery or verification is run merely to fish for an intermittent pass.

FLA-181 tracks idle restored partition health visibility; durable-offset and
relation recovery are verified separately. FLA-182 tracks rootless host-network
memory discovery; ordinary private/container/pod networking passes. STR-152 tracks
native collection aggregates with variable-width selector companions. STR-153
tracks inconsistent live-entry versus checkpoint-page admission. The current
supported SQL/coherent resource configuration avoids those unsupported shapes;
these are disclosed product gaps, not silently implemented capabilities.

Historical replay has produced July 25 data. Neither 90-day coverage nor replay
completion is established. Raw retention, timestamp policies and broker limits
are unchanged; no retention hold is introduced. Production traffic remains on the
existing route until actual correction/update/cutover evidence is recorded.

## Commands and retained receipts

Read-only workflow inspection:

```sh
gh run view 39903686741 --repo jasadams/arcstream-cloud
gh run view 39908124229 --repo jasadams/flaredb
gh run view 39908160611 --repo jasadams/flaredb
gh run view 39914604592 --repo jasadams/arcstream-cloud
gh run view 39923538819 --repo jasadams/arcstream-cloud
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
| `arc18-flaredb-query-contention-20261023/` | `baseline-summary.json`, `image-caa0cfe-256-summary.json`, `independent-image-caa0cfe-review.json`, complete HTTP outcomes/recovery receipts |
| `arc18-image-pins-20261023/` | `native-rollout-current-state.json`, source/provenance captures, `launch6-artifact/`, `update-caa0cfe-audit/`, `update-caa0cfe-retry-audit/` |
| `arc18-proof-json-recovery-20261023/` | independent control/recovery reviews, actual recorded-lineage validation, `required-ci-39923538819.json` |
