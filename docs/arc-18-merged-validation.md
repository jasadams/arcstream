> Historical source-bound report. Preserve the results below for their recorded
> versions; use the [23 October delivery snapshot](arc-18-native-delivery-20261023.md)
> for current integration and deployment status.

# Native integration on merged engine source

Validated 22 October 2026 against Streamr
`c5e066652755c76a2ac6cba46f190109a9fa421d`, which includes STR-146/29/60/64
(PRs 74/75/77/76). Arcstream starts at fetched cbcb901 and prepared PR32 content.
The original dirty checkouts were preserved. No production work was performed.

## Results

| Application slice | Exact result | Recovery scope |
| --- | --- | --- |
| COUNT → native state table | Four literal old/new/action rows pass | Real worker SIGKILL, CP1/source1/108 committed bytes, restored old=1/new=2, two fresh rows beyond pre-loss physical slots |
| Out-of-order SESSION | Full 12-field summary matches event-time identity/start/latest metadata | Real worker SIGKILL after two retained input rows; one fresh summary after restore; committed prefix empty, so no nonempty continuity claim for this case |
| Three completed SESSION summaries | All three full summaries match | Real worker SIGKILL, 318 committed bytes retained, two fresh summaries |
| Completed session count/average | Four exact per-key CDC images including before/after, millisecond averaging, tenant isolation, late rejection and two-day lifetime retention | Real isolated Kafka input and RocksDB; actual worker SIGKILL at source prefix3, 143 committed bytes retained, three fresh images |

All cases used the public pipeline API, a source-pinned production CLI built in
dev mode with matching precompiled RocksDB, and configured RocksDB state. File
inputs control finite prefixes only. The Kafka case uses an owned broker/topic,
ordinary source events and no client throughput pacing. Its output is a file
sink: it does not qualify Kafka output transactions or a full deployed graph.
Existing STR-146 engine qualification is reused within its recorded scope.

No cross-key ordering is assumed; full records, multiplicity and per-key CDC
chains are checked where applicable. No incorrect intermediate image is removed.
All owned engine processes, containers and Kafka network/broker were cleaned.
No throughput, production peak-RSS or full-profile parity claim follows.

## Full profile remains blocked

The prepared 20-field profile aggregate → snapshot MERGE query does not compile
on the merged source. Its actual error is:

```text
per-event aggregate MERGE supports native non-distinct COUNT/MAX and grouped selectors only
```

The smaller `test/streamr-reference/merged-contract-probes/` queries isolate
calendar aggregate exclusion, array aggregate exclusion and finalized SESSION
ownership rejection. Their exact SQL/errors and positive COUNT recovery are
preserved. The direct route is implemented; broader prepared compositions were
an assumption, not a delivered capability. No engine change or workaround was
added in response to these failures.

The 13 unfinished profile fields remain explicit. In particular native closed
SESSION metrics are not silently redefined as live session-start counts, and
closed summaries do not establish a current-session active/duration API.
Publication metadata still needs native SQL with explicit context. Whether
additional MERGE producers are necessary must be established from these actual
requirements and supported native alternatives before proposing an extension.

## Provenance

Evidence: `/home/jason/qa-evidence/arc18-integrated-validation-20261022/`.
The maintained `dev-pin2/source-manifest.json` records 1,331 compiled source
hashes; independent comparison to merged c5e06665 found zero mismatches.
CLI ELF SHA256:
`d8f179232d0fcc0f3df12658ee383caf166840c43d817d2f65d760c523a10e4d`.
Immutable build/runtime image: bad108ab1443e4b6fff271c31476586460a62bf1282857b24e3f1bf30d72aa5e.
Build profile dev, native mode prebuilt, shared maintained build/run queue.
Existing real UI assets were supplied only for CLI packaging, not UI validation.

Exact commands, configs, SQL, expected/actual output, checkpoint objects, fault
and restore receipts are retained per case. The initial two-row COUNT attempt
failed before fault injection because no fresh suffix remained; its four-row
replacement preserves every gate. Default-image native-pin and source-pin
preflight failures are preserved and are not labelled engine query defects.
