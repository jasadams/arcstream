# Complete native release graph fixture

The eleven raw inputs and literal cumulative profile snapshots extend the
independently authored mixed/release fixture, not an engine capture. The native 21-field CDC after-image plus c/u-derived action makes 22
profile fields checked for every emitted image, preserving each person's
sequence. Every CDC before-image must equal its previous complete after-image;
deletes, duplicate creates, orphan updates and unexpected fields fail. No
intermediate CDC output is discarded. Cross-person order alone is unspecified. No aggregate is computed by
the fixture runner.

The input omits canonical IDs. UUID generation is native application SQL.
Canonical values are normalized only after checking UUID syntax, exact raw
payload preservation, unique ordered event IDs, and per-tenant equivalence.
The expected equivalence is one canonical per tenant. Both tenants reuse the
same anonymous string. Input 8 introduces a new anonymous string with the
already-bound `logged-user`; input 9 uses that new anonymous string without a
user ID. These steps prove user lookup and persisted anonymous binding. An
unexpected split, cross-tenant reuse, duplicate or merge fails.

Input 6 is seven seconds behind the maximum and must be rejected under the
existing five-second admission contract. The identity checkpoint's raw Kafka
next-offset still advances; downstream offsets are not mistaken for raw input
positions. Closed-session summaries are independently literal: the first span
is five seconds and the second 2.5 seconds. The existing session-summary query
publishes integer durations (5 and 2); the release profile retains fractional
completed average 3.75. These distinct public contracts are checked explicitly.

The graph emits no directed identity merge in this minimal fixture. The merges
pipeline is started and its committed topic must remain empty. Directional merge
correctness is not claimed by this fixture.

`native_graph_fixture.py` owns broker, network, all eleven single-partition
Kafka topics and five exact rendered SQL queries. Recovery topics are
compact-only. It retains ordinary bounded native RocksDB allocations and invokes
the runtime only under the maintained shared run queue. Cleanup removes owned
containers and their volumes by actual IDs, never unrelated resources.

`native_graph_runtime.py` kills the actual profiles owner after the second
committed image and identity owner after input five, when both anonymous and user
bindings are populated. It requires both typed
identity tables, profile disk-backed aggregate state, artifact hashes, fresh
worker generations, exact selected-checkpoint restoration logs, preserved
committed Kafka prefixes and fresh results. All pipelines stop at a checkpoint,
then finite read-committed broker-end captures validate the complete output.
Prepared files and unit tests are not runtime evidence. A failed native query,
owner layout or exact output is retained as a failure; no pacing, altered time,
output filtering or relaxed oracle is permitted.

Current failure status: the retained `graph-3` run produced native profile CDC
`c,d,c` while introducing a previously absent LEFT JOIN side. The strict
application decoder rejected the delete. This is a real unresolved graph result;
no retractions are filtered or coalesced and no literal expectations changed.
Native materialization SQL is under investigation. The harness now reads bounded
allocations from maintained `deploy/streamr-native/runtime-resources.json`, with
a source-hash/allocation receipt; this configuration wiring is not a rerun or a
claim that the graph failure is fixed.
