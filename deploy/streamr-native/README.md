# Native Streamr MVP candidate

The MVP candidate uses Streamr's native SQL/state engine and Arcstream-owned
identity, profile and session policy. It is isolated from existing deployments.
See [the application contract](../../docs/arc-17-native-mvp.md) for exact behavior,
evidence and acceptance limits. Production cutover requires explicit authorization.

The engine contract is merge `ad0a4814d5b7c07d74dce54753f5e6e9aaa7a062`.
Official Rust UDF compilation additionally requires the existing-interface export
annotation repair in [Streamr PR70](https://github.com/jasadams/streamr/pull/70),
implementation `a86297dc049c0c3a5a85f965c782cc571ae0d2d2`. The candidate
manifest records these separately; historical executable receipts retain their
actual source pins.

Prepare a new isolated candidate with explicit clock policy:

```sh
python3 deploy/streamr-native/prepare_mvp.py prepare /tmp/arc-mvp-candidate \
  --broker YOUR_ISOLATED_BROKER:9092 --topic-prefix arc-mvp-candidate \
  --clock-mode replay
```

`live` preserves current observation-clock profile policy; `replay` preserves valid
historical source timestamps and logical replay duration. Use a new namespace and
state when changing modes. The normalizer admits every input without a transport
admission floor, carries the original application `event_time`, and supplies separate
profile-assigned and raw session clocks. State owners maintain input progress;
native TUMBLE pipelines return generation-checked lifecycle controls. Reserved
trusted clock controls enter only the internal `execution-events` topic using
all three reserved identity/type fields listed in the manifest. Never publish
those controls to the public unified analytics topic.

The manifest contains eleven parallelism-one pipelines and all required
single-partition topics. Create each topic with its declared cleanup policy;
commit recovery topics must be compacted and distinct per sink. Use a compatible
Streamr configuration and its maintained build/run queue. The candidate uses
complete primary-key lookups and configured native memory or RocksDB state.
Profile rows can exceed the engine's tiny pilot defaults. Merge the generated
`worker-configuration-patch.json` into the isolated engine configuration, retaining
all other explicit resource budgets and setting a dedicated state directory and
chosen backend. The patch declares candidate row/decode/scan bounds; compare them
with the accepted runtime receipt and existing admission formulas. It is not a
production sizing claim, and guards remain enabled.

The `analytics/` directory contains broker/topic-mapped copies of existing backend
configurations, with explicit committed isolation for Pinot. They are review
artifacts for a fresh isolated backend catalog/storage. They preserve existing
schemas and dedup policy: session storage currently has ten scalar fields, and
Pinot's existing session-ID-only dedup key does not qualify tenant isolation or
session reuse. The selected FlareDB image is source-bound and startup-tested; actual
transactional ingestion and backend materialization remain qualification gates. No backend API writes occur during
preparation or submission.

Submit the prepared candidate only to an isolated Streamr catalog:

```sh
scripts/dev-build python3 deploy/streamr-native/prepare_mvp.py submit \
  /tmp/arc-mvp-candidate --api-url http://YOUR_ISOLATED_ENGINE/api/v1
```

Submission validates native queries, pins query/UDF bytes, and resumes identical
existing pipelines after partial submission. It refuses conflicting existing
queries or parallelism. `manifest.json` contains consumer environment settings
for the unchanged unified-event and full33 profile-update contracts. Each sink
commits independently; the graph does not provide one transaction across topics.
CDC before/after chains remain available on `profile-changelog`; the flat adapter
publishes complete current profiles for existing application consumers.

Run lightweight adapter/oracle checks with:

```sh
python3 -m unittest discover -s test/streamr-reference -p 'test_*.py'
python3 -m unittest discover -s test/streamr-reference/profile-mvp -p 'test_*.py'
```

Real runtime qualification uses `native_mvp_recovery.py`, `profile_runtime.py` and
`native_mvp_kafka.py` through the maintained shared queue, with separately pinned
engine binary, plugin source, image, fixture and oracle. Runtime receipts, exact
commands and outstanding acceptance are recorded on ARC-17 and in the application
contract. These tools create isolated resources and do not switch application
traffic. They do not establish production readiness or throughput regression.

## Earlier identity-only fixture

These queries resolve identities through tenant-partitioned anonymous-ID and
user-ID state tables. The identity pipeline writes one capture record per input;
two downstream pipelines publish unified events and identity merges. The two
output topics commit independently.

Use an isolated broker and Streamr catalog. Create the four `arc-native-*` data
topics named in the SQL, and the three distinct recovery topics with one partition
and `cleanup.policy=compact`. Submit each query with parallelism one. The fixture
relies on the declared input order.

Run the initial fixture against that instance:

```sh
python3 deploy/streamr-native/fixture-run.py \
  --api-url http://localhost:18150/api/v1/ \
  --broker-container YOUR_BROKER_CONTAINER \
  --evidence-dir /path/to/new-attempt
```

The runner compares every output field against the reference fixture, allowing
UUID renaming while preserving identity lineage and merge direction. It verifies
the first 13 committed unified events and two merges. This initial fixture check
does not establish absence of extra records, restart correctness, sustained load,
or profile/session qualification. Recovery validation must additionally scan
complete committed topics and verify stable literal IDs across checkpoint
restoration and new input.

Run the adapter and comparator checks with:

```sh
python3 -m unittest discover -s test/streamr-reference -p 'test_*.py'
```

No existing application deployment or production topics are changed by these
files. The original Flink identity fixture remains the comparison oracle.

The reference fixture and comparator were reused from Arcstream commit
`87d20e52a5690fb7f3b6b93a44afd01f3b4495fa` (ARC-17 reference work). The native
identity body was preserved from the existing ARC-13 evaluation, with Kafka
source/sink configuration added here. Existing reference worktrees and PRs remain
separate; this increment provides the native state-table integration.

The pinned FlareDB image uses a scratch runtime. Compose runs the SQL readiness
probe in the separate pinned `flaredb-health` curl container; query-api waits for
its successful `POST /query/sql` with `SELECT 1`. The FlareDB image runs as
UID/GID `10001:10001`. Before a separately authorized cutover, verify that the
existing data volume is writable by that identity and preserve its contents;
this integration does not change volume ownership or restart production.
Compose sends SIGINT on stop, matching this release's orderly shutdown handler.
