# Native production-host test deployment foundation

This separate Compose project attaches to existing Kafka. It creates no broker,
Flink, Pinot, event generator or timer service, does not change the main site's
routing, and contains no data deletion or retention override. Its durable volumes
hold Streamr's SQLite catalog/checkpoints/RocksDB and a dedicated FlareDB database.

Source base: fetched Arcstream main cbcb901be8482c01acce3471b4b6ba3a4242c3d0,
then fast-forwarded PR32 dependency 6a13f634c538ae7688e766e8e8ea91220fc98cb6.
Guidance read: AGENTS.md, CLAUDE.md, .claude/build-test.md, .claude/team.md,
docs/arc-18-native-status.md and current ARC-18 ledger. No production service
was contacted. Source-only checks are deliberately separate from runtime proof.

## Required inputs

Export NATIVE_NAMESPACE (fresh, lowercase), KAFKA_BROKERS (reachable from these
containers), RAW_EVENTS_TOPIC (existing input), NATIVE_TEST_PORT (dedicated), and
all six image references with an explicit `@sha256:<64 hex digits>`:
STREAMR_IMAGE, FLAREDB_IMAGE, QUERY_API_IMAGE, DASHBOARD_IMAGE, PYTHON_IMAGE,
GATEWAY_IMAGE. STREAMR_IMAGE must expose the Streamr binary as its entrypoint;
PYTHON_IMAGE must provide python3; GATEWAY_IMAGE must provide nginx.
No image version or release tag is invented or published by this foundation.

The Streamr image must be the source-qualified native build, with process workers
and RocksDB included. The application images must include the approved 22-field
profile contract. The renderer reuses the maintained `prepare.py --profile-release`
and requires its explicit `--raw-events-topic` interface (being integrated by the
coordinating agent); it does not rewrite rendered SQL.

Supply exactly profiles.json, events.json and sessions.json in a reviewed native
FlareDB load-tables directory. The legacy flat profiles config is **not compatible**
with native Debezium output and must not be copied as a substitute. Exact current
FlareDB ingestion/schema compatibility remains an integration input, not a guessed
new HTTP endpoint. FlareDB initialization uses the existing published
`--load-tables /config --brokers ...` CLI used in the repository's maintained
compose. It only initializes this project's dedicated database volume.

## Render, inspect, then run when authorized

From this checkout:

```sh
python3 deploy/native-test/render.py --flaredb-config /absolute/reviewed/native-tables
python3 -m unittest discover -s deploy/native-test -p test_foundation.py -v
docker compose -f deploy/native-test/compose.yaml config
# After source qualification and production-host test authorization:
docker compose -f deploy/native-test/compose.yaml up -d
```

The renderer fails if a bundle already exists, retaining prior evidence rather
than replacing configuration. Save `bundle/images.json`, `bundle/streamr.json`,
`bundle/queries/manifest.json`, rendered Compose and bootstrap logs with the test
receipt. Configure namespace topics and transaction recovery journals under the
existing Kafka administration policy before launch. Recovery journals must retain
transaction markers using the qualified compact-only policy; no raw topic settings
are changed here. All output topics and API consumer groups use the namespace.
The raw source reads earliest available committed Kafka input.

Bootstrap waits finitely for the API, validates all five queries before creating
any jobs, preflights conflicting names/SQL/udfs/checkpoint intervals across the
whole graph, and skips identical existing jobs on retry. A failed creation is
recoverable by rerunning bootstrap; concurrent bootstrap processes for one
namespace are not supported. Existing conflicting jobs are never changed or
stopped. Streamr APIs are internal to the Compose network; the only host binding is
the gateway on loopback by default, with GraphQL/WebSocket and dashboard sharing
one origin. Use a separately authorized test-host ingress to access it remotely.

`runtime-resources.json` is copied without relaxing RocksDB or state limits. Its
100-row JOIN cap comes from the small integration fixture and is **not** a proved
90-day replay allocation. The production-host test needs an explicitly reviewed
bounded allocation for its real cardinality; no memory-only fallback is provided.

Qualification still required: actual all-five-job run, FlareDB stored rows and
browser results, native profile publication behavior, worker restart with committed
output continuity, image entrypoint and container volume permissions. This source
foundation does not itself prove deployment readiness or authorize production
traffic cutover. Do not use `down --volumes` against retained test evidence.
