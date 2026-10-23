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
and requires its explicit `--raw-events-topic` interface (included in this checkout); it does not rewrite rendered SQL.

The bundle uses the current reviewed native flat22 profiles schema rendered by
`prepare.py`, plus the maintained events/sessions load-tables schemas. Only their
Kafka topics/brokers change: `<namespace>-unified-events` and
`<namespace>-sessions`. Their timestamp/int fields match the native SQL projections;
SESSION pages/event_types are extra output fields outside these analytics schemas.
Source inspection of published FlareDB95b58cb found that JSON arrays are rejected
by Utf8 fields, so the profile schema correctly declares native List(Utf8).
That source also rejects List in mutable ingestion and falls back to display text
for HTTP list results. Both generic gaps require runnable reproduction and repair
before this schema can ingest and serve complete profiles; schema rendering is
not a pass. Extra SESSION fields and string timestamps also remain in the runtime
check. No invented schema endpoint or CDC adapter is
involved. Initialization uses the existing published
`--load-tables /config --brokers ...` CLI on the dedicated database volume.

## Render, inspect, then run when authorized

From this checkout:

```sh
python3 deploy/native-test/render.py
python3 -m unittest discover -s deploy/native-test -p test_foundation.py -v
docker compose -f deploy/native-test/compose.yaml config
# After source qualification and production-host test authorization:
docker compose -f deploy/native-test/compose.yaml up -d
```

The renderer fails if a bundle already exists, retaining prior evidence rather
than replacing configuration. Save `bundle/receipt.json` (source and rendered file hashes), `bundle/images.json`, `bundle/streamr.json`,
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

`runtime-resources.json` is copied without relaxing RocksDB or state limits. The
current native profile route uses state-table MERGE rather than an ordinary JOIN;
the legacy configured JOIN cap100 is therefore not its retained profile limit.
The remaining allocation question is the actual source-qualified graph's disk-owner
count, typed owner footprint and combined queued-write reservation against the
shared 8-database/96MiB-write pool, across all process workers. Source-only rendering
cannot establish those runtime totals or a 90-day replay allocation. No memory-only
fallback or data-size claim is provided.

Source-only full-bundle rendering against the coordinating publication-repair
checkout passed: five query requests, all three schema topics correctly namespaced,
existing raw topic retained, no services or containers started. The immutable
placeholder image references used for that rendering test are never runnable
release image pins.

Qualification still required: actual all-five-job run, FlareDB stored rows and
browser results, native profile publication behavior, worker restart with committed
output continuity, image entrypoint and container volume permissions. This source
foundation does not itself prove deployment readiness or authorize production
traffic cutover. Do not use `down --volumes` against retained test evidence.
