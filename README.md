# Arcstream

A real-time Customer Data Platform built for high-throughput analytics workloads. Ingests raw events, performs stateful identity resolution, maintains live user profiles, computes session summaries, and serves everything through a GraphQL API with WebSocket subscriptions — all running on Kubernetes.

## Quick Start

Run the entire stack locally with one command (requires [Docker](https://docs.docker.com/get-docker/) and ~12GB RAM):

```bash
curl -fsSL https://raw.githubusercontent.com/jasadams/arcstream/main/deploy/install.sh | bash
```

This pulls pre-built images and starts Redpanda, Flink, Pinot, ScyllaDB, MinIO, and the application services via Docker Compose. After 2-4 minutes the dashboard will be available at [http://localhost:3000](http://localhost:3000).

```
Stop:    cd arcstream && docker compose down
Logs:    cd arcstream && docker compose logs -f
Reset:   cd arcstream && docker compose down -v
```

## Architecture

```
                                ┌──────────────────────────────────────────────────────────────┐
                                │                     Apache Flink                             │
                                │                                                              │
  ┌──────────────┐    ┌─────────┴──┐    ┌─────────────────────┐    ┌────────────────────┐      │
  │    Event      │    │            │    │  Identity Resolution │    │  Profile Updater   │      │
  │   Producer    │───▶│  Redpanda  │───▶│  (stateful, keyed   │───▶│  (ScyllaDB writes, │      │
  │   (Rust)      │    │  (Kafka)   │    │   by tenant+anon)   │    │   window counters) │      │
  └──────────────┘    │            │    └─────────┬───────────┘    └────────────────────┘      │
                      │            │              │                                            │
                      │            │    ┌─────────▼───────────┐    ┌────────────────────┐      │
                      │            │◀───│  unified-events     │    │  Sessionization    │      │
                      │            │    │  topic               │───▶│  (30min timeout,  │      │
                      │            │    └─────────────────────┘    │   session summary) │      │
                      └─────┬──────┘                               └────────────────────┘      │
                            │                                                                  │
                            │           ┌─────────────────────┐    ┌────────────────────┐      │
                            │           │  Iceberg Writer      │    │  Cold storage:     │      │
                            └──────────▶│  (Flink SQL)         │───▶│  MinIO (S3/Parquet)│      │
                                        └─────────────────────┘    └────────────────────┘      │
                                                                                               │
                                └──────────────────────────────────────────────────────────────┘
                                           │                              │
                                           ▼                              ▼
                                   ┌──────────────┐              ┌──────────────┐
                                   │ Apache Pinot  │              │  ScyllaDB    │
                                   │ (OLAP, star-  │              │  (live       │
                                   │  tree index)  │              │   profiles)  │
                                   └──────┬───────┘              └──────┬───────┘
                                          │                             │
                                          ▼                             ▼
                                   ┌─────────────────────────────────────────┐
                                   │           Query API (Rust/Axum)         │
                                   │  GraphQL queries + WebSocket subs      │
                                   └───────────────────┬─────────────────────┘
                                                       │
                                                       ▼
                                              ┌──────────────────┐
                                              │    Dashboard     │
                                              │  (Leptos SSR +   │
                                              │   WASM hydrate)  │
                                              └──────────────────┘
```

## How It Works

### Event Ingestion

A Rust event producer generates realistic user behavior — page views, clicks, signups, logins, feature usage — across multiple tenants. Users have behavioral personas (power user, regular, casual, tourist) that govern session length, think time, conversion probability, and return rates. Events are published to Redpanda using tenant ID as the partition key.

### Identity Resolution

A stateful Flink job maintains an identity graph in RocksDB, mapping anonymous IDs and user IDs to canonical player IDs. When a user signs in on a new device, the anonymous ID from that device gets linked to their existing canonical ID. When two previously separate canonical IDs are discovered to be the same person, the job emits a merge event and collapses them — the user ID's canonical wins.

### Profile Aggregation

A second Flink job maintains live user profiles in ScyllaDB. Each event updates rolling window counters (1d/7d/30d/90d for both events and sessions), page view counts, feature usage, device info, and session state. Profile changes are emitted to a Kafka topic for downstream consumption. Session timeout timers close sessions after 30 minutes of inactivity and schedule decay timers to update windowed counters as events age out of each window.

### Sessionization

A third Flink job groups events into sessions keyed by canonical ID, with a 30-minute inactivity timeout. When a session closes, it emits a summary with duration, event count, pages visited, device info, and event type breakdown. These summaries feed into Pinot for session analytics.

### Cold Storage

A Flink SQL job writes unified events to Apache Iceberg tables on MinIO (S3-compatible), partitioned by tenant ID, in Parquet format.

### Analytics Serving

Apache Pinot ingests unified events and session summaries from Redpanda in real time. A star-tree index pre-aggregates metrics across dimensions (tenant, date, hour, country, event type, device, browser, page) at ingestion time, enabling sub-millisecond analytical queries at high concurrency. ScyllaDB serves individual profile lookups with sub-millisecond latency.

### Query API

A Rust/Axum GraphQL API exposes:
- **Queries**: tenant listings, user profiles, event history, dashboard stats (DAU/sessions/events), time series (events/users/sessions over time), breakdowns (device, browser, country, page)
- **Subscriptions**: real-time profile updates and live event streams via WebSocket, backed by Kafka consumers broadcasting to connected clients

### Dashboard

A Leptos application with server-side rendering and WASM hydration. Pages include user list, user detail (with live-updating profile via WebSocket), event timeline, and analytics stats with time-series charts. Profile updates stream in real time without polling.

## Tech Stack

| Layer | Technology |
|---|---|
| Event producer | Rust, rdkafka, tokio |
| Stream processing | Apache Flink 1.20 (DataStream API), Java, RocksDB state |
| Event bus | Redpanda (Kafka-compatible) |
| Live profiles | ScyllaDB |
| OLAP analytics | Apache Pinot (star-tree pre-aggregation) |
| Cold storage | Apache Iceberg on MinIO (Parquet) |
| Query API | Rust, Axum, async-graphql (queries + subscriptions) |
| Dashboard | Rust, Leptos (SSR + WASM), WebSocket |
| Infrastructure | Kubernetes (k3s), Longhorn PVs |

## Project Structure

```
event-producer/          Rust CLI — realistic multi-tenant event simulation
flink/
  identity-resolution/   Java — 4 Flink jobs (identity, profiles, sessions, iceberg)
  sql/                   Flink SQL — Iceberg cold storage writer
query-api/               Rust/Axum — GraphQL API with WebSocket subscriptions
dashboard/               Rust/Leptos — SSR + WASM frontend
pinot/                   Pinot table schemas and star-tree index configs
docker/                  Multi-stage Dockerfiles for Rust services
k8s/                     Kubernetes manifests (gitignored, cluster-specific)
```

## Deployment

This runs on Kubernetes. The `k8s/` directory is gitignored because it contains cluster-specific configuration. To deploy on your own cluster:

1. Set up the infrastructure services: Redpanda, Apache Pinot (with Zookeeper), ScyllaDB, MinIO, Flink (JobManager + TaskManager)
2. Create the Pinot tables using the schemas in `pinot/`
3. Build and deploy the Rust services using the Dockerfiles in `docker/`
4. Build and deploy the Flink jobs from `flink/identity-resolution/`
5. Submit the Flink SQL job from `flink/sql/iceberg-writer.sql`

All services read connection details from environment variables — no hardcoded endpoints in application code.

## Key Design Decisions

**Pinot over ClickHouse** — Star-tree pre-aggregation handles 1000+ concurrent dashboard queries. ClickHouse maxes out around 50 concurrent analytical queries before degrading.

**ScyllaDB over Redis** — Profiles scale beyond available RAM. ScyllaDB's shard-per-core architecture maintains sub-millisecond reads with disk-backed storage.

**Flink DataStream over Flink SQL** — Identity resolution requires complex stateful logic (identity graph, merge detection, timer-based session management) that doesn't fit SQL's declarative model. The Iceberg writer uses Flink SQL because it's a straightforward stream-to-table copy.

**GraphQL subscriptions over polling** — Profile updates flow from Flink through Kafka to the Query API's broadcast channels to connected WebSocket clients. One Kafka consumer fans out to all dashboard sessions with no per-client polling.

## License

MIT
