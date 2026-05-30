# Dev Mode Panel — Implementation Plan

## What it is

A toggleable dev panel on the arcstream dashboard that lets you:
1. **Switch analytics backend** between Pinot and FlareDB in real time
2. **See query stats** — every GraphQL query shows the SQL that was sent, which backend handled it, how long it took, and whether a star-tree index was used
3. **Compare backends** — run the same dashboard page against both and see the differences

## Current state

The backend switch infrastructure is partially built but has issues:
- `query-api`: `FlareDBClient` implements `PinotQuerier` trait, `BackendSelector` holds both clients, `X-Backend` header selects which runs (this works — tested via curl)
- `dashboard`: `BackendToggle` component in nav, `/set-backend` endpoint — **broken** because Leptos WASM router intercepts the `<a>` click client-side and never hits the server

## Architecture

### Layer 1: FlareDB HTTP API — query metadata (rust-olap)

FlareDB's `/query/sql` response currently returns:
```json
{"result_table": {"data_schema": {...}, "rows": [...]}}
```

Add an optional `query_stats` field returned when `X-Include-Stats: true` header is present:
```json
{
  "result_table": {...},
  "query_stats": {
    "elapsed_ms": 12,
    "path": "STAR_TREE",          // "STAR_TREE" | "FULL_SCAN" | "PARTIAL"
    "segments_total": 45,
    "segments_indexed": 43,
    "rows_scanned": 100000
  }
}
```

**Files to change:**
- `rust-olap/src/http_api.rs` — add `QueryStats` to `QueryResponse`, populate from the tracing info that `SegmentQueryExec` already logs
- `rust-olap/src/query_exec.rs` — return stats alongside the batch (the info is already computed at lines 249-265, just not exposed)

The stats data is already computed and logged via `tracing::info!` — this is just surfacing it in the HTTP response.

### Layer 2: query-api — pass stats through GraphQL (arcstream)

The `PinotQuerier` trait currently returns `Result<String, String>` (JSONL body). Extend it to optionally return query metadata.

**Option A (minimal change):** Add a second trait method:
```rust
#[async_trait]
pub trait PinotQuerier: Send + Sync {
    async fn query(&self, sql: &str) -> Result<String, String>;
    async fn query_with_stats(&self, sql: &str) -> Result<(String, Option<QueryStats>), String> {
        // Default implementation for Pinot (no stats)
        self.query(sql).await.map(|body| (body, None))
    }
}
```

**Option B (cleaner):** Return a struct instead of a String:
```rust
pub struct QueryResult {
    pub body: String,
    pub stats: Option<QueryStats>,
    pub backend: String,  // "pinot" or "flaredb"
    pub sql: String,      // the actual SQL sent
}
```

Option B is better — it lets the GraphQL layer pass stats to the dashboard without changing every resolver.

**Add a GraphQL extension for stats.** async-graphql supports response extensions:
```rust
// In each resolver, after the query:
let result = querier.query_with_stats(&sql).await?;
if let Some(stats) = result.stats {
    ctx.insert_http_header("X-Query-Stats", serde_json::to_string(&stats).unwrap());
}
```

Or better: accumulate all query stats for a single GraphQL request (which may fan out to multiple SQL queries) and return them as a GraphQL extension:
```json
{
  "data": { "dashboardStats": { ... } },
  "extensions": {
    "queryStats": [
      {"sql": "SELECT COUNT(*) FROM events", "backend": "flaredb", "elapsed_ms": 3, "path": "STAR_TREE", "segments_indexed": 45},
      {"sql": "SELECT COUNT(*) FROM profiles", "backend": "flaredb", "elapsed_ms": 1, "path": "FULL_SCAN", "segments_indexed": 0}
    ]
  }
}
```

**Files to change:**
- `arcstream/query-api/src/db/pinot.rs` — update trait, add `QueryStats` struct, `QueryResult` struct
- `arcstream/query-api/src/db/flaredb.rs` — implement `query_with_stats`, parse stats from FlareDB response
- `arcstream/query-api/src/schema/stats.rs` — use `query_with_stats`, accumulate stats in context
- `arcstream/query-api/src/schema/mod.rs` — same for tenant/user/event queries

### Layer 3: Dashboard — dev mode UI (arcstream)

#### 3a. Dev mode toggle

A persistent toggle (stored in `localStorage` via WASM) that shows/hides the dev panel. This is a client-side concern — no server round-trip needed.

```
┌─────────────────────────────────────────────────────────┐
│ CDP Dashboard    Architecture  Profiles  Events  [⚙ Dev]│
├─────────────────────────────────────────────────────────┤
│ ┌─ Dev Panel ────────────────────────────────────────┐  │
│ │ Backend: [● Pinot] [○ FlareDB]                     │  │
│ │                                                    │  │
│ │ Last query: 3 SQL statements, 47ms total           │  │
│ │ ┌────────────────────────────────────────────────┐ │  │
│ │ │ SELECT COUNT(*) FROM events                    │ │  │
│ │ │ Backend: flaredb | 12ms | STAR_TREE | 45/45 idx│ │  │
│ │ ├────────────────────────────────────────────────┤ │  │
│ │ │ SELECT COUNT(*) FROM profiles                  │ │  │
│ │ │ Backend: flaredb | 1ms  | FULL_SCAN | 0/2 idx  │ │  │
│ │ ├────────────────────────────────────────────────┤ │  │
│ │ │ SELECT DISTINCTCOUNTHLL(session_id)...          │ │  │
│ │ │ Backend: flaredb | 34ms | PARTIAL   | 43/45 idx│ │  │
│ │ └────────────────────────────────────────────────┘ │  │
│ └────────────────────────────────────────────────────┘  │
│                                                         │
│ [normal dashboard content below]                        │
```

#### 3b. Backend switch mechanism

The current approach (server-side `RwLock<String>` + redirect) is wrong for several reasons:
- Leptos client-side router intercepts `<a>` clicks
- Server-side state is shared across all users
- Page reload loses dashboard state

**Better approach:** Client-side signal + HTTP header.

1. Store `backend` in a Leptos `RwSignal<String>` (persisted to `localStorage`)
2. The dashboard's `graphql_query` function reads the signal and sets the `X-Backend` header
3. The query-api extracts the header and routes to the right client (this part already works)
4. No server endpoint needed — the switch is instant, client-side only

For SSR: the first render uses a default backend (from env var or cookie). After WASM hydration, the client-side signal takes over.

#### 3c. Query stats display

1. Dashboard reads `extensions.queryStats` from every GraphQL response
2. Stores them in a `RwSignal<Vec<QueryStatEntry>>` 
3. The dev panel renders them as a scrollable log
4. Each entry shows: SQL (truncated), backend, elapsed_ms, index path, segments
5. Color-coded: green for STAR_TREE, yellow for PARTIAL, red for FULL_SCAN

**Files to change:**
- `arcstream/dashboard/src/app.rs` — remove `BackendToggle` component, add `DevModeToggle` and `DevPanel` components
- `arcstream/dashboard/src/server/query_api.rs` — parse `extensions.queryStats` from responses, pass to signal
- `arcstream/dashboard/src/server/mod.rs` — remove `backend: Arc<RwLock<String>>` from `AppState`
- `arcstream/dashboard/src/main.rs` — remove `/set-backend` endpoint
- `arcstream/dashboard/style/main.css` — dev panel styles

### Layer 4: Pinot stats (nice to have)

Pinot already returns query stats in its response:
```json
{
  "timeUsedMs": 12,
  "numSegmentsQueried": 4,
  "numSegmentsProcessed": 4,
  "numEntriesScannedInFilter": 0,
  "numDocsScanned": 138009
}
```

The `PinotClient` currently discards these. Parse them into the same `QueryStats` struct so both backends show comparable stats.

**File:** `arcstream/query-api/src/db/pinot.rs` — parse stats from existing Pinot response fields

## Implementation order

1. **Fix the `rel="external"` bug** — one-line fix in `app.rs` so the current toggle works while we build the rest
2. **FlareDB: add query stats to HTTP response** — surface the data that's already computed
3. **query-api: `QueryResult` struct + stats in GraphQL extensions** — plumb stats through
4. **Dashboard: dev panel UI** — signal-based backend switch + stats display
5. **Pinot: parse stats from response** — feature parity for comparison

Steps 1-2 can be done independently. Step 3 depends on 2. Step 4 depends on 3. Step 5 is independent.

## Scope / non-goals

- No performance benchmarking UI — this is observability, not load testing
- No query editor — use FlareDB's built-in SQL UI at `:8090` for ad-hoc queries
- No persistent query history — the stats log is ephemeral (current page load only)
- No A/B split mode (running both backends simultaneously for the same query and showing side-by-side results) — that's a future enhancement
