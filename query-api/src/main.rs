mod db;
mod schema;
mod streaming;

use axum::{
    extract::State,
    http::HeaderMap,
    response::Html,
    routing::{get, post},
    Router,
};
use std::sync::Arc;
use tokio::sync::broadcast;
use tower_http::cors::CorsLayer;

use db::flaredb::FlareDBClient;
use db::pinot::{PinotClient, PinotQuerier};
use db::LiveProfileProvider;
use schema::query_stats::QueryStatsCollector;
use schema::{AppSchema, QueryRoot};
use schema::subscription::SubscriptionRoot;
use streaming::types::{LiveEventMessage, ProfileUpdateMessage};

/// One analytics backend; the query trait and the live-profile trait resolve to
/// the same underlying client.
#[derive(Clone)]
struct Backend {
    querier: Arc<dyn PinotQuerier>,
    live: Arc<dyn LiveProfileProvider>,
}

/// Selects the analytics backend per request. Pinot is the DEFAULT; the FlareDB
/// variants are opt-in via the `x-backend` header (set by the dashboard toggle):
/// `flaredb` → the original FlareDB backend, `flaredb-m3` → the M3 single-copy
/// Iceberg build. Unknown/missing backends fall back to Pinot.
#[derive(Clone)]
struct BackendSelector {
    pinot: Backend,
    flaredb: Option<Backend>,
    flaredb_m3: Option<Backend>,
}

impl BackendSelector {
    fn select(&self, name: &str) -> &Backend {
        match name {
            "flaredb-m3" | "flare-m3" | "m3" => self.flaredb_m3.as_ref().unwrap_or(&self.pinot),
            "flare" | "flaredb" => self.flaredb.as_ref().unwrap_or(&self.pinot),
            _ => &self.pinot,
        }
    }
}

struct AppState {
    schema: AppSchema,
    backends: BackendSelector,
}

async fn health() -> &'static str {
    "ok"
}

async fn graphql_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    // Default to Pinot; the dashboard opts into FlareDB via `x-backend: flaredb`.
    let backend_name = headers
        .get("x-backend")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("pinot");
    let backend = state.backends.select(backend_name);

    let collector = Arc::new(QueryStatsCollector::new());
    let mut gql_req = req.into_inner();
    gql_req = gql_req.data(Arc::clone(&collector));
    // Per-request backend selection overrides the schema-level default.
    gql_req = gql_req.data(backend.querier.clone());
    gql_req = gql_req.data(backend.live.clone());
    let mut response = state.schema.execute(gql_req).await;

    let entries = collector.take();
    if !entries.is_empty() {
        if let Ok(json_val) = serde_json::to_value(&entries) {
            if let Ok(gql_val) = async_graphql::Value::from_json(json_val) {
                response.extensions.insert("queryStats".to_owned(), gql_val);
            }
        }
    }

    response.into()
}

async fn graphql_playground() -> Html<String> {
    Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql")
            .subscription_endpoint("/graphql/ws"),
    ))
}

#[tokio::main]
async fn main() {
    let kafka_brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "redpanda.data-pipeline.svc.cluster.local:9092".into());

    // Pinot is the default analytics backend.
    let pinot_client = Arc::new(PinotClient {
        broker_url: std::env::var("PINOT_BROKER_URL").unwrap_or_else(|_| {
            "http://pinot-broker.data-pipeline.svc.cluster.local:8099/query/sql".into()
        }),
        http: reqwest::Client::new(),
    });
    let pinot = Backend {
        querier: pinot_client.clone() as Arc<dyn PinotQuerier>,
        live: pinot_client as Arc<dyn LiveProfileProvider>,
    };
    eprintln!("Pinot backend (default) configured");

    // FlareDB is the opt-in experimental backend (enabled when FLAREDB_URL set).
    let flaredb_backend = std::env::var("FLAREDB_URL").ok().map(|url| {
        eprintln!("FlareDB backend enabled at {url}");
        let c = Arc::new(FlareDBClient {
            url,
            http: reqwest::Client::new(),
        });
        Backend {
            querier: c.clone() as Arc<dyn PinotQuerier>,
            live: c as Arc<dyn LiveProfileProvider>,
        }
    });

    // FlareDB M3 (single-copy Iceberg build) — opt-in when FLAREDB_M3_URL set.
    let flaredb_m3_backend = std::env::var("FLAREDB_M3_URL").ok().map(|url| {
        eprintln!("FlareDB M3 backend enabled at {url}");
        let c = Arc::new(FlareDBClient {
            url,
            http: reqwest::Client::new(),
        });
        Backend {
            querier: c.clone() as Arc<dyn PinotQuerier>,
            live: c as Arc<dyn LiveProfileProvider>,
        }
    });

    let backends = BackendSelector {
        pinot: pinot.clone(),
        flaredb: flaredb_backend,
        flaredb_m3: flaredb_m3_backend,
    };

    let (profile_tx, _) = broadcast::channel::<ProfileUpdateMessage>(1024);
    let (event_tx, _) = broadcast::channel::<LiveEventMessage>(2048);

    let consumer_tx = profile_tx.clone();
    let consumer_brokers = kafka_brokers.clone();
    tokio::spawn(async move {
        streaming::consumer::run(
            consumer_tx,
            &consumer_brokers,
            "query-api-subscriptions",
            "profile-updates",
        )
        .await;
    });

    let event_consumer_tx = event_tx.clone();
    let event_consumer_brokers = kafka_brokers.clone();
    tokio::spawn(async move {
        streaming::event_consumer::run(
            event_consumer_tx,
            &event_consumer_brokers,
            "query-api-events",
            "unified-events",
        )
        .await;
    });

    let schema = async_graphql::Schema::build(
        QueryRoot::default(),
        async_graphql::EmptyMutation,
        SubscriptionRoot,
    )
    // Schema-level default (Pinot) covers subscriptions and any path that does
    // not go through graphql_handler's per-request selection.
    .data(pinot.querier.clone())
    .data(pinot.live.clone())
    .data(profile_tx)
    .data(event_tx)
    .limit_depth(5)
    .limit_complexity(1000)
    .finish();

    let state = Arc::new(AppState {
        schema: schema.clone(),
        backends,
    });

    let enable_playground = std::env::var("ENABLE_PLAYGROUND")
        .map(|v| v == "1" || v == "true")
        .unwrap_or(false);

    let mut app = Router::new()
        .route("/health", get(health))
        .route("/graphql", post(graphql_handler))
        .route_service(
            "/graphql/ws",
            async_graphql_axum::GraphQLSubscription::new(schema),
        )
        .layer(CorsLayer::permissive())
        .with_state(state);

    if enable_playground {
        app = app.route("/graphql/playground", get(graphql_playground));
        eprintln!("GraphQL Playground enabled at /graphql/playground");
    }

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let addr = format!("0.0.0.0:{port}");
    eprintln!("Query API listening on {addr}");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
