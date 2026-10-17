mod db;
mod optional_text;
mod schema;
mod streaming;
mod subscription_config;

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
use db::pinot::PinotQuerier;
use db::LiveProfileProvider;
use schema::query_stats::QueryStatsCollector;
use schema::subscription::SubscriptionRoot;
use schema::{AppSchema, QueryRoot};
use streaming::types::{LiveEventMessage, ProfileUpdateMessage};

/// One analytics backend; the query trait and the live-profile trait resolve to
/// the same underlying client.
#[derive(Clone)]
struct Backend {
    querier: Arc<dyn PinotQuerier>,
    live: Arc<dyn LiveProfileProvider>,
}

/// FlareDB serves the demo, including requests carrying old backend cookies.
#[derive(Clone)]
struct BackendSelector {
    flaredb: Backend,
}

impl BackendSelector {
    fn select(&self, _name: &str) -> &Backend {
        &self.flaredb
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
    // Old browser backend headers remain compatible after the FlareDB cutover.
    let backend_name = headers
        .get("x-backend")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("flaredb");
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

    let client = Arc::new(FlareDBClient {
        url: std::env::var("FLAREDB_URL")
            .unwrap_or_else(|_| "http://flaredb.data-pipeline.svc.cluster.local:8099".into()),
        http: reqwest::Client::new(),
    });
    let flaredb = Backend {
        querier: client.clone() as Arc<dyn PinotQuerier>,
        live: client as Arc<dyn LiveProfileProvider>,
    };
    eprintln!("FlareDB backend configured");
    let backends = BackendSelector {
        flaredb: flaredb.clone(),
    };

    let (profile_tx, _) = broadcast::channel::<ProfileUpdateMessage>(1024);
    let (event_tx, _) = broadcast::channel::<LiveEventMessage>(2048);

    let subscriptions = subscription_config::SubscriptionConfig::from_env();
    let consumer_tx = profile_tx.clone();
    let consumer_brokers = kafka_brokers.clone();
    tokio::spawn(async move {
        streaming::consumer::run(
            consumer_tx,
            &consumer_brokers,
            &subscriptions.profiles_group_id,
            &subscriptions.profiles_topic,
        )
        .await;
    });

    let event_consumer_tx = event_tx.clone();
    let event_consumer_brokers = kafka_brokers.clone();
    tokio::spawn(async move {
        streaming::event_consumer::run(
            event_consumer_tx,
            &event_consumer_brokers,
            &subscriptions.events_group_id,
            &subscriptions.events_topic,
        )
        .await;
    });

    let schema = async_graphql::Schema::build(
        QueryRoot::default(),
        async_graphql::EmptyMutation,
        SubscriptionRoot,
    )
    // HTTP and subscription queries use the same FlareDB backend.
    .data(flaredb.querier.clone())
    .data(flaredb.live.clone())
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
