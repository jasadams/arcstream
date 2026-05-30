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
use db::scylla::{LiveProfileProvider, ScyllaClient};
use schema::query_stats::QueryStatsCollector;
use schema::{AppSchema, QueryRoot};
use schema::subscription::SubscriptionRoot;
use streaming::types::{LiveEventMessage, ProfileUpdateMessage};

#[derive(Clone)]
pub struct BackendSelector {
    pub pinot: Arc<dyn PinotQuerier>,
    pub flaredb: Option<Arc<dyn PinotQuerier>>,
}

impl BackendSelector {
    fn select(&self, name: &str) -> Arc<dyn PinotQuerier> {
        match name {
            "flare" | "flaredb" => self.flaredb.clone().unwrap_or_else(|| self.pinot.clone()),
            _ => self.pinot.clone(),
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
    let backend_name = headers
        .get("x-backend")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("pinot");
    let client = state.backends.select(backend_name);
    let collector = Arc::new(QueryStatsCollector::new());
    let mut gql_req = req.into_inner();
    gql_req = gql_req.data(Arc::clone(&collector));
    gql_req = gql_req.data(client);
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
    let scylla_contact_points = std::env::var("SCYLLA_CONTACT_POINTS")
        .unwrap_or_else(|_| "scylladb.data-pipeline.svc.cluster.local:9042".into());

    let kafka_brokers = std::env::var("KAFKA_BROKERS")
        .unwrap_or_else(|_| "redpanda.data-pipeline.svc.cluster.local:9092".into());

    let pinot: Arc<dyn PinotQuerier> = Arc::new(PinotClient {
        broker_url: std::env::var("PINOT_BROKER_URL")
            .unwrap_or_else(|_| "http://pinot-broker.data-pipeline.svc.cluster.local:8099/query/sql".into()),
        http: reqwest::Client::new(),
    });

    let flaredb: Option<Arc<dyn PinotQuerier>> = std::env::var("FLAREDB_URL").ok().map(|url| {
        eprintln!("FlareDB backend enabled at {url}");
        Arc::new(FlareDBClient {
            url,
            http: reqwest::Client::new(),
        }) as Arc<dyn PinotQuerier>
    });

    let backends = BackendSelector {
        pinot: pinot.clone(),
        flaredb,
    };

    let scylla_session = loop {
        match scylla::SessionBuilder::new()
            .known_node(&scylla_contact_points)
            .build()
            .await
        {
            Ok(session) => break session,
            Err(e) => {
                eprintln!("ScyllaDB not ready, retrying in 5s: {e}");
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        }
    };

    let scylla_client = Arc::new(ScyllaClient {
        session: Arc::new(scylla_session),
    });

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
    .data(pinot as Arc<dyn PinotQuerier>)
    .data(scylla_client as Arc<dyn LiveProfileProvider>)
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
