mod db;
mod schema;
mod streaming;

use axum::{
    extract::State,
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
use schema::{AppSchema, QueryRoot};
use schema::subscription::SubscriptionRoot;
use streaming::types::{LiveEventMessage, ProfileUpdateMessage};

struct AppState {
    schema: AppSchema,
}

async fn health() -> &'static str {
    "ok"
}

async fn graphql_handler(
    State(state): State<Arc<AppState>>,
    req: async_graphql_axum::GraphQLRequest,
) -> async_graphql_axum::GraphQLResponse {
    let collector = Arc::new(QueryStatsCollector::new());
    let mut gql_req = req.into_inner();
    gql_req = gql_req.data(Arc::clone(&collector));
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

    let flaredb = Arc::new(FlareDBClient {
        url: std::env::var("FLAREDB_URL")
            .unwrap_or_else(|_| "http://flaredb.data-pipeline.svc.cluster.local:8099".into()),
        http: reqwest::Client::new(),
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
    .data(flaredb.clone() as Arc<dyn PinotQuerier>)
    .data(flaredb as Arc<dyn LiveProfileProvider>)
    .data(profile_tx)
    .data(event_tx)
    .limit_depth(5)
    .limit_complexity(1000)
    .finish();

    let state = Arc::new(AppState {
        schema: schema.clone(),
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
