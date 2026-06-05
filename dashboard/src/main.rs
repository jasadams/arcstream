#[cfg(feature = "ssr")]
#[tokio::main]
async fn main() {
    use axum::Router;
    use axum::routing::get;
    use leptos::prelude::*;
    use leptos_axum::{generate_route_list, LeptosRoutes};
    use tower_http::services::ServeDir;
    use dashboard::app::{App, shell};
    use dashboard::server::AppState;

    let app_state = AppState {
        query_api_url: std::env::var("QUERY_API_URL")
            .unwrap_or_else(|_| "http://query-api.data-pipeline.svc.cluster.local:8080/graphql".into()),
        http: reqwest::Client::new(),
    };

    let conf = get_configuration(None).unwrap();
    let leptos_options = conf.leptos_options;
    let addr = leptos_options.site_addr;
    let routes = generate_route_list(App);

    let pkg_dir = format!("{}/{}", leptos_options.site_root, leptos_options.site_pkg_dir);
    let pkg_service = ServeDir::new(&pkg_dir)
        .precompressed_br()
        .precompressed_gzip();

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .nest_service("/pkg", pkg_service)
        .leptos_routes_with_context(
            &leptos_options,
            routes,
            {
                let state = app_state.clone();
                move || provide_context(state.clone())
            },
            {
                let leptos_options = leptos_options.clone();
                move || shell(leptos_options.clone())
            },
        )
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options);

    eprintln!("CDP Dashboard listening on {addr}");
    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app.into_make_service()).await.unwrap();
}
