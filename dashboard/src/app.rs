use std::collections::HashMap;
use leptos::prelude::*;
use leptos::reactive::owner::Owner;
use leptos_meta::*;
use leptos_router::components::*;
use leptos_router::path;
use leptos_router::SsrMode;
use crate::components::about_page::AboutPage;
use crate::util::SVG_GITHUB;
use crate::components::event_list::EventListPage;
use crate::components::user_list::UserListPage;
use crate::components::user_detail::UserDetailPage;
use crate::components::event_detail::EventDetailPage;
use crate::components::stats_page::StatsPage;
use crate::server::api::UserProfile;

pub fn shell(options: LeptosOptions) -> impl IntoView {
    view! {
        <!DOCTYPE html>
        <html lang="en">
            <head>
                <meta charset="utf-8"/>
                <meta name="viewport" content="width=device-width, initial-scale=1"/>
                <meta name="description" content="Real-time Customer Data Platform reference architecture. Redpanda, Flink, ScyllaDB, Pinot — every piece real, running live."/>
                <meta property="og:title" content="Arcstream CDP"/>
                <meta property="og:description" content="Real-time Customer Data Platform reference architecture. Every piece real, running live."/>
                <meta property="og:type" content="website"/>
                <meta property="og:url" content="https://cdp.alytic.com.au"/>
                <meta property="og:image" content="https://cdp.alytic.com.au/og-image.png"/>
                <meta property="og:image:width" content="1200"/>
                <meta property="og:image:height" content="630"/>
                <meta name="twitter:card" content="summary_large_image"/>
                <meta name="twitter:title" content="Arcstream CDP"/>
                <meta name="twitter:description" content="Real-time Customer Data Platform reference architecture. Every piece real, running live."/>
                <meta name="twitter:image" content="https://cdp.alytic.com.au/og-image.png"/>
                <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='6' fill='%23111115'/><circle cx='16' cy='16' r='6' fill='%23D4944C'/></svg>"/>
                <title>"CDP Dashboard"</title>
                <link rel="preconnect" href="https://fonts.bunny.net"/>
                <link href="https://fonts.bunny.net/css?family=satoshi:400,500,600,700|dm-sans:400,500,600|geist-mono:400,500&display=swap" rel="stylesheet"/>
                <script defer data-key="eyJzIjoiZjE0ZDNiYmM0MDFmZWZhNSIsInciOiJ3b3Jrc3BhY2UtOTlmMjRkMDUtNjczZDI1YjgiLCJkIjpbImNkcC5hbHl0aWMuY29tLmF1IiwibG9jYWxob3N0Il19.Y1cUg_xGMFvQ2IpMn4iesVBke9ODdaD11geqGMf2UYU" src="https://analytics.kyomi.ai/k.js"></script>
                <AutoReload options=options.clone()/>
                <HydrationScripts options/>
                <leptos_meta::Stylesheet id="leptos" href="/pkg/dashboard.css"/>
            </head>
            <body>
                <App/>
            </body>
        </html>
    }
}

#[derive(Clone, Copy)]
pub struct Tick(pub ReadSignal<u64>);

pub type UserRow = (String, RwSignal<UserProfile>, RwSignal<bool>);

/// Client-side dev mode state. Persisted to localStorage and cookies.
#[derive(Clone, Copy)]
pub struct DevMode {
    pub enabled: RwSignal<bool>,
    pub backend: RwSignal<String>,
    pub query_stats: RwSignal<Vec<crate::server::QueryStatEntry>>,
}

#[derive(Clone, Copy)]
pub struct UserListCache {
    pub page: RwSignal<u32>,
    pub rows: RwSignal<Vec<UserRow>>,
    pub lookup: StoredValue<HashMap<String, RwSignal<UserProfile>>>,
    pub last_fetched_page: StoredValue<Option<u32>>,
    pub total: RwSignal<Option<u64>>,
    pub owner: StoredValue<Owner>,
}

#[component]
pub fn App() -> impl IntoView {
    provide_meta_context();

    let (tick, _set_tick) = signal(0u64);
    provide_context(Tick(tick));

    let app_owner = Owner::current().expect("App must have owner");
    provide_context(UserListCache {
        page: RwSignal::new(0),
        rows: RwSignal::new(Vec::new()),
        lookup: StoredValue::new(HashMap::new()),
        last_fetched_page: StoredValue::new(None),
        total: RwSignal::new(None),
        owner: StoredValue::new(app_owner),
    });

    // Initialize DevMode with defaults; WASM side restores from localStorage
    let dev_mode = DevMode {
        enabled: RwSignal::new(false),
        backend: RwSignal::new(String::new()),
        query_stats: RwSignal::new(Vec::new()),
    };
    provide_context(dev_mode);

    #[cfg(feature = "hydrate")]
    {
        use wasm_bindgen::prelude::*;
        use crate::websocket;

        let (profile_sig, event_sig) = websocket::provide_stream_contexts();

        // Restore dev mode state from localStorage AFTER hydration
        // to avoid SSR/WASM mismatch (SSR renders enabled=false).
        Effect::new(move || {
            if let Some(s) = web_sys::window()
                .and_then(|w| w.local_storage().ok().flatten())
            {
                if let Ok(Some(b)) = s.get_item("dev-backend") {
                    dev_mode.backend.set(b);
                }
                if let Ok(Some(v)) = s.get_item("dev-mode") {
                    dev_mode.enabled.set(v == "true");
                }
            }
        });

        Effect::new(move || {
            let window = web_sys::window().expect("no window");

            let setup = Closure::<dyn FnMut()>::once(move || {
                websocket::start_websockets(profile_sig, event_sig);

                let tick_cb = Closure::<dyn FnMut()>::new(move || {
                    let _ = _set_tick.try_update(|t| *t += 1);
                });
                let w = web_sys::window().expect("no window");
                let _ = w.set_interval_with_callback_and_timeout_and_arguments_0(
                    tick_cb.as_ref().unchecked_ref(),
                    1000,
                );
                tick_cb.forget();
            });
            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                setup.as_ref().unchecked_ref(),
                0,
            );
            setup.forget();
        });
    }

    view! {
        <Router>
            <a class="skip-link" href="#main">"Skip to content"</a>
            <header>
                <h1>"CDP Dashboard"</h1>
                <nav>
                    <A href="/">"Architecture"</A>
                    <A href="/profiles">"Profiles"</A>
                    <A href="/events">"Events"</A>
                    <A href="/analytics">"Analytics"</A>
                    <DevModeToggle />
                    <a href="https://github.com/jasadams/arcstream" target="_blank" rel="noopener" class="github-link" inner_html=SVG_GITHUB></a>
                </nav>
            </header>
            <main id="main" class="container">
                <DevPanel />
                <Routes fallback=|| view! { <p>"Not found"</p> }>
                    <Route path=path!("/") view=AboutPage ssr=SsrMode::Async />
                    <Route path=path!("/profiles") view=UserListPage ssr=SsrMode::Async />
                    <Route path=path!("/events/:tenant/:canonical_id/:event_id") view=EventDetailPage ssr=SsrMode::Async />
                    <Route path=path!("/events") view=EventListPage ssr=SsrMode::Async />
                    <Route path=path!("/analytics") view=StatsPage ssr=SsrMode::Async />
                    <Route path=path!("/profiles/:tenant/:id") view=UserDetailPage ssr=SsrMode::Async />
                </Routes>
            </main>
        </Router>
    }
}

/// Gear icon button in the nav that toggles the dev panel visibility.
#[component]
fn DevModeToggle() -> impl IntoView {
    let dev = expect_context::<DevMode>();
    let toggle = move |_| {
        dev.enabled.update(|e| *e = !*e);
        #[cfg(feature = "hydrate")]
        {
            let enabled = dev.enabled.get();
            if let Some(s) = web_sys::window()
                .and_then(|w| w.local_storage().ok().flatten())
            {
                let _ = s.set_item("dev-mode", &enabled.to_string());
            }
            if !enabled {
                if let Some(doc) = web_sys::window().and_then(|w| w.document()) {
                    use wasm_bindgen::JsCast;
                    if let Some(html_doc) = doc.dyn_ref::<web_sys::HtmlDocument>() {
                        let _ = html_doc.set_cookie("dev-backend=; path=/; max-age=0");
                    }
                }
            }
        }
    };
    view! {
        <button class="dev-toggle" on:click=toggle title="Toggle dev panel">
            "\u{2699} Dev"
        </button>
    }
}

/// Collapsible dev panel shown below the header when dev mode is enabled.
#[component]
fn DevPanel() -> impl IntoView {
    let dev = expect_context::<DevMode>();
    let backend = dev.backend;

    let set_backend = move |name: &str| {
        let name = name.to_owned();
        backend.set(name.clone());
        #[cfg(feature = "hydrate")]
        {
            // Persist to localStorage
            if let Some(s) = web_sys::window()
                .and_then(|w| w.local_storage().ok().flatten())
            {
                let _ = s.set_item("dev-backend", &name);
            }
            // Set cookie so SSR server functions can read the backend choice
            if let Some(doc) = web_sys::window().and_then(|w| w.document()) {
                use wasm_bindgen::JsCast;
                if let Some(html_doc) = doc.dyn_ref::<web_sys::HtmlDocument>() {
                    let _ = html_doc.set_cookie(&format!("dev-backend={name}; path=/; max-age=86400"));
                }
            }
        }
    };

    let is_pinot = move || {
        let b = backend.get();
        b == "pinot" || b.is_empty()
    };
    let is_flare = move || {
        let b = backend.get();
        b == "flare" || b == "flaredb"
    };

    view! {
        <Show when=move || dev.enabled.get()>
            <div class="dev-panel">
                <div class="dev-panel-header">
                    "Dev Panel"
                </div>
                <div class="dev-backend-switch">
                    <span class="dev-label">"Backend:"</span>
                    <button
                        class="dev-backend-btn"
                        class:active=is_pinot
                        on:click=move |_| set_backend("pinot")
                    >
                        "Pinot"
                    </button>
                    <button
                        class="dev-backend-btn"
                        class:active=is_flare
                        on:click=move |_| set_backend("flare")
                    >
                        "FlareDB"
                    </button>
                </div>
                <div class="dev-stats-log">
                    <div class="dev-label">"Query Log"</div>
                    {move || {
                        dev.query_stats.get().into_iter().map(|entry| {
                            let sql_preview = entry.sql.char_indices()
                                .nth(60)
                                .map(|(i, _)| format!("{}...", &entry.sql[..i]))
                                .unwrap_or_else(|| entry.sql.clone());
                            let path_class = match entry.path.as_deref() {
                                Some("STAR_TREE") => "dev-path star-tree",
                                Some("PARTIAL") => "dev-path partial",
                                Some("FULL_SCAN") => "dev-path full-scan",
                                _ => "dev-path",
                            };
                            let path_label = entry.path.clone().unwrap_or_else(|| "\u{2014}".to_string());
                            let elapsed = entry.elapsed_ms.map(|ms| format!("{ms}ms"));
                            let segments = entry.segments_indexed.zip(entry.segments_total)
                                .map(|(i, t)| format!("{i}/{t} idx"));
                            view! {
                                <div class="dev-stat-entry">
                                    <div class="dev-stat-sql">{sql_preview}</div>
                                    <div class="dev-stat-meta">
                                        <span class=path_class>{path_label}</span>
                                        <span class="dev-backend-label">{entry.backend.clone()}</span>
                                        {elapsed.map(|e| view! { <span class="dev-elapsed">{e}</span> })}
                                        {segments.map(|s| view! { <span class="dev-segments">{s}</span> })}
                                    </div>
                                </div>
                            }
                        }).collect_view()
                    }}
                </div>
            </div>
        </Show>
    }
}
