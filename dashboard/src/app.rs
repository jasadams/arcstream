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
                {
                    std::env::var("DEV_PANEL").is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true")).then(|| view! {
                        <meta name="dev-panel" content="1"/>
                    })
                }
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

/// Whether the dev panel feature is available (controlled by `DEV_PANEL` env var).
/// Uses a signal so SSR and WASM both start with `false` (no hydration mismatch),
/// then an Effect sets it from the env var after hydration.
#[derive(Clone, Copy)]
pub struct DevPanelAvailable(pub RwSignal<bool>);

#[derive(Clone, Copy)]
pub struct DevMode {
    pub enabled: RwSignal<bool>,
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

    let dev_panel_available = RwSignal::new(false);
    provide_context(DevPanelAvailable(dev_panel_available));

    let dev_mode = DevMode {
        enabled: RwSignal::new(false),
        query_stats: RwSignal::new(Vec::new()),
    };
    provide_context(dev_mode);

    #[cfg(feature = "hydrate")]
    {
        use wasm_bindgen::prelude::*;
        use crate::websocket;

        websocket::provide_stream_contexts();

        // Check if SSR set the dev-panel meta tag
        Effect::new(move || {
            let available = web_sys::window()
                .and_then(|w| w.document())
                .and_then(|d| d.query_selector("meta[name=dev-panel]").ok().flatten())
                .is_some();
            dev_panel_available.set(available);
        });

        Effect::new(move || {
            if let Some(s) = web_sys::window()
                .and_then(|w| w.local_storage().ok().flatten())
            {
                if let Ok(Some(v)) = s.get_item("dev-mode") {
                    dev_mode.enabled.set(v == "true");
                }
            }
        });

        Effect::new(move || {
            let window = web_sys::window().expect("no window");

            let setup = Closure::<dyn FnMut()>::once(move || {
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
                    <BackendToggle />
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
/// Only rendered when `DEV_PANEL=1` env var is set.
#[component]
fn DevModeToggle() -> impl IntoView {
    let available = expect_context::<DevPanelAvailable>().0;
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
        }
    };
    view! {
        <Show when=move || available.get()>
            <button class="dev-toggle" on:click=toggle title="Toggle dev panel">
                "\u{2699} Dev"
            </button>
        </Show>
    }
}

/// Nav toggle that switches the analytics backend per-browser. Stores the choice
/// in a `backend` cookie (default Pinot) which the dashboard server forwards to
/// query-api as `x-backend`; clicking cycles through the available backends and
/// reloads so data re-fetches. Three options: Pinot (default), FlareDB (the
/// original backend), and FlareDB M3 (single-copy Iceberg build).
#[component]
fn BackendToggle() -> impl IntoView {
    let current = RwSignal::new("pinot".to_string());

    // After hydration, reflect the actual cookie value (avoids an SSR mismatch).
    #[cfg(feature = "hydrate")]
    leptos::prelude::Effect::new(move |_| {
        current.set(read_backend_cookie_client());
    });

    let toggle = move |_| {
        #[cfg(feature = "hydrate")]
        {
            let cur = current.get_untracked();
            // Cycle: Pinot → FlareDB → FlareDB M3 → Pinot.
            let next = match cur.as_str() {
                "flare" | "flaredb" => "flaredb-m3",
                "flaredb-m3" | "flare-m3" | "m3" => "pinot",
                _ => "flare",
            };
            set_backend_cookie_client(next);
            if let Some(w) = web_sys::window() {
                let _ = w.location().reload();
            }
        }
    };

    view! {
        <button
            class="backend-toggle"
            on:click=toggle
            title="Analytics backend — click to cycle (Pinot / FlareDB / FlareDB M3)"
        >
            {move || {
                let c = current.get();
                match c.as_str() {
                    "flare" | "flaredb" => "FlareDB",
                    "flaredb-m3" | "flare-m3" | "m3" => "FlareDB M3",
                    _ => "Pinot",
                }
            }}
        </button>
    }
}

#[cfg(feature = "hydrate")]
fn read_backend_cookie_client() -> String {
    use wasm_bindgen::JsCast;
    web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.dyn_into::<web_sys::HtmlDocument>().ok())
        .and_then(|hd| hd.cookie().ok())
        .and_then(|cookies| {
            cookies
                .split(';')
                .find_map(|kv| kv.trim().strip_prefix("backend=").map(str::to_owned))
        })
        .unwrap_or_else(|| "pinot".to_owned())
}

#[cfg(feature = "hydrate")]
fn set_backend_cookie_client(val: &str) {
    use wasm_bindgen::JsCast;
    if let Some(hd) = web_sys::window()
        .and_then(|w| w.document())
        .and_then(|d| d.dyn_into::<web_sys::HtmlDocument>().ok())
    {
        let _ = hd.set_cookie(&format!("backend={val}; path=/; max-age=31536000; SameSite=Lax"));
    }
}

#[component]
fn DevPanel() -> impl IntoView {
    let available = expect_context::<DevPanelAvailable>().0;
    let dev = expect_context::<DevMode>();

    view! {
        <Show when=move || available.get() && dev.enabled.get()>
            <div class="dev-panel">
                <div class="dev-panel-header">
                    "Dev Panel"
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
