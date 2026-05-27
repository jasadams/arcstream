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
                <meta name="twitter:card" content="summary"/>
                <link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='6' fill='%23111115'/><circle cx='16' cy='16' r='6' fill='%23D4944C'/></svg>"/>
                <title>"CDP Dashboard"</title>
                <link rel="preconnect" href="https://fonts.bunny.net"/>
                <link href="https://fonts.bunny.net/css?family=satoshi:400,500,600,700|dm-sans:400,500,600|geist-mono:400,500&display=swap" rel="stylesheet"/>
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

    #[cfg(feature = "hydrate")]
    {
        use wasm_bindgen::prelude::*;
        use crate::websocket;

        let (profile_sig, event_sig) = websocket::provide_stream_contexts();

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
                    <a href="https://github.com/jasadams/arcstream" target="_blank" rel="noopener" class="github-link" inner_html=SVG_GITHUB></a>
                </nav>
            </header>
            <main id="main" class="container">
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
