use leptos::prelude::*;
use leptos_meta::Title;
use leptos_router::components::A;
use crate::app::Tick;
use crate::components::stats_bar::RollingCounter;
use crate::server::api::get_dashboard_stats;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArchNode {
    Producer,
    Redpanda,
    Streamr,
    LiveUpdates,
    FlareDB,
    NativeState,
    QueryApi,
    Dashboard,
}

impl ArchNode {
    fn title(&self) -> &'static str {
        match self {
            Self::Producer => "Event Producer",
            Self::Redpanda => "Redpanda",
            Self::Streamr => "Streamr",
            Self::LiveUpdates => "Live Updates",
            Self::FlareDB => "FlareDB",
            Self::NativeState => "Native State",
            Self::QueryApi => "Query API",
            Self::Dashboard => "Dashboard",
        }
    }

    fn description(&self) -> &'static str {
        match self {
            Self::Producer => "Generates simulated clickstream traffic for multiple tenants: page views, clicks, signups, logins and feature usage. Behavioral personas create different session lengths and return patterns.",
            Self::Redpanda => "The Kafka-compatible event bus carries raw events, resolved events, profile changes, session summaries and the internal messages connecting native pipelines.",
            Self::Streamr => "Native SQL pipelines resolve identities, maintain core profile metrics and emit completed session summaries. Arcstream owns schemas and business policies; Streamr executes SQL, state tables, event-time windows and checkpoints.",
            Self::LiveUpdates => "Committed Kafka event and profile topics feed the Query API. WebSocket subscriptions notify browsers when new data arrives, so live views can refresh without periodic polling.",
            Self::FlareDB => "The analytics backend serves events, profiles and completed sessions. It consumes the native pipeline outputs and answers the SQL queries used by the dashboard's GraphQL API.",
            Self::NativeState => "Native operators retain tenant-scoped identity mappings, core profiles and session state in RocksDB. Persistent checkpoints retain state and source positions for recovery.",
            Self::QueryApi => "The Rust GraphQL service reads FlareDB for profiles and analytics, consumes committed Kafka updates, and broadcasts live notifications to connected browsers.",
            Self::Dashboard => "The interface you are viewing combines server-rendered HTML with Rust WASM hydration. Profiles, event streams and analytics use the same FlareDB backend and receive live update notifications.",
        }
    }

    fn points(&self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Producer => &[
                ("Behavioral Personas", "Different activity patterns drive session length, page depth and return frequency"),
                ("Multiple Tenants", "Each event carries a tenant identifier through identity resolution and aggregation"),
                ("Simulated Traffic", "The public demo uses generated data"),
            ],
            Self::Redpanda => &[
                ("Committed Records", "Native application consumers read committed transactional output"),
                ("Dedicated Topics", "The native demo uses its own input, output and recovery topics"),
                ("Ordered Input", "The MVP pipelines run with one partition and parallelism one"),
            ],
            Self::Streamr => &[
                ("Identity Resolution", "Arcstream policy connects anonymous and known identifiers within each tenant"),
                ("Profile Lifecycle", "Maintains event counters, calendar windows, activity and timeout state"),
                ("Session Summaries", "Emits a completed summary when input progress passes the inactivity deadline"),
                ("Application-owned Functions", "Business rules remain in Arcstream rather than the engine"),
            ],
            Self::LiveUpdates => &[
                ("Events", "Resolved events feed the live event view"),
                ("Profiles", "Complete profile after-images feed the subscription adapter"),
                ("Notifications", "One server-side consumer broadcasts updates to connected viewers"),
            ],
            Self::FlareDB => &[
                ("Three Tables", "Events, profiles and completed sessions support the demo's queries"),
                ("Profile Upserts", "Profiles are keyed by tenant and canonical identity"),
                ("SQL Analytics", "Aggregations and breakdowns are requested through the Query API"),
            ],
            Self::NativeState => &[
                ("State Tables", "Native primary-key lookups read application state"),
                ("RocksDB", "Disk-backed state uses explicit memory and row bounds"),
                ("Persistent Checkpoints", "State and committed source positions support restart recovery"),
            ],
            Self::QueryApi => &[
                ("Typed GraphQL", "Queries expose tenants, profiles, event histories and dashboard statistics"),
                ("WebSocket Subscriptions", "Live event and profile notifications use graphql-transport-ws"),
                ("Single Backend", "Every demo analytics request is served by FlareDB"),
            ],
            Self::Dashboard => &[
                ("SSR + WASM", "Server-rendered pages hydrate for browser interactivity"),
                ("Reactive Signals", "Rust signals update views as data arrives"),
                ("Live or Paused", "Freeze profiles and events while inspecting them"),
            ],
        }
    }
}

const DECISIONS: &[(&str, &str, &str)] = &[
    (
        "Native SQL with application-owned policy",
        "Keep engine execution and CDP business rules separate",
        "Streamr provides native SQL, state tables, configurable storage and checkpoint recovery. Arcstream supplies schemas, tenant identity rules, rolling counters and lifecycle functions.",
    ),
    (
        "FlareDB for demo analytics",
        "One backend for profiles, events and session queries",
        "The Query API reads the three FlareDB tables through SQL. The dashboard uses this same backend for every viewer, including browsers carrying older backend-selection cookies.",
    ),
    (
        "Explicit clock policy",
        "Source event time governs replay and live processing",
        "Both replay and live processing use original source event time with a five-second disorder watermark. Native calendar aggregates update retained metrics as source time progresses; SESSION publishes completed windows. Elapsed wall time alone does not close an idle stream.",
    ),
    (
        "Committed streaming output",
        "Transactions and checkpoints support restart continuity",
        "Each native Kafka sink commits its own output transaction and consumers read committed records. Checkpoints retain source positions and state. This does not create one transaction across all output topics or analytics storage.",
    ),
    (
        "GraphQL subscriptions",
        "Stream notifications to connected browsers",
        "The Query API broadcasts committed event and profile updates over WebSockets. Views can refresh as updates arrive, and viewers can pause the live tables while inspecting them.",
    ),
];

const TECH_STACK: &[(&str, &str, &str)] = &[
    ("Rust", "Application Services", "Event producer, business functions, Query API and dashboard"),
    ("Streamr", "Stateful Streaming", "Native SQL, state tables, event-time timers and checkpoints"),
    ("Redpanda", "Event Bus", "Kafka-compatible input, output and transaction recovery topics"),
    ("RocksDB", "Native State", "Disk-backed identity, profile and native session state"),
    ("FlareDB", "Analytics", "SQL over events, profiles and completed sessions"),
    ("Leptos", "Frontend Framework", "Rust SSR, WASM hydration and reactive signals"),
    ("GraphQL", "API Layer", "Typed queries and WebSocket subscriptions"),
];

const P1: &str = "M140,155 L185,155";
const P2: &str = "M305,155 L350,155";
const P3: &str = "M495,120 C525,120 525,72 550,72";
const P4: &str = "M495,155 L550,155";
const P5: &str = "M495,195 C525,195 525,242 550,242";
const P6: &str = "M680,72 C710,72 710,115 730,115";
const P7: &str = "M680,155 C710,155 710,115 730,115";
const P8: &str = "M860,115 L910,115";

const DEFS_SVG: &str = concat!(
    r#"<pattern id="arch-dots" width="24" height="24" patternUnits="userSpaceOnUse">"#,
    r#"<circle cx="12" cy="12" r="0.6" fill="rgba(255,255,255,0.04)"/>"#,
    r#"</pattern>"#,
    r#"<filter id="particle-glow" x="-200%" y="-200%" width="500%" height="500%">"#,
    r#"<feGaussianBlur in="SourceGraphic" stdDeviation="2" result="blur"/>"#,
    r#"<feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>"#,
    r#"</filter>"#,
);

fn particles_svg() -> String {
    let paths: &[(&str, f64, usize)] = &[
        (P1, 1.8, 2),
        (P2, 1.8, 2),
        (P3, 2.5, 3),
        (P4, 2.0, 2),
        (P5, 2.5, 3),
        (P6, 2.5, 3),
        (P7, 2.5, 3),
        (P8, 1.8, 2),
    ];
    let mut svg = String::with_capacity(4096);
    for (path, dur, count) in paths {
        for i in 0..*count {
            let delay = -(dur * i as f64 / *count as f64);
            let r = if i % 2 == 0 { 3 } else { 2 };
            let op = if i == 0 { "0.9" } else { "0.5" };
            svg.push_str(&format!(
                r##"<circle r="{r}" fill="#D4944C" filter="url(#particle-glow)">"##
            ));
            svg.push_str(&format!(
                r##"<animateMotion dur="{dur}s" repeatCount="indefinite" begin="{delay:.1}s" path="{path}"/>"##
            ));
            svg.push_str(&format!(
                r##"<animate attributeName="opacity" values="0;{op};{op};0" keyTimes="0;0.1;0.9;1" dur="{dur}s" repeatCount="indefinite" begin="{delay:.1}s"/>"##
            ));
            svg.push_str("</circle>");
        }
    }
    svg
}

#[component]
pub fn AboutPage() -> impl IntoView {
    let active = RwSignal::new(None::<ArchNode>);
    let expanded = RwSignal::new(None::<usize>);
    let particles = particles_svg();

    // Live Pulse Bar: periodic stats refresh using Tick context
    let tick = use_context::<Tick>();
    let stats = Resource::new(
        move || tick.map(|t| t.0.get()).unwrap_or(0),
        |_| get_dashboard_stats(),
    );

    let total_users = RwSignal::new(0u64);
    let total_events = RwSignal::new(0u64);
    let events_per_sec = RwSignal::new(0u64);
    let prev_snapshot = RwSignal::new(None::<(u64, f64)>);
    let rate_samples = StoredValue::new(std::collections::VecDeque::<f64>::with_capacity(10));
    let pulse_loaded = RwSignal::new(false);

    Effect::new(move || {
        if let Some(Ok(result)) = stats.get() {
            let s = &result.data;
            total_users.set(s.total_users);

            #[cfg(feature = "hydrate")]
            {
                let now = web_sys::window()
                    .and_then(|w| w.performance())
                    .map(|p| p.now())
                    .unwrap_or(0.0);
                if let Some((prev_events, prev_time)) = prev_snapshot.get_untracked() {
                    let elapsed_secs = (now - prev_time) / 1000.0;
                    if elapsed_secs > 0.0 {
                        let diff = s.total_events.saturating_sub(prev_events);
                        let sample = diff as f64 / elapsed_secs;
                        rate_samples.update_value(|buf| {
                            if buf.len() >= 10 {
                                buf.pop_front();
                            }
                            buf.push_back(sample);
                        });
                        let avg = rate_samples.with_value(|buf| {
                            if buf.is_empty() { 0.0 } else { buf.iter().sum::<f64>() / buf.len() as f64 }
                        });
                        events_per_sec.set(avg.round() as u64);
                    }
                }
                prev_snapshot.set(Some((s.total_events, now)));
            }

            #[cfg(not(feature = "hydrate"))]
            {
                let _ = &prev_snapshot;
                let _ = &rate_samples;
            }

            total_events.set(s.total_events);

            if !pulse_loaded.get_untracked() {
                pulse_loaded.set(true);
            }
        }
    });

    view! {
        <Title text="Architecture — CDP Dashboard"/>
        <div class="about-page">
            <div class="pulse-bar">
                <div class="pulse-metric">
                    <span class="pulse-dot"></span>
                    <span class="pulse-value">
                        {move || if pulse_loaded.get() {
                            view! { <RollingCounter value=Signal::from(events_per_sec) /> }.into_any()
                        } else {
                            view! { <span>"\u{2014}"</span> }.into_any()
                        }}
                    </span>
                    <span class="pulse-label">"Events/sec"</span>
                </div>
                <div class="pulse-metric">
                    <span class="pulse-dot"></span>
                    <span class="pulse-value">
                        {move || if pulse_loaded.get() {
                            view! { <RollingCounter value=Signal::from(total_users) /> }.into_any()
                        } else {
                            view! { <span>"\u{2014}"</span> }.into_any()
                        }}
                    </span>
                    <span class="pulse-label">"Total Profiles"</span>
                </div>
                <div class="pulse-metric">
                    <span class="pulse-dot"></span>
                    <span class="pulse-value">
                        {move || if pulse_loaded.get() {
                            view! { <RollingCounter value=Signal::from(total_events) /> }.into_any()
                        } else {
                            view! { <span>"\u{2014}"</span> }.into_any()
                        }}
                    </span>
                    <span class="pulse-label">"Total Events"</span>
                </div>
            </div>

            <div class="about-hero">
                <span class="about-badge">"Reference Architecture"</span>
                <h2 class="about-title">"Real-time Customer Data Platform"</h2>
                <p class="about-desc">
                    "From raw clickstream events to live profiles and dashboards. "
                    "Identity resolution, sessionization, and profile aggregation "
                    "powered by native Streamr SQL and FlareDB."
                </p>
                <p class="about-hint">"Click any component to explore"</p>
            </div>

            <div class="about-diagram">
                <svg viewBox="0 0 1060 290" class="arch-svg">
                    <defs inner_html=DEFS_SVG />
                    <rect width="1060" height="290" fill="url(#arch-dots)" />

                    <path d=P1 class="pipe-base" />
                    <path d=P2 class="pipe-base" />
                    <path d=P3 class="pipe-base" />
                    <path d=P4 class="pipe-base" />
                    <path d=P5 class="pipe-base" />
                    <path d=P6 class="pipe-base" />
                    <path d=P7 class="pipe-base" />
                    <path d=P8 class="pipe-base" />

                    <path d=P1 class="pipe-flow" />
                    <path d=P2 class="pipe-flow" />
                    <path d=P3 class="pipe-flow" />
                    <path d=P4 class="pipe-flow" />
                    <path d=P5 class="pipe-flow" />
                    <path d=P6 class="pipe-flow" />
                    <path d=P7 class="pipe-flow" />
                    <path d=P8 class="pipe-flow" />

                    <g inner_html=particles.clone() style="pointer-events:none" />

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::Producer)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::Producer) { None } else { Some(ArchNode::Producer) })>
                        <rect x="15" y="130" width="125" height="50" rx="8" class="arch-rect" />
                        <text x="77" y="152" class="arch-label">"Event Producer"</text>
                        <text x="77" y="168" class="arch-sublabel">"Rust, tokio"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::Redpanda)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::Redpanda) { None } else { Some(ArchNode::Redpanda) })>
                        <rect x="185" y="130" width="120" height="50" rx="8" class="arch-rect" />
                        <text x="245" y="152" class="arch-label">"Redpanda"</text>
                        <text x="245" y="168" class="arch-sublabel">"Kafka-compatible"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::Streamr)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::Streamr) { None } else { Some(ArchNode::Streamr) })>
                        <rect x="350" y="75" width="145" height="160" rx="8" class="arch-rect" />
                        <text x="422" y="98" class="arch-label">"Streamr"</text>
                        <line x1="368" y1="108" x2="477" y2="108" class="arch-divider" />
                        <text x="422" y="127" class="arch-job">"Identity Resolution"</text>
                        <text x="422" y="147" class="arch-job">"Profile Updater"</text>
                        <text x="422" y="167" class="arch-job">"Sessionization"</text>
                        <text x="422" y="187" class="arch-job">"Lifecycle Timers"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::LiveUpdates)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::LiveUpdates) { None } else { Some(ArchNode::LiveUpdates) })>
                        <rect x="550" y="47" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="615" y="69" class="arch-label">"Live Updates"</text>
                        <text x="615" y="85" class="arch-sublabel">"Committed Kafka"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::FlareDB)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::FlareDB) { None } else { Some(ArchNode::FlareDB) })>
                        <rect x="550" y="130" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="615" y="152" class="arch-label">"FlareDB"</text>
                        <text x="615" y="168" class="arch-sublabel">"SQL analytics"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::NativeState)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::NativeState) { None } else { Some(ArchNode::NativeState) })>
                        <rect x="550" y="217" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="615" y="239" class="arch-label">"Native State"</text>
                        <text x="615" y="255" class="arch-sublabel">"RocksDB + checkpoints"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::QueryApi)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::QueryApi) { None } else { Some(ArchNode::QueryApi) })>
                        <rect x="730" y="90" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="795" y="112" class="arch-label">"Query API"</text>
                        <text x="795" y="128" class="arch-sublabel">"GraphQL + WS"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::Dashboard)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::Dashboard) { None } else { Some(ArchNode::Dashboard) })>
                        <rect x="910" y="90" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="975" y="112" class="arch-label">"Dashboard"</text>
                        <text x="975" y="128" class="arch-sublabel">"Leptos SSR+WASM"</text>
                    </g>
                </svg>
            </div>

            {move || active.get().map(|node| view! {
                <div class="node-detail">
                    <div class="node-detail-header">
                        <div>
                            <h3 class="node-detail-title">{node.title()}</h3>
                            <p class="node-detail-desc">{node.description()}</p>
                        </div>
                        <button class="node-detail-close"
                                on:click=move |_| active.set(None)>
                            "\u{2715}"
                        </button>
                    </div>
                    <div class="node-detail-grid">
                        {node.points().iter().map(|(t, d)| view! {
                            <div class="detail-point">
                                <div class="detail-point-title">{*t}</div>
                                <div class="detail-point-text">{*d}</div>
                            </div>
                        }).collect::<Vec<_>>()}
                    </div>
                </div>
            })}

            <div class="install-section">
                <span class="install-label">"Run it yourself"</span>
                <code class="install-cmd">"curl -fsSL https://raw.githubusercontent.com/jasadams/arcstream/main/deploy/install.sh | bash"</code>
                {
                    let copied = RwSignal::new(false);
                    view! {
                        <button class=move || if copied.get() { "install-copy copied" } else { "install-copy" }
                                on:click=move |_| {
                                    #[cfg(feature = "hydrate")]
                                    {
                                        use wasm_bindgen::prelude::*;
                                        let cmd = "curl -fsSL https://raw.githubusercontent.com/jasadams/arcstream/main/deploy/install.sh | bash";
                                        if let Some(window) = web_sys::window() {
                                            let clipboard = window.navigator().clipboard();
                                            let _ = clipboard.write_text(cmd);
                                            copied.set(true);
                                            let cb = Closure::once(move || { copied.set(false); });
                                            let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
                                                cb.as_ref().unchecked_ref(),
                                                2_000,
                                            );
                                            cb.forget();
                                        }
                                    }
                                }>
                            {move || if copied.get() { "Copied!" } else { "Copy" }}
                        </button>
                    }
                }
            </div>

            <div class="see-live-link">
                <A href="/profiles">"See live data \u{2192}"</A>
            </div>

            <div class="about-section">
                <h2>"Processing Behavior"</h2>
                <div class="guarantee-grid">
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Committed Kafka Output"</div>
                        <div class="guarantee-text">"Each native sink commits transactions; application consumers read committed records."</div>
                    </div>
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Tenant-scoped Identity"</div>
                        <div class="guarantee-text">"Native state tables retain identity mappings according to Arcstream tenant and identity policies."</div>
                    </div>
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Explicit Event Clocks"</div>
                        <div class="guarantee-text">"Replay and live processing preserve source event timestamps and the configured five-second watermark; native windows close when event time advances."</div>
                    </div>
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Checkpoint Recovery"</div>
                        <div class="guarantee-text">"Native state and source positions are checkpointed to persistent storage for restart recovery."</div>
                    </div>
                </div>
            </div>

            <div class="about-section">
                <h2>"Tech Stack"</h2>
                <div class="tech-grid">
                    {TECH_STACK.iter().map(|(name, role, detail)| view! {
                        <div class="tech-card">
                            <div class="tech-name">{*name}</div>
                            <div class="tech-role">{*role}</div>
                            <div class="tech-detail">{*detail}</div>
                        </div>
                    }).collect::<Vec<_>>()}
                </div>
            </div>

            <div class="about-section">
                <h2>"Design Decisions"</h2>
                <div class="decisions-list">
                    {DECISIONS.iter().enumerate().map(|(i, (title, summary, detail))| {
                        let is_open = move || expanded.get() == Some(i);
                        view! {
                            <div class="decision-card" class:expanded=is_open>
                                <button class="decision-header"
                                        on:click=move |_| expanded.update(|e| *e = if *e == Some(i) { None } else { Some(i) })>
                                    <div>
                                        <div class="decision-title">{*title}</div>
                                        <div class="decision-summary">{*summary}</div>
                                    </div>
                                    <span class="decision-chevron">{"\u{203a}"}</span>
                                </button>
                                {move || is_open().then(|| view! {
                                    <div class="decision-content">
                                        <p>{*detail}</p>
                                    </div>
                                })}
                            </div>
                        }
                    }).collect::<Vec<_>>()}
                </div>
            </div>
        </div>
    }
}
