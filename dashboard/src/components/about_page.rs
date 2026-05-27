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
    Flink,
    ScyllaDB,
    Pinot,
    Iceberg,
    QueryApi,
    Dashboard,
}

impl ArchNode {
    fn title(&self) -> &'static str {
        match self {
            Self::Producer => "Event Producer",
            Self::Redpanda => "Redpanda",
            Self::Flink => "Apache Flink",
            Self::ScyllaDB => "ScyllaDB",
            Self::Pinot => "Apache Pinot",
            Self::Iceberg => "Apache Iceberg",
            Self::QueryApi => "Query API",
            Self::Dashboard => "Dashboard",
        }
    }

    fn description(&self) -> &'static str {
        match self {
            Self::Producer => "Simulates realistic clickstream traffic from multiple game studios. Generates page views, clicks, signups, logins, and feature usage across tenants. Users follow behavioral personas \u{2014} power users, regulars, casuals, tourists \u{2014} each with distinct session patterns, think times, and conversion probability.",
            Self::Redpanda => "The event bus connecting every component in the pipeline. All data flows through Redpanda topics \u{2014} raw events from the producer, unified events after identity resolution, profile updates to the Query API, and session summaries to Pinot. Kafka-compatible, built on the Seastar framework.",
            Self::Flink => "The brain of the pipeline. Four jobs transform raw events into usable data: identity resolution merges anonymous and known users, the profile updater maintains rolling window counters in ScyllaDB, sessionization groups events into 30-minute timeout sessions, and the Iceberg writer archives everything to cold storage.",
            Self::ScyllaDB => "Source of truth for live user profiles. Flink's Profile Updater writes directly here on every event \u{2014} rolling window counters, session state, device history. The Query API reads individual profiles with sub-millisecond latency. Also the sole store for PII (email, name), keeping it out of the analytics tier for GDPR compliance.",
            Self::Pinot => "Serves every dashboard chart \u{2014} event counts, time series, breakdowns by device/browser/country, and paginated user lists. Star-tree indexes pre-compute aggregations at ingestion time, so each query is an index lookup rather than a scan. Sub-millisecond latency at 10,000+ concurrent queries.",
            Self::Iceberg => "Cold storage for offline and ad-hoc analysis. Every unified event is archived here in Parquet format on MinIO, partitioned by tenant. When events age out of Pinot's 90-day hot tier, Iceberg retains them indefinitely for historical queries via Trino or Spark \u{2014} ML training, compliance audits, long-range trend analysis.",
            Self::QueryApi => "The single gateway between the data tier and the dashboard. Queries ScyllaDB for individual profiles, queries Pinot for analytics and aggregations, and streams live updates to all connected browsers via WebSocket. One Kafka consumer fans out profile changes and new events to every connected client in real time.",
            Self::Dashboard => "The interface you're looking at. Shows live user profiles, event streams, and analytics charts \u{2014} all updating in real time via WebSocket without polling. Server-rendered HTML for instant first paint, then WASM hydrates for interactivity. Every page reflects pipeline state within milliseconds of Flink processing an event.",
        }
    }

    fn points(&self) -> &'static [(&'static str, &'static str)] {
        match self {
            Self::Producer => &[
                ("Behavioral Personas", "Power users, regulars, casuals, tourists \u{2014} each governs session length, page depth, conversion probability, and return rate"),
                ("Multi-tenant Simulation", "Multiple game studios sharing one pipeline. Tenant ID as Kafka partition key for ordered per-tenant processing"),
                ("Realistic Timing", "Normal distributions for think time and session gaps, diurnal traffic patterns, weekend effects"),
            ],
            Self::Redpanda => &[
                ("Five Topics", "raw-events, unified-events, profile-updates, session-events, identity-merges"),
                ("7-day Retention", "Replay window for pipeline failures \u{2014} matches the timestamp clamping window for consistency"),
                ("Seastar Framework", "Same async, shard-per-core C++ framework as ScyllaDB \u{2014} designed for predictable tail latencies"),
            ],
            Self::Flink => &[
                ("Identity Resolution", "Maintains an identity graph in RocksDB. Maps anonymous IDs to canonical IDs. Detects and collapses identity merges"),
                ("Profile Updater", "Rolling window counters (1d/7d/30d/90d), session state, device tracking. Writes directly to ScyllaDB"),
                ("Sessionization", "Groups events into 30-minute timeout sessions. Emits summaries with duration, pages visited, and event breakdown"),
                ("Exactly-once Delivery", "Kafka transactions + RocksDB checkpoints ensure no duplicates and no data loss across the entire pipeline"),
            ],
            Self::ScyllaDB => &[
                ("Partition Key Design", "(tenant_id, canonical_id) \u{2014} single-partition reads are O(1), regardless of total data size"),
                ("Scales Beyond RAM", "Disk-backed with memory as cache. 200M profiles is trivial. No Redis-style RAM ceiling"),
                ("Who Uses It", "Discord (trillions of messages), Starbucks, Samsung, Zillow, Comcast"),
            ],
            Self::Pinot => &[
                ("Star-tree Index", "Pre-computes COUNT(*), DISTINCTCOUNTHLL, SUM across declared dimensions at ingestion \u{2014} queries are O(1) lookups"),
                ("Two Tables", "Events (append-only, star-tree) for dashboard charts. Profiles (upsert, inverted indexes) for audience lists"),
                ("HLL Sketches", "Approximate COUNT DISTINCT with ~1-2% error. Mergeable across segments, pre-computed in star-tree metrics"),
                ("Who Uses It", "LinkedIn (billions of queries/day), Uber, Stripe, Walmart, Microsoft Teams, Slack"),
            ],
            Self::Iceberg => &[
                ("Parquet on MinIO", "Columnar storage with compression on S3-compatible object storage"),
                ("90+ Day Archive", "Events that age out of Pinot's hot tier persist for historical analysis and compliance"),
                ("Future Path", "Trino or Spark queries for ad-hoc analysis when real-time is not needed"),
            ],
            Self::QueryApi => &[
                ("Typed GraphQL", "async-graphql 7 with queries for tenants, user profiles, event history, and dashboard statistics"),
                ("WebSocket Subscriptions", "Live profile updates and event streams via graphql-transport-ws protocol with automatic reconnection"),
                ("Broadcast Channels", "tokio::broadcast fans out one Kafka consumer to all connected WebSocket clients \u{2014} O(1) per message, not O(clients)"),
            ],
            Self::Dashboard => &[
                ("SSR + WASM", "Server renders full HTML (no loading spinners). WASM hydrates for interactivity. Progressive enhancement."),
                ("Reactive Signals", "RwSignal for mutable state, Signal::derive for computed values \u{2014} surgical DOM updates, no virtual DOM diffing"),
                ("Zero Polling", "All live data via WebSocket subscriptions. New users appear, counters update, events stream \u{2014} without a single setInterval"),
            ],
        }
    }
}

const DECISIONS: &[(&str, &str, &str)] = &[
    (
        "Pinot over ClickHouse",
        "Sub-ms queries at 1000+ QPS vs ~50 QPS concurrent limit",
        "ClickHouse optimizes for 'few big queries fast' \u{2014} each query gets a thread pool, parallel column scans, hash table aggregations. At ~50 concurrent queries, threads fight for CPU and IO. Pinot's star-tree index pre-computes aggregations at ingestion time. A query matching pre-computed dimensions is an index lookup \u{2014} near-zero CPU per query. 1000 concurrent dashboard queries at near-zero cost per query.",
    ),
    (
        "ScyllaDB over Redis",
        "Sub-ms reads that scale beyond available RAM",
        "Redis is purely in-memory. 200M profiles at ~1.5KB each means ~300GB RAM for a single dataset. ScyllaDB maintains sub-millisecond reads with disk-backed storage \u{2014} memory serves as cache, not the entire dataset. Same shard-per-core architecture as Redpanda means no lock contention at any scale.",
    ),
    (
        "Flink DataStream over SQL",
        "Complex stateful logic that doesn't fit SQL's declarative model",
        "Identity resolution requires maintaining an identity graph with merge detection, timer-based session management, and rolling window decay \u{2014} all keyed by tenant and user ID. Flink SQL can't express 'when two canonical IDs merge, collapse the graph and re-key all downstream state.' The Iceberg Writer uses Flink SQL because it's a straightforward stream-to-table copy.",
    ),
    (
        "Event-time over Processing-time",
        "Correct results during replay, backfill, and catch-up scenarios",
        "Processing-time timers produce catastrophically wrong results during replay. Replaying 90 days of events: a 30-minute session timeout never fires (events arrive faster than real time), so 200 sessions collapse into one continuous session. Event-time timers fire when the watermark advances, producing identical results whether processing live or replaying history.",
    ),
    (
        "GraphQL Subscriptions over Polling",
        "One Kafka consumer fans out to all connected clients",
        "The Query API maintains tokio broadcast channels. A single Kafka consumer reads from profile-updates and unified-events topics, broadcasting to all WebSocket clients. No per-client polling, no N+1 queries, no stale data windows. Profile updates arrive within milliseconds of Flink processing them.",
    ),
    (
        "PII Separation for GDPR",
        "Personal data never touches the append-only analytics tier",
        "Pinot's append-only events table cannot delete individual rows. Solution: PII (email, name, phone) lives exclusively in ScyllaDB. Pinot stores only canonical_id \u{2014} an opaque UUID. On erasure request, null PII columns in ScyllaDB. The Pinot data becomes pseudonymous with no re-identification path. GDPR recital 26 excludes data that cannot be attributed to a natural person.",
    ),
];

const TECH_STACK: &[(&str, &str, &str)] = &[
    ("Rust", "Event Producer, Query API, Dashboard", "Zero-cost abstractions, memory safety, async with tokio"),
    ("Java", "Stream Processing", "Required by Flink DataStream API for stateful processing"),
    ("Apache Flink", "Stateful Streaming", "Event-time semantics, RocksDB state, checkpoints to MinIO"),
    ("Redpanda", "Event Bus", "Kafka-compatible, Seastar framework, single binary, no ZooKeeper"),
    ("ScyllaDB", "Live Profiles", "Shard-per-core, sub-ms reads, CQL compatible, disk-backed"),
    ("Apache Pinot", "OLAP Analytics", "Star-tree pre-aggregation, sub-ms at 10k QPS"),
    ("Apache Iceberg", "Cold Storage", "Parquet on MinIO, 90+ day retention, partitioned by tenant"),
    ("Leptos", "Frontend Framework", "Rust SSR + WASM hydration, reactive signals, zero JavaScript"),
    ("GraphQL", "API Layer", "async-graphql with WebSocket subscriptions, broadcast channels"),
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
    let active_sessions = RwSignal::new(0u64);
    let events_per_sec = RwSignal::new(0u64);
    let prev_snapshot = RwSignal::new(None::<(u64, f64)>);
    let rate_samples = StoredValue::new(std::collections::VecDeque::<f64>::with_capacity(10));
    let pulse_loaded = RwSignal::new(false);

    Effect::new(move || {
        if let Some(Ok(s)) = stats.get() {
            total_users.set(s.total_users);
            active_sessions.set(s.active_sessions);

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
                <div class="pulse-metric">
                    <span class="pulse-dot"></span>
                    <span class="pulse-value">
                        {move || if pulse_loaded.get() {
                            view! { <RollingCounter value=Signal::from(active_sessions) /> }.into_any()
                        } else {
                            view! { <span>"\u{2014}"</span> }.into_any()
                        }}
                    </span>
                    <span class="pulse-label">"Active Sessions"</span>
                </div>
            </div>

            <div class="about-hero">
                <span class="about-badge">"Reference Architecture"</span>
                <h2 class="about-title">"Real-time Customer Data Platform"</h2>
                <p class="about-desc">
                    "From raw clickstream events to live dashboards in milliseconds. "
                    "Identity resolution, sessionization, and profile aggregation "
                    "streaming through a pipeline built for scale."
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
                       class:active=move || active.get() == Some(ArchNode::Flink)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::Flink) { None } else { Some(ArchNode::Flink) })>
                        <rect x="350" y="75" width="145" height="160" rx="8" class="arch-rect" />
                        <text x="422" y="98" class="arch-label">"Apache Flink"</text>
                        <line x1="368" y1="108" x2="477" y2="108" class="arch-divider" />
                        <text x="422" y="127" class="arch-job">"Identity Resolution"</text>
                        <text x="422" y="147" class="arch-job">"Profile Updater"</text>
                        <text x="422" y="167" class="arch-job">"Sessionization"</text>
                        <text x="422" y="187" class="arch-job">"Iceberg Writer"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::ScyllaDB)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::ScyllaDB) { None } else { Some(ArchNode::ScyllaDB) })>
                        <rect x="550" y="47" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="615" y="69" class="arch-label">"ScyllaDB"</text>
                        <text x="615" y="85" class="arch-sublabel">"Live profiles"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::Pinot)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::Pinot) { None } else { Some(ArchNode::Pinot) })>
                        <rect x="550" y="130" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="615" y="152" class="arch-label">"Apache Pinot"</text>
                        <text x="615" y="168" class="arch-sublabel">"Star-tree OLAP"</text>
                    </g>

                    <g class="arch-node"
                       class:active=move || active.get() == Some(ArchNode::Iceberg)
                       on:click=move |_| active.update(|a| *a = if *a == Some(ArchNode::Iceberg) { None } else { Some(ArchNode::Iceberg) })>
                        <rect x="550" y="217" width="130" height="50" rx="8" class="arch-rect" />
                        <text x="615" y="239" class="arch-label">"Apache Iceberg"</text>
                        <text x="615" y="255" class="arch-sublabel">"Cold storage"</text>
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
                <h2>"Data Guarantees"</h2>
                <div class="guarantee-grid">
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Exactly-once Delivery"</div>
                        <div class="guarantee-text">"Kafka transactions + Flink checkpoints ensure no duplicates and no data loss across the entire pipeline"</div>
                    </div>
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Event Deduplication"</div>
                        <div class="guarantee-text">"RocksDB keyed state drops duplicate event IDs within a 10-minute window at the pipeline entry point"</div>
                    </div>
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Timestamp Clamping"</div>
                        <div class="guarantee-text">"Client clocks clamped to \u{00b1}7 days of server time \u{2014} prevents watermark corruption from untrusted sources"</div>
                    </div>
                    <div class="guarantee-card">
                        <div class="guarantee-title">"Checkpoint Recovery"</div>
                        <div class="guarantee-text">"RocksDB snapshots + Kafka offsets saved to MinIO every 60s. Pod restart resumes with at most 60s reprocessing"</div>
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
