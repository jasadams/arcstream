use leptos::prelude::*;
use leptos_meta::Title;
use leptos_router::hooks::use_query_map;
use crate::app::Tick;
use crate::server::stats_api::*;
use crate::components::stats_bar::RollingCounter;
use chartml_core::ChartML;
use chartml_core::theme::Theme;
use chartml_chart_cartesian::CartesianRenderer;
use chartml_chart_pie::PieRenderer;
use chartml_chart_metric::MetricRenderer;
use chartml_leptos::{ChartMLChart, ChartMLRef};

fn create_chartml() -> ChartMLRef {
    let mut c = ChartML::new();
    let mut theme = Theme::dark();
    theme.bg = "transparent".into();
    theme.grid = "#2e2e38".into();
    theme.axis_line = "#2e2e38".into();
    theme.text = "#e8e8ec".into();
    theme.text_secondary = "#9898ae".into();
    theme.text_strong = "#e8e8ec".into();
    theme.label_font_family = "DM Sans, sans-serif".into();
    theme.numeric_font_family = "Geist Mono, monospace".into();
    theme.legend_font_family = "DM Sans, sans-serif".into();
    c.set_theme(theme);
    c.set_default_palette(vec![
        "#D4944C".into(),
        "#4A90D9".into(),
        "#9B6DD7".into(),
        "#36B5A0".into(),
        "#D96B8C".into(),
        "#E5C547".into(),
        "#7E92A6".into(),
        "#46BA7E".into(),
    ]);
    c.register_renderer("bar", CartesianRenderer::new());
    c.register_renderer("line", CartesianRenderer::new());
    c.register_renderer("area", CartesianRenderer::new());
    c.register_renderer("pie", PieRenderer::new());
    c.register_renderer("doughnut", PieRenderer::new());
    c.register_renderer("metric", MetricRenderer::new());
    ChartMLRef::new(c)
}

fn humanize_event_type(raw: &str) -> &str {
    match raw {
        "page_view" => "Page View",
        "click" => "Click",
        "signup" => "Sign Up",
        "login" => "Login",
        "feature_used" => "Feature Used",
        _ => raw,
    }
}

fn range_label(range: &TimeRange) -> &'static str {
    match range {
        TimeRange::Day => "24h",
        TimeRange::Week => "7d",
        TimeRange::Month => "30d",
        TimeRange::Quarter => "90d",
    }
}

fn range_period_label(range: &TimeRange) -> &'static str {
    match range {
        TimeRange::Day => "last 24 hours",
        TimeRange::Week => "last 7 days",
        TimeRange::Month => "last 30 days",
        TimeRange::Quarter => "last 90 days",
    }
}


fn build_stacked_bar_spec(data: &[GroupedTimeSeriesPoint]) -> String {
    let mut rows_yaml = String::new();
    for pt in data {
        rows_yaml.push_str(&format!(
            "    - bucket: \"{}\"\n      group: \"{}\"\n      value: {}\n",
            pt.bucket, humanize_event_type(&pt.group), pt.value
        ));
    }
    format!(
        r#"type: chart
version: 1
title: ""
data:
  provider: inline
  rows:
{rows_yaml}visualize:
  type: bar
  mode: stacked
  columns: bucket
  rows: value
  marks:
    color: group
  axes:
    rows:
      label: ""
style:
  height: 280
"#
    )
}

fn build_line_spec(data: &[TimeSeriesPoint], y_label: &str) -> String {
    let mut rows_yaml = String::new();
    for pt in data {
        rows_yaml.push_str(&format!(
            "    - bucket: \"{}\"\n      value: {}\n",
            pt.bucket, pt.value
        ));
    }
    format!(
        r#"type: chart
version: 1
title: ""
data:
  provider: inline
  rows:
{rows_yaml}visualize:
  type: line
  columns: bucket
  rows: value
  axes:
    rows:
      label: "{y_label}"
style:
  height: 280
"#
    )
}

fn build_bar_spec(data: &[BreakdownRow]) -> String {
    let mut rows_yaml = String::new();
    for row in data {
        let escaped = row.label.replace('"', "\\\"");
        rows_yaml.push_str(&format!(
            "    - label: \"{escaped}\"\n      value: {}\n",
            row.value
        ));
    }
    format!(
        r#"type: chart
version: 1
title: ""
data:
  provider: inline
  rows:
{rows_yaml}visualize:
  type: bar
  orientation: horizontal
  columns: label
  rows: value
  axes:
    rows:
      label: ""
style:
  height: 280
"#
    )
}

fn build_doughnut_spec(data: &[BreakdownRow]) -> String {
    let mut rows_yaml = String::new();
    for row in data {
        let escaped = row.label.replace('"', "\\\"");
        rows_yaml.push_str(&format!(
            "    - label: \"{escaped}\"\n      value: {}\n",
            row.value
        ));
    }
    format!(
        r#"type: chart
version: 1
title: ""
data:
  provider: inline
  rows:
{rows_yaml}visualize:
  type: doughnut
  columns: label
  rows: value
style:
  height: 280
"#
    )
}

#[component]
pub fn StatsPage() -> impl IntoView {
    let query = use_query_map();
    let initial_range = query.read().get("range").and_then(|r| match r.as_str() {
        "24h" => Some(TimeRange::Day),
        "7d" => Some(TimeRange::Week),
        "30d" => Some(TimeRange::Month),
        "90d" => Some(TimeRange::Quarter),
        _ => None,
    }).unwrap_or(TimeRange::Week);
    let range = RwSignal::new(initial_range);

    let tick = use_context::<Tick>();

    let hourly_tick = move || {
        let t = tick.map(|t| t.0.get()).unwrap_or(0) / 3600;
        (t, range.get())
    };

    let events_data = Resource::new(hourly_tick, |(_t, range)| get_events_over_time(range));
    let users_data = Resource::new(hourly_tick, |(_t, range)| get_users_over_time(range));
    let sessions_data = Resource::new(hourly_tick, |(_t, range)| get_sessions_over_time(range));
    let duration_data = Resource::new(hourly_tick, |(_t, range)| get_avg_session_duration(range));
    let pages_data = Resource::new(hourly_tick, |(_t, range)| get_top_pages(range));
    let devices_data = Resource::new(hourly_tick, |(_t, range)| get_device_breakdown(range));
    let browsers_data = Resource::new(hourly_tick, |(_t, range)| get_browser_breakdown(range));
    let countries_data = Resource::new(hourly_tick, |(_t, range)| get_country_breakdown(range));
    let summary_data = Resource::new(
        move || (tick.map(|t| t.0.get()).unwrap_or(0), range.get()),
        |(_tick, range)| get_analytics_summary(range),
    );

    let summary_users = RwSignal::new(0u64);
    let summary_sessions = RwSignal::new(0u64);
    let summary_events = RwSignal::new(0u64);
    let summary_dur_min = RwSignal::new(0u64);
    let summary_dur_sec = RwSignal::new(0u64);
    let summary_eps_whole = RwSignal::new(0u64);
    let summary_eps_tenth = RwSignal::new(0u64);
    let summary_loaded = RwSignal::new(false);

    Effect::new(move || {
        if let Some(Ok(s)) = summary_data.get() {
            summary_users.set(s.users);
            summary_sessions.set(s.sessions);
            summary_events.set(s.events);
            let total_sec = s.avg_duration_sec.round() as u64;
            summary_dur_min.set(total_sec / 60);
            summary_dur_sec.set(total_sec % 60);
            let eps_x10 = (s.events_per_session * 10.0).round() as u64;
            summary_eps_whole.set(eps_x10 / 10);
            summary_eps_tenth.set(eps_x10 % 10);
            if !summary_loaded.get_untracked() {
                summary_loaded.set(true);
            }
        }
    });

    let ranges = [TimeRange::Day, TimeRange::Week, TimeRange::Month, TimeRange::Quarter];

    view! {
        <Title text="Analytics — CDP Dashboard"/>
        <div class="stats-header">
            <h2>"Live Analytics"</h2>
            <div class="range-selector">
                {ranges.into_iter().map(|r| {
                    let label = range_label(&r);
                    let r_clone = r.clone();
                    view! {
                        <button
                            class:active=move || range.get() == r_clone
                            on:click={
                                let r = r.clone();
                                move |_| {
                                    range.set(r.clone());
                                    #[cfg(feature = "hydrate")]
                                    {
                                        let param = match r {
                                            TimeRange::Day => "24h",
                                            TimeRange::Week => "7d",
                                            TimeRange::Month => "30d",
                                            TimeRange::Quarter => "90d",
                                        };
                                        let nav = leptos_router::hooks::use_navigate();
                                        nav(&format!("/analytics?range={param}"), Default::default());
                                    }
                                }
                            }
                        >
                            {label}
                        </button>
                    }
                }).collect::<Vec<_>>()}
            </div>
        </div>

        <div class="live-callout">
            <span class="live-dot"></span>
            "Showing live results from hundreds of millions of rows \u{2014} events stream through Redpanda, processed by Flink, delivered by Pinot in sub-second."
        </div>

        <div class="metric-cards">
            <div class="metric-card">
                <div class="metric-label">"Events"</div>
                <div class="metric-value">
                    {move || if summary_loaded.get() {
                        view! { <RollingCounter value=Signal::from(summary_events) /> }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
                <div class="metric-subtitle">{move || range_period_label(&range.get())}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">"Sessions"</div>
                <div class="metric-value">
                    {move || if summary_loaded.get() {
                        view! { <RollingCounter value=Signal::from(summary_sessions) /> }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
                <div class="metric-subtitle">{move || range_period_label(&range.get())}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">"Users"</div>
                <div class="metric-value">
                    {move || if summary_loaded.get() {
                        view! { <RollingCounter value=Signal::from(summary_users) /> }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
                <div class="metric-subtitle">{move || range_period_label(&range.get())}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">"Avg Duration"</div>
                <div class="metric-value">
                    {move || if summary_loaded.get() {
                        view! {
                            <RollingCounter value=Signal::from(summary_dur_min) />
                            "m "
                            <RollingCounter value=Signal::from(summary_dur_sec) />
                            "s"
                        }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
                <div class="metric-subtitle">{move || range_period_label(&range.get())}</div>
            </div>
            <div class="metric-card">
                <div class="metric-label">"Events / Session"</div>
                <div class="metric-value">
                    {move || if summary_loaded.get() {
                        view! {
                            <RollingCounter value=Signal::from(summary_eps_whole) />
                            "."
                            <RollingCounter value=Signal::from(summary_eps_tenth) />
                        }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
                <div class="metric-subtitle">{move || range_period_label(&range.get())}</div>
            </div>
        </div>

        <div class="charts-grid">
            <div class="chart-panel span-2">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        events_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_stacked_bar_spec(&data);
                                    view! { <ChartPanel title="Events" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>

            <div class="chart-panel">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        users_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_line_spec(&data, "");
                                    view! { <ChartPanel title="Active Users" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>

            <div class="chart-panel">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        sessions_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_line_spec(&data, "");
                                    view! { <ChartPanel title="Sessions" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>

            <div class="chart-panel">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        duration_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_line_spec(&data, "seconds");
                                    view! { <ChartPanel title="Avg Session Duration" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>

            <div class="chart-panel">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        devices_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_doughnut_spec(&data);
                                    view! { <ChartPanel title="Devices" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>

            <div class="chart-panel span-2">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        pages_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_bar_spec(&data);
                                    view! { <ChartPanel title="Top Pages" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>

            <div class="chart-panel">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        browsers_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_doughnut_spec(&data);
                                    view! { <ChartPanel title="Browsers" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>

            <div class="chart-panel">
                <Suspense fallback=move || view! { <div class="chart-loading">"Loading..."</div> }>
                    {move || {
                        countries_data.get().map(|result| {
                            match result {
                                Ok(data) if !data.is_empty() => {
                                    let spec = build_bar_spec(&data);
                                    view! { <ChartPanel title="Top Countries" spec /> }.into_any()
                                }
                                _ => view! { <div class="chart-empty">"No data"</div> }.into_any(),
                            }
                        })
                    }}
                </Suspense>
            </div>
        </div>
    }
}

#[component]
fn ChartPanel(title: &'static str, spec: String) -> impl IntoView {
    let chartml = create_chartml();
    let spec_signal = Signal::derive(move || spec.clone());
    view! {
        <h3 class="chart-title">{title}</h3>
        <ChartMLChart spec=spec_signal chartml=chartml />
    }
}
