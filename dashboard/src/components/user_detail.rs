use leptos::prelude::*;
use leptos_meta::Title;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;
use crate::components::avatar::marble_avatar_svg;
use crate::components::petname::petname;
use crate::components::event_list::{event_type_label, event_type_class};
use crate::components::relative_time::RelativeTime;
use crate::components::stats_bar::RollingCounter;
use crate::server::api::{get_live_profile, get_events, EventRow, LiveProfile};
use crate::util::*;

type TimelineEntry = (String, RwSignal<EventRow>, RwSignal<bool>);

#[component]
pub fn UserDetailPage() -> impl IntoView {
    let params = use_params_map();
    let tenant_id = move || {
        params.read().get("tenant").unwrap_or_default()
    };
    let canonical_id = move || {
        params.read().get("id").unwrap_or_default()
    };

    let profile = Resource::new(
        move || (tenant_id(), canonical_id()),
        |(tenant, cid)| get_live_profile(tenant, cid),
    );

    let events = Resource::new(
        move || (tenant_id(), canonical_id()),
        |(tenant, cid)| get_events(tenant, cid),
    );

    #[cfg(feature = "hydrate")]
    let (live_profile, live_events) = {
        use crate::websocket::{ProfileStream, EventStream};

        let (live_profile, set_live_profile) = signal(Option::<LiveProfile>::None);
        let (live_events, set_live_events) = signal(Vec::<TimelineEntry>::new());

        Effect::new(move || {
            if let Some(Ok(p)) = profile.get() {
                set_live_profile.set(Some(p));
            }
        });

        Effect::new(move || {
            if let Some(Ok(fetched)) = events.get() {
                let entries: Vec<TimelineEntry> = fetched.into_iter().map(|e| {
                    let eid = e.event_id.clone();
                    (eid, RwSignal::new(e), RwSignal::new(false))
                }).collect();
                set_live_events.set(entries);
            }
        });

        let profile_stream = use_context::<ProfileStream>();
        Effect::new(move || {
            let Some(ProfileStream(sig)) = profile_stream else { return };
            let Some(update) = sig.get() else { return };
            let cid = canonical_id();
            if update.canonical_id == cid {
                set_live_profile.set(Some(update.profile));
            }
        });

        let event_stream = use_context::<EventStream>();
        Effect::new(move || {
            let Some(EventStream(sig)) = event_stream else { return };
            let Some(event) = sig.get() else { return };
            let cid = canonical_id();
            if event.canonical_id == cid {
                let eid = event.event_id.clone();
                let row = EventRow {
                    event_id: event.event_id,
                    event_type: event.event_type,
                    tenant_id: event.tenant_id,
                    event_time: event.event_time,
                    canonical_id: event.canonical_id,
                    anonymous_id: event.anonymous_id,
                    user_id: event.user_id,
                    page_url: event.page_url,
                    device_type: event.device_type,
                    browser: event.browser,
                    country: event.country,
                };
                set_live_events.update(|current| {
                    if !current.iter().any(|(id, _, _)| *id == eid) {
                        current.insert(0, (eid, RwSignal::new(row), RwSignal::new(true)));
                    }
                });
            }
        });

        (live_profile, live_events)
    };

    #[cfg(not(feature = "hydrate"))]
    let (live_profile, live_events) = {
        let (lp, _) = signal(Option::<LiveProfile>::None);
        let (le, _) = signal(Vec::<TimelineEntry>::new());
        (lp, le)
    };

    let total_events = RwSignal::new(0u64);
    let total_sessions = RwSignal::new(0u64);
    let events_7d = RwSignal::new(0u64);
    let sessions_7d = RwSignal::new(0u64);
    let avg_session = RwSignal::new(0u64);
    let page_views = RwSignal::new(0u64);
    let clicks = RwSignal::new(0u64);
    let logins = RwSignal::new(0u64);
    let feature_uses = RwSignal::new(0u64);

    let profile_data = Memo::new(move |_| {
        live_profile.get()
            .or_else(|| profile.get().and_then(|r| r.ok()))
    });

    Effect::new(move || {
        if let Some(p) = profile_data.get() {
            total_events.set(p.total_events);
            total_sessions.set(p.total_sessions);
            events_7d.set(p.events_7d);
            sessions_7d.set(p.sessions_7d);
            avg_session.set(p.avg_session_duration_sec);
            page_views.set(p.page_views);
            clicks.set(p.clicks);
            logins.set(p.logins);
            feature_uses.set(p.feature_uses);
        }
    });

    let page_title = move || format!("{} — CDP Dashboard", petname(&canonical_id()));

    view! {
        <Title text=page_title/>
        <A href="/profiles" attr:class="back-link">"\u{2190} Back to Profiles"</A>

        <Suspense fallback=move || view! {
            <div class="profile-header" style="animation:none">
                <div class="profile-identity">
                    <div class="skel-circle" style="width:48px;height:48px"></div>
                    <div>
                        <h2><div class="skel skel-bar w-80"></div></h2>
                        <div class="subtitle">
                            <div class="skel skel-bar w-full" style="margin-bottom:4px"></div>
                            <div class="subtitle-meta"><div class="skel skel-bar w-80"></div></div>
                        </div>
                    </div>
                </div>
                <div class="stats-row">
                    {(0..6).map(|_| view! {
                        <div class="stat-card">
                            <div class="label"><div class="skel skel-bar w-64"></div></div>
                            <div class="value"><div class="skel skel-bar w-48"></div></div>
                        </div>
                    }).collect::<Vec<_>>()}
                </div>
                <div class="stats-row">
                    {(0..4).map(|_| view! {
                        <div class="stat-card">
                            <div class="label"><div class="skel skel-bar w-64"></div></div>
                            <div class="value"><div class="skel skel-bar w-48"></div></div>
                        </div>
                    }).collect::<Vec<_>>()}
                </div>
                <div class="section-title"><div class="skel skel-bar w-48"></div></div>
                <div class="tag-list"><span class="empty-hint">{"\u{00a0}"}</span></div>
                <div class="section-title"><div class="skel skel-bar w-48"></div></div>
                <div class="tag-list"><span class="empty-hint">{"\u{00a0}"}</span></div>
            </div>
        }>
        {move || { let _ = profile.get(); Some(view! {
        <div class="profile-header">
            <div class="profile-identity">
                {move || {
                    let cid = canonical_id();
                    let avatar_svg = marble_avatar_svg(&cid, 48);
                    let display_name = petname(&cid);
                    view! {
                        <span class="profile-avatar" inner_html=avatar_svg></span>
                        <div>
                            <h2>{display_name}</h2>
                            <div class="subtitle">
                                {move || profile_data.get().map(|p| {
                                    let active = p.current_session_active;
                                    let user_label = if !p.user_id.is_empty() {
                                        format!("User: {}", p.user_id)
                                    } else {
                                        "Anonymous".to_string()
                                    };
                                    let active_text = if active { " \u{00b7} Active now" } else { "" };
                                    let first_seen_ts = p.first_seen.clone();
                                    let last_seen_ts = p.last_seen.clone();
                                    let device_browser = if !p.last_device.is_empty() || !p.last_browser.is_empty() {
                                        format!("{} \u{00b7} {}", p.last_device, p.last_browser)
                                    } else {
                                        String::new()
                                    };
                                    let last_page = p.last_page.clone();
                                    view! {
                                        {if active {
                                            view! { <span class="active-dot"></span> }.into_any()
                                        } else {
                                            view! { <span></span> }.into_any()
                                        }}
                                        <span class="mono">{p.canonical_id.clone()}</span>
                                        " \u{00b7} "
                                        {user_label} " \u{00b7} " {p.tenant_id.clone()} {active_text}
                                        <div class="subtitle-meta">
                                            "First seen " <RelativeTime timestamp=first_seen_ts />
                                            " \u{00b7} Last seen "
                                            <RelativeTime timestamp=last_seen_ts />
                                            {if !device_browser.is_empty() {
                                                view! { <span>" \u{00b7} " {device_browser}</span> }.into_any()
                                            } else {
                                                view! { <span></span> }.into_any()
                                            }}
                                            {if !last_page.is_empty() {
                                                view! { <span>" \u{00b7} " {last_page}</span> }.into_any()
                                            } else {
                                                view! { <span></span> }.into_any()
                                            }}
                                        </div>
                                    }
                                })}
                            </div>
                        </div>
                    }
                }}
            </div>
            <div class="stats-row">
                <div class="stat-card">
                    <div class="label">"Total Events"</div>
                    <div class="value"><RollingCounter value=Signal::from(total_events) /></div>
                </div>
                <div class="stat-card">
                    <div class="label">"Sessions"</div>
                    <div class="value"><RollingCounter value=Signal::from(total_sessions) /></div>
                </div>
                <div class="stat-card">
                    <div class="label">"Events (7d)"</div>
                    <div class="value"><RollingCounter value=Signal::from(events_7d) /></div>
                </div>
                <div class="stat-card">
                    <div class="label">"Sessions (7d)"</div>
                    <div class="value"><RollingCounter value=Signal::from(sessions_7d) /></div>
                </div>
                <div class="stat-card">
                    <div class="label">"Avg Session"</div>
                    <div class="value"><RollingCounter value=Signal::from(avg_session) />"s"</div>
                </div>
                <div class="stat-card">
                    <div class="label">"Country"</div>
                    <div class="value">
                        {move || profile_data.get().map(|p| {
                            let flag = country_flag(&p.last_country);
                            let cname = country_name(&p.last_country);
                            view! {
                                <span class="flag flag-lg">{flag}</span>
                                " "
                                {cname}
                            }
                        })}
                    </div>
                </div>
            </div>
            <div class="stats-row">
                <div class="stat-card">
                    <div class="label">"Page Views"</div>
                    <div class="value"><RollingCounter value=Signal::from(page_views) /></div>
                </div>
                <div class="stat-card">
                    <div class="label">"Clicks"</div>
                    <div class="value"><RollingCounter value=Signal::from(clicks) /></div>
                </div>
                <div class="stat-card">
                    <div class="label">"Logins"</div>
                    <div class="value"><RollingCounter value=Signal::from(logins) /></div>
                </div>
                <div class="stat-card">
                    <div class="label">"Feature Uses"</div>
                    <div class="value"><RollingCounter value=Signal::from(feature_uses) /></div>
                </div>
            </div>
            <div class="section-title">"Top Pages"</div>
            <div class="tag-list">
                {move || {
                    let pages = profile_data.get().map(|p| {
                        p.top_pages.iter().filter(|pg| !pg.is_empty()).cloned().collect::<Vec<_>>()
                    });
                    match pages {
                        Some(pg) if !pg.is_empty() => pg.into_iter().map(|pg| {
                            view! { <span class="badge badge-event">{pg}</span> }
                        }).collect::<Vec<_>>().into_any(),
                        _ => view! { <span class="empty-hint">"None yet"</span> }.into_any(),
                    }
                }}
            </div>
            <div class="section-title">"Top Features"</div>
            <div class="tag-list">
                {move || {
                    let features = profile_data.get().map(|p| {
                        p.top_features.iter().filter(|pg| !pg.is_empty()).cloned().collect::<Vec<_>>()
                    });
                    match features {
                        Some(f) if !f.is_empty() => f.into_iter().map(|f| {
                            view! { <span class="badge badge-active">{f}</span> }
                        }).collect::<Vec<_>>().into_any(),
                        _ => view! { <span class="empty-hint">"None yet"</span> }.into_any(),
                    }
                }}
            </div>
        </div>
        }) }}
        </Suspense>

        <div class="section-title">"Recent Events"</div>
        <Suspense fallback=move || view! { <div class="timeline-placeholder"></div> }>
            {move || {
                let current = live_events.get();
                if current.is_empty() {
                    return events.get().map(|result| match result {
                        Ok(evts) => render_timeline(evts).into_any(),
                        Err(e) => view! { <div class="loading">{format!("Error: {e}")}</div> }.into_any()
                    });
                }
                Some(view! {
                    <div class="timeline" aria-live="polite">
                        <For
                            each=move || live_events.get()
                            key=|entry| entry.0.clone()
                            let:entry
                        >
                            <TimelineItemView event=entry.1 is_new=entry.2 />
                        </For>
                    </div>
                }.into_any())
            }}
        </Suspense>
    }
}

fn render_timeline(events: Vec<EventRow>) -> impl IntoView {
    view! {
        <div class="timeline">
            {events.into_iter().map(|e| {
                let event_time = e.event_time.clone();
                let icon_svg = device_svg(&e.device_type);
                let event_href = format!("/events/{}/{}/{}", e.tenant_id, e.canonical_id, e.event_id);
                view! {
                    <div class="timeline-item">
                        <A href=event_href>
                            <div class="time"><RelativeTime timestamp=event_time /></div>
                            <div class="detail">
                                <span class=format!("badge {}", event_type_class(&e.event_type))>{event_type_label(&e.event_type)}</span>
                                " "
                                {e.page_url.clone()}
                                " "
                                <span class="timeline-device">
                                    <span class="device-icon active" inner_html=icon_svg></span>
                                    {format!(" {} · {}", e.device_type, e.browser)}
                                </span>
                            </div>
                        </A>
                    </div>
                }
            }).collect::<Vec<_>>()}
        </div>
    }
}

#[component]
fn TimelineItemView(
    event: RwSignal<EventRow>,
    is_new: RwSignal<bool>,
) -> impl IntoView {
    let e = event.get_untracked();
    let event_time = e.event_time.clone();
    let icon_svg = device_svg(&e.device_type);
    let event_href = format!("/events/{}/{}/{}", e.tenant_id, e.canonical_id, e.event_id);

    view! {
        <div
            class=move || if is_new.get() { "timeline-item timeline-item-new" } else { "timeline-item" }
            on:animationend=move |_| { is_new.set(false); }
        >
            <A href=event_href>
                <div class="time"><RelativeTime timestamp=event_time /></div>
                <div class="detail">
                    <span class=format!("badge {}", event_type_class(&e.event_type))>{event_type_label(&e.event_type)}</span>
                    " "
                    {e.page_url.clone()}
                    " "
                    <span class="timeline-device">
                        <span class="device-icon active" inner_html=icon_svg></span>
                        {format!(" {} · {}", e.device_type, e.browser)}
                    </span>
                </div>
            </A>
        </div>
    }
}
