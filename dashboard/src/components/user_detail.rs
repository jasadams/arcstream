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

    // Seeds the route-owned `live_events` signal (read only by the client effect
    // below, never inside <Suspense> in the view). Rendering the timeline from the
    // signal instead of this resource is what stops late-resolving Suspense fragments
    // from orphaning DOM on navigation. The `#[allow]` covers SSR, where the seeding
    // effect doesn't run so the binding is unused.
    #[cfg_attr(feature = "ssr", allow(unused_variables))]
    let events = Resource::new(
        move || (tenant_id(), canonical_id()),
        |(tenant, cid)| get_events(tenant, cid),
    );

    #[cfg(feature = "hydrate")]
    let (live_profile, live_events) = {
        use std::rc::Rc;
        use std::cell::RefCell;
        use crate::websocket::{ProfileStream, EventStream, WsHandle};

        let (live_profile, set_live_profile) = signal(Option::<LiveProfile>::None);
        let (live_events, set_live_events) = signal(Vec::<TimelineEntry>::new());

        Effect::new(move || {
            if let Some(Ok(Some(p))) = profile.get() {
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

        let profile_signal = expect_context::<ProfileStream>().0;
        let event_signal = expect_context::<EventStream>().0;

        Effect::new(move || {
            let Some(update) = profile_signal.get() else { return };
            let cid = canonical_id();
            if update.canonical_id == cid {
                set_live_profile.set(Some(update.profile));
            }
        });

        Effect::new(move || {
            let Some(event) = event_signal.get() else { return };
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

        let handles: Rc<RefCell<Vec<WsHandle>>> = Rc::new(RefCell::new(Vec::new()));
        let handles_cleanup = send_wrapper::SendWrapper::new(handles.clone());

        Effect::new(move || {
            let tid = tenant_id();
            let cid = canonical_id();

            for h in handles.borrow_mut().drain(..) {
                h.disconnect();
            }

            let h1 = crate::websocket::subscribe_profile_update(&tid, &cid, profile_signal);
            let h2 = crate::websocket::subscribe_live_events_filtered(&tid, event_signal);
            handles.borrow_mut().extend([h1, h2]);
        });

        on_cleanup(move || {
            for h in handles_cleanup.borrow_mut().drain(..) {
                h.disconnect();
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
            .or_else(|| profile.get().and_then(|r| r.ok()).flatten())
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

        // Layout shift prevention: this skeleton uses the REAL CSS classes (stat-card,
        // label, value, subtitle, etc.) with &nbsp; content so heights are computed by
        // the same rules as real content. Do NOT replace with skel-bar elements or a
        // min-height — those drift when any font/padding/margin changes.
        <Suspense fallback=move || view! {
            <div class="profile-header profile-placeholder">
                <div class="profile-identity">
                    <span class="profile-avatar"><div class="skel-circle" style="width:48px;height:48px"></div></span>
                    <div>
                        <h2>{"\u{00a0}"}</h2>
                        <div class="subtitle">
                            // Real subtitle has two lines: canonical_id · User · tenant
                            // then subtitle-meta with First seen · Last seen · device · page
                            <span class="mono">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                            " \u{00b7} "
                            {"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}
                            <div class="subtitle-meta">
                                {"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0} \u{00b7} \u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}
                            </div>
                        </div>
                    </div>
                </div>
                <div class="stats-row">
                    {(0..6).map(|_| view! {
                        <div class="stat-card"><div class="label">{"\u{00a0}"}</div><div class="value">{"\u{00a0}"}</div></div>
                    }).collect::<Vec<_>>()}
                </div>
                <div class="stats-row">
                    {(0..4).map(|_| view! {
                        <div class="stat-card"><div class="label">{"\u{00a0}"}</div><div class="value">{"\u{00a0}"}</div></div>
                    }).collect::<Vec<_>>()}
                </div>
                <div class="section-title">{"\u{00a0}"}</div>
                <div class="tag-list">
                    <span class="badge badge-event">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                    <span class="badge badge-event">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                    <span class="badge badge-event">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                </div>
                <div class="section-title">{"\u{00a0}"}</div>
                <div class="tag-list">
                    <span class="badge badge-active">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                    <span class="badge badge-active">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                    <span class="badge badge-active">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                </div>
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
        // Render the feed purely from the route-owned `live_events` signal (seeded
        // from the `events` resource and the live WS stream by the effects above).
        //
        // Previously this read the `events` resource inside <Suspense>. On client-side
        // navigation away before the resource resolved, the late-resolved fragment
        // mounted at the router outlet marker — which by then lived in <main> after the
        // next route's content — and was never disposed, so the timeline bled onto the
        // following page (e.g. /analytics). A <For> over a route-owned signal is torn
        // down synchronously when the route's owner is disposed, so it cannot leak.
        <div class="timeline" aria-live="polite">
            <For
                each=move || live_events.get()
                key=|entry| entry.0.clone()
                let:entry
            >
                <TimelineItemView event=entry.1 is_new=entry.2 />
            </For>
        </div>
        {move || live_events.get().is_empty().then(|| view! {
            <div class="timeline-placeholder"></div>
        })}
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
    let event_href = format!("/events/{}/{}/{}?t={}", e.tenant_id, e.canonical_id, e.event_id, &e.event_time[..10.min(e.event_time.len())]);

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
