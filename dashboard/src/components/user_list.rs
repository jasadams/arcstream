use std::collections::HashMap;
use leptos::prelude::*;
use leptos_meta::Title;
use crate::app::UserListCache;
use crate::components::avatar::marble_avatar_svg;
use crate::components::device_icons::DeviceIcons;
use crate::components::petname::petname;
use crate::components::relative_time::RelativeTime;
use crate::components::stats_bar::RollingCounter;
use crate::components::live_toggle::LiveToggle;
use crate::server::api::{get_users, get_user_count, get_dashboard_stats, UserProfile};
use crate::util::*;

#[cfg(feature = "hydrate")]
const MAX_NEW_PROFILE_AGE_SECS: i64 = 300;

#[component]
pub fn UserListPage() -> impl IntoView {
    let cache = expect_context::<UserListCache>();
    let page = cache.page;
    let rows = cache.rows;
    let lookup = cache.lookup;
    let last_page = cache.last_fetched_page;

    let users = Resource::new(move || page.get(), get_users);

    let paused = RwSignal::new(false);
    let tick = use_context::<crate::app::Tick>();
    let gated_tick = Memo::new(move |_| {
        if paused.get() { None } else { tick.map(|t| t.0.get()) }
    });
    let total_count = Resource::new(
        move || gated_tick.get(),
        |_| get_user_count(),
    );

    Effect::new(move || {
        if let Some(Ok(count)) = total_count.get() {
            cache.total.set(Some(count.total));
        }
    });

    #[cfg(feature = "hydrate")]
    {
        use crate::websocket::ProfileStream;
        let stream = use_context::<ProfileStream>();

        Effect::new(move || {
            let Some(ProfileStream(sig)) = stream else { return };
            let Some(update) = sig.get() else { return };
            if paused.get_untracked() { return; }

            let cid = update.canonical_id.clone();
            let existing = lookup.with_value(|m| m.get(&cid).copied());

            if let Some(profile_sig) = existing {
                profile_sig.update(|user| {
                    user.total_events = update.profile.total_events;
                    user.total_sessions = update.profile.total_sessions;
                    user.last_seen = update.profile.last_seen.clone();
                    user.last_country = update.profile.last_country.clone();
                    user.last_device = update.profile.last_device.clone();
                    user.last_browser = update.profile.last_browser.clone();
                    user.events_1d = update.profile.events_1d;
                    user.events_7d = update.profile.events_7d;
                    user.events_30d = update.profile.events_30d;
                    user.events_90d = update.profile.events_90d;
                    user.sessions_1d = update.profile.sessions_1d;
                    user.sessions_7d = update.profile.sessions_7d;
                });
            } else if update.action == "create"
                && page.get_untracked() == 0
                && is_recent(&update.profile.first_seen, MAX_NEW_PROFILE_AGE_SECS)
            {
                cache.owner.with_value(|owner| {
                    owner.with(|| {
                        let new_user = UserProfile {
                            tenant_id: update.profile.tenant_id.clone(),
                            canonical_id: cid.clone(),
                            first_seen: update.profile.first_seen.clone(),
                            last_seen: update.profile.last_seen.clone(),
                            total_events: update.profile.total_events,
                            total_sessions: update.profile.total_sessions,
                            page_views: update.profile.page_views,
                            clicks: update.profile.clicks,
                            signups: 0,
                            logins: update.profile.logins,
                            feature_uses: update.profile.feature_uses,
                            last_country: update.profile.last_country.clone(),
                            last_device: update.profile.last_device.clone(),
                            last_browser: update.profile.last_browser.clone(),
                            events_1d: update.profile.events_1d,
                            events_7d: update.profile.events_7d,
                            events_30d: update.profile.events_30d,
                            events_90d: update.profile.events_90d,
                            sessions_1d: update.profile.sessions_1d,
                            sessions_7d: update.profile.sessions_7d,
                            sessions_30d: 0,
                            sessions_90d: 0,
                            total_closed_sessions: 0,
                            avg_session_duration_sec: update.profile.avg_session_duration_sec,
                        };
                        let sig = RwSignal::new(new_user);
                        let is_new = RwSignal::new(true);
                        lookup.update_value(|m| { m.insert(cid.clone(), sig); });
                        rows.update(|r| {
                            r.insert(0, (cid, sig, is_new));
                            if r.len() > PAGE_SIZE as usize {
                                let removed: Vec<String> = r.drain(PAGE_SIZE as usize..)
                                    .map(|(id, _, _)| id)
                                    .collect();
                                lookup.update_value(|m| {
                                    for id in &removed { m.remove(id); }
                                });
                            }
                        });
                    })
                });
            }
        });
    }

    let stats = Resource::new(
        move || gated_tick.get(),
        |_| get_dashboard_stats(),
    );

    let total_users_stat = RwSignal::new(0u64);
    let total_events_stat = RwSignal::new(0u64);
    let active_sessions_stat = RwSignal::new(0u64);
    let stats_loaded = RwSignal::new(false);

    Effect::new(move || {
        if let Some(Ok(s)) = stats.get() {
            total_users_stat.set(s.total_users);
            total_events_stat.set(s.total_events);
            active_sessions_stat.set(s.active_sessions);
            if !stats_loaded.get_untracked() {
                stats_loaded.set(true);
            }
        }
    });

    let has_cache = rows.with_untracked(|r| !r.is_empty());

    if has_cache {
        last_page.set_value(None);
        Effect::new(move || {
            let current_page = page.get();
            let Some(Ok(fetched)) = users.get() else { return };
            if last_page.get_value() == Some(current_page) {
                for user in &fetched {
                    if let Some(sig) = lookup.with_value(|m| m.get(&user.canonical_id).copied()) {
                        sig.set(user.clone());
                    }
                }
            } else {
                last_page.set_value(Some(current_page));
                cache.owner.with_value(|owner| {
                    owner.with(|| {
                        let mut nr = Vec::with_capacity(fetched.len());
                        let mut nl = HashMap::with_capacity(fetched.len());
                        for user in fetched {
                            let cid = user.canonical_id.clone();
                            let sig = RwSignal::new(user);
                            let is_new = RwSignal::new(false);
                            nl.insert(cid.clone(), sig);
                            nr.push((cid, sig, is_new));
                        }
                        lookup.set_value(nl);
                        rows.set(nr);
                    })
                });
            }
        });
    }

    view! {
        <Title text="Profiles — CDP Dashboard"/>
        <div class="page-header-row">
            <div class="page-title-group">
                <h2>"User Profiles"</h2>
                <LiveToggle paused />
            </div>
            <div class="compact-stats">
                <div class="compact-stat">
                    <span class="compact-stat-value">
                        {move || if stats_loaded.get() {
                            view! { <RollingCounter value=Signal::from(total_users_stat) /> }.into_any()
                        } else {
                            view! { <span>"\u{2014}"</span> }.into_any()
                        }}
                    </span>
                    <span class="compact-stat-label">"profiles"</span>
                </div>
                <div class="compact-stat">
                    <span class="compact-stat-value">
                        {move || if stats_loaded.get() {
                            view! { <RollingCounter value=Signal::from(total_events_stat) /> }.into_any()
                        } else {
                            view! { <span>"\u{2014}"</span> }.into_any()
                        }}
                    </span>
                    <span class="compact-stat-label">"events"</span>
                </div>
                <div class="compact-stat">
                    <span class="compact-stat-value">
                        {move || if stats_loaded.get() {
                            view! { <RollingCounter value=Signal::from(active_sessions_stat) /> }.into_any()
                        } else {
                            view! { <span>"\u{2014}"</span> }.into_any()
                        }}
                    </span>
                    <span class="compact-stat-label">"sessions"</span>
                </div>
            </div>
        </div>
        {if has_cache {
            view! { <UserTable rows=rows page=page total=cache.total /> }.into_any()
        } else {
            view! {
                <Transition fallback=move || view! { <SkeletonTable /> }>
                    {move || {
                        let current_page = page.get();
                        if rows.with_untracked(|r| r.is_empty()) || last_page.get_value() != Some(current_page) {
                            match users.get() {
                                Some(Ok(fetched)) => {
                                    last_page.set_value(Some(current_page));
                                    cache.owner.with_value(|owner| {
                                        owner.with(|| {
                                            let mut nr = Vec::with_capacity(fetched.len());
                                            let mut nl = HashMap::with_capacity(fetched.len());
                                            for user in fetched {
                                                let cid = user.canonical_id.clone();
                                                let sig = RwSignal::new(user);
                                                let is_new = RwSignal::new(false);
                                                nl.insert(cid.clone(), sig);
                                                nr.push((cid, sig, is_new));
                                            }
                                            lookup.set_value(nl);
                                            rows.set(nr);
                                        })
                                    });
                                }
                                Some(Err(e)) => {
                                    return Some(view! { <div class="loading">{format!("Error: {e}")}</div> }.into_any());
                                }
                                None => {}
                            }
                        }

                        let r = rows.get();
                        if r.is_empty() { return None; }
                        Some(view! { <UserTable rows=rows page=page total=cache.total /> }.into_any())
                    }}
                </Transition>
            }.into_any()
        }}
    }
}

#[component]
fn SkeletonTable() -> impl IntoView {
    view! {
        <table class="skeleton-table" aria-label="User profiles">
            <thead>
                <tr>
                    <th>"User"</th>
                    <th class="hide-mobile">"Joined"</th>
                    <th>"Last Active"</th>
                    <th>"Events"</th>
                    <th class="hide-mobile">"Sessions"</th>
                    <th>"Country"</th>
                    <th class="hide-mobile">"Devices"</th>
                </tr>
            </thead>
            <tbody>
                {(0..8).map(|_| view! {
                    <tr class="skeleton-row">
                        <td>
                            <div class="user-identity">
                                <span class="user-avatar"><div class="skel-circle"></div></span>
                                <div class="user-names">
                                    <span class="user-petname">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                                    <span class="user-id-short">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                                </div>
                            </div>
                        </td>
                        <td class="hide-mobile">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</td>
                        <td>{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</td>
                        <td>{"\u{00a0}\u{00a0}\u{00a0}"}</td>
                        <td class="hide-mobile">{"\u{00a0}\u{00a0}\u{00a0}"}</td>
                        <td>
                            <span class="flag">{"\u{00a0}"}</span>
                            {" \u{00a0}\u{00a0}\u{00a0}\u{00a0}"}
                        </td>
                        <td class="hide-mobile">{"\u{00a0}\u{00a0}\u{00a0}"}</td>
                    </tr>
                }).collect::<Vec<_>>()}
            </tbody>
        </table>
    }
}

#[component]
fn UserTable(
    rows: RwSignal<Vec<(String, RwSignal<UserProfile>, RwSignal<bool>)>>,
    page: RwSignal<u32>,
    total: RwSignal<Option<u64>>,
) -> impl IntoView {
    view! {
        <table aria-label="User profiles">
            <thead>
                <tr>
                    <th>"User"</th>
                    <th class="hide-mobile">"Joined"</th>
                    <th>"Last Active"</th>
                    <th>"Events"</th>
                    <th class="hide-mobile">"Sessions"</th>
                    <th>"Country"</th>
                    <th class="hide-mobile">"Devices"</th>
                </tr>
            </thead>
            <tbody>
                <For
                    each=move || rows.get()
                    key=|(cid, _, _)| cid.clone()
                    let:entry
                >
                    <UserRowView cid=entry.0 user=entry.1 is_new=entry.2 />
                </For>
            </tbody>
        </table>
        {move || {
            let current_page = page.get();
            let count = rows.get().len() as u32;
            let has_next = count >= PAGE_SIZE;
            let range_start = current_page * PAGE_SIZE + 1;
            let range_end = current_page * PAGE_SIZE + count;
            let total_val = total.get().unwrap_or(0);
            let total_pages = if total_val > 0 { (total_val as f64 / PAGE_SIZE as f64).ceil() as u64 } else { 0 };
            view! {
                <div class="pagination">
                    <span class="pagination-range">
                        <strong>{format!("{range_start}\u{2013}{range_end}")}</strong>
                        {if total_val > 0 { format!(" of {total_val}") } else { String::new() }}
                    </span>
                    <div class="pagination-controls">
                        <button
                            on:click=move |_| page.update(|p| *p = p.saturating_sub(1))
                            disabled=move || page.get() == 0
                            title="Previous"
                            inner_html=r#"<svg viewBox="0 0 24 24" style="width:14px;height:14px;fill:none;stroke:currentColor;stroke-width:2.5;stroke-linecap:round;stroke-linejoin:round"><polyline points="15 18 9 12 15 6"/></svg>"#
                        ></button>
                        <span class="pagination-page">
                            {if total_pages > 0 {
                                format!("Page {} of {}", current_page + 1, total_pages)
                            } else {
                                format!("Page {}", current_page + 1)
                            }}
                        </span>
                        <button
                            on:click=move |_| page.update(|p| *p += 1)
                            disabled=move || !has_next
                            title="Next"
                            inner_html=r#"<svg viewBox="0 0 24 24" style="width:14px;height:14px;fill:none;stroke:currentColor;stroke-width:2.5;stroke-linecap:round;stroke-linejoin:round"><polyline points="9 6 15 12 9 18"/></svg>"#
                        ></button>
                    </div>
                </div>
            }
        }}
    }
}

#[component]
fn UserRowView(
    cid: String,
    user: RwSignal<UserProfile>,
    is_new: RwSignal<bool>,
) -> impl IntoView {
    let tenant_id = user.get_untracked().tenant_id.clone();
    let nav_path = format!("/profiles/{}/{}", tenant_id, cid);
    let avatar_svg = marble_avatar_svg(&cid, 28);
    let display_name = petname(&cid);
    let short_id = truncate_id(&cid);

    let first_seen = Signal::derive(move || user.get().first_seen.clone());
    let last_seen = Signal::derive(move || user.get().last_seen.clone());

    view! {
        <tr
            class=move || if is_new.try_get().unwrap_or(false) { "row-new" } else { "" }
            on:animationend=move |_| { let _ = is_new.try_set(false); }
        >
            <td>
                <a href=nav_path class="row-link">
                    <div class="user-identity">
                        <span class="user-avatar" inner_html=avatar_svg.clone()></span>
                        <div class="user-names">
                            <span class="user-petname">{display_name.clone()}</span>
                            <span class="user-id-short">{short_id.clone()}</span>
                        </div>
                    </div>
                </a>
            </td>
            <td class="hide-mobile"><RelativeTime timestamp=first_seen /></td>
            <td><RelativeTime timestamp=last_seen /></td>
            <td>{move || user.get().total_events.to_string()}</td>
            <td class="hide-mobile">{move || user.get().total_sessions.to_string()}</td>
            <td>{move || {
                let u = user.get();
                let flag = country_flag(&u.last_country);
                let name = country_name(&u.last_country);
                view! { <><span class="flag">{flag}</span>" "{name}</> }
            }}</td>
            <td class="hide-mobile">{move || {
                let device = user.get().last_device.clone();
                view! { <DeviceIcons active=device /> }
            }}</td>
        </tr>
    }
}
