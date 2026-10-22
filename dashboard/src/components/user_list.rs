use std::collections::HashMap;
use leptos::prelude::*;
use leptos_meta::Title;
use crate::app::{DevMode, UserListCache};
use crate::components::avatar::marble_avatar_svg;
use crate::components::device_icons::DeviceIcons;
use crate::components::petname::petname;
use crate::components::relative_time::RelativeTime;
use crate::components::stats_bar::RollingCounter;
use crate::components::live_toggle::LiveToggle;
use crate::server::api::{get_users, get_user_count, get_dashboard_stats, UserProfile, LiveProfile};
use crate::util::*;

// A complete snapshot is sufficient to upsert a row even when transport
// coalescing delivered its update instead of its original create.
fn apply_profile_snapshot(cache: UserListCache, profile: &LiveProfile) {
    let key = (profile.tenant_id.clone(), profile.canonical_id.clone());
    let user = UserProfile::from(profile);
    if let Some(existing) = cache.lookup.with_value(|m| m.get(&key).copied()) {
        existing.set(user);
    } else if cache.page.get_untracked() == 0 {
        cache.owner.with_value(|owner| owner.with(|| {
            let sig = RwSignal::new(user);
            let is_new = RwSignal::new(true);
            cache.lookup.update_value(|m| { m.insert(key.clone(), sig); });
            cache.rows.update(|rows| rows.push((key, sig, is_new)));
        }));
    }
    // Keep the same first-seen descending order as the fetched first page.
    // An admitted earlier event can move an existing person back in that order.
    cache.rows.update(|rows| {
        rows.sort_by(|left, right| {
            let left_seen = left.1.with_untracked(|p| parse_timestamp(&p.first_seen));
            let right_seen = right.1.with_untracked(|p| parse_timestamp(&p.first_seen));
            right_seen.cmp(&left_seen).then_with(|| left.0.cmp(&right.0))
        });
        if rows.len() > PAGE_SIZE as usize {
            let removed: Vec<crate::app::ProfileKey> = rows.drain(PAGE_SIZE as usize..)
                .map(|(key, _, _)| key).collect();
            cache.lookup.update_value(|m| {
                for key in removed { m.remove(&key); }
            });
        }
    });
}

#[component]
pub fn UserListPage() -> impl IntoView {
    let cache = expect_context::<UserListCache>();
    let page = cache.page;
    let rows = cache.rows;
    let lookup = cache.lookup;
    let last_page = cache.last_fetched_page;

    let dev = expect_context::<DevMode>();
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

    // Clear stats when page changes
    Effect::new(move || {
        let _ = page.get();
        dev.query_stats.set(Vec::new());
    });

    Effect::new(move || {
        if let Some(Ok(count)) = total_count.get() {
            cache.total.set(Some(count.total));
        }
    });

    #[cfg(feature = "hydrate")]
    {
        use crate::websocket::ProfileStream;

        let ProfileStream(profile_signal) = expect_context::<ProfileStream>();

        let ws_handle = send_wrapper::SendWrapper::new(
            crate::websocket::subscribe_profile_updates(profile_signal),
        );
        on_cleanup(move || ws_handle.disconnect());

        Effect::new(move || {
            let Some(update) = profile_signal.get() else { return };
            if paused.get_untracked() { return; }

            apply_profile_snapshot(cache, &update.profile);
        });
    }

    let stats = Resource::new(
        move || gated_tick.get(),
        |_| get_dashboard_stats(),
    );

    let total_users_stat = RwSignal::new(0u64);
    let total_events_stat = RwSignal::new(0u64);
    let stats_loaded = RwSignal::new(false);

    Effect::new(move || {
        if let Some(Ok(result)) = stats.get() {
            dev.query_stats.update(|s| s.extend(result.stats.clone()));
            total_users_stat.set(result.data.total_users);
            total_events_stat.set(result.data.total_events);
            if !stats_loaded.get_untracked() {
                stats_loaded.set(true);
            }
        }
    });

    let has_cache = rows.with_untracked(|r| !r.is_empty());

    if has_cache {
        last_page.set_value(None);
        Effect::new(move || {
            let Some(Ok(result)) = users.get() else { return };
            dev.query_stats.update(|s| s.extend(result.stats));
            let fetched = result.data;
            let current_page = page.get_untracked();
            if last_page.get_value() == Some(current_page) {
                for user in &fetched {
                    if let Some(sig) = lookup.with_value(|m| m.get(&(user.tenant_id.clone(), user.canonical_id.clone())).copied()) {
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
                            let key = (user.tenant_id.clone(), user.canonical_id.clone());
                            let sig = RwSignal::new(user);
                            let is_new = RwSignal::new(false);
                            nl.insert(key.clone(), sig);
                            nr.push((key, sig, is_new));
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
            </div>
        </div>
        {if has_cache {
            view! { <UserTable rows=rows page=page total=cache.total /> }.into_any()
        } else {
            view! {
                <Transition fallback=move || view! { <SkeletonTable /> }>
                    {move || {
                        let raw = users.get();
                        let current_page = page.get_untracked();
                        if rows.with_untracked(|r| r.is_empty()) || last_page.get_value() != Some(current_page) {
                            match raw {
                                Some(Ok(result)) => {
                                    dev.query_stats.update(|s| s.extend(result.stats));
                                    let fetched = result.data;
                                    last_page.set_value(Some(current_page));
                                    cache.owner.with_value(|owner| {
                                        owner.with(|| {
                                            let mut nr = Vec::with_capacity(fetched.len());
                                            let mut nl = HashMap::with_capacity(fetched.len());
                                            for user in fetched {
                                                let key = (user.tenant_id.clone(), user.canonical_id.clone());
                                                let sig = RwSignal::new(user);
                                                let is_new = RwSignal::new(false);
                                                nl.insert(key.clone(), sig);
                                                nr.push((key, sig, is_new));
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
                    <th>{"\u{00a0}"}</th>
                    <th>{"\u{00a0}"}</th>
                    <th>{"\u{00a0}"}</th>
                    <th class="hide-mobile">{"\u{00a0}"}</th>
                    <th>{"\u{00a0}"}</th>
                    <th class="hide-mobile">{"\u{00a0}"}</th>
                </tr>
            </thead>
            <tbody>
                {(0..25).map(|_| view! {
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
    rows: RwSignal<Vec<crate::app::UserRow>>,
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
                    <UserRowView cid=entry.0.1 user=entry.1 is_new=entry.2 />
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

#[cfg(test)]
mod profile_snapshot_tests {
    use super::*;

    fn profile(tenant: &str) -> LiveProfile {
        serde_json::from_value(serde_json::json!({
            "tenantId":tenant, "canonicalId":"shared-person",
            "firstSeen":"2020-01-01 00:00:01.000", "lastSeen":"2020-01-01 00:00:02.000",
            "totalEvents":1, "events1D":1,"events7D":1,"events30D":1,"events90D":1,
            "pageViews":1,"clicks":0,"logins":0,"featureUses":0,
            "lastCountry":"AU","lastDevice":"desktop","lastBrowser":"Firefox",
            "avgSessionDurationSec":null,"topPages":["/home"],"topFeatures":[]
        })).unwrap()
    }

    #[test]
    fn update_only_first_snapshot_upserts_and_refreshes_the_complete_row() {
        let owner = Owner::new();
        owner.with(|| {
            let cache = UserListCache {
                page: RwSignal::new(0), rows: RwSignal::new(Vec::new()),
                lookup: StoredValue::new(HashMap::new()), last_fetched_page: StoredValue::new(None),
                total: RwSignal::new(None), owner: StoredValue::new(owner.clone()),
            };
            // The first observed snapshot may be an update after Kafka/API coalescing.
            // No fabricated create action or wall-clock recency test is needed.
            let mut first = profile("tenant-a");
            apply_profile_snapshot(cache, &first);
            let key = ("tenant-a".to_string(), "shared-person".to_string());
            let signal = cache.lookup.with_value(|m| m[&key]);
            assert_eq!(signal.get_untracked().avg_session_duration_sec, None);

            first.first_seen = "2020-01-01 00:00:00.500".into();
            first.last_seen = "2020-01-01 00:00:03.000".into();
            first.total_events = 9;
            first.page_views = 2; first.clicks = 3; first.logins = 1; first.feature_uses = 3;
            first.events_1d = 0; first.events_7d = 4; first.events_30d = 8; first.events_90d = 9;
            first.avg_session_duration_sec = Some(2.125);
            first.last_country = "NZ".into(); first.last_device = "mobile".into(); first.last_browser = "Safari".into();
            apply_profile_snapshot(cache, &first);
            assert_eq!(serde_json::to_value(signal.get_untracked()).unwrap(), serde_json::to_value(UserProfile::from(&first)).unwrap());
            assert_eq!(cache.rows.get_untracked().len(), 1);

            let mut other_tenant = profile("tenant-b");
            other_tenant.total_events = 100;
            apply_profile_snapshot(cache, &other_tenant);
            assert_eq!(cache.rows.get_untracked().len(), 2);
            assert_eq!(cache.lookup.with_value(|m| m.len()), 2);
            assert_eq!(signal.get_untracked().total_events, 9);
            assert_eq!(cache.lookup.with_value(|m| m[&("tenant-b".into(), "shared-person".into())]).get_untracked().total_events, 100);
        });
    }

    #[test]
    fn historical_upserts_and_earlier_corrections_keep_newest_first_before_trimming() {
        let owner = Owner::new();
        owner.with(|| {
            let cache = UserListCache {
                page: RwSignal::new(0), rows: RwSignal::new(Vec::new()),
                lookup: StoredValue::new(HashMap::new()), last_fetched_page: StoredValue::new(None),
                total: RwSignal::new(None), owner: StoredValue::new(owner.clone()),
            };
            // Fill one page with literal event dates; no wall-clock recency rule.
            for day in 1..=PAGE_SIZE {
                let mut row = profile("tenant-a");
                row.canonical_id = format!("person-{day}");
                row.first_seen = format!("2020-01-{day:02} 00:00:01.000");
                apply_profile_snapshot(cache, &row);
            }
            let mut historical = profile("tenant-a");
            historical.canonical_id = "older-unknown".into();
            historical.first_seen = "2019-12-31 23:59:59.000".into();
            apply_profile_snapshot(cache, &historical);
            let keys = cache.rows.get_untracked().into_iter().map(|r| r.0.1).collect::<Vec<_>>();
            assert_eq!(keys.len(), PAGE_SIZE as usize);
            assert_eq!(keys.first().unwrap(), "person-25");
            assert_eq!(keys.last().unwrap(), "person-1");
            assert!(!cache.lookup.with_value(|m| m.contains_key(&("tenant-a".into(), "older-unknown".into()))));

            let mut correction = profile("tenant-a");
            correction.canonical_id = "person-25".into();
            correction.first_seen = "2019-12-30 00:00:00.000".into();
            apply_profile_snapshot(cache, &correction);
            let keys = cache.rows.get_untracked().into_iter().map(|r| r.0.1).collect::<Vec<_>>();
            assert_eq!(keys.first().unwrap(), "person-24");
            assert_eq!(keys.last().unwrap(), "person-25");

            let mut replacement = profile("tenant-a");
            replacement.canonical_id = "newest-unknown".into();
            replacement.first_seen = "2020-01-26 00:00:00.000".into();
            apply_profile_snapshot(cache, &replacement);
            let keys = cache.rows.get_untracked().into_iter().map(|r| r.0.1).collect::<Vec<_>>();
            assert_eq!(keys.first().unwrap(), "newest-unknown");
            assert!(!keys.iter().any(|key| key == "person-25"));
            assert!(!cache.lookup.with_value(|m| m.contains_key(&("tenant-a".into(), "person-25".into()))));
        });
    }

}
