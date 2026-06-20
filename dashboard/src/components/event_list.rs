use leptos::prelude::*;
use leptos_meta::Title;
use crate::app::DevMode;
use crate::components::avatar::marble_avatar_svg;
use crate::components::petname::petname;
use crate::components::relative_time::RelativeTime;
use crate::components::live_toggle::LiveToggle;
use crate::server::api::{get_all_events, EventRow};
use crate::util::*;
use leptos::reactive::owner::Owner;

#[cfg(feature = "hydrate")]
const MAX_EVENTS: usize = 200;

pub fn event_type_label(et: &str) -> &'static str {
    match et {
        "page_view" => "Page View",
        "click" => "Click",
        "signup" => "Signup",
        "login" => "Login",
        "feature_used" => "Feature",
        _ => "Event",
    }
}

pub fn event_type_class(et: &str) -> &'static str {
    match et {
        "page_view" => "badge-page-view",
        "click" => "badge-click",
        "signup" => "badge-signup",
        "login" => "badge-login",
        "feature_used" => "badge-feature",
        _ => "badge-default",
    }
}

type EventEntry = (String, RwSignal<EventRow>, RwSignal<bool>);

#[component]
pub fn EventListPage() -> impl IntoView {
    let dev = expect_context::<DevMode>();
    Effect::new(move || {
        dev.query_stats.set(Vec::new());
    });
    let events = Resource::new(
        || (),
        |_| get_all_events(None, None),
    );

    let paused = RwSignal::new(true);
    let rows: RwSignal<Vec<EventEntry>> = RwSignal::new(Vec::new());
    // Route-owned "initial fetch resolved" flag. Drives the skeleton/empty/table
    // switch from a signal instead of reading the `events` resource inside the view,
    // which would leave orphaned DOM on navigation (see user_detail.rs for the full
    // explanation of the late-resolving-Suspense leak).
    let loaded = RwSignal::new(false);
    let error: RwSignal<Option<String>> = RwSignal::new(None);
    let owner = Owner::current().expect("component must have owner");

    {
        let owner = owner.clone();
        Effect::new(move || {
            match events.get() {
                Some(Ok(result)) => {
                    dev.query_stats.update(|s| s.extend(result.stats));
                    let new_rows: Vec<EventEntry> = owner.with(|| {
                        result.data
                            .into_iter()
                            .map(|e| {
                                let eid = e.event_id.clone();
                                (eid, RwSignal::new(e), RwSignal::new(false))
                            })
                            .collect()
                    });
                    rows.set(new_rows);
                    error.set(None);
                    loaded.set(true);
                }
                Some(Err(e)) => {
                    error.set(Some(e.to_string()));
                    loaded.set(true);
                }
                None => {}
            }
        });
    }

    #[cfg(feature = "hydrate")]
    {
        use crate::websocket::EventStream;

        let EventStream(event_signal) = expect_context::<EventStream>();

        let ws_handle = send_wrapper::SendWrapper::new(
            crate::websocket::subscribe_live_events(event_signal),
        );
        on_cleanup(move || ws_handle.disconnect());

        let owner = owner.clone();

        Effect::new(move || {
            let Some(event) = event_signal.get() else { return };
            if paused.get_untracked() { return; }

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

            owner.with(|| {
                rows.update(|r| {
                    r.insert(0, (eid, RwSignal::new(row), RwSignal::new(true)));
                    if r.len() > MAX_EVENTS {
                        r.truncate(MAX_EVENTS);
                    }
                });
            });
        });
    }

    view! {
        <Title text="Events — CDP Dashboard"/>
        <div class="page-header-row">
            <div class="page-title-group">
                <h2>"Events"</h2>
                <LiveToggle paused />
            </div>
        </div>
        // Render from the route-owned `rows`/`loaded`/`error` signals, never from a
        // resource read inside the view. Reading the resource inside <Suspense> left
        // orphaned table DOM on navigation (the late-resolved fragment mounted into
        // <main> after the route owner was disposed). Signals are torn down
        // synchronously on navigation, so nothing can leak. The skeleton is shown
        // until the initial fetch resolves; on the server `loaded` is false, and the
        // client hydrates the same skeleton before the seeding effect runs, so there
        // is no hydration mismatch.
        {move || {
            if let Some(e) = error.get() {
                return view! { <div class="loading">{format!("Error: {e}")}</div> }.into_any();
            }
            if !loaded.get() {
                return view! {
                    <table class="event-table skeleton-table" aria-label="Live events">
                        <thead>
                            <tr>
                                <th>{"\u{00a0}"}</th>
                                <th>{"\u{00a0}"}</th>
                                <th>{"\u{00a0}"}</th>
                                <th>{"\u{00a0}"}</th>
                                <th class="hide-mobile">{"\u{00a0}"}</th>
                                <th class="hide-mobile">{"\u{00a0}"}</th>
                            </tr>
                        </thead>
                        <tbody>
                            {(0..100).map(|_| view! {
                                <tr class="skeleton-row">
                                    <td>{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</td>
                                    <td><span class="badge-event badge-default">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span></td>
                                    <td>
                                        <div class="user-identity compact">
                                            <span class="user-avatar"><div class="skel-circle" style="width:22px;height:22px"></div></span>
                                            <span class="user-petname">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</span>
                                        </div>
                                    </td>
                                    <td class="page-url">{"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}</td>
                                    <td class="hide-mobile">
                                        <span class="device-inline">{"\u{00a0}\u{00a0}"}</span>
                                        {"\u{00a0}\u{00a0}\u{00a0}\u{00a0}\u{00a0}"}
                                    </td>
                                    <td class="hide-mobile">
                                        <span class="flag">{"\u{00a0}"}</span>
                                        {" \u{00a0}\u{00a0}\u{00a0}\u{00a0}"}
                                    </td>
                                </tr>
                            }).collect::<Vec<_>>()}
                        </tbody>
                    </table>
                }.into_any();
            }
            if rows.get().is_empty() {
                return view! {
                    <div class="empty-state">
                        <svg class="empty-icon" viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
                            <circle cx="24" cy="24" r="20" stroke="currentColor" stroke-width="1.5" opacity="0.3"/>
                            <circle cx="24" cy="24" r="12" stroke="currentColor" stroke-width="1.5" opacity="0.5"/>
                            <circle cx="24" cy="24" r="4" fill="currentColor" opacity="0.6"/>
                        </svg>
                        <p>"Waiting for events"</p>
                        <span class="empty-sub">"Events stream here in real-time as users interact with your application."</span>
                    </div>
                }.into_any();
            }
            view! {
                <table class="event-table" aria-label="Live events">
                    <thead>
                        <tr>
                            <th>"Time"</th>
                            <th>"Type"</th>
                            <th>"User"</th>
                            <th>"Page"</th>
                            <th class="hide-mobile">"Device"</th>
                            <th class="hide-mobile">"Country"</th>
                        </tr>
                    </thead>
                    <tbody aria-live="polite">
                        <For
                            each=move || rows.get()
                            key=|(eid, _, _)| eid.clone()
                            let:entry
                        >
                            <EventRowView event=entry.1 is_new=entry.2 />
                        </For>
                    </tbody>
                </table>
            }.into_any()
        }}
    }
}

#[component]
fn EventRowView(
    event: RwSignal<EventRow>,
    is_new: RwSignal<bool>,
) -> impl IntoView {
    let e = event.get_untracked();
    let avatar_svg = marble_avatar_svg(&e.canonical_id, 22);
    let display_name = petname(&e.canonical_id);
    let nav_path = format!("/events/{}/{}/{}?t={}", e.tenant_id, e.canonical_id, e.event_id, &e.event_time[..10.min(e.event_time.len())]);
    let event_time = e.event_time.clone();
    let badge_class = format!("badge-event {}", event_type_class(&e.event_type));
    let label = event_type_label(&e.event_type);
    let flag = country_flag(&e.country);
    let cname = country_name(&e.country);
    let device_icon = device_svg(&e.device_type);

    view! {
        <tr
            class=move || if is_new.get() { "row-new" } else { "" }
            on:animationend=move |_| { is_new.set(false); }
            on:click=move |_| {
                let nav = leptos_router::hooks::use_navigate();
                nav(&nav_path, Default::default());
            }
        >
            <td><RelativeTime timestamp=event_time /></td>
            <td><span class=badge_class>{label}</span></td>
            <td>
                <span class="user-identity compact">
                    <span class="user-avatar" inner_html=avatar_svg></span>
                    <span class="user-petname">{display_name}</span>
                </span>
            </td>
            <td class="page-url">{e.page_url}</td>
            <td class="hide-mobile">
                <span class="device-inline" inner_html=device_icon></span>
                {format!(" {}", e.browser)}
            </td>
            <td class="hide-mobile">
                <span class="flag">{flag}</span>
                " "
                {cname}
            </td>
        </tr>
    }
}
