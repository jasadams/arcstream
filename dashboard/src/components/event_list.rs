use leptos::prelude::*;
use leptos_meta::Title;
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
    let events = Resource::new(
        || (),
        |_| get_all_events(None, None),
    );

    let paused = RwSignal::new(true);
    let rows: RwSignal<Vec<EventEntry>> = RwSignal::new(Vec::new());
    let owner = Owner::current().expect("component must have owner");

    {
        let owner = owner.clone();
        Effect::new(move || {
            if let Some(Ok(fetched)) = events.get() {
                let new_rows: Vec<EventEntry> = owner.with(|| {
                    fetched
                        .into_iter()
                        .map(|e| {
                            let eid = e.event_id.clone();
                            (eid, RwSignal::new(e), RwSignal::new(false))
                        })
                        .collect()
                });
                rows.set(new_rows);
            }
        });
    }

    #[cfg(feature = "hydrate")]
    {
        use crate::websocket::EventStream;

        let stream = use_context::<EventStream>();
        let owner = owner.clone();

        Effect::new(move || {
            let Some(EventStream(sig)) = stream else { return };
            let Some(event) = sig.get() else { return };
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
        <Suspense fallback=move || view! {
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
        }>
            {move || {
                let r = rows.get();
                if r.is_empty() {
                    return events.get().map(|result| match result {
                        Ok(_) => view! {
                            <div class="empty-state">
                                <svg class="empty-icon" viewBox="0 0 48 48" fill="none" xmlns="http://www.w3.org/2000/svg">
                                    <circle cx="24" cy="24" r="20" stroke="currentColor" stroke-width="1.5" opacity="0.3"/>
                                    <circle cx="24" cy="24" r="12" stroke="currentColor" stroke-width="1.5" opacity="0.5"/>
                                    <circle cx="24" cy="24" r="4" fill="currentColor" opacity="0.6"/>
                                </svg>
                                <p>"Waiting for events"</p>
                                <span class="empty-sub">"Events stream here in real-time as users interact with your application."</span>
                            </div>
                        }.into_any(),
                        Err(e) => view! { <div class="loading">{format!("Error: {e}")}</div> }.into_any()
                    });
                }
                Some(view! {
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
                }.into_any())
            }}
        </Suspense>
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
    let nav_path = format!("/events/{}/{}/{}", e.tenant_id, e.canonical_id, e.event_id);
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
