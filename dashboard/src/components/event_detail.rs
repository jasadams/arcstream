use leptos::prelude::*;
use leptos_meta::Title;
use leptos_router::components::A;
use leptos_router::hooks::use_params_map;
use crate::components::avatar::marble_avatar_svg;
use crate::components::petname::petname;
use crate::components::event_list::{event_type_label, event_type_class};
use crate::components::relative_time::RelativeTime;
use crate::server::api::{get_event, get_events, EventRow};
use crate::util::*;

#[component]
pub fn EventDetailPage() -> impl IntoView {
    let params = use_params_map();
    let tenant_id = move || {
        params.read().get("tenant").unwrap_or_default()
    };
    let canonical_id = move || {
        params.read().get("canonical_id").unwrap_or_default()
    };
    let event_id = move || {
        params.read().get("event_id").unwrap_or_default()
    };

    let event_resource = Resource::new(
        move || (tenant_id(), event_id()),
        |(tenant, eid)| get_event(tenant, eid),
    );

    let nearby_resource = Resource::new(
        move || (tenant_id(), canonical_id()),
        |(tenant, cid)| get_events(tenant, cid),
    );

    let page_title = move || {
        event_resource.get()
            .and_then(|r| r.ok())
            .map(|e| format!("{} Event — CDP Dashboard", event_type_label(&e.event_type)))
            .unwrap_or_else(|| "Event — CDP Dashboard".to_string())
    };

    view! {
        <Title text=page_title/>
        <A href="/events" attr:class="back-link">"\u{2190} Back to Events"</A>

        <Suspense fallback=move || view! {
            <div class="profile-header">
                <div class="event-header">
                    <div class="skel skel-bar w-64" style="height: 24px; margin-bottom: 8px"></div>
                    <div class="skel skel-bar w-80" style="height: 16px; margin-bottom: 6px"></div>
                    <div class="skel skel-bar w-full" style="height: 14px"></div>
                </div>
                <div class="section-title">"User"</div>
                <div class="skel skel-user" style="padding: 12px 0">
                    <div class="skel-circle"></div>
                    <div class="skel-lines"><div class="skel-bar w-80"></div><div class="skel-bar w-48"></div></div>
                </div>
                <div class="section-title">"Event Properties"</div>
                <div class="event-props">
                    {(0..6).map(|_| view! {
                        <div class="prop-label"><div class="skel skel-bar w-48"></div></div>
                        <div class="prop-value"><div class="skel skel-bar w-80"></div></div>
                    }).collect::<Vec<_>>()}
                </div>
            </div>
        }>
            {move || {
                event_resource.get().map(|result| match result {
                    Ok(e) => {
                        view! {
                            <div class="profile-header">
                                <div class="event-header">
                                    <span class=format!("badge {}", event_type_class(&e.event_type))>
                                        {event_type_label(&e.event_type)}
                                    </span>
                                    <div class="event-time">
                                        {absolute_time(&e.event_time)}
                                        " · "
                                        <RelativeTime timestamp=e.event_time.clone() />
                                    </div>
                                    <div class="event-id mono">{e.event_id.clone()}</div>
                                </div>

                                <div class="section-title">"User"</div>
                                <A href=format!("/profiles/{}/{}", e.tenant_id, e.canonical_id) attr:class="event-user-card">
                                    <span inner_html=marble_avatar_svg(&e.canonical_id, 28)></span>
                                    <div>
                                        <div class="user-name">{petname(&e.canonical_id)}</div>
                                        <div class="user-meta">
                                            <span class="mono">{e.canonical_id.clone()}</span>
                                            " · "
                                            {if e.user_id.is_empty() { "Anonymous".to_string() } else { e.user_id.clone() }}
                                        </div>
                                    </div>
                                </A>

                                <div class="section-title">"Event Properties"</div>
                                <div class="event-props">
                                    <div class="prop-label">"Event ID"</div>
                                    <div class="prop-value mono">{e.event_id.clone()}</div>

                                    <div class="prop-label">"Page URL"</div>
                                    <div class="prop-value mono">{e.page_url.clone()}</div>

                                    <div class="prop-label">"Device"</div>
                                    <div class="prop-value">
                                        <span class="device-inline" inner_html=device_svg(&e.device_type)></span>
                                        " "{e.device_type.clone()}" · "{e.browser.clone()}
                                    </div>

                                    <div class="prop-label">"Country"</div>
                                    <div class="prop-value">
                                        <span class="flag">{country_flag(&e.country)}</span>
                                        " "{country_name(&e.country)}
                                    </div>

                                    <div class="prop-label">"Anonymous ID"</div>
                                    <div class="prop-value mono">{e.anonymous_id.clone()}</div>

                                    <div class="prop-label">"Tenant"</div>
                                    <div class="prop-value mono">{e.tenant_id.clone()}</div>
                                </div>
                            </div>

                            <div class="section-title">"Nearby Events"</div>
                            <Suspense fallback=move || view! { <div class="timeline-placeholder"></div> }>
                                {move || {
                                    let current_eid = event_id();
                                    nearby_resource.get().map(|result| match result {
                                        Ok(evts) => {
                                            view! {
                                                <div class="timeline">
                                                    {evts.into_iter().map(|ev| {
                                                        let is_current = ev.event_id == current_eid;
                                                        let item_class = if is_current {
                                                            "timeline-item timeline-item-current"
                                                        } else {
                                                            "timeline-item"
                                                        };
                                                        let href = format!("/events/{}/{}/{}", ev.tenant_id, ev.canonical_id, ev.event_id);
                                                        let event_time = ev.event_time.clone();
                                                        let icon_svg = device_svg(&ev.device_type);
                                                        view! {
                                                            <div class=item_class>
                                                                <A href=href>
                                                                    <div class="time"><RelativeTime timestamp=event_time /></div>
                                                                    <div class="detail">
                                                                        <span class=format!("badge {}", event_type_class(&ev.event_type))>
                                                                            {event_type_label(&ev.event_type)}
                                                                        </span>
                                                                        " "
                                                                        {ev.page_url.clone()}
                                                                        " "
                                                                        <span class="timeline-device">
                                                                            <span class="device-icon active" inner_html=icon_svg></span>
                                                                            {format!(" {}", ev.browser)}
                                                                        </span>
                                                                    </div>
                                                                </A>
                                                            </div>
                                                        }
                                                    }).collect::<Vec<_>>()}
                                                </div>
                                            }.into_any()
                                        }
                                        Err(e) => view! { <div class="loading">{format!("Error: {e}")}</div> }.into_any(),
                                    })
                                }}
                            </Suspense>
                        }.into_any()
                    }
                    Err(e) => view! { <div class="loading">{format!("Error: {e}")}</div> }.into_any(),
                })
            }}
        </Suspense>
    }
}
