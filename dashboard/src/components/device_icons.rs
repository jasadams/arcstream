use leptos::prelude::*;
use crate::util::{SVG_MONITOR, SVG_TABLET, SVG_MOBILE};

#[component]
pub fn DeviceIcons(#[prop(into)] active: String) -> impl IntoView {
    let active = active.to_lowercase();
    let desktop_cls = if active == "desktop" { "device-icon active" } else { "device-icon" };
    let tablet_cls = if active == "tablet" { "device-icon active" } else { "device-icon" };
    let mobile_cls = if active == "mobile" { "device-icon active" } else { "device-icon" };

    view! {
        <span class="device-icons">
            <span class=desktop_cls title="Desktop" inner_html=SVG_MONITOR></span>
            <span class=tablet_cls title="Tablet" inner_html=SVG_TABLET></span>
            <span class=mobile_cls title="Mobile" inner_html=SVG_MOBILE></span>
        </span>
    }
}
