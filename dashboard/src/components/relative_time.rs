use leptos::prelude::*;
use crate::app::Tick;
use crate::util::{relative_time, absolute_time};

#[component]
pub fn RelativeTime(
    #[prop(into)] timestamp: Signal<String>,
) -> impl IntoView {
    let tick = use_context::<Tick>();

    view! {
        <span
            class="time-relative"
            title=move || absolute_time(&timestamp.get())
        >
            {move || {
                if let Some(Tick(t)) = tick {
                    let _ = t.get();
                }
                relative_time(&timestamp.get())
            }}
        </span>
    }
}
