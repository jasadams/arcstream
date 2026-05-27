use leptos::prelude::*;

#[component]
pub fn LiveToggle(paused: RwSignal<bool>) -> impl IntoView {
    let toggle = move |_| paused.set(!paused.get_untracked());

    view! {
        <button
            class=move || if paused.get() { "live-toggle paused" } else { "live-toggle" }
            on:click=toggle
        >
            <span class="live-dot"></span>
            {move || if paused.get() { "Paused" } else { "Live" }}
        </button>
    }
}
