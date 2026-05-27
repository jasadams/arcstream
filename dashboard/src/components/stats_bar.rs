use leptos::prelude::*;
use crate::app::Tick;
use crate::server::api::get_dashboard_stats;

const MAX_DIGITS: usize = 10;

#[component]
pub fn StatsBar() -> impl IntoView {
    let tick = use_context::<Tick>();
    let stats = Resource::new(
        move || tick.map(|t| t.0.get()).unwrap_or(0),
        |_| get_dashboard_stats(),
    );

    let total_users = RwSignal::new(0u64);
    let total_events = RwSignal::new(0u64);
    let active_sessions = RwSignal::new(0u64);
    let loaded = RwSignal::new(false);

    Effect::new(move || {
        if let Some(Ok(s)) = stats.get() {
            total_users.set(s.total_users);
            total_events.set(s.total_events);
            active_sessions.set(s.active_sessions);
            if !loaded.get_untracked() {
                loaded.set(true);
            }
        }
    });

    view! {
        <div class="stats-row">
            <div class="stat-card">
                <div class="label">"Total Users"</div>
                <div class="value">
                    {move || if loaded.get() {
                        view! { <RollingCounter value=Signal::from(total_users) /> }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
            </div>
            <div class="stat-card">
                <div class="label">"Total Events"</div>
                <div class="value">
                    {move || if loaded.get() {
                        view! { <RollingCounter value=Signal::from(total_events) /> }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
            </div>
            <div class="stat-card">
                <div class="label">"Active Sessions"</div>
                <div class="value">
                    {move || if loaded.get() {
                        view! { <RollingCounter value=Signal::from(active_sessions) /> }.into_any()
                    } else {
                        view! { <span>"\u{2014}"</span> }.into_any()
                    }}
                </div>
            </div>
        </div>
    }
}

#[component]
pub fn RollingCounter(value: Signal<u64>) -> impl IntoView {
    let num_digits = Memo::new(move |_| {
        let n = value.get();
        if n == 0 { 1usize } else { (n as f64).log10().floor() as usize + 1 }
    });

    view! {
        <span class="rolling-counter">
            {(0..MAX_DIGITS).rev().map(|pos| {
                let visible = Memo::new(move |_| pos < num_digits.get());
                let show_comma = pos > 0 && pos % 3 == 0;

                view! {
                    <span
                        class="counter-slot"
                        style=move || if visible.get() { "" } else { "display:none" }
                    >
                        <RollingDigit value=value position=pos />
                        {show_comma.then(|| view! { <span class="counter-comma">","</span> })}
                    </span>
                }
            }).collect::<Vec<_>>()}
        </span>
    }
}

/// Single rolling digit with a 30-item strip (0-9 × 3).
/// Home positions are 10-19 (middle decade).
/// Direction is determined by the TOTAL value change, not per-digit shortest path.
#[component]
fn RollingDigit(value: Signal<u64>, position: usize) -> impl IntoView {
    let extract_digit = move |v: u64| ((v / 10u64.pow(position as u32)) % 10) as u32;

    let initial_val = value.get_untracked();
    let initial_d = extract_digit(initial_val);
    let pos = RwSignal::new(10i32 + initial_d as i32);
    let animate = RwSignal::new(true);
    let prev_value = StoredValue::new(initial_val);
    let prev_digit = StoredValue::new(initial_d);

    Effect::new(move || {
        let new_val = value.get();
        let new_d = extract_digit(new_val);
        let old_val = prev_value.get_value();
        let old_d = prev_digit.get_value();
        prev_value.set_value(new_val);
        prev_digit.set_value(new_d);

        if new_d == old_d { return; }

        let going_up = new_val >= old_val;

        let target = if going_up {
            if new_d < old_d { 20 + new_d as i32 } else { 10 + new_d as i32 }
        } else {
            if new_d > old_d { new_d as i32 } else { 10 + new_d as i32 }
        };

        animate.set(true);
        pos.set(target);
    });

    view! {
        <span class="rolling-digit">
            <span
                class="digit-roll"
                class:no-transition=move || !animate.get()
                style=move || format!("transform: translateY(-{}em)", pos.get())
                on:transitionend=move |_| {
                    let current = pos.get_untracked();
                    if current < 10 || current >= 20 {
                        let home = 10 + extract_digit(value.get_untracked()) as i32;
                        animate.set(false);
                        pos.set(home);
                        #[cfg(feature = "hydrate")]
                        {
                            use wasm_bindgen::prelude::*;
                            use wasm_bindgen::JsCast;
                            let cb = Closure::once(move || {
                                animate.set(true);
                            });
                            let _ = web_sys::window().unwrap()
                                .set_timeout_with_callback_and_timeout_and_arguments_0(
                                    cb.as_ref().unchecked_ref(),
                                    50,
                                );
                            cb.forget();
                        }
                    }
                }
            >
                {(0..30).map(|i| {
                    view! { <span>{(i % 10).to_string()}</span> }
                }).collect::<Vec<_>>()}
            </span>
        </span>
    }
}
