use serde::de::DeserializeOwned;
use serde::Deserialize;
use std::rc::Rc;
use std::cell::{Cell, RefCell};
use wasm_bindgen::prelude::*;
use wasm_bindgen::JsCast;
use web_sys::{MessageEvent, WebSocket, CloseEvent};
use leptos::prelude::*;

use crate::server::api::LiveProfile;

#[derive(Clone, Debug, Deserialize)]
pub struct ProfileUpdateMessage {
    #[serde(alias = "canonicalId")]
    pub canonical_id: String,
    #[serde(alias = "tenantId")]
    pub tenant_id: String,
    pub profile: LiveProfile,
    #[serde(alias = "changedFields")]
    pub changed_fields: Vec<String>,
    pub timestamp: String,
    pub trigger: String,
    #[serde(default)]
    pub action: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LiveEventMessage {
    #[serde(alias = "eventId")]
    pub event_id: String,
    #[serde(alias = "eventType")]
    pub event_type: String,
    #[serde(alias = "tenantId")]
    pub tenant_id: String,
    #[serde(alias = "eventTime")]
    pub event_time: String,
    #[serde(alias = "canonicalId")]
    pub canonical_id: String,
    #[serde(alias = "anonymousId")]
    pub anonymous_id: String,
    #[serde(alias = "userId")]
    pub user_id: String,
    #[serde(alias = "pageUrl")]
    pub page_url: String,
    #[serde(alias = "deviceType")]
    pub device_type: String,
    pub browser: String,
    pub country: String,
}

#[derive(Clone, Copy)]
pub struct ProfileStream(pub RwSignal<Option<ProfileUpdateMessage>>);

#[derive(Clone, Copy)]
pub struct EventStream(pub RwSignal<Option<LiveEventMessage>>);

pub struct WsHandle {
    cancelled: Rc<Cell<bool>>,
    ws: Rc<RefCell<Option<WebSocket>>>,
}

impl WsHandle {
    pub fn disconnect(&self) {
        self.cancelled.set(true);
        if let Some(ws) = self.ws.borrow_mut().take() {
            ws.set_onclose(None);
            ws.set_onerror(None);
            ws.set_onmessage(None);
            ws.set_onopen(None);
            let _ = ws.close();
        }
    }
}

pub fn provide_stream_contexts() {
    let profile_signal: RwSignal<Option<ProfileUpdateMessage>> = RwSignal::new(None);
    let event_signal: RwSignal<Option<LiveEventMessage>> = RwSignal::new(None);
    provide_context(ProfileStream(profile_signal));
    provide_context(EventStream(event_signal));
}

const PROFILE_FIELDS: &str = "canonicalId tenantId profile { canonicalId userId tenantId firstSeen lastSeen totalEvents totalSessions events1D events7D events30D events90D sessions1D sessions7D avgSessionDurationSec currentSessionActive currentSessionDurationSec pageViews clicks logins featureUses lastPage lastCountry lastDevice lastBrowser topPages topFeatures } changedFields timestamp trigger action";

const EVENT_FIELDS: &str = "eventId eventType tenantId eventTime canonicalId anonymousId userId pageUrl deviceType browser country";

pub fn subscribe_profile_updates(
    signal: RwSignal<Option<ProfileUpdateMessage>>,
) -> WsHandle {
    let query = format!("subscription {{ profileUpdates {{ {} }} }}", PROFILE_FIELDS);
    let msg = serde_json::json!({
        "type": "subscribe", "id": "1",
        "payload": { "query": query }
    })
    .to_string();
    connect_graphql_ws(msg, "profileUpdates", make_profile_cb(signal))
}

pub fn subscribe_profile_update(
    tenant_id: &str,
    canonical_id: &str,
    signal: RwSignal<Option<ProfileUpdateMessage>>,
) -> WsHandle {
    let query = format!(
        "subscription($tenantId: String!, $canonicalId: String!) {{ profileUpdate(tenantId: $tenantId, canonicalId: $canonicalId) {{ {} }} }}",
        PROFILE_FIELDS
    );
    let msg = serde_json::json!({
        "type": "subscribe", "id": "1",
        "payload": {
            "query": query,
            "variables": { "tenantId": tenant_id, "canonicalId": canonical_id }
        }
    })
    .to_string();
    connect_graphql_ws(msg, "profileUpdate", make_profile_cb(signal))
}

pub fn subscribe_live_events(
    signal: RwSignal<Option<LiveEventMessage>>,
) -> WsHandle {
    let query = format!("subscription {{ liveEvents {{ {} }} }}", EVENT_FIELDS);
    let msg = serde_json::json!({
        "type": "subscribe", "id": "1",
        "payload": { "query": query }
    })
    .to_string();
    connect_graphql_ws(msg, "liveEvents", make_event_cb(signal))
}

pub fn subscribe_live_events_filtered(
    tenant_id: &str,
    signal: RwSignal<Option<LiveEventMessage>>,
) -> WsHandle {
    let query = format!(
        "subscription($tenantId: String) {{ liveEvents(tenantId: $tenantId) {{ {} }} }}",
        EVENT_FIELDS
    );
    let msg = serde_json::json!({
        "type": "subscribe", "id": "1",
        "payload": {
            "query": query,
            "variables": { "tenantId": tenant_id }
        }
    })
    .to_string();
    connect_graphql_ws(msg, "liveEvents", make_event_cb(signal))
}

fn make_profile_cb(
    signal: RwSignal<Option<ProfileUpdateMessage>>,
) -> Rc<dyn Fn(ProfileUpdateMessage)> {
    Rc::new(move |msg: ProfileUpdateMessage| {
        leptos::task::spawn_local(async move {
            let _ = signal.try_set(Some(msg));
        });
    })
}

fn make_event_cb(signal: RwSignal<Option<LiveEventMessage>>) -> Rc<dyn Fn(LiveEventMessage)> {
    Rc::new(move |msg: LiveEventMessage| {
        leptos::task::spawn_local(async move {
            let _ = signal.try_set(Some(msg));
        });
    })
}

#[derive(Deserialize)]
struct GqlWsMessage {
    #[serde(rename = "type")]
    msg_type: String,
    #[serde(default)]
    _id: Option<String>,
    #[serde(default)]
    payload: Option<serde_json::Value>,
}

fn build_ws_url(path: &str) -> String {
    let window = web_sys::window().expect("no window");
    let location = window.location();
    let protocol = location.protocol().unwrap_or_else(|_| "http:".into());
    let host = location.host().unwrap_or_else(|_| "localhost".into());
    let ws_protocol = if protocol == "https:" { "wss:" } else { "ws:" };
    format!("{ws_protocol}//{host}{path}")
}

fn connect_graphql_ws<T: DeserializeOwned + 'static>(
    subscribe_msg: String,
    data_field: &'static str,
    on_update: Rc<dyn Fn(T)>,
) -> WsHandle {
    let cancelled = Rc::new(Cell::new(false));
    let ws_ref = Rc::new(RefCell::new(None));
    do_connect(
        subscribe_msg,
        data_field,
        on_update,
        cancelled.clone(),
        ws_ref.clone(),
    );
    WsHandle {
        cancelled,
        ws: ws_ref,
    }
}

fn do_connect<T: DeserializeOwned + 'static>(
    subscribe_msg: String,
    data_field: &'static str,
    on_update: Rc<dyn Fn(T)>,
    cancelled: Rc<Cell<bool>>,
    ws_ref: Rc<RefCell<Option<WebSocket>>>,
) {
    if cancelled.get() {
        return;
    }

    let url = build_ws_url("/graphql/ws");

    let ws = match WebSocket::new_with_str(&url, "graphql-transport-ws") {
        Ok(ws) => ws,
        Err(_) => {
            schedule_reconnect(subscribe_msg, data_field, on_update, cancelled, ws_ref);
            return;
        }
    };

    *ws_ref.borrow_mut() = Some(ws.clone());

    let ws_clone = ws.clone();
    let msg_for_open = subscribe_msg.clone();
    let onopen = Closure::<dyn FnMut()>::new(move || {
        let init = r#"{"type":"connection_init"}"#;
        let _ = ws_clone.send_with_str(init);
        let _ = ws_clone.send_with_str(&msg_for_open);
    });
    ws.set_onopen(Some(onopen.as_ref().unchecked_ref()));
    onopen.forget();

    let on_update_msg = on_update.clone();
    let onmessage = Closure::<dyn FnMut(MessageEvent)>::new(move |event: MessageEvent| {
        if let Some(text) = event.data().as_string() {
            if let Ok(msg) = serde_json::from_str::<GqlWsMessage>(&text) {
                match msg.msg_type.as_str() {
                    "connection_ack" => {}
                    "ping" => {
                        if let Some(ws) = event
                            .target()
                            .and_then(|t| t.dyn_into::<WebSocket>().ok())
                        {
                            let _ = ws.send_with_str(r#"{"type":"pong"}"#);
                        }
                    }
                    "next" => {
                        if let Some(payload) = msg.payload {
                            if let Some(data) = payload.get("data") {
                                if let Some(update_val) = data.get(data_field) {
                                    if let Ok(update) =
                                        serde_json::from_value::<T>(update_val.clone())
                                    {
                                        on_update_msg(update);
                                    }
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    });
    ws.set_onmessage(Some(onmessage.as_ref().unchecked_ref()));
    onmessage.forget();

    let closed = Rc::new(Cell::new(false));

    let reconnect_cb = on_update.clone();
    let closed_close = closed.clone();
    let cancelled_close = cancelled.clone();
    let ws_ref_close = ws_ref.clone();
    let msg_close = subscribe_msg.clone();
    let onclose = Closure::<dyn FnMut(CloseEvent)>::new(move |_: CloseEvent| {
        if !closed_close.get() {
            closed_close.set(true);
            schedule_reconnect(
                msg_close.clone(),
                data_field,
                reconnect_cb.clone(),
                cancelled_close.clone(),
                ws_ref_close.clone(),
            );
        }
    });
    ws.set_onclose(Some(onclose.as_ref().unchecked_ref()));
    onclose.forget();

    let err_cb = on_update;
    let closed_err = closed;
    let cancelled_err = cancelled;
    let ws_ref_err = ws_ref;
    let msg_err = subscribe_msg;
    let onerror = Closure::<dyn FnMut()>::new(move || {
        if !closed_err.get() {
            closed_err.set(true);
            schedule_reconnect(
                msg_err.clone(),
                data_field,
                err_cb.clone(),
                cancelled_err.clone(),
                ws_ref_err.clone(),
            );
        }
    });
    ws.set_onerror(Some(onerror.as_ref().unchecked_ref()));
    onerror.forget();
}

fn schedule_reconnect<T: DeserializeOwned + 'static>(
    subscribe_msg: String,
    data_field: &'static str,
    on_update: Rc<dyn Fn(T)>,
    cancelled: Rc<Cell<bool>>,
    ws_ref: Rc<RefCell<Option<WebSocket>>>,
) {
    if cancelled.get() {
        return;
    }

    let timeout = Closure::<dyn FnMut()>::new(move || {
        do_connect(
            subscribe_msg.clone(),
            data_field,
            on_update.clone(),
            cancelled.clone(),
            ws_ref.clone(),
        );
    });
    let window = web_sys::window().expect("no window");
    let _ = window.set_timeout_with_callback_and_timeout_and_arguments_0(
        timeout.as_ref().unchecked_ref(),
        3000,
    );
    timeout.forget();
}
