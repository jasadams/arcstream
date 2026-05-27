use async_graphql::{Context, Subscription};
use futures_util::Stream;
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::streaming::types::{LiveEventMessage, ProfileUpdateMessage};

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    async fn profile_updates(
        &self,
        ctx: &Context<'_>,
        tenant_id: Option<String>,
    ) -> impl Stream<Item = ProfileUpdateMessage> {
        let sender = ctx
            .data::<broadcast::Sender<ProfileUpdateMessage>>()
            .expect("broadcast sender not in context");

        BroadcastStream::new(sender.subscribe()).filter_map(move |result| match result {
            Ok(msg) => {
                if let Some(ref tid) = tenant_id {
                    if &msg.tenant_id != tid {
                        return None;
                    }
                }
                Some(msg)
            }
            _ => None,
        })
    }

    async fn profile_update(
        &self,
        ctx: &Context<'_>,
        tenant_id: String,
        canonical_id: String,
    ) -> impl Stream<Item = ProfileUpdateMessage> {
        let sender = ctx
            .data::<broadcast::Sender<ProfileUpdateMessage>>()
            .expect("broadcast sender not in context");

        BroadcastStream::new(sender.subscribe()).filter_map(move |result| match result {
            Ok(msg)
                if msg.tenant_id == tenant_id && msg.canonical_id == canonical_id =>
            {
                Some(msg)
            }
            _ => None,
        })
    }

    async fn live_events(
        &self,
        ctx: &Context<'_>,
        tenant_id: Option<String>,
    ) -> impl Stream<Item = LiveEventMessage> {
        let sender = ctx
            .data::<broadcast::Sender<LiveEventMessage>>()
            .expect("event broadcast sender not in context");

        BroadcastStream::new(sender.subscribe()).filter_map(move |result| match result {
            Ok(msg) => {
                if let Some(ref tid) = tenant_id {
                    if &msg.tenant_id != tid {
                        return None;
                    }
                }
                Some(msg)
            }
            _ => None,
        })
    }
}
