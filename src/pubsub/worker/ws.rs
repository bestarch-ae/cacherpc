use std::collections::{HashMap, HashSet};

use actix::{io::SinkWrite, ActorContext, Context, Running, StreamHandler};
use actix_http::ws;
use bytes::BytesMut;
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::time::Instant;

use tracing::{error, info, warn};

use crate::{
    pubsub::subscription::Subscription,
    types::{AccountContext, AccountInfo, Commitment, Pubkey, SolanaContext},
};

use super::AccountUpdateManager;
use crate::metrics::pubsub_metrics as metrics;

type WsSink = SinkWrite<
    awc::ws::Message,
    futures_util::stream::SplitSink<
        actix_codec::Framed<awc::BoxedSocket, awc::ws::Codec>,
        awc::ws::Message,
    >,
>;

#[derive(Debug)]
pub(super) enum InflightRequest {
    Sub(Subscription, Commitment),
    Unsub(Subscription, Commitment),
    SlotSub(u64),
}

pub(super) enum Connection {
    Disconnected,
    Connecting,
    Connected { sink: WsSink },
}

impl Connection {
    pub(super) fn is_connected(&self) -> bool {
        matches!(self, Connection::Connected { .. })
    }

    pub(super) fn send(&mut self, msg: ws::Message) -> Result<(), ws::Message> {
        if let Connection::Connected { sink, .. } = self {
            sink.write(msg)?
        }
        Ok(())
    }
}

impl AccountUpdateManager {
    fn process_ws_message(
        &mut self,
        ctx: &mut Context<Self>,
        text: &[u8],
    ) -> Result<(), serde_json::Error> {
        #[derive(Deserialize, Debug)]
        struct AnyMessage<'a> {
            #[serde(borrow)]
            result: Option<&'a RawValue>,
            #[serde(borrow)]
            method: Option<&'a str>,
            id: Option<u64>,
            #[serde(borrow)]
            params: Option<&'a RawValue>,
            #[serde(borrow)]
            error: Option<&'a RawValue>,
        }
        let value: AnyMessage<'_> = serde_json::from_slice(text)?;
        match value {
            // subscription error
            AnyMessage {
                error: Some(error),
                id: Some(id),
                ..
            } => {
                if let Some((req, _time)) = self.inflight.remove(&id) {
                    match req {
                        InflightRequest::Sub(sub, commitment) => {
                            warn!(self.actor_id, request_id = id, error = ?error, key = %sub.key(), commitment = ?commitment, "subscribe failed");
                            metrics()
                                .subscribe_errors
                                .with_label_values(&[&self.actor_name])
                                .inc();
                            self.subs.remove(&(sub, commitment));
                            if sub.is_account() {
                                self.active_accounts.remove(&(sub.key(), commitment));
                            }
                            self.purge_key(ctx, &sub, commitment);
                        }
                        InflightRequest::Unsub(sub, commitment) => {
                            warn!(self.actor_id, request_id = id, error = ?error, key = %sub.key(), commitment = ?commitment, "unsubscribe failed");
                            metrics()
                                .unsubscribe_errors
                                .with_label_values(&[&self.actor_name])
                                .inc();
                            // it's unclear if we're subscribed now or not, so
                            // remove subscription *and* key to resubscribe later
                            if let Some(id) = self.sub_to_id.remove(&(sub, commitment)) {
                                self.id_to_sub.remove(&id);
                            }
                            self.subs.remove(&(sub, commitment));
                            if sub.is_account() {
                                self.active_accounts.remove(&(sub.key(), commitment));
                            }
                            // no need to call `purge_key` as unsubscription request can only be
                            // called from `Purge` command, which calls it for us
                        }
                        InflightRequest::SlotSub(_) => {
                            warn!(self.actor_id, request_id = id, error = ?error, "slot subscribe failed");
                        }
                    }
                }
                metrics()
                    .inflight_entries
                    .with_label_values(&[&self.actor_name])
                    .set(self.inflight.len() as i64);
                self.update_status();
            }
            // subscription response
            AnyMessage {
                result: Some(result),
                id: Some(id),
                ..
            } => {
                if let Some((req, sent_at)) = self.inflight.remove(&id) {
                    match req {
                        InflightRequest::Sub(sub, commitment) => {
                            let sub_id: u64 = serde_json::from_str(result.get())?;
                            self.id_to_sub.insert(sub_id, (sub, commitment));
                            self.sub_to_id.insert((sub, commitment), sub_id);

                            if let Subscription::Account(key) = sub {
                                if let Some(key_ref) = self
                                    .accounts
                                    .get(&key)
                                    .and_then(|data| data.get_ref(commitment))
                                {
                                    self.active_accounts.insert((key, commitment), key_ref);
                                }
                            }

                            metrics()
                                .id_sub_entries
                                .with_label_values(&[&self.actor_name])
                                .set(self.id_to_sub.len() as i64);

                            metrics()
                                .sub_id_entries
                                .with_label_values(&[&self.actor_name])
                                .set(self.sub_to_id.len() as i64);

                            info
                                !(self.actor_id, message = "subscribed to stream",
                                sub_id = sub_id, sub = %sub, commitment = ?commitment, time = ?sent_at.elapsed());
                            metrics()
                                .time_to_subscribe
                                .with_label_values(&[&self.actor_name])
                                .observe(sent_at.elapsed().as_secs_f64());
                            metrics()
                                .subscriptions_active
                                .with_label_values(&[&self.actor_name])
                                .inc();
                        }
                        InflightRequest::Unsub(sub, commitment) => {
                            let is_ok: bool = serde_json::from_str(result.get())?;
                            if is_ok {
                                if let Some(sub_id) = self.sub_to_id.remove(&(sub, commitment)) {
                                    self.id_to_sub.remove(&sub_id);
                                    let created_at = self.subs.remove(&(sub, commitment));
                                    if sub.is_account() {
                                        self.active_accounts.remove(&(sub.key(), commitment));
                                    }
                                    info!(
                                        self.actor_id,
                                        message = "unsubscribed from stream",
                                        sub_id = sub_id,
                                        key = %sub.key(),
                                        sub = %sub,
                                        time = ?sent_at.elapsed(),
                                    );
                                    metrics()
                                        .subscriptions_active
                                        .with_label_values(&[&self.actor_name])
                                        .dec();
                                    if let Some(times) = created_at {
                                        metrics()
                                            .subscription_lifetime
                                            .observe(times.since_creation().as_secs_f64());
                                    }
                                // no need to call `purge_key` as unsubscription request can only be
                                // called from `Purge` command, which calls it for us
                                } else {
                                    warn
                                        !(self.actor_id, sub = %sub, commitment = ?commitment, "unsubscribe for unknown subscription");
                                }
                                metrics()
                                    .id_sub_entries
                                    .with_label_values(&[&self.actor_name])
                                    .set(self.id_to_sub.len() as i64);

                                metrics()
                                    .sub_id_entries
                                    .with_label_values(&[&self.actor_name])
                                    .set(self.sub_to_id.len() as i64);
                            } else {
                                warn!(self.actor_id, message = "unsubscribe failed", key = %sub.key());
                            }
                        }
                        InflightRequest::SlotSub(_) => {
                            info!(self.actor_id, message = "subscribed to slots");
                        }
                    }
                }
                self.update_status();
            }
            // notification
            AnyMessage {
                method: Some(method),
                params: Some(params),
                ..
            } => {
                match method {
                    "accountNotification" => {
                        #[derive(Deserialize, Debug)]
                        struct Params {
                            result: AccountContext,
                            subscription: u64,
                        }
                        let params: Params = serde_json::from_str(params.get())?;
                        let slot = params.result.context.slot;
                        if let Some((sub, commitment)) = self.id_to_sub.get(&params.subscription) {
                            // overwrite account entry in cache
                            self.accounts
                                .insert(sub.key(), params.result, *commitment, true);
                        } else {
                            warn!(
                                self.actor_id,
                                message = "unknown subscription",
                                sub = params.subscription
                            );
                        }
                        metrics()
                            .pubsub_account_slot
                            .with_label_values(&[&self.actor_name])
                            .set(slot as i64);
                        metrics()
                            .notifications_received
                            .with_label_values(&[&self.actor_name, "accountNotification"])
                            .inc();
                    }
                    "programNotification" => {
                        #[derive(Deserialize, Debug)]
                        struct Value {
                            account: AccountInfo,
                            pubkey: Pubkey,
                        }
                        #[derive(Deserialize, Debug)]
                        struct Result {
                            context: SolanaContext,
                            value: Value,
                        }
                        #[derive(Deserialize, Debug)]
                        struct Params {
                            result: Result,
                            subscription: u64,
                        }
                        let params: Params = serde_json::from_str(params.get())?;
                        if let Some((program_sub, commitment)) =
                            self.id_to_sub.get(&params.subscription)
                        {
                            let slot = params.result.context.slot;
                            let program_key = program_sub.key();

                            if let Some(meta) = self.subs.get_mut(&(*program_sub, *commitment)) {
                                if meta.first_slot.is_none() {
                                    info!(program = %program_key, slot, "first update for program");
                                    meta.first_slot.replace(slot);
                                }
                            }
                            metrics()
                                .pubsub_program_slot
                                .with_label_values(&[&self.actor_name])
                                .set(slot as i64);

                            let key = params.result.value.pubkey;
                            let account_info = &params.result.value.account;
                            let data = &account_info.data;

                            let filter_groups =
                                match self.additional_keys.get(&(program_key, *commitment)) {
                                    Some(tree) => {
                                        let filtration_starts = Instant::now();
                                        let mut groups = HashSet::new();
                                        tree.map_matches(data, |filter| {
                                            groups.insert(filter);
                                        });

                                        metrics()
                                        .filtration_time
                                        .with_label_values(&[&self.actor_name])
                                        .observe(filtration_starts.elapsed().as_micros() as f64);
                                        groups
                                    }
                                    None => HashSet::new(),
                                };

                            // overwrite account entry in cache
                            let key_ref = self.accounts.insert(
                                key,
                                AccountContext {
                                    value: Some(params.result.value.account),
                                    context: params.result.context,
                                },
                                *commitment,
                                true,
                            );

                            let should_remove_account = self.program_accounts.update_account(
                                &(program_key, *commitment),
                                key_ref,
                                filter_groups,
                                slot,
                            );
                            if should_remove_account {
                                self.accounts.remove(&key, *commitment);
                            }
                        } else {
                            warn!(
                                self.actor_id,
                                message = "unknown subscription",
                                sub = params.subscription
                            );
                        }
                        metrics()
                            .notifications_received
                            .with_label_values(&[&self.actor_name, "programNotification"])
                            .inc();
                    }
                    "slotNotification" => {
                        #[derive(Deserialize)]
                        struct SlotInfo {
                            slot: u64,
                        }
                        #[derive(Deserialize)]
                        struct Params {
                            result: SlotInfo,
                        }
                        let params: Params = serde_json::from_str(params.get())?;
                        //info!("slot {} root {} parent {}", params.result.slot, params.result.root, params.result.parent);
                        let slot = params.result.slot; // TODO: figure out which slot validator *actually* reports
                        metrics()
                            .pubsub_slot
                            .with_label_values(&[&self.actor_name])
                            .set(slot as i64);
                        metrics()
                            .notifications_received
                            .with_label_values(&[&self.actor_name, "slotNotification"])
                            .inc();
                        self.check_slot(slot, ctx);
                    }
                    _ => {
                        warn!(
                            self.actor_id,
                            message = "unknown notification",
                            method = method
                        );
                    }
                }
            }
            any => {
                warn!(self.actor_id, msg = ?any, text = ?text, "unidentified websocket message");
            }
        }

        Ok(())
    }
}
impl StreamHandler<Result<awc::ws::Frame, awc::error::WsProtocolError>> for AccountUpdateManager {
    fn handle(
        &mut self,
        item: Result<awc::ws::Frame, awc::error::WsProtocolError>,
        ctx: &mut Context<Self>,
    ) {
        self.last_received_at = Instant::now();

        let item = match item {
            Ok(item) => item,
            Err(err) => {
                error!(self.actor_id, error = %err, "websocket read error");
                metrics()
                    .websocket_errors
                    .with_label_values(&[&self.actor_name, "read"])
                    .inc();
                ctx.stop();
                return;
            }
        };

        let _ = (|| -> Result<(), serde_json::Error> {
            use awc::ws::Frame;

            match item {
                Frame::Ping(data) => {
                    metrics().bytes_received.with_label_values(&[&self.actor_name]).inc_by(data.len() as u64);
                    if let Connection::Connected { sink, .. } = &mut self.connection {
                        if sink.write(awc::ws::Message::Pong(data)).is_err() {
                            warn!("Websocket channel is closed!");
                        }
                    }
                }
                Frame::Pong(_) => {
                    // do nothing
                }
                Frame::Text(text) => {
                    metrics().bytes_received.with_label_values(&[&self.actor_name]).inc_by(text.len() as u64);
                    self.process_ws_message(ctx, &text).map_err(|err| {
                            error!(error = %err, bytes = ?text, "error while parsing message");
                            err
                        })?
                }
                Frame::Close(reason) => {
                    warn!(self.actor_id, reason = ?reason, "websocket closing");
                    ctx.stop();
                }
                Frame::Binary(msg) => {
                    warn!(self.actor_id, msg = ?msg, "unexpected binary message");
                }
                Frame::Continuation(msg) => match msg {
                    ws ::Item::FirstText(bytes) => {
                        metrics().bytes_received.with_label_values(&[&self.actor_name])
                            .inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                    }
                    ws::Item::Continue(bytes) => {
                        metrics().bytes_received
                            .with_label_values(&[&self.actor_name])
                            .inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                    }
                    ws::Item::Last(bytes) => {
                        metrics().bytes_received
                            .with_label_values(&[&self.actor_name])
                            .inc_by(bytes.len() as u64);
                        self.buffer.extend(&bytes);
                        let text = std::mem::replace(&mut self.buffer, BytesMut
                            ::new());
                        self.process_ws_message(ctx, &text).map_err(|err| {
                            error!(error = %err, bytes = ?text, "error while parsing fragmented message");
                            err
                        })?;
                    }
                    ws::Item::FirstBinary(_) => {
                        warn!(self.actor_id, msg = ?msg, "unexpected continuation message");
                    }
                },
            }
            Ok(())
        })()
        .map_err(|err| {
            error!(message = "error handling Frame", error = ?err);
        });
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!(self.actor_id, "websocket connected");
        // subscribe to slots
        let request_id = self.next_request_id();
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "slotSubscribe",
        });
        self.inflight.insert(
            request_id,
            (InflightRequest::SlotSub(request_id), Instant::now()),
        );
        metrics()
            .inflight_entries
            .with_label_values(&[&self.actor_name])
            .set(self.inflight.len() as i64);

        let _ = self.send(&request);

        // restore subscriptions
        info!(self.actor_id, "adding subscriptions");
        let subs_len = self.subs.len();
        let subs = std::mem::replace(&mut self.subs, HashMap::with_capacity(subs_len));

        for ((sub, commitment), _) in subs {
            self.subscribe(sub, commitment).unwrap();
            // TODO: it would be nice to retrieve current state for
            // everything we had before
        }
    }

    fn finished(&mut self, _: &mut Context<Self>) {
        info!(self.actor_id, "websocket stream finished");
        // no need to restart actor here, as there's a lot of other
        // ways to detect stream termination and restart actor
    }
}

impl actix::io::WriteHandler<awc::error::WsProtocolError> for AccountUpdateManager {
    fn error(&mut self, err: awc::error::WsProtocolError, _ctx: &mut Self::Context) -> Running {
        error!(self.actor_id, message = "websocket write error", error = ?err);
        metrics()
            .websocket_errors
            .with_label_values(&[&self.actor_name, "write"])
            .inc();
        Running::Stop
    }

    fn finished(&mut self, _ctx: &mut Self::Context) {
        info!(self.actor_id, "writer closed");
    }
}
