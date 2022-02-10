use std::future::Future;
use std::pin::Pin;
use std::task::Poll;

use actix::{Context, Handler, Message};
use futures_util::future::Join;
use pin_project::pin_project;

use crate::metrics::pubsub_metrics as metrics;
use crate::types::{Commitment, Pubkey};

use super::manager::PubSubManager;
use super::worker::AccountUpdateManager;

#[derive(Debug, Eq, PartialEq, Copy, Clone, Hash)]
pub enum Subscription {
    Account(Pubkey),
    Program(Pubkey),
}

#[derive(Message)]
#[rtype(result = "()")]
pub(super) struct PubSubSubscribe {
    pub(super) key: Pubkey,
    pub(super) commitment: Commitment,
    pub(super) owner: Pubkey,
}

type IsSubActiveRequest = actix::prelude::Request<AccountUpdateManager, IsSubActive>;
#[allow(clippy::large_enum_variant)]
#[pin_project (project = SubscriptionActiveProject)]
pub enum SubscriptionActive {
    Ready(bool),
    RequestWithOwner(#[pin] Join<IsSubActiveRequest, IsSubActiveRequest>),
    Request(#[pin] IsSubActiveRequest),
}

#[derive(Message, Debug)]
#[rtype(result = "bool")]
pub struct IsSubActive {
    pub(crate) sub: Subscription,
    pub(crate) commitment: Commitment,
}

impl Subscription {
    pub fn key(&self) -> Pubkey {
        match *self {
            Subscription::Account(key) => key,
            Subscription::Program(key) => key,
        }
    }

    pub fn is_account(&self) -> bool {
        matches!(self, Subscription::Account(_))
    }
}

impl std::fmt::Display for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (prefix, key) = match self {
            Subscription::Account(key) => ("Account", key),
            Subscription::Program(key) => ("Program", key),
        };
        write!(f, "{}({})", prefix, key)
    }
}

impl Handler<IsSubActive> for AccountUpdateManager {
    type Result = bool;

    fn handle(&mut self, item: IsSubActive, _: &mut Context<Self>) -> bool {
        self.sub_to_id.contains_key(&(item.sub, item.commitment))
    }
}

impl Future for SubscriptionActive {
    type Output = bool;

    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        match self.project() {
            SubscriptionActiveProject::Ready(res) => Poll::Ready(*res),
            SubscriptionActiveProject::RequestWithOwner(req) => {
                match req.poll(cx) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready((Ok(true), _)) => {
                        metrics().subscriptions_skipped.inc(); // owner subscription exists
                        Poll::Ready(true)
                    }
                    Poll::Ready((_, Ok(true))) => Poll::Ready(true),
                    Poll::Ready(_) => Poll::Ready(false),
                }
            }

            SubscriptionActiveProject::Request(req) => match req.poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(value)) => Poll::Ready(value),
                Poll::Ready(Err(_)) => Poll::Ready(false),
            },
        }
    }
}

impl Handler<PubSubSubscribe> for PubSubManager {
    type Result = ();
    fn handle(&mut self, msg: PubSubSubscribe, _: &mut Self::Context) -> Self::Result {
        let sub = Subscription::Account(msg.key);
        self.subscribe(sub, msg.commitment, None, Some(msg.owner));
    }
}
