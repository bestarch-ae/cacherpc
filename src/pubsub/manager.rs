use std::iter::repeat;
use std::time::Duration;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, SpawnHandle};
use futures_util::future::join;
use serde::{Deserialize, Serialize};
use tracing::warn;

use crate::metrics::pubsub_metrics as metrics;
use crate::{
    filter::Filters,
    types::{AccountsDb, Commitment, ProgramAccountsDb, Pubkey},
};

use super::subscription::{IsSubActive, Subscription, SubscriptionActive};
use super::worker::{autocmd::AccountCommand, AccountUpdateManager};

#[derive(Clone)]
pub struct PubSubManager {
    workers: Vec<(actix::Addr<AccountUpdateManager>, Arc<AtomicBool>)>,
    /// indicates whether ws reconnections is running, and
    /// contains the spawn handles to cancel those reconnections
    reconnections: VecDeque<(Addr<AccountUpdateManager>, SpawnHandle)>,
    subscriptions_allowed: Arc<AtomicBool>,
}

pub struct WorkerConfig {
    pub ttl: Duration,      // time to live
    pub slot_distance: u32, // slot distance
    pub websocket_url: String,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct InitManager(pub Addr<PubSubManager>);

#[derive(Deserialize, Debug, Serialize, Message)]
#[rtype(result = "String")]
pub(crate) enum WsReconnectInstruction {
    Init { delay: u64, interval: u64 },
    Status,
    Abort,
}

#[derive(Message)]
#[rtype(result = "()")]
pub struct ForceReconnect;

impl PubSubManager {
    pub fn init(
        connections: u32,
        accounts: AccountsDb,
        program_accounts: ProgramAccountsDb,
        rpc_slot: Arc<AtomicU64>,
        worker_config: WorkerConfig,
        subscriptions_allowed: Arc<AtomicBool>,
    ) -> (Self, Addr<Self>) {
        let mut workers = Vec::new();
        let config = Arc::new(worker_config);
        for id in 0..connections {
            let connected = Arc::new(AtomicBool::new(false));
            let addr = AccountUpdateManager::init(
                id,
                accounts.clone(),
                program_accounts.clone(),
                Arc::clone(&connected),
                rpc_slot.clone(),
                Arc::clone(&config),
            );
            workers.push((addr, connected))
        }
        let reconnections = VecDeque::with_capacity(workers.len());
        let manager = PubSubManager {
            workers,
            subscriptions_allowed,
            reconnections,
        };
        let actor = manager.clone().start();
        // make sure all the workers, have an address of pubsub manager
        for (w, _) in &manager.workers {
            w.do_send(InitManager(Addr::clone(&actor)));
        }

        (manager, actor)
    }

    fn get_idx_by_key(&self, key: (Pubkey, Commitment)) -> usize {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let hash = hasher.finish();
        (hash % self.workers.len() as u64) as usize
    }

    fn get_addr_by_key(&self, key: (Pubkey, Commitment)) -> actix::Addr<AccountUpdateManager> {
        let idx = self.get_idx_by_key(key);
        self.workers[idx].0.clone()
    }

    pub fn websocket_connected(&self, key: (Pubkey, Commitment)) -> bool {
        let idx = self.get_idx_by_key(key);
        self.workers[idx].1.load(Ordering::Relaxed)
    }

    pub fn subscription_active(
        &self,
        sub: Subscription,
        commitment: Commitment,
        owner: Option<Pubkey>,
    ) -> SubscriptionActive {
        if self.websocket_connected((sub.key(), commitment)) {
            let addr = self.get_addr_by_key((sub.key(), commitment));

            owner
                .map(|key| (self.get_addr_by_key((key, commitment)), key))
                .map(|(owner_addr, key)| {
                    SubscriptionActive::RequestWithOwner(join(
                        owner_addr.send(IsSubActive {
                            sub: Subscription::Program(key),
                            commitment,
                        }),
                        addr.send(IsSubActive { sub, commitment }),
                    ))
                })
                .unwrap_or_else(|| {
                    SubscriptionActive::Request(addr.send(IsSubActive { sub, commitment }))
                })
        } else {
            SubscriptionActive::Ready(false)
        }
    }

    pub fn reset(
        &self,
        sub: Subscription,
        commitment: Commitment,
        filters: Option<Filters>,
        owner: Option<Pubkey>,
    ) {
        let addr = self.get_addr_by_key((sub.key(), commitment));
        addr.do_send(AccountCommand::Reset(sub, commitment, filters, owner))
    }

    #[inline]
    pub fn can_subscribe(&self) -> bool {
        self.subscriptions_allowed.load(Ordering::Relaxed)
    }

    pub fn subscribe(
        &self,
        sub: Subscription,
        commitment: Commitment,
        filters: Option<Filters>,
        owner: Option<Pubkey>,
    ) {
        let addr = self.get_addr_by_key((sub.key(), commitment));
        addr.do_send(AccountCommand::Subscribe(sub, commitment, filters, owner))
    }

    pub fn unsubscribe(&self, key: Pubkey, commitment: Commitment) {
        let addr = self.get_addr_by_key((key, commitment));
        addr.do_send(AccountCommand::Unsubscribe(key, commitment))
    }

    pub fn workers_count(&self) -> usize {
        self.workers.len()
    }

    pub fn active_connection_count(&self) -> usize {
        self.workers
            .iter()
            .fold(0, |acc, w| acc + w.1.load(Ordering::Relaxed) as usize)
    }
}

impl Actor for PubSubManager {
    type Context = Context<Self>;
}

impl Handler<WsReconnectInstruction> for PubSubManager {
    type Result = String;

    fn handle(&mut self, msg: WsReconnectInstruction, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            WsReconnectInstruction::Init { delay, interval } => {
                if !self.reconnections.is_empty() {
                    return "WS reconnection is already in progress, abort or wait for it to finish".into();
                }

                metrics()
                    .forced_reconnections_remaining
                    .set(self.workers_count() as i64);
                metrics().forced_reconnections_finished.set(0);
                // construct the delays iterator, for running reconnect tasks at specific intervals
                let interval = repeat(interval)
                    .enumerate()
                    .map(|(i, interval)| delay + i as u64 * interval);
                let iter = self.workers.iter().map(|(w, _)| w).cloned().zip(interval);

                for (worker, delay) in iter {
                    let handle =
                        ctx.run_later(Duration::from_secs(delay), |actor: &mut Self, _ctx| {
                            if let Some((worker, _)) = actor.reconnections.pop_front() {
                                worker.do_send(ForceReconnect);
                                metrics().forced_reconnections_remaining.dec();
                                metrics().forced_reconnections_finished.inc();
                            } else {
                                warn!("reconnections are empty, shouldn't have happened");
                            }
                        });
                    self.reconnections.push_back((worker, handle));
                }

                "WS reconnections initiated".into()
            }
            WsReconnectInstruction::Status => {
                if self.reconnections.is_empty() {
                    return "No WS reconnection is in progress".into();
                }
                format!(
                    "WS reconnection is running\n
                    finished reconnections count: {}\n
                    remaining reconnections: {}",
                    self.workers_count() - self.reconnections.len(),
                    self.reconnections.len(),
                )
            }
            WsReconnectInstruction::Abort => {
                if self.reconnections.is_empty() {
                    return "No WS reconnection is running to abort".into();
                }
                let unfinished = self.reconnections.len();
                while let Some((_, handle)) = self.reconnections.pop_front() {
                    ctx.cancel_future(handle);
                }
                format!(
                    "WS reconnection has been aborted\n\
                    finished reconnections count: {}\n\
                    unfinished reconnections: {}",
                    self.workers_count() - unfinished,
                    unfinished,
                )
            }
        }
    }
}
