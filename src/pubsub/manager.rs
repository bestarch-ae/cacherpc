use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;

use actix::{Actor, Addr, Context, Message};
use futures_util::future::join;

use crate::{
    filter::Filters,
    types::{AccountsDb, Commitment, ProgramAccountsDb, Pubkey},
};

use super::subscription::{IsSubActive, Subscription, SubscriptionActive};
use super::worker::{autocmd::AccountCommand, AccountUpdateManager};

#[derive(Clone)]
pub struct PubSubManager {
    workers: Vec<(actix::Addr<AccountUpdateManager>, Arc<AtomicBool>)>,
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

impl PubSubManager {
    pub fn init(
        connections: u32,
        accounts: AccountsDb,
        program_accounts: ProgramAccountsDb,
        rpc_slot: Arc<AtomicU64>,
        worker_config: WorkerConfig,
        subscriptions_allowed: Arc<AtomicBool>,
    ) -> Self {
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
        let manager = PubSubManager {
            workers,
            subscriptions_allowed,
        };
        let actor = manager.clone().start();
        // make sure all the workers, have an address of pubsub manager
        for (w, _) in &manager.workers {
            w.do_send(InitManager(Addr::clone(&actor)));
        }

        manager
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
}

impl Actor for PubSubManager {
    type Context = Context<Self>;
}
