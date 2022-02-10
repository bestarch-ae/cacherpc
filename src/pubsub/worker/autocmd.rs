use actix::{Context, Handler, Message, StreamHandler};
use tracing::{error, info};

use crate::metrics::pubsub_metrics as metrics;
use crate::pubsub::subscription::{PubSubSubscribe, Subscription};
use crate::{
    filter::Filters,
    types::{Commitment, Pubkey},
};

use super::AccountUpdateManager;

#[derive(Message, Debug)]
#[rtype(result = "()")]
pub enum AccountCommand {
    Subscribe(
        Subscription,
        Commitment,
        Option<Filters>,
        Option<Pubkey>, /*optional account owner*/
    ),
    Reset(
        Subscription,
        Commitment,
        Option<Filters>,
        Option<Pubkey>, /*optional account owner*/
    ),
    Purge(Subscription, Commitment),
    // same as purge, but doesn't remove data from cache, used only for account subscriptions
    Unsubscribe(Pubkey, Commitment),
}

impl Handler<AccountCommand> for AccountUpdateManager {
    type Result = ();

    fn handle(&mut self, item: AccountCommand, ctx: &mut Context<Self>) {
        let _ = (|| -> Result<(), serde_json::Error> {
            match item {
                AccountCommand::Subscribe(sub, commitment, filters, owner) => {
                    // if account owner exists
                    if let Some(pubkey) = owner {
                        let program_key = (pubkey, commitment);
                        // then try to add it to tracked keys in program accounts cache
                        let success = self
                            .accounts
                            .get(&sub.key())
                            .and_then(|state| state.get_ref(commitment))
                            .map(|acc_ref| {
                                self.program_accounts
                                    .track_account_key(program_key, acc_ref)
                            })
                            .unwrap_or_default();
                        // if real program entry existed, `success` will be set to true,
                        // indicating that owner program is tracking subscription for
                        // the account, so there's no need create another one
                        if success {
                            info!("account subscription skipped");
                            metrics().subscriptions_skipped.inc();
                            self.purge_queue
                                .as_ref()
                                .unwrap()
                                .insert((sub, commitment), self.config.ttl);
                            return Ok(());
                        }
                    }
                    metrics()
                        .commands
                        .with_label_values(&[&self.actor_name, "subscribe"])
                        .inc();
                    self.subscribe(sub, commitment)?;
                    if !sub.is_account() {
                        self.reset_filter(ctx, sub, commitment, filters);
                    }
                }
                AccountCommand::Purge(sub, commitment) => {
                    metrics()
                        .commands
                        .with_label_values(&[&self.actor_name, "purge"])
                        .inc();
                    // for program subscription, before we purge everything from
                    // cache, we have to resubscribe for all of its accounts,
                    // which had recent activity, and are stored in tracked_keys
                    if !sub.is_account() {
                        let program_key = (sub.key(), commitment);
                        // we save the tracked keys, before purging the program's cache entries
                        let tracked_keys = self.program_accounts.get_tracked_keys(&program_key);
                        // for a program, we have to remove data from cache first, so that new
                        // account subscriptions can create a new entry for their owner, and add
                        // themselves to tracked accounts list
                        self.purge_key(ctx, &sub, commitment);
                        let manager = self
                            .manager
                            .as_ref()
                            .expect("PubSub manager is not setup in worker");
                        for key in tracked_keys {
                            manager.do_send(PubSubSubscribe {
                                key,
                                commitment,
                                owner: sub.key(),
                            });
                        }
                    }

                    if self.connection.is_connected() {
                        self.unsubscribe(sub, commitment)?;
                    } else {
                        self.subs.remove(&(sub, commitment));
                    }
                    if sub.is_account() {
                        self.active_accounts.remove(&(sub.key(), commitment));
                        self.purge_key(ctx, &sub, commitment);
                    }
                }
                AccountCommand::Unsubscribe(pubkey, commitment) => {
                    metrics()
                        .commands
                        .with_label_values(&[&self.actor_name, "unsubsribe"])
                        .inc();
                    self.active_accounts.remove(&(pubkey, commitment));
                    let sub = Subscription::Account(pubkey);
                    if self.connection.is_connected() {
                        self.unsubscribe(sub, commitment)?;
                    } else {
                        self.subs.remove(&(sub, commitment));
                    }
                    self.purge_key(ctx, &sub, commitment);
                }

                AccountCommand::Reset(sub, commitment, filters, owner) => {
                    metrics()
                        .commands
                        .with_label_values(&[&self.actor_name, "reset"])
                        .inc();
                    if let Some(time) = self.subs.get_mut(&(sub, commitment)) {
                        metrics()
                            .time_until_reset
                            .observe(time.update().as_secs_f64());
                    }

                    if let Some(owner_pubkey) = owner {
                        let acc_ref = self
                            .accounts
                            .get(&sub.key())
                            .and_then(|state| state.get_ref(commitment));
                        // if owner was provided for reset, it means, that this account
                        // subscription is being tracked by owner program, by tracking this
                        // account's key in program's cache entry we make sure, that when the
                        // program's subscription ends, it will resubscribe for all tracked
                        // accounts
                        if let Some(acc_ref) = acc_ref {
                            self.program_accounts
                                .track_account_key((owner_pubkey, commitment), acc_ref);
                        }
                    }

                    self.purge_queue
                        .as_ref()
                        .unwrap()
                        .reset((sub, commitment), self.config.ttl);
                    if !sub.is_account() {
                        self.reset_filter(ctx, sub, commitment, filters);
                    }
                }
            }
            Ok(())
        })()
        .map_err(|err| {
            error !(error = %err, "error handling AccountCommand");
        });
    }
}

impl StreamHandler<AccountCommand> for AccountUpdateManager {
    fn handle(&mut self, item: AccountCommand, ctx: &mut Context<Self>) {
        let _ = <Self as Handler<AccountCommand>>::handle(self, item, ctx);
    }

    fn finished(&mut self, _ctx: &mut Context<Self>) {
        info!(self.actor_id, "purge stream finished");
    }
}
