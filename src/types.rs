use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use dashmap::mapref::entry::Entry;
use dashmap::{
    mapref::one::{Ref, RefMut},
    DashMap,
};
use either::Either;
use serde::{Deserialize, Serialize};

use crate::filter::Filters;
use crate::metrics::db_metrics as metrics;

use tokio::sync::{Semaphore, SemaphorePermit};

type AccountSet = HashSet<Arc<Pubkey>>;
type ProgramAccountsKey = (Pubkey, Commitment);

#[derive(Default)]
pub struct ProgramState {
    pub slot: u64,
    // account keys, that were requested using various filteres
    filtered_keys: HashMap<Filters, AccountSet>,
    // account keys, that were requested without any filters
    unfiltered_keys: Option<AccountSet>,
    observed_filters: HashMap<Pubkey, HashSet<Filters>>,
    tracked_keys: AccountSet,
    // set to true after first insert, should be set to false,
    // before program removal from cache, so that resubscription
    // for accounts of the owner program can be made
    active: bool,
}
#[derive(Default, Clone)]
pub struct ProgramAccountsDb {
    map: Arc<DashMap<ProgramAccountsKey, ProgramState>>,
}

impl ProgramState {
    pub fn get_account_keys(&self, filters: &Option<Filters>) -> Option<&HashSet<Arc<Pubkey>>> {
        filters
            .as_ref()
            .and_then(|filters| self.filtered_keys.get(filters))
            .or_else(|| self.unfiltered_keys.as_ref())
    }

    fn insert_account_keys(&mut self, filters: Option<Filters>, keys: AccountSet) {
        let old = if let Some(filters) = filters {
            self.filtered_keys.insert(filters, keys)
        } else {
            self.unfiltered_keys.replace(keys)
        };
        if old.is_none() {
            metrics().program_account_entries.inc();
        }
    }

    fn has_unfiltered(&self) -> bool {
        self.unfiltered_keys.is_some()
    }

    fn insert_single_key(&mut self, filters: Option<&Filters>, key: Arc<Pubkey>) {
        if let Some(filters) = filters {
            self.filtered_keys
                .entry(filters.clone())
                .or_insert_with(|| {
                    metrics().program_account_entries.inc();
                    AccountSet::new()
                })
                .insert(key);
        } else if let Some(set) = self.unfiltered_keys.as_mut() {
            set.insert(key);
        }
        // else we shouldn't insert single key, as there's no record for unfiltered request
    }

    // used to remove keys for given filter, or for no filters
    fn remove_account_keys(&mut self, filters: Option<Filters>) -> Option<AccountSet> {
        let mut res = if let Some(filters) = filters {
            self.filtered_keys.remove(&filters)
        } else {
            self.unfiltered_keys.take()
        };
        if let Some(keys) = res.as_mut() {
            metrics().program_account_entries.dec();
            for key in &self.tracked_keys {
                keys.remove(key);
            }
        }
        res
    }

    fn remove_single_key(&mut self, filters: &Filters, key: &Arc<Pubkey>) {
        let keys = self.filtered_keys.get_mut(filters);
        if let Some(keys) = keys {
            keys.remove(key);
            if keys.is_empty() {
                self.filtered_keys.remove(filters);
                metrics().program_account_entries.dec();
            }
        }
    }

    pub fn tracked_keys(&self) -> &AccountSet {
        &self.tracked_keys
    }
}

impl ProgramAccountsDb {
    pub fn track_account_key(&self, program_key: ProgramAccountsKey, key_ref: Arc<Pubkey>) -> bool {
        let mut program_state = self.map.entry(program_key).or_default();
        // track refrerence counts, in case owner program is ever requested
        // and put in cache, it will just unsubscribe from all those tracked
        // account keys, to avoid subscription duplication
        program_state.tracked_keys.insert(key_ref);

        // if state is not active, then this state is actually dummy state,
        // and no real program subscription exists, as such we can keep creating account
        // subscriptions, as no owner program is tracking them for the owned account
        program_state.active
    }

    pub fn untrack_account_key(&self, program_key: &ProgramAccountsKey, key_ref: Arc<Pubkey>) {
        self.map
            .get_mut(program_key)
            .map(|mut state| state.tracked_keys.remove(&key_ref));
    }

    pub fn get_slot(&self, key: &ProgramAccountsKey) -> Option<Slot> {
        self.map.get(key).map(|state| state.slot)
    }

    pub fn get_state(
        &self,
        key: ProgramAccountsKey,
    ) -> Option<Ref<'_, ProgramAccountsKey, ProgramState>> {
        self.map.get(&key)
    }

    pub fn insert(
        &self,
        key: ProgramAccountsKey,
        account_key_refs: AccountSet,
        filters: Option<Filters>,
    ) -> RefMut<'_, ProgramAccountsKey, ProgramState> {
        let mut state = self.map.entry(key).or_default();
        if let Some(filters) = filters.as_ref() {
            account_key_refs.iter().for_each(|key| {
                state
                    .observed_filters
                    .entry(**key)
                    .or_default()
                    .insert(filters.clone());
            })
        }
        state.insert_account_keys(filters, account_key_refs);
        state.active = true;

        state
    }

    pub fn remove_keys_for_filter(
        &self,
        key: &ProgramAccountsKey,
        filters: Option<Filters>,
    ) -> AccountSet {
        self.map
            .get_mut(key)
            .map(|mut state| state.remove_account_keys(filters))
            .flatten()
            .unwrap_or_default()
    }

    // method is used to check whether we are inserting program data for the first time
    pub fn has_active_entry(&self, key: &ProgramAccountsKey) -> bool {
        self.map
            .get(key)
            .map(|state| state.active)
            .unwrap_or_default()
    }

    pub fn get_tracked_keys(&self, key: &ProgramAccountsKey) -> Vec<Pubkey> {
        self.map
            .get(key)
            .map(|state| state.tracked_keys.iter().map(|k| **k).collect())
            .unwrap_or_default()
    }

    pub fn remove_all(&self, key: &ProgramAccountsKey) -> impl Iterator<Item = Arc<Pubkey>> {
        let mut state = match self.map.remove(key) {
            Some(state) => state.1,
            None => return Either::Left(std::iter::empty()),
        };

        metrics()
            .program_account_entries
            .sub((state.filtered_keys.len() + state.unfiltered_keys.is_some() as usize) as i64);

        let mut keys = state
            .filtered_keys
            .into_values()
            .flatten()
            .collect::<AccountSet>();
        keys.extend(state.unfiltered_keys.into_iter().flatten());
        let tracked_keys = std::mem::take(&mut state.tracked_keys);
        Either::Right(
            keys.into_iter()
                .filter(move |key| !tracked_keys.contains(key)),
        )
    }

    pub fn update_account(
        &self,
        key: &ProgramAccountsKey,
        acc_ref_key: Arc<Pubkey>,
        filter_groups: HashSet<Filters>,
        slot: Slot,
    ) -> bool {
        let mut can_be_removed = true;
        let mut state = match self.map.get_mut(key) {
            Some(state) if state.active => state,
            _ => return can_be_removed,
        };

        // register it within unfiltered keys
        if state.has_unfiltered() {
            state.insert_single_key(None, Arc::clone(&acc_ref_key));
            can_be_removed = false;
        }

        // update slot for this program
        state.slot = slot;

        let has_new_or_old_filters = !filter_groups.is_empty()
            || state
                .observed_filters
                .get(&*acc_ref_key)
                .map_or(false /* no set == empty */, |set| !set.is_empty());

        if has_new_or_old_filters {
            let old_groups = state
                .observed_filters
                .remove(&*acc_ref_key)
                .unwrap_or_default();
            let diff = old_groups.symmetric_difference(&filter_groups);
            for filter in diff {
                if old_groups.contains(filter) {
                    // Account no longer matches filter
                    state.remove_single_key(filter, &acc_ref_key);
                } else {
                    // Account is new to the filter
                    state.insert_single_key(Some(filter), Arc::clone(&acc_ref_key));
                }
            }
            can_be_removed = can_be_removed && filter_groups.is_empty();
            // Insert new filters for this account
            state.observed_filters.insert(*acc_ref_key, filter_groups);
        }

        can_be_removed && !state.tracked_keys.contains(&acc_ref_key)
    }
}

#[derive(Clone)]
pub struct AccountsDb {
    map: Arc<DashMap<Pubkey, AccountState>>,
    slot: Arc<[AtomicU64; 3]>,
}

pub type Slot = u64;

#[derive(Debug)]
struct Account {
    data: Option<AccountInfo>,
    slot: Slot,
    refcount: Arc<Pubkey>,
}

#[derive(Debug)]
pub struct AccountState {
    data: [Option<Account>; 3],
    key: Pubkey,
}

impl AccountState {
    pub fn get(&self, commitment: Commitment) -> Option<(Option<&AccountInfo>, Slot)> {
        match &(self.data)[commitment.as_idx()] {
            None => None,
            Some(data) => {
                let mut result = None;
                let mut slot = data.slot;
                for acc in self.data.iter().take(commitment.as_idx() + 1).flatten() {
                    if acc.slot >= slot {
                        result = Some((acc.data.as_ref(), acc.slot));
                        slot = acc.slot;
                    }
                }
                result
            }
        }
    }

    pub fn get_ref(&self, commitment: Commitment) -> Option<Arc<Pubkey>> {
        self.data[commitment.as_idx()]
            .as_ref()
            .map(|data| data.refcount.clone())
    }

    fn insert(&mut self, commitment: Commitment, data: AccountContext) -> Arc<Pubkey> {
        let new_len = data.value.as_ref().map(|info| info.data.len()).unwrap_or(0);
        let position = &mut (self.data)[commitment.as_idx()];
        let rc = if let Some(old) = position {
            metrics()
                .account_bytes
                .sub(old.data.as_ref().map(|info| info.data.len()).unwrap_or(0) as i64);
            old.data = data.value;
            old.slot = old.slot.max(data.context.slot);
            old.refcount.clone()
        } else {
            let refcount = Arc::new(self.key);
            *position = Some(Account {
                data: data.value,
                slot: data.context.slot,
                refcount: refcount.clone(),
            });
            refcount
        };

        metrics().account_bytes.add(new_len as i64);
        rc
    }

    fn remove(&mut self, commitment: Commitment) {
        let position = &mut (self.data)[commitment.as_idx()];
        if let Some(account) = position {
            /* only one reference - our own */
            if Arc::strong_count(&account.refcount) <= 1 {
                metrics().account_bytes.sub(
                    account
                        .data
                        .as_ref()
                        .map(|data| data.data.len())
                        .unwrap_or(0) as i64,
                );
                *position = None;
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.data.iter().all(Option::is_none)
    }
}

impl Default for AccountsDb {
    fn default() -> Self {
        Self::new()
    }
}

impl AccountsDb {
    pub fn new() -> Self {
        AccountsDb {
            map: Arc::new(DashMap::new()),
            slot: Arc::new([AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)]),
        }
    }

    pub fn get(&self, key: &Pubkey) -> Option<Ref<'_, Pubkey, AccountState>> {
        self.map.get(key)
    }

    pub fn insert(&self, key: Pubkey, data: AccountContext, commitment: Commitment) -> Arc<Pubkey> {
        let mut entry = self.map.entry(key).or_insert(AccountState {
            key,
            data: [None, None, None],
        });
        self.update_slot(commitment, data.context.slot);
        let key_ref = entry.insert(commitment, data);
        drop(entry);
        metrics().account_entries.set(self.map.len() as i64);
        key_ref
    }

    pub fn remove(&self, key: &Pubkey, commitment: Commitment) {
        if let Entry::Occupied(mut entry) = self.map.entry(*key) {
            let account_state = entry.get_mut();
            account_state.remove(commitment);
            if account_state.is_empty() {
                entry.remove();
            } else {
                drop(entry);
            }
            metrics().account_entries.set(self.map.len() as i64);
        }
    }

    fn update_slot(&self, commitment: Commitment, val: u64) {
        self.slot[commitment.as_idx()].fetch_max(val, Ordering::AcqRel);
    }

    pub fn get_owner(&self, key: &Pubkey, commitment: Commitment) -> Option<Pubkey> {
        let state = self.map.get(key)?;
        state
            .get(commitment)
            .and_then(|(info, _)| info)
            .map(|info| info.owner)
    }

    #[allow(unused)]
    pub fn get_slot(&self, commitment: Commitment) -> u64 {
        self.slot[commitment.as_idx()].load(Ordering::Acquire)
    }
}

#[derive(Serialize, Debug, Copy, Clone, Eq, PartialEq, Hash)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    Finalized,
    Confirmed,
    Processed,
}

impl Commitment {
    fn as_idx(self) -> usize {
        match self {
            Commitment::Finalized => 0,
            Commitment::Confirmed => 1,
            Commitment::Processed => 2,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Commitment::Confirmed => "confirmed",
            Commitment::Processed => "processed",
            Commitment::Finalized => "finalized",
        }
    }
}

impl<'de> Deserialize<'de> for Commitment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct Visitor;
        impl<'de> serde::de::Visitor<'de> for Visitor {
            type Value = Commitment;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use serde::de::Error;
                match v {
                    "finalized" | "max" => Ok(Commitment::Finalized),
                    "singleGossip" | "confirmed" => Ok(Commitment::Confirmed),
                    "recent" | "processed" => Ok(Commitment::Processed),
                    _ => Err(Error::custom("unsupported commitment")),
                }
            }
        }

        deserializer.deserialize_str(Visitor)
    }
}

impl Default for Commitment {
    fn default() -> Self {
        Commitment::Finalized
    }
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub enum Encoding {
    #[serde(skip)]
    Default,
    Base58,
    Base64,
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
    JsonParsed,
}

impl Encoding {
    pub fn as_str(&self) -> &'static str {
        match self {
            Encoding::Default => "default",
            Encoding::Base58 => "base58",
            Encoding::Base64 => "base64",
            Encoding::Base64Zstd => "base64_zstd",
            Encoding::JsonParsed => "json_parsed",
        }
    }

    pub fn is_base58(&self) -> bool {
        matches!(self, Encoding::Default | Encoding::Base58)
    }
}

impl Default for Encoding {
    fn default() -> Self {
        Encoding::Default
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub lamports: u64,
    pub data: AccountData,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
}

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, Ord, PartialOrd)]
pub struct Pubkey([u8; 32]);

impl From<Pubkey> for [u8; 32] {
    fn from(key: Pubkey) -> [u8; 32] {
        key.0
    }
}

#[cfg(test)]
impl Pubkey {
    fn zero() -> Self {
        Pubkey([0; 32])
    }
}

impl std::fmt::Display for Pubkey {
    fn fmt(&self, w: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        w.write_str(&bs58::encode(&self.0).into_string())
    }
}

impl Serialize for Pubkey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::Error;
        let mut buf = [0; 64];
        let len = bs58::encode(self.0)
            .into(&mut buf[..])
            .map_err(|_| Error::custom("can't b58encode"))?;
        serializer.serialize_str(unsafe { std::str::from_utf8_unchecked(&buf[..len]) })
    }
}

impl<'de> Deserialize<'de> for Pubkey {
    fn deserialize<D>(deserializer: D) -> Result<Pubkey, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct PubkeyVisitor;
        impl<'de> serde::de::Visitor<'de> for PubkeyVisitor {
            type Value = Pubkey;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use serde::de::Error;
                let mut buf = [0; 32];
                let len = bs58::decode(v)
                    .into(&mut buf)
                    .map_err(|_| Error::custom("can't b58decode"))?;
                if len != buf.len() {
                    return Err(Error::custom("bad size"));
                }
                Ok(Pubkey(buf))
            }
        }

        deserializer.deserialize_str(PubkeyVisitor)
    }
}

#[derive(Debug)]
#[cfg_attr(test, derive(Clone))]
pub struct AccountData {
    pub data: Bytes,
}

impl AccountData {
    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'de> Deserialize<'de> for AccountData {
    fn deserialize<D>(deserializer: D) -> Result<AccountData, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct AccountDataVisitor;

        impl<'de> serde::de::Visitor<'de> for AccountDataVisitor {
            type Value = AccountData;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("[]")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                let data = bs58::decode(v)
                    .into_vec()
                    .map_err(|_| serde::de::Error::custom("can't decode"))?;
                Ok(AccountData {
                    data: Bytes::from(data),
                })
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                use std::borrow::Cow;

                let err = || serde::de::Error::custom("can't decode");
                let dat: Cow<'_, str> = seq.next_element()?.ok_or_else(|| todo!())?;
                let encoding: Cow<'_, str> = seq.next_element()?.ok_or_else(|| todo!())?;
                let data = match encoding.as_ref() {
                    "base58" => bs58::decode(dat.as_ref()).into_vec().map_err(|_| err())?,
                    "base64" => base64::decode(dat.as_ref()).map_err(|_| err())?,
                    "base64+zstd" => {
                        let vec = base64::decode(dat.as_ref()).map_err(|_| err())?;
                        zstd::decode_all(std::io::Cursor::new(vec)).map_err(|_| err())?
                    }
                    _ => return Err(serde::de::Error::custom("unsupported encoding")),
                };
                Ok(AccountData {
                    data: Bytes::from(data),
                })
            }
        }
        deserializer.deserialize_any(AccountDataVisitor)
    }
}

impl Serialize for AccountData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;
        let mut seq = serializer.serialize_seq(Some(2))?;
        seq.serialize_element(&bs58::encode(&self.data).into_string())?;
        seq.serialize_element("base58")?;
        seq.end()
    }
}

#[derive(Serialize, Deserialize, Debug)]
// TODO: Refactor this into WithContext<T>
pub struct AccountContext {
    pub context: SolanaContext,
    pub value: Option<AccountInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct SolanaContext {
    pub slot: Slot,
}

pub struct BytesChain {
    bytes: Vec<Bytes>,
    position: usize,
    current_reader: Option<bytes::buf::Reader<Bytes>>,
}

impl Default for BytesChain {
    fn default() -> Self {
        BytesChain::new()
    }
}

impl BytesChain {
    pub fn new() -> Self {
        BytesChain {
            bytes: Vec::new(),
            position: 0,
            current_reader: None,
        }
    }

    pub fn push(&mut self, bytes: Bytes) {
        self.bytes.push(bytes);
    }
}

pub struct SemaphoreQueue {
    wait_quota: Semaphore,
    semaphore: Semaphore,
}

impl SemaphoreQueue {
    pub fn new(queue_size: usize, permits: usize) -> Self {
        Self {
            wait_quota: Semaphore::new(queue_size),
            semaphore: Semaphore::new(permits),
        }
    }

    pub async fn acquire(&self) -> Option<SemaphorePermit<'_>> {
        let _queue_slot = self.wait_quota.try_acquire().ok()?;
        let permit = self.semaphore.acquire().await.ok()?;

        Some(permit)
    }

    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub fn queue_permits(&self) -> usize {
        self.wait_quota.available_permits()
    }

    pub async fn apply_limit(&self, old_limit: usize, new_limit: usize) {
        if new_limit > old_limit {
            self.semaphore.add_permits(new_limit - old_limit);
        } else {
            for _ in 0..old_limit - new_limit {
                if let Ok(permit) = self.semaphore.acquire().await {
                    permit.forget();
                }
            }
        }
    }

    pub async fn apply_queue_size(&self, old_size: usize, new_size: usize) {
        if new_size > old_size {
            self.wait_quota.add_permits(new_size - old_size);
        } else {
            for _ in 0..old_size - new_size {
                if let Ok(permit) = self.wait_quota.acquire().await {
                    permit.forget();
                }
            }
        }
    }
}

impl std::io::Read for BytesChain {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        loop {
            if self.current_reader.is_none() {
                if let Some(reader) = self
                    .bytes
                    .get(self.position)
                    .map(|bytes| bytes.clone().reader())
                {
                    self.current_reader = Some(reader);
                } else {
                    return Ok(0);
                }
            }

            let reader = self.current_reader.as_mut().unwrap();
            match reader.read(buf) {
                Ok(0) if !buf.is_empty() => {
                    self.current_reader.take();
                    self.position += 1;
                    continue;
                }
                whatever => {
                    return whatever;
                }
            }
        }
    }
}

#[test]
fn data() {
    let dat = r#"["2UzHM","base58"]"#;
    let data: AccountData = serde_json::from_str(dat).unwrap();
    println!("{:?}", data);
}

#[test]
fn kek() {
    let dat = r#"{"data":["2UzHM","base58"],"executable":false,"lamports":918720,"owner":"pdRUarXshQQAumQ12xntHo7xppX6Au9NdSskmWpahLJ","rentEpoch":0}"#;
    let data: AccountInfo = serde_json::from_str(dat).unwrap();
    println!("{:?}", data);
}

#[test]
fn pooq() {
    let dat = r#"{"data":["2UzHM","base58"],"executable":false,"lamports":918720,"owner":"pdRUarXshQQAumQ12xntHo7xppX6Au9NdSskmWpahLJ","rentEpoch":0}"#;
    let val: serde_json::Value = serde_json::from_str(dat).unwrap();
    let data: AccountInfo = serde_json::from_value(val).unwrap();
    println!("{:?}", data);
}

#[test]
fn db_refs() {
    let programs = ProgramAccountsDb::default();
    let accounts = AccountsDb::new();

    let acc_ref = accounts.insert(
        Pubkey::zero(),
        AccountContext {
            context: SolanaContext { slot: 0 },
            value: Some(AccountInfo {
                executable: false,
                lamports: 1,
                owner: Pubkey::zero(),
                rent_epoch: 1,
                data: AccountData { data: Bytes::new() },
            }),
        },
        Commitment::Confirmed,
    );

    assert_eq!(accounts.map.len(), 1);

    let mut set = HashSet::new();
    set.insert(acc_ref);

    programs.insert((Pubkey::zero(), Commitment::Confirmed), set, None);
    assert_eq!(accounts.map.len(), 1);
    assert_eq!(programs.map.len(), 1);

    accounts.remove(&Pubkey::zero(), Commitment::Confirmed);
    assert_eq!(accounts.map.len(), 1);

    let program_accounts = programs.remove_all(&(Pubkey::zero(), Commitment::Confirmed));
    assert_eq!(programs.map.len(), 0);
    assert_eq!(accounts.map.len(), 1);

    for arc in program_accounts {
        let key = *arc;
        drop(arc);
        accounts.remove(&key, Commitment::Confirmed);
    }
    assert_eq!(accounts.map.len(), 0);
}

#[test]
fn accounts_insert_remove() {
    let accounts = AccountsDb::new();

    let acc_ref = accounts.insert(
        Pubkey::zero(),
        AccountContext {
            context: SolanaContext { slot: 0 },
            value: Some(AccountInfo {
                executable: false,
                lamports: 1,
                owner: Pubkey::zero(),
                rent_epoch: 1,
                data: AccountData { data: Bytes::new() },
            }),
        },
        Commitment::Confirmed,
    );

    assert_eq!(accounts.map.len(), 1);

    let acc_ref2 = accounts.insert(
        Pubkey::zero(),
        AccountContext {
            context: SolanaContext { slot: 0 },
            value: Some(AccountInfo {
                executable: false,
                lamports: 1,
                owner: Pubkey::zero(),
                rent_epoch: 1,
                data: AccountData { data: Bytes::new() },
            }),
        },
        Commitment::Confirmed,
    );
    assert_eq!(accounts.map.len(), 1);
    drop(acc_ref);
    accounts.remove(&Pubkey::zero(), Commitment::Confirmed);
    assert_eq!(accounts.map.len(), 1);
    drop(acc_ref2);
    accounts.remove(&Pubkey::zero(), Commitment::Confirmed);
    assert_eq!(accounts.map.len(), 0);
}
