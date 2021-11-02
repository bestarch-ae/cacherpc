use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use dashmap::mapref::entry::Entry;
use dashmap::{mapref::one::Ref, DashMap};
use either::Either;
use serde::{Deserialize, Serialize};

use crate::filter::Filters;
use crate::metrics::db_metrics as metrics;

pub struct ProgramState {
    account_keys: [Option<HashSet<Arc<Pubkey>>>; 3],
    slots: [u64; 3],
}

impl ProgramState {
    pub fn get(&self, commitment: Commitment) -> Option<&HashSet<Arc<Pubkey>>> {
        self.account_keys[commitment.as_idx()].as_ref()
    }

    pub fn get_slot(&self, commitment: Commitment) -> Option<&u64> {
        self.slots.get(commitment.as_idx())
    }

    fn insert(&mut self, commitment: Commitment, data: HashSet<Arc<Pubkey>>) {
        self.account_keys[commitment.as_idx()] = Some(data)
    }

    fn add(&mut self, commitment: Commitment, data: Arc<Pubkey>, slot: Slot) {
        if let Some(ref mut keys) = self.account_keys[commitment.as_idx()] {
            keys.insert(data);
        } else {
            let mut set = HashSet::new();
            set.insert(data);
            self.insert(commitment, set);
        }
        self.slots[commitment.as_idx()] = slot;
    }

    fn remove(&mut self, commitment: Commitment, data: &Pubkey) {
        if let Some(ref mut keys) = self.account_keys[commitment.as_idx()] {
            keys.remove(data);
        }
    }

    fn take_commitment(&mut self, commitment: Commitment) -> Option<HashSet<Arc<Pubkey>>> {
        self.account_keys[commitment.as_idx()].take()
    }

    fn is_empty(&self) -> bool {
        self.account_keys.iter().all(Option::is_none)
    }
}

impl Default for ProgramState {
    fn default() -> Self {
        ProgramState {
            account_keys: [None, None, None],
            slots: [0; 3],
        }
    }
}

type ProgramAccountsKey = (Pubkey, Option<Filters>);

#[derive(Clone)]
pub struct ProgramAccountsDb {
    map: Arc<DashMap<ProgramAccountsKey, ProgramState>>,
    observed_filters: Arc<DashMap<Pubkey, HashSet<Filters>>>,
}

impl Default for ProgramAccountsDb {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgramAccountsDb {
    pub fn new() -> Self {
        ProgramAccountsDb {
            map: Arc::new(DashMap::new()),
            observed_filters: Arc::new(DashMap::new()),
        }
    }

    pub fn get(
        &self,
        key: &Pubkey,
        filters: Option<Filters>,
    ) -> Option<Ref<'_, ProgramAccountsKey, ProgramState>> {
        if let Some(found) = self.map.get(&(*key, filters)) {
            Some(found)
        } else {
            self.map.get(&(*key, None))
        }
    }

    pub fn insert(
        &self,
        key: Pubkey,
        data: HashSet<Arc<Pubkey>>,
        commitment: Commitment,
        filters: Option<Filters>,
    ) {
        if let Some(filters) = filters.as_ref() {
            data.iter().for_each(|key| {
                self.observed_filters
                    .entry(**key)
                    .or_default()
                    .insert(filters.clone());
            })
        }

        let mut entry = self.map.entry((key, filters)).or_default();
        entry.insert(commitment, data);
        drop(entry);
        metrics().program_account_entries.set(self.map.len() as i64);
    }

    pub fn update_account(
        &self,
        key: &Pubkey,
        data: Arc<Pubkey>,
        filter_groups: HashSet<Filters>,
        commitment: Commitment,
        slot: Slot,
    ) {
        // add to global
        if let Some(mut entry) = self.map.get_mut(&(*key, None)) {
            entry.add(commitment, data.clone(), slot);
        }

        let has_new_or_old_filters = !filter_groups.is_empty()
            || self
                .observed_filters
                .get(&*data)
                .map_or(false /* no set == empty */, |set| !set.is_empty());

        if has_new_or_old_filters {
            let mut old_groups = self.observed_filters.entry(*data).or_default();
            let diff = old_groups.symmetric_difference(&filter_groups);
            for filter in diff {
                let state = self.map.get_mut(&(*key, Some(filter.clone())));
                match state {
                    // Account no longer matches filter
                    Some(mut state) if old_groups.contains(filter) => {
                        state.remove(commitment, &data);
                    }
                    // Account is new to the filter
                    Some(mut state) /* !old_groups.contains(&filter) */ => {
                        state.add(commitment, Arc::clone(&data), slot);
                    }
                    None => (), // State not found
                }
            }
            *old_groups = filter_groups;
        }
    }

    pub fn remove_all(
        &self,
        key: &Pubkey,
        commitment: Commitment,
        filters: Option<Filters>,
    ) -> impl Iterator<Item = Pubkey> {
        let iter = if let Entry::Occupied(mut entry) = self.map.entry((*key, filters.clone())) {
            let state = entry.get_mut();
            let keys = state.take_commitment(commitment);
            if state.is_empty() {
                entry.remove();
            } else {
                drop(entry);
            }

            if let Some(filters) = filters {
                keys.as_ref().into_iter().flatten().for_each(|key| {
                    if let Some(mut set) = self.observed_filters.get_mut(&*key) {
                        set.remove(&filters);
                    }
                })
            }

            let iter = keys.into_iter().flatten().map(|arc| {
                let key = *arc;
                drop(arc);
                key
            });

            Either::Left(iter)
        } else {
            Either::Right(std::iter::empty())
        };
        metrics().program_account_entries.set(self.map.len() as i64);
        iter
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
            old.slot = data.context.slot;
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

impl Pubkey {
    #[cfg(test)]
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
    let programs = ProgramAccountsDb::new();
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
    programs.insert(Pubkey::zero(), set, Commitment::Confirmed, None);
    assert_eq!(accounts.map.len(), 1);
    assert_eq!(programs.map.len(), 1);

    accounts.remove(&Pubkey::zero(), Commitment::Confirmed);
    assert_eq!(accounts.map.len(), 1);

    let program_accounts = programs.remove_all(&Pubkey::zero(), Commitment::Confirmed, None);
    assert_eq!(programs.map.len(), 0);
    assert_eq!(accounts.map.len(), 1);

    for key in program_accounts {
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
