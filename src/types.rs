use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::{mapref::one::Ref, DashMap};
use serde::{Deserialize, Serialize};
use smallvec::{smallvec, SmallVec};

use crate::metrics::db_metrics as metrics;

pub(crate) struct ProgramState([Option<HashSet<Arc<Pubkey>>>; 3]);

impl ProgramState {
    pub fn get(&self, commitment: Commitment) -> Option<&HashSet<Arc<Pubkey>>> {
        (self.0)[commitment.as_idx()].as_ref()
    }

    pub fn into_accounts(self) -> impl Iterator<Item = Pubkey> {
        std::array::IntoIter::new(self.0)
            .flat_map(|set| set.into_iter())
            .flatten()
            .map(|arc| *arc)
    }

    fn insert(
        &mut self,
        commitment: Commitment,
        data: HashSet<Arc<Pubkey>>,
    ) -> Option<HashSet<Arc<Pubkey>>> {
        (self.0)[commitment.as_idx()].replace(data)
    }

    fn add(&mut self, commitment: Commitment, data: Arc<Pubkey>) {
        if let Some(ref mut keys) = (self.0)[commitment.as_idx()] {
            keys.insert(data);
        } else {
            let mut set = HashSet::new();
            set.insert(data);
            self.insert(commitment, set);
        }
    }

    fn remove(&mut self, commitment: Commitment, data: &Pubkey) {
        if let Some(ref mut keys) = (self.0)[commitment.as_idx()] {
            keys.remove(data);
        }
    }
}

impl Default for ProgramState {
    fn default() -> Self {
        ProgramState([None, None, None])
    }
}

type ProgramAccountsKey = (Pubkey, Option<SmallVec<[Filter; 2]>>);

#[derive(Clone)]
pub(crate) struct ProgramAccountsDb {
    map: Arc<DashMap<ProgramAccountsKey, ProgramState>>,
}

impl ProgramAccountsDb {
    pub fn new() -> Self {
        ProgramAccountsDb {
            map: Arc::new(DashMap::new()),
        }
    }

    pub fn get(
        &self,
        key: &Pubkey,
        filters: Option<SmallVec<[Filter; 2]>>,
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
        filters: Option<SmallVec<[Filter; 2]>>,
    ) -> Option<HashSet<Arc<Pubkey>>> {
        let mut entry = self.map.entry((key, filters)).or_default();
        entry.insert(commitment, data)
    }

    // We only add here and do not create new entries because it would be incorrect (incomplete
    // result).
    pub fn add(
        &self,
        key: &Pubkey,
        data: Arc<Pubkey>,
        filters: Option<SmallVec<[Filter; 2]>>,
        commitment: Commitment,
    ) -> bool {
        let mut added = false;
        // add to global
        if let Some(mut entry) = self.map.get_mut(&(*key, None)) {
            entry.add(commitment, data.clone());
            added = true;
        }

        // add with filter
        if filters.is_some() {
            if let Some(mut entry) = self.map.get_mut(&(*key, filters)) {
                entry.add(commitment, data);
                added = true;
            }
        }
        added
    }

    pub fn remove_all(
        &self,
        key: &Pubkey,
        filters: Option<SmallVec<[Filter; 2]>>,
    ) -> Option<ProgramState> {
        self.map.remove(&(*key, filters)).map(|(_, state)| state)
    }

    pub fn remove(
        &self,
        program_key: &Pubkey,
        account_key: &Pubkey,
        filters: SmallVec<[Filter; 2]>,
        commitment: Commitment,
    ) {
        if let Some(mut accounts) = self.map.get_mut(&(*program_key, Some(filters))) {
            accounts.value_mut().remove(commitment, account_key);
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }
}

#[derive(Clone)]
pub(crate) struct AccountsDb {
    map: Arc<DashMap<Pubkey, AccountState>>,
    slot: Arc<[AtomicU64; 3]>,
}

type Slot = u64;

#[derive(Debug)]
struct Account {
    data: Option<AccountInfo>,
    slot: Slot,
    refcount: Arc<Pubkey>,
}

pub(crate) struct AccountState {
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
                tracing::info!(key = %self.key, "removing account");
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.data.iter().all(Option::is_none)
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
        entry.insert(commitment, data)
    }

    pub fn remove(&self, key: &Pubkey, commitment: Commitment) {
        use dashmap::mapref::entry::Entry;
        if let Entry::Occupied(mut entry) = self.map.entry(*key) {
            let account_state = entry.get_mut();
            account_state.remove(commitment);
            if account_state.is_empty() {
                entry.remove();
            }
        }
    }

    fn update_slot(&self, commitment: Commitment, val: u64) {
        self.slot[commitment.as_idx()].fetch_max(val, Ordering::AcqRel);
    }

    #[allow(unused)]
    pub fn get_slot(&self, commitment: Commitment) -> u64 {
        self.slot[commitment.as_idx()].load(Ordering::Acquire)
    }

    pub fn len(&self) -> usize {
        self.map.len()
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
pub(crate) enum Encoding {
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
}

impl Default for Encoding {
    fn default() -> Self {
        Encoding::Default
    }
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct AccountInfo {
    pub lamports: u64,
    pub data: AccountData,
    pub owner: Pubkey,
    pub executable: bool,
    pub rent_epoch: u64,
}

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug, Ord, PartialOrd)]
pub(crate) struct Pubkey([u8; 32]);

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

#[derive(Deserialize, Debug, Hash, Eq, PartialEq, Clone, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub enum Filter {
    Memcmp {
        offset: usize,
        #[serde(deserialize_with = "decode_base58")]
        bytes: SmallVec<[u8; 128]>,
    },
    DataSize(u64),
}

fn decode_base58<'de, D>(de: D) -> Result<SmallVec<[u8; 128]>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct Base58Visitor;
    impl<'de> serde::de::Visitor<'de> for Base58Visitor {
        type Value = SmallVec<[u8; 128]>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("string")
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            use serde::de::Error;
            let mut buf = smallvec![0; 128];
            let len = bs58::decode(v)
                .into(&mut buf)
                .map_err(|_| Error::custom("can't b58decode"))?;
            if len > 128 {
                return Err(Error::custom("bad size"));
            }
            buf.truncate(len);
            Ok(buf)
        }
    }
    de.deserialize_str(Base58Visitor)
}

impl Filter {
    pub(crate) fn matches(&self, data: &AccountData) -> bool {
        match self {
            Filter::DataSize(len) => data.data.len() as u64 == *len,
            Filter::Memcmp { offset, bytes } => {
                let len = bytes.len();
                match data.data.get(*offset..*offset + len) {
                    Some(slice) => slice == &bytes[..len],
                    None => false,
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct AccountData {
    pub data: Bytes,
}

impl AccountData {
    pub fn len(&self) -> usize {
        self.data.len()
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
                let err = || serde::de::Error::custom("can't decode");
                let dat: &str = seq.next_element()?.ok_or_else(|| todo!())?;
                let encoding: &str = seq.next_element()?.ok_or_else(|| todo!())?;
                let data = match encoding {
                    "base58" => bs58::decode(&dat).into_vec().map_err(|_| err())?,
                    "base64" => base64::decode(&dat).map_err(|_| err())?,
                    "base64+zstd" => {
                        let vec = base64::decode(&dat).map_err(|_| err())?;
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
pub(crate) struct AccountContext {
    pub context: SolanaContext,
    pub value: Option<AccountInfo>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct SolanaContext {
    pub slot: u64,
}

#[test]
fn data() {
    let dat = r#"["2UzHM","base58"]"#;
    let data: AccountData = serde_json::from_str(&dat).unwrap();
    println!("{:?}", data);
}

#[test]
fn kek() {
    let dat = r#"{"data":["2UzHM","base58"],"executable":false,"lamports":918720,"owner":"pdRUarXshQQAumQ12xntHo7xppX6Au9NdSskmWpahLJ","rentEpoch":0}"#;
    let data: AccountInfo = serde_json::from_str(&dat).unwrap();
    println!("{:?}", data);
}

#[test]
fn pooq() {
    let dat = r#"{"data":["2UzHM","base58"],"executable":false,"lamports":918720,"owner":"pdRUarXshQQAumQ12xntHo7xppX6Au9NdSskmWpahLJ","rentEpoch":0}"#;
    let val: serde_json::Value = serde_json::from_str(&dat).unwrap();
    let data: AccountInfo = serde_json::from_value(val).unwrap();
    println!("{:?}", data);
}
