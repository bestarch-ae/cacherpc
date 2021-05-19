use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::{mapref::one::Ref, DashMap};
use serde::{Deserialize, Serialize};

pub(crate) struct ProgramState([Option<HashSet<Pubkey>>; 3]);

impl ProgramState {
    pub fn get(&self, commitment: Commitment) -> Option<&HashSet<Pubkey>> {
        (self.0)[commitment.as_idx()].as_ref()
    }

    pub fn into_accounts(self) -> impl Iterator<Item = Pubkey> {
        std::array::IntoIter::new(self.0)
            .flat_map(|set| set.into_iter())
            .flatten()
    }

    fn insert(&mut self, commitment: Commitment, data: HashSet<Pubkey>) {
        (self.0)[commitment.as_idx()] = Some(data);
    }

    fn add(&mut self, commitment: Commitment, data: Pubkey) {
        if let Some(ref mut keys) = (self.0)[commitment.as_idx()] {
            keys.insert(data);
        } else {
            let mut set = HashSet::new();
            set.insert(data);
            self.insert(commitment, set);
        }
    }
}

impl Default for ProgramState {
    fn default() -> Self {
        ProgramState([None, None, None])
    }
}

#[derive(Clone)]
pub(crate) struct ProgramAccountsDb {
    map: Arc<DashMap<Pubkey, ProgramState>>,
}

impl ProgramAccountsDb {
    pub fn new() -> Self {
        ProgramAccountsDb {
            map: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, key: &Pubkey) -> Option<Ref<'_, Pubkey, ProgramState>> {
        self.map.get(key)
    }

    pub fn insert(&self, key: Pubkey, data: HashSet<Pubkey>, commitment: Commitment) {
        let mut entry = self.map.entry(key).or_default();
        entry.insert(commitment, data);
    }

    pub fn add(&self, key: &Pubkey, data: Pubkey, commitment: Commitment) {
        if let Some(mut state) = self.map.get_mut(&key) {
            state.add(commitment, data);
        }
    }

    pub fn remove(&self, key: &Pubkey) -> Option<ProgramState> {
        self.map.remove(key).map(|(_, state)| state)
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
}

pub(crate) struct AccountState([Option<Account>; 3]);

impl Default for AccountState {
    fn default() -> AccountState {
        AccountState([None, None, None])
    }
}

impl AccountState {
    pub fn get(&self, commitment: Commitment) -> Option<(Option<&AccountInfo>, Slot)> {
        let mut result = None;
        let mut slot = 0;
        for acc in self.0.iter().take(commitment.as_idx() + 1).flatten() {
            if acc.slot >= slot {
                result = Some((acc.data.as_ref(), acc.slot));
                slot = acc.slot;
            }
        }
        result
    }

    fn insert(&mut self, commitment: Commitment, data: AccountContext) {
        (self.0)[commitment.as_idx()] = Some(Account {
            data: data.value,
            slot: data.context.slot,
        })
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

    pub fn insert(&self, key: Pubkey, data: AccountContext, commitment: Commitment) {
        let mut entry = self.map.entry(key).or_default();
        self.update_slot(commitment, data.context.slot);
        entry.insert(commitment, data);
    }

    pub fn remove(&self, key: &Pubkey) {
        self.map.remove(key);
    }

    fn update_slot(&self, commitment: Commitment, val: u64) {
        self.slot[commitment.as_idx()].fetch_max(val, Ordering::AcqRel);
    }

    pub fn get_slot(&self, commitment: Commitment) -> u64 {
        self.slot[commitment.as_idx()].load(Ordering::Acquire)
    }
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone, Eq, PartialEq, Hash)]
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
}

impl Default for Commitment {
    fn default() -> Self {
        Commitment::Finalized
    }
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone)]
pub(crate) enum Encoding {
    #[serde(skip)]
    Default,
    #[serde(rename = "base58")]
    Base58,
    #[serde(rename = "base64")]
    Base64,
    #[serde(rename = "base64+zstd")]
    Base64Zstd,
    // TODO: json parsed
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

#[derive(Hash, Eq, PartialEq, Copy, Clone, Debug)]
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

#[derive(Debug)]
pub(crate) struct AccountData {
    pub data: Bytes,
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
