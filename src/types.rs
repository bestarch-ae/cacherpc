use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::{mapref::one::Ref, DashMap};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub(crate) struct AccountsDb {
    map: Arc<DashMap<Pubkey, AccountState>>,
    slot: Arc<[AtomicU64; 3]>,
}

type Slot = u64;

struct Account {
    data: Option<AccountInfo>,
    #[allow(dead_code)] // TODO
    slot: Slot,
}

pub(crate) struct AccountState([Option<Account>; 3]);

impl Default for AccountState {
    fn default() -> AccountState {
        AccountState([None, None, None])
    }
}

impl AccountState {
    pub fn get(&self, commitment: Commitment) -> Option<Option<&AccountInfo>> {
        let mut result = None;
        for acc in self.0.iter().take(commitment.as_idx() + 1) {
            if acc.is_some() {
                result = acc.as_ref().map(|state| state.data.as_ref());
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

    pub fn remove(&self, key: &Pubkey, _commitment: Commitment) {
        self.map.remove(key);
    }

    fn update_slot(&self, commitment: Commitment, val: u64) {
        self.slot[commitment.as_idx()].fetch_max(val, Ordering::AcqRel);
    }
}

#[derive(Clone)]
pub(crate) struct AtomicSlot(Arc<AtomicU64>);

impl Default for AtomicSlot {
    fn default() -> Self {
        AtomicSlot(Arc::new(AtomicU64::new(1)))
    }
}

impl AtomicSlot {
    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Acquire)
    }

    pub fn update(&self, value: u64) {
        self.0.fetch_max(value, Ordering::AcqRel);
    }
}

#[derive(Serialize, Debug, Deserialize, Copy, Clone, Eq, PartialEq)]
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

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("string")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                use serde::de::Error;
                let mut buf = [0; 32];
                bs58::decode(v)
                    .into(&mut buf)
                    .map_err(|_| Error::custom("can't b58decode"))?;
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

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
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
