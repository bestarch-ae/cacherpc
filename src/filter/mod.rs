use serde::Deserialize;
use smallvec::{smallvec, SmallVec};

use crate::types::AccountData;

#[cfg_attr(test, macro_use)]
mod filters;
#[cfg(test)]
mod tests;
mod tree;

pub use filters::{Filters, NormalizeError};
pub use tree::FilterTree;

type Range = (usize, usize);
type Pattern = SmallVec<[u8; 128]>;

// Reserved for internal use.
// This is ok since memcmp with empty pattern normalizes to "always true"
const RESERVED_RANGE: Range = (usize::MAX, usize::MAX);

#[derive(Deserialize, Debug, Hash, Eq, PartialEq, Clone, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub struct Memcmp {
    pub offset: usize,
    #[serde(deserialize_with = "decode_base58")]
    pub bytes: Pattern,
}

impl Memcmp {
    pub(crate) fn matches(&self, data: &AccountData) -> bool {
        let len = self.bytes.len();
        match data.data.get(self.offset..self.offset + len) {
            Some(slice) => slice == &self.bytes[..len],
            None => false,
        }
    }

    fn range(&self) -> Range {
        (self.offset, self.offset + self.bytes.len())
    }
}

#[derive(Deserialize, Debug, Hash, Eq, PartialEq, Clone, Ord, PartialOrd)]
#[serde(rename_all = "camelCase")]
pub enum Filter {
    DataSize(u64),
    Memcmp(Memcmp),
}

impl Filter {
    pub fn matches(&self, data: &AccountData) -> bool {
        match self {
            Filter::DataSize(len) => data.data.len() as u64 == *len,
            Filter::Memcmp(memcmp) => memcmp.matches(data),
        }
    }
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
