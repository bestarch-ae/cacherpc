use std::collections::HashMap;

use serde::Deserialize;
use smallvec::{smallvec, SmallVec};

use crate::types::AccountData;

#[cfg_attr(test, macro_use)]
mod filters;
mod tree;

pub(crate) use filters::{Filters, NormalizeError};
pub(crate) use tree::FilterTree;

type Range = (usize, usize);
type Pattern = SmallVec<[u8; 128]>;

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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn filters_order() {
        let f1 = Filter::Memcmp(Memcmp {
            offset: 1,
            bytes: SmallVec::new(),
        });
        let f2 = Filter::DataSize(0);
        assert!(f2 < f1);
    }

    #[test]
    fn deserialize_filters() {
        let filter: Filter = serde_json::from_str(
            r#"{"memcmp":{"offset":13,"bytes":"HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1"}}"#,
        )
        .unwrap();
        let expected = Filter::Memcmp(Memcmp {
            offset: 13,
            bytes: smallvec::smallvec![
                245, 59, 247, 123, 252, 249, 77, 70, 227, 252, 215, 248, 153, 192, 98, 123, 28,
                232, 159, 173, 44, 138, 177, 107, 137, 62, 126, 139, 186, 244, 124, 90
            ],
        });
        assert_eq!(filter, expected);

        let filter: Filter = serde_json::from_str(r#"{"dataSize":42069}"#).unwrap();
        let expected = Filter::DataSize(42069);
        assert_eq!(filter, expected);
    }
}
