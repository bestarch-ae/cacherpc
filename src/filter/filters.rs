use serde::{ser::SerializeSeq, Deserialize, Serialize};
use smallvec::SmallVec;
use thiserror::Error;

use crate::types::AccountData;

use super::{Filter, Memcmp, RESERVED_RANGE};

/// Filter container that guarantees filters do not conflict with each other
/// and determines the application order for each filter combination.  
#[derive(Deserialize, Debug, Hash, Eq, PartialEq, Clone, Ord, PartialOrd)]
pub struct Filters {
    pub(super) data_size: Option<u64>,
    pub(super) memcmp: SmallVec<[Memcmp; 2]>,
}

#[derive(Debug, Error, PartialEq)]
pub enum NormalizeError {
    #[error("duplicate data size")]
    DuplicateDataSize,
    #[error("non equal memcmp into one range")]
    ConflictingMemcmp,
    #[error("empty filter vec")]
    Empty,
}

impl Filters {
    pub fn new_normalized<T>(filters: T) -> Result<Self, NormalizeError>
    where
        T: IntoIterator<Item = Filter>,
    {
        use NormalizeError::*;

        let mut amount = 0;
        let mut data_size = None;
        let mut memcmp_vec = SmallVec::<[Memcmp; 2]>::new();

        for filter in filters {
            amount += 1;
            match filter {
                Filter::DataSize(size) => {
                    // There is no point filtering for two different sizes
                    if data_size.replace(size).map_or(false, |old| old != size) {
                        return Err(DuplicateDataSize);
                    }
                }
                // TODO: check that overlapping ranges match
                Filter::Memcmp(new) => {
                    if new.bytes.is_empty() || new.range() == RESERVED_RANGE {
                        const _: () = {
                            // Just to statically assert that reserved range is empty
                            let _: [u8; RESERVED_RANGE.1 - RESERVED_RANGE.0] = [];
                        };
                        // This is always true
                        continue;
                    }

                    let same_range = memcmp_vec
                        .iter()
                        .find(|filter| filter.range() == new.range());
                    match same_range {
                        Some(old)  if old.bytes != new.bytes => return Err(ConflictingMemcmp),
                        Some(_) /* if old.bytes == new.bytes */ => (),
                        None => memcmp_vec.push(new),
                    }
                }
            }
        }

        if amount == 0 {
            return Err(NormalizeError::Empty);
        }

        memcmp_vec.sort_unstable();

        Ok(Self {
            data_size,
            memcmp: memcmp_vec,
        })
    }

    #[inline]
    pub(crate) fn memcmp_len(&self) -> usize {
        self.memcmp.len()
    }

    /// Check whether one filter (self), will include all the
    /// data of other filter, because it's less restrictive
    #[inline]
    pub fn is_proper_superset_of(&self, other: &Self) -> bool {
        match self.memcmp.len() {
            // if memcmp is empty, then check whether dataSize equals, if
            // they do, then self will include all the data of other filter
            0 => self.data_size == other.data_size,
            // if memcmp has just one element, then we should check
            // whether it's contained within the other's memcmp
            1 => {
                self.data_size == other.data_size
                    && self
                        .memcmp
                        .first()
                        .map(|mmcp| other.memcmp.contains(mmcp))
                        .unwrap_or_default()
            }
            // if the filter has 2 or more memcmp, then it's impossible
            // that it's a proper superset of the any other filter
            _ => false,
        }
    }

    /// Check whether two filters have an intersection, i.e. filter which will include,
    /// the union of the data of both filters (because it's less restrictive)
    pub fn intersection(&self, other: &Self) -> Option<Self> {
        if self.data_size != other.data_size {
            return None;
        }
        for m in self.memcmp.iter() {
            if other.memcmp.contains(m) {
                // return the first memcmp that is contained within both filters
                let mut memcmp = SmallVec::<[Memcmp; 2]>::new();
                memcmp.push(m.clone());
                return Some(Self {
                    data_size: self.data_size,
                    memcmp,
                });
            }
        }
        None
    }

    pub fn matches(&self, data: &AccountData) -> bool {
        if self
            .data_size
            .map_or(false, |size| data.len() as u64 != size)
        {
            return false;
        }

        self.memcmp.iter().all(|memcmp| memcmp.matches(data))
    }

    #[inline]
    fn len(&self) -> usize {
        let mut len = 0;
        if self.data_size.is_some() {
            len += 1;
        }
        len += self.memcmp.len();
        len
    }
}

impl Serialize for Filters {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(self.len()))?;
        if let Some(ds) = self.data_size {
            let filter = Filter::DataSize(ds);
            seq.serialize_element(&filter)?;
        }
        for m in &self.memcmp {
            let filter = Filter::Memcmp(m.clone());
            seq.serialize_element(&filter)?;
        }
        seq.end()
    }
}

#[cfg(test)]
macro_rules! filters {
    (@parse @cmp $offset:literal: [$($byte:literal),*]) => {
        $crate::filter::Filter::Memcmp(Memcmp {
            offset: $offset,
            bytes: smallvec::smallvec![$($byte),*],
        })
    };
    (@parse @size $datasize:literal) => { $crate::filter::Filter::DataSize($datasize) };
    ($(@$tag:ident $arg:literal $(: [$($add_args:literal),*])? ),*) => {{
        let filters = vec![$(
            filters!(@parse @$tag $arg $(: [$($add_args),*])?)
        ),*];
        $crate::filter::Filters::new_normalized(filters)
    }};
}
