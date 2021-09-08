use serde::Deserialize;
use smallvec::SmallVec;
use thiserror::Error;

use crate::types::AccountData;

use super::{Filter, Memcmp};

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

    pub fn matches(&self, data: &AccountData) -> bool {
        if self
            .data_size
            .map_or(false, |size| data.len() as u64 != size)
        {
            return false;
        }

        self.memcmp.iter().all(|memcmp| memcmp.matches(data))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize() {
        // Multiple datasizes always normalize to false
        assert_eq!(
            filters!(@size 420, @size 69, @size 420),
            Err(NormalizeError::DuplicateDataSize)
        );

        // Equal datasizes are stripped
        assert_eq!(
            filters!(@size 420, @size 420).unwrap(),
            filters!(@size 420).unwrap(),
        );

        // Order is determined
        let filters1 =
            filters!(@cmp 13: [2, 4, 5, 7, 2, 4], @size 42, @cmp 1098: [3, 4, 3, 3, 3]).unwrap();
        let filters2 =
            filters!(@cmp 1098: [3, 4, 3, 3, 3], @cmp 13: [2, 4, 5, 7, 2, 4], @size 42).unwrap();
        let filters3 =
            filters!(@size 42, @cmp 13: [2, 4, 5, 7, 2, 4], @cmp 1098: [3, 4, 3, 3, 3]).unwrap();

        assert_eq!(filters1, filters2);
        assert_eq!(filters2, filters3);
        assert_eq!(filters1, filters3);
    }
}
