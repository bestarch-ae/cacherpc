use std::cmp::Eq;
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;

use dashmap::{mapref::entry::Entry, DashMap};
use futures_util::future::{ready, Either};
use tokio::sync::Notify;

pub use guard::LockGuard;
pub use r#ref::{Ref, RefMut};

pub struct AsyncMap<K, V> {
    map: Arc<DashMap<K, DataOrLock<V>>>,
}

/// Type that represents the current state of an entry.
/// - `Data` means there's already data
/// - `Lock` means some thread has already locked the entry and others must wait to aacquire the data
enum DataOrLock<T> {
    Data(T),
    Lock(Arc<Notify>),
}

impl<T> DataOrLock<T> {
    pub fn is_lock(&self) -> bool {
        matches!(self, DataOrLock::Lock(..))
    }
}

/// The result of [`AsyncMap::get_or_lock`].
pub enum MaybeLocked<K: Eq + Hash, T, F> {
    /// The desired entry is either locked or exists
    /// and this variant holds the future that resolves to the Ref to the value
    Data(F),
    /// The desired entry does not exist and the caller must insert it with the provided [`LockGuard`]
    Locked(LockGuard<K, T>),
}

impl<K: Eq + Hash + Clone, V> AsyncMap<K, V> {
    pub fn get_or_lock(
        &self,
        key: K,
    ) -> MaybeLocked<K, V, impl Future<Output = Option<Ref<'_, K, V>>>> {
        let ref_ = self.map.get(&key);

        // One may want to "dedup" the following code by using the Entry API from the start
        // which is a slippery slope since entries acquire write lock.
        let notify = match ref_.as_deref() {
            Some(DataOrLock::Data(..)) => {
                let data = Some(Ref::checked_data(ref_.expect("is some")).expect("is data"));

                return MaybeLocked::Data(Either::Left(ready(data)));
            }
            Some(DataOrLock::Lock(notify)) => Arc::clone(&notify),
            None => match self.map.entry(key.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(DataOrLock::Lock(Arc::new(Notify::new())));
                    return MaybeLocked::Locked(LockGuard::new(Arc::clone(&self.map), key));
                }
                // Remaining two branches mean we're extremely unlucky
                Entry::Occupied(entry) => match entry.get() {
                    DataOrLock::Data(..) => {
                        let data =
                            Some(Ref::checked_data(entry.into_ref().downgrade()).expect("is data"));
                        return MaybeLocked::Data(Either::Left(ready(data)));
                    }
                    DataOrLock::Lock(notify) => Arc::clone(&notify),
                },
            },
        };

        drop(ref_);

        // TODO: Implement reacquiring
        let fut = async move {
            notify.notified().await;
            // We got notified, lets get the data
            self.map.get(&key).map(Ref::checked_data).flatten()
        };
        MaybeLocked::Data(Either::Right(fut))
    }

    pub fn insert(&self, key: K, value: V) -> Option<V> {
        let previous_value = self.map.insert(key, DataOrLock::Data(value));
        if let Some(value) = previous_value {
            match value {
                DataOrLock::Lock(notify) => {
                    notify.notify();
                    None
                }
                DataOrLock::Data(data) => Some(data),
            }
        } else {
            None
        }
    }

    pub fn get(&self, key: &K) -> Option<Ref<'_, K, V>> {
        self.map.get(key).map(Ref::checked_data).flatten()
    }

    pub fn get_mut(&self, key: &K) -> Option<RefMut<'_, K, V>> {
        self.map.get_mut(key).map(RefMut::checked_data).flatten()
    }

    pub fn remove(&self, key: &K) -> Option<(K, V)> {
        if let Some((key, value)) = self.map.remove(key) {
            match value {
                DataOrLock::Data(data) => Some((key, data)),
                DataOrLock::Lock(notify) => {
                    notify.notify();
                    None
                }
            }
        } else {
            None
        }
    }
}

mod r#ref {
    use std::cmp::Eq;
    use std::hash::Hash;
    use std::ops::{Deref, DerefMut};

    use dashmap::mapref::one::{Ref as DashRef, RefMut as DashRefMut};

    use super::DataOrLock;

    pub struct Ref<'a, K, V> {
        ref_: DashRef<'a, K, DataOrLock<V>>,
    }

    impl<'a, K: Eq + Hash, V> Ref<'a, K, V> {
        // TODO: consider making an unsafe `new` method
        pub(super) fn checked_data(ref_: DashRef<'a, K, DataOrLock<V>>) -> Option<Self> {
            match ref_.value() {
                DataOrLock::Data(..) => Some(Self { ref_ }),
                DataOrLock::Lock(..) => None,
            }
        }

        pub fn value(&self) -> &V {
            match self.ref_.value() {
                DataOrLock::Data(data) => data,
                DataOrLock::Lock(..) => {
                    unreachable!("AsyncMap Ref must be created only from DataOrLock::Data ref")
                }
            }
        }
    }

    impl<'a, K: Eq + Hash, V> Deref for Ref<'a, K, V> {
        type Target = V;
        fn deref(&self) -> &Self::Target {
            self.value()
        }
    }

    pub struct RefMut<'a, K, V> {
        ref_: DashRefMut<'a, K, DataOrLock<V>>,
    }

    impl<'a, K: Eq + Hash, V> RefMut<'a, K, V> {
        // TODO: consider making an unsafe `new` method
        pub(super) fn checked_data(ref_: DashRefMut<'a, K, DataOrLock<V>>) -> Option<Self> {
            match ref_.value() {
                DataOrLock::Data(..) => Some(Self { ref_ }),
                DataOrLock::Lock(..) => None,
            }
        }

        pub fn value(&self) -> &V {
            match self.ref_.value() {
                DataOrLock::Data(data) => data,
                DataOrLock::Lock(..) => {
                    unreachable!("AsyncMap RefMut must be created only from DataOrLock::Data ref")
                }
            }
        }

        pub fn value_mut(&mut self) -> &mut V {
            match self.ref_.value_mut() {
                DataOrLock::Data(data) => data,
                DataOrLock::Lock(..) => {
                    unreachable!("AsyncMap RefMut must be created only from DataOrLock::Data ref")
                }
            }
        }
    }

    impl<'a, K: Eq + Hash, V> Deref for RefMut<'a, K, V> {
        type Target = V;

        fn deref(&self) -> &Self::Target {
            self.value()
        }
    }

    impl<'a, K: Eq + Hash, V> DerefMut for RefMut<'a, K, V> {
        fn deref_mut(&mut self) -> &mut Self::Target {
            self.value_mut()
        }
    }
}

mod guard {
    use std::cmp::Eq;
    use std::hash::Hash;
    use std::sync::Arc;

    use dashmap::DashMap;

    use super::DataOrLock;

    /// Represents a locked [`super::AsyncMap`] entry.
    pub struct LockGuard<K: Eq + Hash, V> {
        state: LockGuardState<K, V>,
    }

    /// This is a separate enum, since we want to have [`Drop`] implementation for [`LockGuard`]
    enum LockGuardState<K: Eq + Hash, V> {
        Locked {
            map: Arc<DashMap<K, DataOrLock<V>>>,
            key: K,
        },
        Fulfilled,
    }

    impl<K: Eq + Hash, V> LockGuard<K, V> {
        pub(super) fn new(map: Arc<DashMap<K, DataOrLock<V>>>, key: K) -> Self {
            Self {
                state: LockGuardState::Locked { map, key },
            }
        }

        pub fn insert(mut self, data: V) {
            let state = std::mem::replace(&mut self.state, LockGuardState::Fulfilled);
            if let LockGuardState::Locked { map, key } = state {
                map.insert(key, DataOrLock::Data(data));
            }
            // TODO?: should we panic on else?
        }
    }

    impl<K: Eq + Hash, V> Drop for LockGuard<K, V> {
        fn drop(&mut self) {
            let state = std::mem::replace(&mut self.state, LockGuardState::Fulfilled);
            if let LockGuardState::Locked { map, key } = state {
                let item = map.remove_if(&key, |_, v| v.is_lock());
                if let Some((_, DataOrLock::Lock(notify))) = item {
                    notify.notify();
                }
            }
        }
    }
}
