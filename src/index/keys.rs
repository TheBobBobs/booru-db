use std::{borrow::Borrow, hash::Hash};

use crate::{
    query::{Queryable, QueryableOwned},
    ID,
};

#[derive(Default)]
pub struct KeysIndexLoader<K> {
    items: fxhash::FxHashMap<K, QueryableOwned>,
}

impl<'k, K: Clone + Eq + Hash + 'k> KeysIndexLoader<K> {
    pub fn new() -> Self {
        Self {
            items: fxhash::FxHashMap::default(),
        }
    }

    pub fn add(&mut self, id: ID, keys: impl IntoIterator<Item = &'k K>) {
        for key in keys.into_iter() {
            if !self.items.contains_key(key) {
                self.items.insert(key.clone(), QueryableOwned::default());
            }
            let queryable = self.items.get_mut(key).unwrap();
            queryable.insert_unchecked(id);
        }
    }

    pub fn load(mut self) -> KeysIndex<K> {
        for queryable in self.items.values_mut() {
            queryable.check_and_convert();
        }
        KeysIndex { items: self.items }
    }
}

pub struct KeysIndex<K: Eq + Hash> {
    pub items: fxhash::FxHashMap<K, QueryableOwned>,
}

impl<'k, K: Clone + Eq + Hash + 'k> KeysIndex<K> {
    pub fn loader() -> KeysIndexLoader<K> {
        KeysIndexLoader::new()
    }

    #[inline(always)]
    pub fn get<'i, Q: ?Sized>(&'i self, k: &Q) -> Option<Queryable<'i>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.items.get(k).map(|queryable| queryable.into())
    }

    #[inline(always)]
    pub fn matched<Q: ?Sized>(&self, k: &Q) -> Option<usize>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.items.get(k).map(|queryable| queryable.matched())
    }

    pub fn insert(&mut self, id: ID, keys: impl IntoIterator<Item = &'k K>) {
        for key in keys.into_iter() {
            if !self.items.contains_key(key) {
                self.items.insert(key.clone(), QueryableOwned::default());
            }
            let queryable = self.items.get_mut(key).unwrap();
            queryable.insert(id);
        }
    }

    pub fn remove(&mut self, id: ID, keys: impl IntoIterator<Item = &'k K>) {
        for key in keys.into_iter() {
            if let Some(queryable) = self.items.get_mut(key) {
                queryable.remove(id);
                if queryable.matched() == 0 {
                    self.items.remove(key);
                }
            }
        }
    }

    pub fn update(&mut self, id: ID, old: &[K], new: &[K]) {
        if old == new {
            return;
        }
        let old = fxhash::FxHashSet::from_iter(old);
        let new = fxhash::FxHashSet::from_iter(new);
        self.remove(id, old.difference(&new).copied());
        self.insert(id, new.difference(&old).copied());
    }
}
