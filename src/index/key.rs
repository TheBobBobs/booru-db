use std::{borrow::Borrow, collections::HashMap, hash::Hash};

use crate::{
    query::{Queryable, QueryableOwned},
    ID,
};

#[derive(Default)]
pub struct KeyIndexLoader<K> {
    items: HashMap<K, QueryableOwned>,
}

impl<K: Clone + Eq + Hash> KeyIndexLoader<K> {
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }

    pub fn add(&mut self, id: ID, key: &K) {
        if !self.items.contains_key(key) {
            self.items.insert(key.clone(), QueryableOwned::default());
        }
        let queryable = self.items.get_mut(key).unwrap();
        queryable.insert_unchecked(id);
    }

    pub fn load(mut self) -> KeyIndex<K> {
        for queryable in self.items.values_mut() {
            queryable.check_and_convert();
        }
        KeyIndex { items: self.items }
    }
}

#[derive(Default)]
pub struct KeyIndex<K: Eq + Hash> {
    pub items: HashMap<K, QueryableOwned>,
}

impl<K: Clone + Eq + Hash> KeyIndex<K> {
    pub fn loader() -> KeyIndexLoader<K> {
        KeyIndexLoader::new()
    }

    #[inline(always)]
    pub fn get<'i, Q: ?Sized>(&'i self, k: &Q) -> Option<Queryable<'i>>
    where
        K: Borrow<Q>,
        Q: Hash + Eq,
    {
        self.items.get(k).map(|queryable| queryable.into())
    }

    pub fn insert(&mut self, id: ID, key: &K) {
        if !self.items.contains_key(key) {
            self.items.insert(key.clone(), QueryableOwned::default());
        }
        let queryable = self.items.get_mut(key).unwrap();
        queryable.insert(id);
    }

    pub fn remove(&mut self, id: ID, key: &K) {
        if let Some(queryable) = self.items.get_mut(key) {
            queryable.remove(id);
            if queryable.matched() == 0 {
                self.items.remove(key);
            }
        }
    }

    pub fn update(&mut self, id: ID, old: &K, new: &K) {
        if old == new {
            return;
        }
        self.remove(id, old);
        self.insert(id, new);
    }
}
