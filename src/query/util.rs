use crate::{Packed, Queryable, ID, PACKED_SIZE};

use super::{Item, Query};

pub fn size_of_checks(max_id: ID) -> usize {
    let max_id = max_id as usize;
    let bits_per_id = std::mem::size_of::<Packed>() * 8 / PACKED_SIZE as usize;
    max_id * bits_per_id
}

pub fn size_of_ids(len: usize) -> usize {
    let bits_per_id = std::mem::size_of::<ID>() * 8;
    len * bits_per_id
}

pub fn to_checks(ids: &[ID]) -> Vec<Packed> {
    if ids.is_empty() {
        return Vec::new();
    }
    let capacity = ((*ids.last().unwrap_or(&0) + 1) as f32 / PACKED_SIZE as f32).ceil() as usize;
    let mut checks = Vec::with_capacity(capacity);
    checks.extend((0..capacity).map(|_| 0));
    Queryable::IDs(ids).apply(&mut checks, false);
    checks
}

pub fn to_ids(checks: &[Packed]) -> Vec<ID> {
    let capacity = checks.iter().map(|c| c.count_ones()).sum::<u32>() as usize;
    let mut ids = Vec::with_capacity(capacity);
    for (index, check) in checks.iter().enumerate() {
        if *check == 0 {
            continue;
        }
        let index = index as u32 * PACKED_SIZE;
        for offset in 0..PACKED_SIZE {
            if check & (1 << offset) != 0 {
                let id = index + offset;
                ids.push(id);
            }
        }
    }
    ids
}

impl<T> Query<T> {
    pub fn new(item: Item<T>, inverse: bool) -> Self {
        Self { item, inverse }
    }

    pub fn is_empty(&self) -> bool {
        match &self.item {
            Item::AndChain(items) | Item::OrChain(items) => items.is_empty(),
            Item::Single(_) => false,
        }
    }

    pub fn item_count(&self) -> usize {
        match &self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                1 + items.iter().map(|item| item.item_count()).sum::<usize>()
            }
            Item::Single(_) => 1,
        }
    }

    pub fn tags(&self) -> Vec<(&T, bool)> {
        let mut tags = Vec::new();
        match &self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                for item in items {
                    tags.extend(item.tags());
                }
            }
            Item::Single(tag) => tags.push((tag, self.inverse)),
        }
        tags
    }
}

impl<T: Clone> Query<T> {
    pub fn try_map<F: Clone + Fn(&T, bool) -> Option<Query<R>>, R>(
        &self,
        f: F,
    ) -> Result<Query<R>, Vec<T>> {
        self.inner_try_map(f, self.inverse)
    }

    fn inner_try_map<F: Clone + Fn(&T, bool) -> Option<Query<R>>, R>(
        &self,
        f: F,
        mut inverse: bool,
    ) -> Result<Query<R>, Vec<T>> {
        inverse ^= self.inverse;
        match &self.item {
            Item::AndChain(query_items) => {
                let mut missing = Vec::new();
                let items: Vec<Query<R>> = query_items
                    .iter()
                    .filter_map(|item| {
                        let item = item.inner_try_map(f.clone(), inverse);
                        match item {
                            Ok(item) => Some(item),
                            Err(m) => {
                                missing.extend(m);
                                None
                            }
                        }
                    })
                    .collect();
                if !missing.is_empty() {
                    Err(missing)
                } else {
                    Ok(Query {
                        item: Item::AndChain(items),
                        inverse: self.inverse,
                    })
                }
            }
            Item::OrChain(query_items) => {
                let len = query_items.len();
                let mut missing = Vec::new();
                let items: Vec<Query<R>> = query_items
                    .iter()
                    .filter_map(|item| {
                        let item = item.inner_try_map(f.clone(), inverse);
                        match item {
                            Ok(item) => Some(item),
                            Err(m) => {
                                missing.extend(m);
                                None
                            }
                        }
                    })
                    .collect();
                if items.is_empty() && len != 0 {
                    Err(missing)
                } else {
                    Ok(Query {
                        item: Item::OrChain(items),
                        inverse: self.inverse,
                    })
                }
            }
            Item::Single(tag) => {
                if let Some(item) = f(tag, self.inverse) {
                    Ok(item)
                } else if inverse {
                    Err(Vec::new())
                } else {
                    Err(vec![tag.clone()])
                }
            }
        }
    }
}
