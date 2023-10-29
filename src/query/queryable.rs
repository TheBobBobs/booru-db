use crate::{Packed, ID, PACKED_SIZE};

use super::util::{size_of_checks, size_of_ids, to_checks, to_ids};

pub fn apply_checks(from: &[Packed], checks: &mut [Packed], inverse: bool) {
    let iter = checks.iter_mut().zip(from.iter());
    if inverse {
        for (check, q_check) in iter {
            *check = !q_check;
        }
    } else {
        for (check, q_check) in iter {
            *check = *q_check;
        }
        if checks.len() > from.len() {
            for check in &mut checks[from.len()..] {
                *check = 0;
            }
        }
    }
}

pub fn apply_ids(from: &[ID], checks: &mut [Packed], inverse: bool) {
    checks.fill(if inverse { Packed::MAX } else { 0 });
    assert_eq!(PACKED_SIZE % 8, 0);
    let ptr = checks.as_mut_ptr() as *mut u8;
    let len = checks.len() * PACKED_SIZE as usize / 8;
    let checks = unsafe { std::slice::from_raw_parts_mut(ptr, len) };
    if inverse {
        for id in from {
            let index = (id / 8) as usize;
            let offset = id % 8;
            if index < checks.len() {
                checks[index] ^= 1 << offset;
            }
        }
    } else {
        for id in from {
            let index = (id / 8) as usize;
            let offset = id % 8;
            if index < checks.len() {
                checks[index] |= 1 << offset;
            }
        }
    }
}

#[derive(Debug)]
pub enum Queryable<'i> {
    Checks(&'i [Packed]),
    ChecksOwned(Vec<Packed>),
    IDs(&'i [ID]),
    IDsOwned(Vec<ID>),
}

impl<'i> From<&'i QueryableOwned> for Queryable<'i> {
    fn from(value: &'i QueryableOwned) -> Self {
        match value {
            QueryableOwned::Checks { checks, .. } => Self::Checks(checks),
            QueryableOwned::IDs { ids } => Self::IDs(ids),
        }
    }
}

impl<'i> Queryable<'i> {
    pub fn run(&self, checks: &mut [Packed], inverse: bool) {
        match self {
            Queryable::Checks(from) => apply_checks(from, checks, inverse),
            Queryable::ChecksOwned(from) => apply_checks(from, checks, inverse),
            Queryable::IDs(from) => apply_ids(from, checks, inverse),
            Queryable::IDsOwned(from) => apply_ids(from, checks, inverse),
        };
    }

    pub fn and(&self, checks: &mut [Packed], inverse: bool) {
        match self {
            Queryable::Checks(mask) => {
                let iter = checks.iter_mut().zip(mask.iter());
                if inverse {
                    for (c, m) in iter {
                        *c &= !m;
                    }
                } else {
                    for (c, m) in iter {
                        *c &= m;
                    }
                }
            }
            Queryable::ChecksOwned(mask) => {
                let iter = checks.iter_mut().zip(mask.iter());
                if inverse {
                    for (c, m) in iter {
                        *c &= !m;
                    }
                } else {
                    for (c, m) in iter {
                        *c &= m;
                    }
                }
            }
            Queryable::IDs(ids) => {
                let mut mask = Vec::from_iter(checks.iter().copied());
                apply_ids(ids, &mut mask, inverse);
                let iter = checks.iter_mut().zip(mask.iter());
                for (c, m) in iter {
                    *c &= m;
                }
            }
            Queryable::IDsOwned(ids) => {
                let mut mask = Vec::from_iter(checks.iter().copied());
                apply_ids(ids, &mut mask, inverse);
                let iter = checks.iter_mut().zip(mask.iter());
                for (c, m) in iter {
                    *c &= m;
                }
            }
        }
    }

    pub fn or(&self, checks: &mut [Packed], inverse: bool) {
        match self {
            Queryable::Checks(mask) => {
                let iter = checks.iter_mut().zip(mask.iter());
                if inverse {
                    for (c, m) in iter {
                        *c |= !m;
                    }
                    for c in checks.iter_mut().skip(mask.len()) {
                        *c = Packed::MAX;
                    }
                } else {
                    for (c, m) in iter {
                        *c |= m;
                    }
                }
            }
            Queryable::ChecksOwned(mask) => {
                let iter = checks.iter_mut().zip(mask.iter());
                if inverse {
                    for (c, m) in iter {
                        *c |= !m;
                    }
                    for c in checks.iter_mut().skip(mask.len()) {
                        *c = Packed::MAX;
                    }
                } else {
                    for (c, m) in iter {
                        *c |= m;
                    }
                }
            }
            Queryable::IDs(ids) => {
                if inverse {
                    let mut mask = checks.to_vec();
                    apply_ids(ids, &mut mask, inverse);
                    let iter = checks.iter_mut().zip(mask.iter());
                    for (c, m) in iter {
                        *c |= m;
                    }
                } else {
                    for id in ids.iter() {
                        let index = (id / PACKED_SIZE) as usize;
                        let offset = id % PACKED_SIZE;
                        if index < checks.len() {
                            checks[index] |= 1 << offset;
                        }
                    }
                }
            }
            Queryable::IDsOwned(ids) => {
                if inverse {
                    let mut mask = checks.to_vec();
                    apply_ids(ids, &mut mask, inverse);
                    let iter = checks.iter_mut().zip(mask.iter());
                    for (c, m) in iter {
                        *c |= m;
                    }
                } else {
                    for id in ids.iter() {
                        let index = (id / PACKED_SIZE) as usize;
                        let offset = id % PACKED_SIZE;
                        if index < checks.len() {
                            checks[index] |= 1 << offset;
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum QueryableOwned {
    Checks { checks: Vec<Packed>, matched: usize },
    IDs { ids: Vec<ID> },
}

impl Default for QueryableOwned {
    fn default() -> Self {
        Self::IDs { ids: Vec::new() }
    }
}

impl From<Vec<Packed>> for QueryableOwned {
    fn from(checks: Vec<Packed>) -> Self {
        let matched = checks.iter().map(|c| c.count_ones()).sum::<u32>() as usize;
        Self::Checks { checks, matched }
    }
}

impl From<Vec<ID>> for QueryableOwned {
    fn from(ids: Vec<ID>) -> Self {
        Self::IDs { ids }
    }
}

impl QueryableOwned {
    pub fn run(&self, checks: &mut [Packed], inverse: bool) {
        Queryable::from(self).run(checks, inverse);
    }

    pub fn contains(&self, id: ID) -> bool {
        match self {
            QueryableOwned::Checks { checks, .. } => {
                let index = (id / PACKED_SIZE) as usize;
                let offset = id % PACKED_SIZE;
                if index >= checks.len() {
                    false
                } else {
                    checks[index] & (1 << offset) != 0
                }
            }
            QueryableOwned::IDs { ids } => ids.binary_search(&id).is_ok(),
        }
    }

    pub fn matched(&self) -> usize {
        match self {
            QueryableOwned::Checks { matched, .. } => *matched,
            QueryableOwned::IDs { ids } => ids.len(),
        }
    }

    /// Safe if id is higher than any id self has.
    pub fn insert_unchecked(&mut self, id: ID) {
        match self {
            QueryableOwned::Checks { checks, matched } => {
                let index = (id / PACKED_SIZE) as usize;
                let offset = id % PACKED_SIZE;
                while index >= checks.len() {
                    checks.push(0);
                }
                *matched += 1;
                checks[index] |= 1 << offset;
            }
            QueryableOwned::IDs { ids } => {
                ids.push(id);
            }
        }
    }

    pub fn insert(&mut self, id: ID) {
        match self {
            QueryableOwned::Checks { checks, matched } => {
                let index = (id / PACKED_SIZE) as usize;
                let offset = id % PACKED_SIZE;
                while index >= checks.len() {
                    checks.push(0);
                }
                if (checks[index] & (1 << offset)) == 0 {
                    *matched += 1;
                    checks[index] |= 1 << offset;
                }
            }
            QueryableOwned::IDs { ids } => {
                if let Err(index) = ids.as_slice().binary_search(&id) {
                    ids.insert(index, id);
                }
            }
        }
        self.check_and_convert();
    }

    pub fn remove(&mut self, id: ID) {
        match self {
            QueryableOwned::Checks { checks, matched } => {
                let index = (id / PACKED_SIZE) as usize;
                let offset = id % PACKED_SIZE;
                if index < checks.len() && (checks[index] & (1 << offset)) != 0 {
                    *matched -= 1;
                    checks[index] &= !(1 << offset);
                }
            }
            QueryableOwned::IDs { ids } => {
                if let Ok(index) = ids.as_slice().binary_search(&id) {
                    ids.remove(index);
                }
            }
        }
        self.check_and_convert();
    }

    pub fn check_and_convert(&mut self) {
        let matched = self.matched();
        let max_id = match self {
            QueryableOwned::Checks { checks, .. } => {
                if checks.is_empty() {
                    return;
                }
                checks.len() as u32 * PACKED_SIZE
            }
            QueryableOwned::IDs { ids } => {
                if ids.is_empty() {
                    return;
                }
                *ids.last().unwrap()
            }
        };
        let checks_size = size_of_checks(max_id);
        let ids_size = size_of_ids(matched);
        match self {
            QueryableOwned::Checks { checks, .. } => {
                if checks_size > ids_size + (1_024 * 64 * 8) {
                    let ids = to_ids(checks);
                    *self = Self::IDs { ids };
                }
            }
            QueryableOwned::IDs { ids } => {
                if ids_size > checks_size + (1_024 * 64 * 8) {
                    let checks = to_checks(ids);
                    *self = Self::Checks {
                        checks,
                        matched: ids.len(),
                    };
                }
            }
        }
    }
}
