use rand::{thread_rng, Rng};

use crate::{Packed, ID, PACKED_SIZE};

const CHECKS_PER_CHUNK: u32 = 10;
const CHECKS_CHUNK_SIZE: u32 = CHECKS_PER_CHUNK * PACKED_SIZE;

#[derive(Clone, Debug)]
pub struct QueryResult {
    checks: Vec<Packed>,
    matched: usize,

    // num of matches for every CHECKS_CHUNK_SIZE IDs
    // [0..640, 640..1280]
    match_counts: Vec<u32>,
}

impl QueryResult {
    pub fn new(checks: Vec<Packed>) -> Self {
        let capacity = (checks.len() as f32 / CHECKS_PER_CHUNK as f32).ceil() as usize;
        let mut match_counts = Vec::with_capacity(capacity);

        let mut matched = 0;
        for counts_index in 0..capacity {
            let mut matches = 0;
            let start = counts_index * CHECKS_PER_CHUNK as usize;
            for check in checks.iter().skip(start).take(CHECKS_PER_CHUNK as usize) {
                matches += check.count_ones();
            }
            matched += matches;
            match_counts.push(matches);
        }

        Self {
            checks,
            matched: matched as usize,
            match_counts,
        }
    }

    #[inline(always)]
    pub fn contains(&self, id: ID) -> bool {
        let index = (id / PACKED_SIZE) as usize;
        let offset = id % PACKED_SIZE;
        if index >= self.checks.len() {
            return false;
        }
        self.checks[index] & (1 << offset) != 0
    }

    #[inline(always)]
    pub fn checks(&self) -> &Vec<Packed> {
        &self.checks
    }

    #[inline(always)]
    pub fn matched(&self) -> usize {
        self.matched
    }

    pub fn insert(&mut self, id: ID) {
        let index = (id / PACKED_SIZE) as usize;
        let offset = id % PACKED_SIZE;
        while self.checks.len() <= index {
            self.checks.push(0);
        }
        if self.checks[index] & (1 << offset) == 0 {
            self.checks[index] |= 1 << offset;
            let counts_index = index / CHECKS_PER_CHUNK as usize;
            while self.match_counts.len() <= counts_index {
                self.match_counts.push(0);
            }
            self.match_counts[counts_index] += 1;
            self.matched += 1;
        }
    }

    pub fn remove(&mut self, id: ID) {
        let index = (id / PACKED_SIZE) as usize;
        let offset = id % PACKED_SIZE;
        if index >= self.checks.len() {
            return;
        }
        if self.checks[index] & (1 << offset) != 0 {
            self.checks[index] ^= 1 << offset;
            self.match_counts[index / CHECKS_PER_CHUNK as usize] -= 1;
            self.matched -= 1;
        }
    }

    pub fn get_match(&self, index: u32) -> Option<ID> {
        if index >= self.matched as u32 {
            return None;
        }
        let mut ids_found = 0;
        for (count_index, count) in self.match_counts.iter().enumerate() {
            if ids_found + count > index {
                let checks_offset = count_index * CHECKS_CHUNK_SIZE as usize / PACKED_SIZE as usize;
                for (check_index, check) in self.checks[checks_offset..].iter().enumerate() {
                    let ones = check.count_ones();
                    if ids_found + ones <= index {
                        ids_found += ones;
                        continue;
                    }
                    for offset in 0..PACKED_SIZE {
                        if check & (1 << offset) != 0 {
                            ids_found += 1;
                            if ids_found > index {
                                let id =
                                    (check_index + checks_offset) as u32 * PACKED_SIZE + offset;
                                return Some(id);
                            }
                        }
                    }
                }
            }
            ids_found += count;
        }
        None
    }

    // TODO use self.get_match then continue from its index
    pub fn get(&self, index: usize, mut limit: usize, reverse: bool) -> Vec<ID> {
        if limit == 0 {
            return Vec::new();
        }
        if index >= self.matched {
            return Vec::new();
        }
        limit = limit.min(self.matched - index);
        let mut ids = Vec::with_capacity(limit);
        let mut ids_found = 0;
        if reverse {
            let max_id = self.checks.len() as u32 * PACKED_SIZE - 1;
            for (id, check) in self.checks.iter().rev().enumerate() {
                let ones = check.count_ones() as usize;
                if ids_found + ones <= index + ids.len() {
                    ids_found += ones;
                    continue;
                }
                let id = max_id - id as u32 * PACKED_SIZE;
                for (offset_index, offset) in (0..PACKED_SIZE).rev().enumerate() {
                    if check & (1 << offset) != 0 {
                        ids_found += 1;
                        if ids_found > index {
                            let id = id - offset_index as u32;
                            ids.push(id);
                            if ids.len() >= limit {
                                return ids;
                            }
                        }
                    }
                }
            }
        } else {
            for (id, check) in self.checks.iter().enumerate() {
                let ones = check.count_ones() as usize;
                if ids_found + ones <= index + ids.len() {
                    ids_found += ones;
                    continue;
                }
                let id = id as u32 * PACKED_SIZE;
                for offset in 0..PACKED_SIZE {
                    if check & (1 << offset) != 0 {
                        ids_found += 1;
                        if ids_found > index {
                            let id = id + offset;
                            ids.push(id);
                            if ids.len() >= limit {
                                return ids;
                            }
                        }
                    }
                }
            }
        };
        ids
    }

    /// removes matches from results to prevent returning duplicates
    pub fn get_random(&mut self, mut limit: usize) -> Vec<ID> {
        if limit == 0 {
            return Vec::new();
        }
        limit = limit.min(self.matched);
        let mut ids = Vec::with_capacity(limit);
        let mut rng = thread_rng();
        for _ in 0..limit {
            let index = rng.gen_range(0..self.matched) as u32;
            let id = self.get_match(index).unwrap();
            self.remove(id);
            ids.push(id);
        }
        ids
    }

    pub fn get_sorted(
        &self,
        sort: impl DoubleEndedIterator<Item = ID>,
        mut index: usize,
        mut limit: usize,
        mut reverse: bool,
    ) -> Vec<ID> {
        if limit == 0 {
            return Vec::new();
        }
        if index >= self.matched {
            return Vec::new();
        }
        limit = limit.min(self.matched - index);
        let mut ids = Vec::with_capacity(limit);
        let mut current_index = 0;
        let backwards = index >= self.matched / 2;
        if backwards {
            reverse = !reverse;
            index = self.matched - index - limit;
        }
        if reverse {
            for id in sort.rev() {
                if self.contains(id) {
                    if current_index >= index {
                        ids.push(id);
                        if ids.len() >= limit {
                            break;
                        }
                    }
                    current_index += 1;
                }
            }
        } else {
            for id in sort {
                if self.contains(id) {
                    if current_index >= index {
                        ids.push(id);
                        if ids.len() >= limit {
                            break;
                        }
                    }
                    current_index += 1;
                }
            }
        }
        if backwards {
            ids.reverse();
        }
        ids
    }
}
