use rand::{thread_rng, Rng};

use crate::{index::ChunkedVec, QueryResult, ID};

pub struct MultiQueryResult {
    pub sources: Vec<String>,
    pub results: Vec<QueryResult>,
    pub matched: usize,
    pub remaining: usize,
}

impl MultiQueryResult {
    pub fn new(results: Vec<(String, QueryResult)>) -> Self {
        let results_ = results;
        let mut sources = Vec::with_capacity(results_.len());
        let mut results = Vec::with_capacity(results_.len());
        let mut matched = 0;
        for (source, result) in results_ {
            matched += result.matched();
            sources.push(source);
            results.push(result);
        }
        let remaining = matched;
        Self {
            sources,
            results,
            matched,
            remaining,
        }
    }

    #[inline(always)]
    pub fn sources(&self) -> &[String] {
        &self.sources
    }

    #[inline(always)]
    pub fn matched(&self) -> usize {
        self.matched
    }

    #[inline(always)]
    pub fn remaining(&self) -> usize {
        self.remaining
    }

    #[inline(always)]
    pub fn contains(&self, source: &str, id: ID) -> bool {
        if let Some(result) = self.get_result(source) {
            return result.contains(id);
        }
        false
    }

    pub fn insert(&mut self, source: &str, id: ID) {
        if let Some(result) = self.get_result_mut(source) {
            if !result.contains(id) {
                result.insert(id);
                self.matched += 1;
                self.remaining += 1;
            }
        } else {
            // TODO this should take original sources and compare to get correct index
            self.sources.push(source.to_string());
            let mut result = QueryResult::new(Vec::new());
            result.insert(id);
            self.results.push(result);
            self.matched += 1;
            self.remaining += 1;
        }
    }

    pub fn remove(&mut self, source: &str, id: ID) {
        if let Some(result) = self.get_result_mut(source) {
            if result.contains(id) {
                result.remove(id);
                self.matched -= 1;
                self.remaining -= 1;
            }
        }
    }

    fn source_index(&self, source: &str) -> Option<usize> {
        self.sources
            .iter()
            .enumerate()
            .find(|(_, s)| s.as_str() == source)
            .map(|(i, _)| i)
    }

    fn get_result(&self, source: &str) -> Option<&QueryResult> {
        let index = self.source_index(source)?;
        self.results.get(index)
    }

    fn get_result_mut(&mut self, source: &str) -> Option<&mut QueryResult> {
        let index = self.source_index(source)?;
        self.results.get_mut(index)
    }

    /// removes matches from results to prevent returning duplicates
    pub fn get_random(&mut self, mut limit: usize) -> Vec<(usize, ID)> {
        limit = limit.min(self.remaining);
        let mut ids = Vec::with_capacity(limit);
        let mut rng = thread_rng();
        for _ in 0..limit {
            let mut r_index = rng.gen_range(0..self.remaining);
            for (result_index, result) in self.results.iter_mut().enumerate() {
                if result.matched() > r_index {
                    let id = result.get_match(r_index as u32).unwrap();
                    ids.push((result_index, id));
                    self.remaining -= 1;
                    result.remove(id);
                    break;
                } else {
                    r_index -= result.matched();
                }
            }
        }
        ids
    }

    pub fn get_sorted<V: Eq + Ord>(
        &self,
        sorted: &Vec<&ChunkedVec<(V, ID)>>,
        mut index: usize,
        mut limit: usize,
        mut reverse: bool,
    ) -> Vec<(usize, ID)> {
        assert_eq!(self.results.len(), sorted.len());
        if limit == 0 {
            return Vec::new();
        }
        if index >= self.remaining {
            return Vec::new();
        }
        limit = limit.min(self.remaining);
        if self.results.len() == 1 {
            return self.results[0]
                .get_sorted(sorted[0].iter().map(|(_, id)| *id), index, limit, reverse)
                .into_iter()
                .map(|id| (0, id))
                .collect();
        }
        let mut ids = Vec::with_capacity(limit);
        let mut ids_found = 0;
        let backwards = index >= self.remaining / 2;
        if backwards {
            reverse = !reverse;
            // TODO use index > middle match
            index = (self.remaining - index).max(limit) - limit;
        }

        if reverse {
            let mut sorted: Vec<_> = sorted
                .iter()
                .map(|sort| sort.iter().rev().peekable())
                .collect();
            loop {
                let mut highest_value: Option<(&V, ID, usize)> = None;
                for (result_index, sort) in sorted.iter_mut().enumerate() {
                    let result = &self.results[result_index];
                    while let Some((value, id)) = sort.peek() {
                        if !result.contains(*id) {
                            sort.next();
                            continue;
                        }
                        let value = (value, *id, result_index);
                        if let Some(highest) = &highest_value {
                            if value > *highest {
                                highest_value = Some(value);
                            }
                        } else {
                            highest_value = Some(value);
                        }
                        break;
                    }
                }
                if let Some(highest) = highest_value {
                    let result_index = highest.2;
                    let id = highest.1;
                    sorted[result_index].next();

                    ids_found += 1;
                    if ids_found > index {
                        ids.push((result_index, id));
                        if ids.len() >= limit {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        } else {
            let mut sorted: Vec<_> = sorted.iter().map(|sort| sort.iter().peekable()).collect();
            loop {
                let mut lowest_value = None;
                for (result_index, sort) in sorted.iter_mut().enumerate() {
                    let result = &self.results[result_index];
                    while let Some((value, id)) = sort.peek() {
                        if !result.contains(*id) {
                            sort.next();
                            continue;
                        }
                        let value = (value, *id, result_index);
                        if let Some(lowest) = &lowest_value {
                            if value < *lowest {
                                lowest_value = Some(value);
                            }
                        } else {
                            lowest_value = Some(value);
                        }
                        break;
                    }
                }
                if let Some(lowest) = lowest_value {
                    let result_index = lowest.2;
                    let id = lowest.1;
                    sorted[result_index].next();

                    ids_found += 1;
                    if ids_found > index {
                        ids.push((result_index, id));
                        if ids.len() >= limit {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        }
        if backwards {
            ids.reverse();
        }
        ids
    }
}
