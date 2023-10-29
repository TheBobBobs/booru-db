use std::{
    cmp::Ordering,
    collections::HashMap,
    ops::Bound::{self, *},
    str::FromStr,
};

use crate::{
    query::{Item, Queryable},
    Query, ID,
};

pub enum RangeQuery<V> {
    EQ(V),
    GT(V),
    GTE(V),
    LT(V),
    LTE(V),
    Range(V, V),
    All,
}

impl<V: Ord> RangeQuery<V> {
    pub fn is_match(&self, v: &V) -> bool {
        match self {
            RangeQuery::EQ(value) => v == value,
            RangeQuery::GT(value) => v > value,
            RangeQuery::GTE(value) => v >= value,
            RangeQuery::LT(value) => v < value,
            RangeQuery::LTE(value) => v <= value,
            RangeQuery::Range(start, end) => v >= start && v <= end,
            RangeQuery::All => true,
        }
    }
}

impl<V: FromStr> FromStr for RangeQuery<V> {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.contains("..") {
            let mut split = s.split("..");
            let min = split.next().ok_or(())?;
            let max = split.next().ok_or(())?;
            let min = min.parse().map_err(|_| ())?;
            let max = max.parse().map_err(|_| ())?;
            Ok(Self::Range(min, max))
        } else if let Some(stripped) = s.strip_prefix(">=") {
            let value = stripped.parse().map_err(|_| ())?;
            Ok(Self::GTE(value))
        } else if let Some(stripped) = s.strip_prefix("<=") {
            let value = stripped.parse().map_err(|_| ())?;
            Ok(Self::LTE(value))
        } else if let Some(stripped) = s.strip_prefix('>') {
            let value = stripped.parse().map_err(|_| ())?;
            Ok(Self::GT(value))
        } else if let Some(stripped) = s.strip_prefix('<') {
            let value = stripped.parse().map_err(|_| ())?;
            Ok(Self::LT(value))
        } else if let Some(stripped) = s.strip_prefix('=') {
            let value = stripped.parse().map_err(|_| ())?;
            Ok(Self::EQ(value))
        } else {
            let value = s.parse().map_err(|_| ())?;
            Ok(Self::EQ(value))
        }
    }
}

#[derive(Default)]
pub struct RangeIndexLoader<V> {
    id_values: HashMap<ID, V>,
    values: Vec<(V, ID)>,
}

impl<V: Clone + Eq + Ord> RangeIndexLoader<V> {
    pub fn new() -> Self {
        Self {
            id_values: HashMap::new(),
            values: Vec::new(),
        }
    }

    pub fn id_values(&self) -> &HashMap<ID, V> {
        &self.id_values
    }

    pub fn values(&self) -> &Vec<(V, ID)> {
        &self.values
    }

    pub fn add(&mut self, id: ID, v: V) {
        self.id_values.insert(id, v.clone());
        self.values.push((v, id));
    }

    pub fn load(mut self) -> RangeIndex<V> {
        self.values.sort_unstable();
        let mut ids = ChunkedVec::new(100_000);
        for (_, id) in &self.values {
            ids.push(*id);
        }
        let mut values = ChunkedVec::new(100_000);
        for value in self.values {
            values.push(value);
        }
        RangeIndex {
            ids,
            id_values: self.id_values,
            values,
        }
    }
}

#[derive(Default)]
pub struct RangeIndex<V> {
    ids: ChunkedVec<ID>,
    id_values: HashMap<ID, V>,
    values: ChunkedVec<(V, ID)>,
}

impl<V: Clone + Eq + Ord> RangeIndex<V> {
    pub fn new() -> Self {
        Self {
            ids: ChunkedVec::new(100_000),
            id_values: HashMap::new(),
            values: ChunkedVec::new(100_000),
        }
    }

    pub fn ids(&self) -> &ChunkedVec<ID> {
        &self.ids
    }

    pub fn id_values(&self) -> &HashMap<ID, V> {
        &self.id_values
    }

    pub fn values(&self) -> &ChunkedVec<(V, ID)> {
        &self.values
    }

    pub fn loader() -> RangeIndexLoader<V> {
        RangeIndexLoader::new()
    }

    pub fn get(&self, query: RangeQuery<V>) -> Query<Queryable<'_>> {
        let range = match query {
            RangeQuery::EQ(value) => self.eq(&value),
            RangeQuery::GT(value) => self.gt(&value),
            RangeQuery::GTE(value) => self.gte(&value),
            RangeQuery::LT(value) => self.lt(&value),
            RangeQuery::LTE(value) => self.lte(&value),
            RangeQuery::Range(min, max) => self.range(&min, &max),
            RangeQuery::All => Some((Bound::Included(0), Bound::Unbounded)),
        };
        if range.is_none() {
            let queryable = Queryable::IDs(&[]);
            let item = Item::Single(queryable);
            return Query::new(item, false);
        }
        let (start, end) = range.unwrap();

        let item = Item::OrChain(
            self.ids
                .as_slices(start, end)
                .into_iter()
                .map(|slice| {
                    let queryable = Queryable::IDs(slice);
                    let item = Item::Single(queryable);
                    Query::new(item, false)
                })
                .collect(),
        );
        Query::new(item, false)
    }

    pub fn insert(&mut self, id: ID, value: V) {
        self.id_values.insert(id, value.clone());

        let value_id = (value, id);
        let Err(index) = self.values.binary_search(&value_id) else {
            return;
        };
        self.ids.insert(index, id);
        self.values.insert(index, value_id);
    }

    pub fn remove(&mut self, id: ID, value: V) {
        self.id_values.remove(&id);

        let value_id = (value, id);
        let Ok(index) = self.values.binary_search(&value_id) else {
            return;
        };
        self.ids.remove(index);
        self.values.remove(index);
    }

    pub fn update(&mut self, id: ID, old: V, new: V) {
        if old == new {
            return;
        }
        self.remove(id, old);
        self.insert(id, new);
    }

    pub fn eq(&self, value: &V) -> Option<(Bound<usize>, Bound<usize>)> {
        let start = self.values.get_first(|probe| probe.0.cmp(value)).ok()?;
        let end = self.values.get_last(|probe| probe.0.cmp(value)).ok()?;
        Some((Included(start), Included(end)))
    }

    pub fn gt(&self, value: &V) -> Option<(Bound<usize>, Bound<usize>)> {
        let start = self
            .values
            .get_last(|probe| probe.0.cmp(value))
            .map(Excluded)
            .unwrap_or_else(Included);
        let end = Unbounded;
        Some((start, end))
    }

    pub fn gte(&self, value: &V) -> Option<(Bound<usize>, Bound<usize>)> {
        let start = self
            .values
            .get_first(|probe| probe.0.cmp(value))
            .map(Included)
            .unwrap_or_else(Included);
        let end = Unbounded;
        Some((start, end))
    }

    pub fn lt(&self, value: &V) -> Option<(Bound<usize>, Bound<usize>)> {
        let start = Unbounded;
        let end = self
            .values
            .get_first(|probe| probe.0.cmp(value))
            .map(Excluded)
            .unwrap_or_else(Excluded);
        Some((start, end))
    }

    pub fn lte(&self, value: &V) -> Option<(Bound<usize>, Bound<usize>)> {
        let start = Unbounded;
        let end = self
            .values
            .get_last(|probe| probe.0.cmp(value))
            .map(Included)
            .unwrap_or_else(Excluded);
        Some((start, end))
    }

    pub fn range(&self, min: &V, max: &V) -> Option<(Bound<usize>, Bound<usize>)> {
        let start = self
            .values
            .get_first(|probe| probe.0.cmp(min))
            .map(Included)
            .unwrap_or_else(Included);
        let end = self
            .values
            .get_last(|probe| probe.0.cmp(max))
            .map(Included)
            .unwrap_or_else(Excluded);
        Some((start, end))
    }
}

#[derive(Debug)]
pub struct ChunkedVec<T> {
    vecs: Vec<Vec<T>>,
    chunk_size: usize,
}

impl<T> Default for ChunkedVec<T> {
    fn default() -> Self {
        Self {
            vecs: Vec::new(),
            chunk_size: 100_000,
        }
    }
}

impl<T> ChunkedVec<T> {
    pub fn new(chunk_size: usize) -> Self {
        assert!(chunk_size >= 2);
        Self {
            vecs: Vec::new(),
            chunk_size,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.vecs.iter().all(|vec| vec.is_empty())
    }

    pub fn len(&self) -> usize {
        self.vecs.iter().map(|v| v.len()).sum()
    }

    pub fn first(&self) -> Option<&T> {
        self.vecs.first()?.first()
    }

    pub fn last(&self) -> Option<&T> {
        self.vecs.last()?.last()
    }

    pub fn get(&self, mut index: usize) -> Option<&T> {
        for vec in &self.vecs {
            if vec.len() <= index {
                index -= vec.len();
                continue;
            }
            return vec.get(index);
        }
        None
    }

    pub fn push(&mut self, element: T) {
        if self.vecs.is_empty() {
            self.vecs.push(vec![element]);
            return;
        }
        self.vecs.last_mut().unwrap().push(element);
        self.check_chunk(self.vecs.len() - 1);
    }

    pub fn as_slices(&self, start: Bound<usize>, end: Bound<usize>) -> Vec<&[T]> {
        if self.is_empty() {
            return vec![&[]];
        }
        let mut start = match start {
            Included(start) => start,
            Excluded(start) => start + 1,
            Unbounded => 0,
        };
        if end == Excluded(start) {
            return vec![&[]];
        }
        let mut end = match end {
            Included(end) => end,
            Excluded(end) => end.max(1) - 1,
            Unbounded => self.len().max(1) - 1,
        };

        let mut slices = Vec::new();
        if start > end {
            return vec![&[]];
        }
        for vec in &self.vecs {
            if vec.len() <= start {
                start -= vec.len();
                end -= vec.len();
                continue;
            }
            if vec.len() > end {
                slices.push(&vec[start..=end]);
                break;
            } else {
                slices.push(&vec[start..]);
                start = 0;
                end -= vec.len();
            }
        }
        slices
    }

    fn check_chunk(&mut self, index: usize) {
        let vec = &mut self.vecs[index];
        if vec.len() >= self.chunk_size * 2 {
            let half = vec.split_off(self.chunk_size);
            vec.shrink_to_fit();
            self.vecs.insert(index + 1, half);
        } else if vec.len() <= self.chunk_size / 2 {
            if vec.is_empty() {
                self.vecs.remove(index);
            } else if index > 0 {
                let vec_len = vec.len();
                let prev_vec = &self.vecs[index - 1];
                if prev_vec.len() + vec_len < self.chunk_size * 2 {
                    let vec = self.vecs.remove(index);
                    self.vecs[index - 1].extend(vec);
                }
            }
        }
    }

    pub fn iter(&self) -> ChunkedVecIterator<T> {
        ChunkedVecIterator::new(self)
    }
}

impl<T: Eq + Ord> ChunkedVec<T> {
    pub fn binary_search(&self, x: &T) -> Result<usize, usize> {
        self.binary_search_by(|p| p.cmp(x))
    }

    pub fn binary_search_by<F>(&self, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(&T) -> Ordering,
    {
        if self.is_empty() {
            return Err(0);
        };
        let mut index = 0;
        for vec in &self.vecs {
            match vec.binary_search_by(&mut f) {
                Ok(i) => return Ok(index + i),
                Err(e) => {
                    if e < vec.len() {
                        return Err(index + e);
                    }
                }
            };
            index += vec.len();
        }
        Err(index)
    }

    pub fn insert(&mut self, mut index: usize, element: T) {
        assert!(index <= self.len());
        if self.vecs.is_empty() {
            self.vecs.push(vec![element]);
            return;
        }
        for (vec_index, vec) in self.vecs.iter_mut().enumerate() {
            if vec.len() < index {
                index -= vec.len();
                continue;
            }
            vec.insert(index, element);
            self.check_chunk(vec_index);
            return;
        }
    }

    pub fn remove(&mut self, mut index: usize) {
        assert!(index <= self.len());
        for (vec_index, vec) in self.vecs.iter_mut().enumerate() {
            if vec.len() <= index {
                index -= vec.len();
                continue;
            }
            vec.remove(index);
            self.check_chunk(vec_index);
            return;
        }
    }

    pub fn get_first<'a, F: FnMut(&'a T) -> Ordering>(&'a self, mut f: F) -> Result<usize, usize> {
        use std::cmp::Ordering::*;
        let mut g_index = 0;
        for (vec_index, vec) in self.vecs.iter().enumerate() {
            if vec_index + 1 < self.vecs.len() {
                if let Some(last) = vec.last() {
                    if f(last) == Less {
                        g_index += vec.len();
                        continue;
                    }
                }
            }
            let index = vec
                .binary_search_by(|probe| f(probe).then(Greater))
                .unwrap_err();
            if let Some(element) = vec.get(index) {
                if f(element) == Equal {
                    return Ok(g_index + index);
                }
            }
            return Err(g_index + index);
        }
        Err(0)
    }

    pub fn get_last<'a, F: FnMut(&'a T) -> Ordering>(&'a self, mut f: F) -> Result<usize, usize> {
        use std::cmp::Ordering::*;
        let mut g_index = self.len();
        for vec in self.vecs.iter().rev() {
            g_index -= vec.len();
            if let Some(first) = vec.first() {
                if f(first) == Greater {
                    continue;
                }
            }
            let index = vec
                .binary_search_by(|probe| f(probe).then(Less))
                .unwrap_err();
            if index > 0 {
                if let Some(element) = vec.get(index - 1) {
                    if f(element) == Equal {
                        return Ok(g_index + index - 1);
                    }
                }
            }
            return Err(g_index + index);
        }
        Err(0)
    }
}

pub struct ChunkedVecIterator<'a, T> {
    chunked_vec: &'a ChunkedVec<T>,

    vecs_index: usize,
    vec_index: usize,

    vecs_index_back: usize,
    vec_index_back: usize,
    exhausted_back: bool,
}

impl<'a, T> ChunkedVecIterator<'a, T> {
    pub fn new(chunked_vec: &'a ChunkedVec<T>) -> Self {
        let vecs_index = 0;
        let vec_index = 0;

        let vecs_index_back = chunked_vec.vecs.len().max(1) - 1;
        let vec_index_back = chunked_vec
            .vecs
            .get(vecs_index_back)
            .map(|v| v.len().max(1) - 1)
            .unwrap_or(0);
        Self {
            chunked_vec,

            vecs_index,
            vec_index,

            vecs_index_back,
            vec_index_back,
            exhausted_back: false,
        }
    }
}

impl<'a, T> Iterator for ChunkedVecIterator<'a, T> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.vecs_index >= self.chunked_vec.vecs.len() {
            return None;
        }
        let vec = &self.chunked_vec.vecs[self.vecs_index];
        let item = vec.get(self.vec_index);
        self.vec_index += 1;
        if self.vec_index >= vec.len() {
            self.vecs_index += 1;
            self.vec_index = 0;
        }
        item
    }
}

impl<'a, T> DoubleEndedIterator for ChunkedVecIterator<'a, T> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.exhausted_back {
            return None;
        }
        let vec = &self.chunked_vec.vecs[self.vecs_index_back];
        let item = vec.get(self.vec_index_back);
        if self.vec_index_back == 0 {
            if self.vecs_index_back == 0 {
                self.exhausted_back = true;
            } else {
                self.vecs_index_back -= 1;
                self.vec_index_back = self.chunked_vec.vecs[self.vecs_index_back].len().max(1) - 1;
            }
        } else {
            self.vec_index_back -= 1;
        }
        item
    }
}
