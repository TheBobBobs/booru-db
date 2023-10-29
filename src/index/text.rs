use std::{collections::HashMap, str::FromStr, sync::Arc};

use crate::ID;

#[derive(Debug)]
pub enum TextQuery {
    StartsWith(String),
    Contains(String),
    EndsWith(String),
}

impl TextQuery {
    pub fn text(&self) -> &str {
        match self {
            Self::StartsWith(text) => text,
            Self::Contains(text) => text,
            Self::EndsWith(text) => text,
        }
    }
}

impl FromStr for TextQuery {
    type Err = ();

    fn from_str(mut s: &str) -> Result<Self, Self::Err> {
        let starts_with = s.len() > 1 && s.ends_with('*');
        if starts_with {
            s = &s[..s.len() - 1];
        }
        let ends_with = s.len() > 1 && s.starts_with('*');
        if ends_with {
            s = &s[1..];
        }
        let s = s.to_string();
        if !(starts_with ^ ends_with) {
            Ok(Self::Contains(s))
        } else if starts_with {
            Ok(Self::StartsWith(s))
        } else {
            Ok(Self::EndsWith(s))
        }
    }
}

#[derive(Default)]
pub struct NgramIndex<const N: usize> {
    strings: HashMap<[u8; N], Vec<(Arc<str>, ID)>>,
}

impl<const N: usize> NgramIndex<N> {
    pub fn new() -> Self {
        Self {
            strings: HashMap::new(),
        }
    }

    fn grams(text: &str) -> Vec<[u8; N]> {
        let mut index = 0;
        let mut grams = Vec::new();
        let bytes: Vec<u8> = text.bytes().collect();
        while index + N <= bytes.len() {
            let mut gram = [0; N];
            for (i, &byte) in bytes[index..index + N].iter().enumerate() {
                gram[i] = byte;
            }
            grams.push(gram);
            index += 1;
        }
        grams.sort();
        grams.dedup();
        grams
    }

    pub fn query(&self, text: &str) -> Option<&[(Arc<str>, ID)]> {
        let mut smallest: Option<&[(Arc<str>, ID)]> = None;
        for gram in Self::grams(text) {
            if let Some(strings) = self.strings.get(&gram) {
                if strings.len() < smallest.map(|s| s.len()).unwrap_or(usize::MAX) {
                    smallest = Some(strings);
                }
            }
        }
        smallest
    }

    pub fn insert(&mut self, id: ID, text: Arc<str>) {
        for gram in Self::grams(&text) {
            let strings = self.strings.entry(gram).or_default();
            let index = strings
                .binary_search_by_key(&id, |s| s.1)
                .unwrap_or_else(|e| e);
            strings.insert(index, (text.clone(), id));
        }
    }

    /// Only use if id is greater than any existing text
    pub fn push(&mut self, id: ID, text: Arc<str>) {
        for gram in Self::grams(&text) {
            let strings = self.strings.entry(gram).or_default();
            strings.push((text.clone(), id));
        }
    }

    pub fn remove(&mut self, id: ID, text: Arc<str>) {
        for gram in Self::grams(&text) {
            let Some(strings) = self.strings.get_mut(&gram) else {
                continue;
            };
            let Ok(index) = strings.binary_search_by_key(&id, |s| s.1) else {
                continue;
            };
            strings.remove(index);
            if strings.is_empty() {
                self.strings.remove(&gram);
            }
        }
    }
}

#[derive(Default)]
pub struct TextIndexLoader {
    next_id: ID,
    ids_by_string: HashMap<Arc<str>, ID>,
    n1gram_index: NgramIndex<1>,
    n2gram_index: NgramIndex<2>,
}

impl TextIndexLoader {
    pub fn new() -> Self {
        Self {
            next_id: 0,
            ids_by_string: HashMap::new(),
            n1gram_index: NgramIndex::new(),
            n2gram_index: NgramIndex::new(),
        }
    }

    pub fn add(&mut self, text: String) {
        let id = self.next_id;
        self.next_id += 1;
        let text: Arc<str> = text.into();
        self.ids_by_string.insert(text.clone(), id);
        self.n1gram_index.push(id, text.clone());
        self.n2gram_index.push(id, text.clone());
    }

    pub fn load(self) -> TextIndex {
        TextIndex {
            next_id: self.next_id,
            ids_by_string: self.ids_by_string,
            n1gram_index: self.n1gram_index,
            n2gram_index: self.n2gram_index,
        }
    }
}

pub struct TextIndex {
    next_id: ID,
    ids_by_string: HashMap<Arc<str>, ID>,
    n1gram_index: NgramIndex<1>,
    n2gram_index: NgramIndex<2>,
}

impl TextIndex {
    pub fn get(&self, query: &TextQuery) -> Vec<Arc<str>> {
        let text = query.text();
        let Some(mut smallest) = (match text.len() {
            0 => None,
            1 => self.n1gram_index.query(text),
            _ => self.n2gram_index.query(text),
        }) else {
            return Vec::new();
        };
        let mut matches = Vec::with_capacity(smallest.len());
        if query.text().len() <= 2 && matches!(query, TextQuery::Contains(_)) {
            for (s, _) in smallest {
                matches.push(s.clone());
            }
        }
        let mut strings;
        if text.len() >= 4 {
            let mut bytes = query.text().bytes();
            let mut grams = Vec::with_capacity((text.len() as f32 / 2.0).ceil() as usize);
            while let (Some(b0), Some(b1)) = (bytes.next(), bytes.next()) {
                grams.push([b0, b1]);
            }
            grams.sort();
            grams.dedup();
            if grams.len() > 1 {
                let mut indexes: Vec<_> = grams
                    .iter()
                    .filter_map(|g| self.n2gram_index.strings.get(g))
                    .collect();
                if grams.len() != indexes.len() {
                    return Vec::new();
                }
                indexes.sort_by_key(|g| g.len());
                strings = indexes[0].clone();
                for strings_b in &indexes[1..] {
                    let mut cursor = 0;
                    strings.retain(|(_, id)| {
                        while let Some((_, id_b)) = strings_b.get(cursor) {
                            if id_b < id {
                                cursor += 1;
                                continue;
                            }
                            if id_b == id {
                                cursor += 1;
                                return true;
                            }
                            return false;
                        }
                        false
                    });
                }
                if strings.len() < smallest.len() {
                    smallest = strings.as_slice();
                }
            }
        }

        match query {
            TextQuery::StartsWith(text) => {
                for (s, _) in smallest {
                    if s.starts_with(text) {
                        matches.push(s.clone());
                    }
                }
            }
            TextQuery::Contains(text) => {
                for (s, _) in smallest {
                    if s.contains(text) {
                        matches.push(s.clone());
                    }
                }
            }
            TextQuery::EndsWith(text) => {
                for (s, _) in smallest {
                    if s.ends_with(text) {
                        matches.push(s.clone());
                    }
                }
            }
        }
        matches
    }

    pub fn insert(&mut self, text: String) {
        let text: Arc<str> = text.into();
        if self.ids_by_string.contains_key(&text) {
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.ids_by_string.insert(text.clone(), id);
        self.n1gram_index.insert(id, text.clone());
        self.n2gram_index.insert(id, text.clone());
    }

    pub fn remove(&mut self, text: String) {
        let text: Arc<str> = text.into();
        if !self.ids_by_string.contains_key(&text) {
            return;
        }
        let id = self.ids_by_string.remove(&text).unwrap();
        self.n1gram_index.remove(id, text.clone());
        self.n2gram_index.remove(id, text.clone());
    }
}
