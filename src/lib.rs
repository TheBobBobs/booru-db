pub mod index;
pub mod query;

pub use index::{RangeQuery, TextQuery};
pub use query::{MultiQueryResult, Query, QueryResult, Queryable, QueryableOwned};

pub type ID = u32;
pub type Packed = u64;
pub const PACKED_SIZE: u32 = 64;

#[macro_export]
macro_rules! type_map {
    ($name:ident, $trait:path) => {
        pub struct $name {
            map: ::std::collections::HashMap<::std::any::TypeId, ::std::boxed::Box<dyn $trait>>,
        }

        #[allow(unused)]
        impl $name {
            pub fn new() -> Self {
                Self {
                    map: ::std::default::Default::default(),
                }
            }

            pub fn get<T: ::std::any::Any + 'static>(&self) -> ::std::option::Option<&T> {
                let key = ::std::any::TypeId::of::<T>();
                self.map.get(&key).and_then(|b| b.as_any().downcast_ref())
            }

            pub fn get_mut<T: ::std::any::Any + 'static>(
                &mut self,
            ) -> ::std::option::Option<&mut T> {
                let key = ::std::any::TypeId::of::<T>();
                self.map
                    .get_mut(&key)
                    .and_then(|b| b.as_any_mut().downcast_mut())
            }

            pub fn insert<T: $trait + 'static>(&mut self, t: T) {
                let key = ::std::any::TypeId::of::<T>();
                self.map.insert(key, ::std::boxed::Box::new(t));
            }

            pub fn insert_boxed(&mut self, t: ::std::boxed::Box<dyn $trait>) {
                let key = t.as_any().type_id();
                self.map.insert(key, t);
            }

            pub fn values(
                &self,
            ) -> ::std::collections::hash_map::Values<
                '_,
                ::std::any::TypeId,
                ::std::boxed::Box<dyn $trait>,
            > {
                self.map.values()
            }

            pub fn values_mut(
                &mut self,
            ) -> ::std::collections::hash_map::ValuesMut<
                '_,
                ::std::any::TypeId,
                ::std::boxed::Box<dyn $trait>,
            > {
                self.map.values_mut()
            }

            pub fn into_values(
                self,
            ) -> ::std::collections::hash_map::IntoValues<
                ::std::any::TypeId,
                ::std::boxed::Box<dyn $trait>,
            > {
                self.map.into_values()
            }
        }
    };
}

pub trait Identifier {
    fn to_idents(self) -> Vec<String>;
}

impl Identifier for &str {
    fn to_idents(self) -> Vec<String> {
        vec![self.to_string()]
    }
}

impl<const N: usize> Identifier for [&str; N] {
    fn to_idents(self) -> Vec<String> {
        self.into_iter().map(|s| s.to_string()).collect()
    }
}

#[macro_export]
macro_rules! db {
    ($post_type:ty) => {
        ::booru_db::type_map!(LoaderMap, ::booru_db::index::IndexLoader<$post_type>);
        ::booru_db::type_map!(IndexMap, ::booru_db::index::Index<$post_type>);

        pub struct DbLoader {
            identifiers: ::std::collections::HashMap<
                ::std::option::Option<::std::string::String>,
                ::std::any::TypeId,
            >,
            loaders: LoaderMap,
        }

        impl DbLoader {
            pub fn new() -> Self {
                Self {
                    identifiers: ::std::collections::HashMap::new(),
                    loaders: LoaderMap::new(),
                }
            }

            pub fn load(self, posts: impl ::std::iter::IntoIterator<Item = $post_type>) -> Db {
                Db::new(self.identifiers, self.loaders, posts)
            }

            pub fn with_default<L: ::booru_db::index::IndexLoader<$post_type>>(
                mut self,
                loader: L,
            ) -> Self {
                let identifier = None;
                self.insert_loader(identifier, loader);
                self
            }

            pub fn with_loader<
                I: ::booru_db::Identifier,
                L: ::booru_db::index::IndexLoader<$post_type>,
            >(
                mut self,
                identifier: I,
                loader: L,
            ) -> Self {
                self.insert_loader(Some(identifier.to_idents()), loader);
                self
            }

            fn insert_loader<L: ::booru_db::index::IndexLoader<$post_type>>(
                &mut self,
                identifiers: ::std::option::Option<::std::vec::Vec<::std::string::String>>,
                loader: L,
            ) {
                let type_id = ::std::any::TypeId::of::<L>();
                let identifiers = identifiers
                    .map(|i| {
                        i.into_iter()
                            .map(|s| ::std::option::Option::Some(s))
                            .collect()
                    })
                    .unwrap_or(::std::vec::Vec::from([::std::option::Option::None]));
                for identifier in identifiers {
                    if self.identifiers.contains_key(&identifier) {
                        panic!("Duplicate Identifier!");
                    }
                    self.identifiers.insert(identifier, type_id);
                }
                self.loaders.insert(loader);
            }
        }

        pub struct Db {
            identifiers: ::std::collections::HashMap<
                ::std::option::Option<::std::string::String>,
                ::std::any::TypeId,
            >,
            indexes: IndexMap,
            base_checks: ::booru_db::query::QueryResult,
        }

        impl Db {
            fn new(
                identifiers: ::std::collections::HashMap<
                    ::std::option::Option<::std::string::String>,
                    ::std::any::TypeId,
                >,
                mut loaders: LoaderMap,
                posts: impl ::std::iter::IntoIterator<Item = $post_type>,
            ) -> Self {
                let mut last_id = ::std::option::Option::None;
                for (id, post) in posts.into_iter().enumerate() {
                    last_id = ::std::option::Option::Some(id);
                    for loader in loaders.values_mut() {
                        loader.add(id as u32, &post);
                    }
                }

                let base_checks = if let ::std::option::Option::Some(last_id) = last_id {
                    let mut checks = vec![
                        ::booru_db::Packed::MAX;
                        (last_id / ::booru_db::PACKED_SIZE as usize) + 1
                    ];
                    if let ::std::option::Option::Some(check) = checks.last_mut() {
                        *check = 0;
                        let end = (last_id % ::booru_db::PACKED_SIZE as usize) + 1;
                        for i in 0..end {
                            *check |= 1 << i;
                        }
                    }
                    ::booru_db::QueryResult::new(checks)
                } else {
                    ::booru_db::QueryResult::new(::std::vec::Vec::new())
                };

                let mut index_identifiers = ::std::collections::HashMap::new();
                let mut indexes = IndexMap::new();
                for (identifier, type_id) in identifiers {
                    let loader = loaders.map.remove(&type_id).unwrap();
                    let index = loader.load();
                    index_identifiers.insert(identifier, index.as_any().type_id());
                    indexes.insert_boxed(index);
                }

                Self {
                    identifiers: index_identifiers,
                    indexes,
                    base_checks,
                }
            }

            pub fn checks(&self) -> &[::booru_db::Packed] {
                self.base_checks.checks()
            }

            pub fn index<T: 'static + ::booru_db::index::Index<$post_type>>(
                &self,
            ) -> ::std::option::Option<&T> {
                self.indexes.get()
            }

            pub fn index_mut<T: 'static + ::booru_db::index::Index<$post_type>>(
                &mut self,
            ) -> ::std::option::Option<&mut T> {
                self.indexes.get_mut()
            }

            fn insert_index<I: ::booru_db::Identifier, T: ::booru_db::index::Index<$post_type>>(
                &mut self,
                identifier: I,
                index: T,
            ) {
                let type_id = ::std::any::TypeId::of::<T>();
                for ident in identifier.to_idents() {
                    let key = ::std::option::Option::Some(ident);
                    if self.identifiers.contains_key(&key) {
                        panic!("Duplicate Identifier!");
                    }
                    self.identifiers.insert(key, type_id);
                }
                self.indexes.insert(index);
            }

            pub fn query(
                &self,
                query: &::booru_db::Query<&str>,
            ) -> ::std::result::Result<
                ::booru_db::QueryResult,
                ::std::vec::Vec<::std::string::String>,
            > {
                let query = query
                    .try_map(|text, inverse| {
                        let (ident, value) = text
                            .split_once(':')
                            .map(|(ident, value)| {
                                let ident = ::std::option::Option::Some(ident.to_string());
                                if self.identifiers.contains_key(&ident) {
                                    (ident, value)
                                } else {
                                    (::std::option::Option::None, *text)
                                }
                            })
                            .unwrap_or((::std::option::Option::None, text));
                        let type_id = self.identifiers.get(&ident);
                        let index = self.indexes.map.get(type_id?).unwrap();
                        index.query(ident.as_deref(), value, inverse)
                    })
                    .map_err(|e| {
                        e.into_iter()
                            .map(|s| s.to_string())
                            .collect::<::std::vec::Vec<_>>()
                    })?;
                let checks = query.run(self.base_checks.checks());
                ::std::result::Result::Ok(::booru_db::QueryResult::new(checks))
            }

            pub fn insert(&mut self, id: ::booru_db::ID, post: &$post_type) {
                self.base_checks.insert(id);
                for index in self.indexes.values_mut() {
                    index.insert(id, post)
                }
            }

            pub fn remove(&mut self, id: ::booru_db::ID, post: &$post_type) {
                self.base_checks.remove(id);
                for index in self.indexes.values_mut() {
                    index.remove(id, post);
                }
            }

            pub fn update(&mut self, id: ::booru_db::ID, old: &$post_type, new: &$post_type) {
                self.base_checks.insert(id);
                for index in self.indexes.values_mut() {
                    index.update(id, old, new);
                }
            }
        }
    };
}

#[derive(Clone, Debug)]
pub enum QueryError {
    InvalidSource,
    MissingTags(Vec<String>),
}
