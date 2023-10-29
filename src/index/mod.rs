mod key;
mod keys;
mod range;
mod text;

use downcast_rs::{impl_downcast, Downcast};
pub use key::{KeyIndex, KeyIndexLoader};
pub use keys::{KeysIndex, KeysIndexLoader};
pub use range::{ChunkedVec, RangeIndex, RangeIndexLoader, RangeQuery};
pub use text::{NgramIndex, TextIndex, TextIndexLoader, TextQuery};

use crate::{Query, Queryable, ID};

pub trait IndexLoader<P>: Downcast + Send + Sync {
    fn add(&mut self, id: ID, post: &P);

    fn load(self: Box<Self>) -> Box<dyn Index<P>>;
}

impl_downcast!(IndexLoader<P>);

pub trait Index<P>: Downcast + Send + Sync {
    fn query<'s>(
        &'s self,
        ident: Option<&str>,
        text: &str,
        inverse: bool,
    ) -> Option<Query<Queryable<'s>>>;

    fn insert(&mut self, id: ID, post: &P);

    fn remove(&mut self, id: ID, post: &P);

    fn update(&mut self, id: ID, old: &P, new: &P);
}

impl_downcast!(Index<P>);
