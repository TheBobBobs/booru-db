pub mod multi_result;
pub mod parse;
pub mod queryable;
pub mod result;
pub mod run;
pub mod simplify;
pub mod util;

pub use multi_result::MultiQueryResult;
pub use queryable::{Queryable, QueryableOwned};
pub use result::QueryResult;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum Item<T> {
    AndChain(Vec<Query<T>>),
    OrChain(Vec<Query<T>>),
    Single(T),
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Query<T> {
    pub item: Item<T>,
    pub inverse: bool,
}
