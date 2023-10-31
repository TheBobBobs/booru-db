use std::{collections::HashMap, time::Instant};

use booru_db::{
    db,
    index::{Index, IndexLoader, KeysIndex, KeysIndexLoader},
    query::Item,
    Query, Queryable, ID,
};

pub struct BooruPost {
    id: u32,
    tags: Vec<String>,
}

// Creates a Db and DbLoader struct for BooruPosts.
db!(BooruPost);

fn main() {
    let posts = vec![
        BooruPost {
            id: 1,
            tags: vec!["1girl".to_string(), "solo".to_string()],
        },
        BooruPost {
            id: 2,
            tags: vec!["solo".to_string()],
        },
    ];

    let db = DbLoader::new()
        // "id" is the prefix for this index. "id:1"
        .with_loader("id", IdIndex::default())
        // the default index used when no prefix is given. "solo"
        .with_default(TagIndexLoader::default())
        .load(posts);

    let query = Query::parse("solo or 1girl").unwrap();
    // result will contain all ids(internal id used by db not post.id) that matched query.
    let start_time = Instant::now();
    let result = db.query(&query).unwrap();
    let elapsed_ns = start_time.elapsed().as_nanos();
    println!("Query: {elapsed_ns}ns");

    let reverse = false;
    // get first 20 matches from result sorted by internal id ascending.
    // results also have get_random and get_sorted
    let page_1 = result.get(0, 20, reverse);

    // get a reference to IdIndex stored in db.
    // used to convert id to post.id
    let id_index: &IdIndex = db.index().unwrap();

    for id in page_1 {
        print!("ID: {id}, ");
        if let Some(post_id) = id_index.id_to_post_id.get(&id) {
            println!("PostID: {post_id}");
        }
    }
}

// IndexLoaders are only used when first loading the db.
// The add method will be called for every post ordered by id ascending.
// Some indexes can use this fact to load data faster.
// for instance if an index stores ids in a sorted list, when loading it doesn't have to find where an id goes in the
// list it can just append it to the end.
#[derive(Default)]
struct TagIndexLoader {
    keys: KeysIndexLoader<String>,
}

impl IndexLoader<BooruPost> for TagIndexLoader {
    fn add(&mut self, id: booru_db::ID, post: &BooruPost) {
        self.keys.add(id, post.tags.iter());
    }

    fn load(self: Box<Self>) -> Box<dyn Index<BooruPost>> {
        let index = TagIndex {
            keys: self.keys.load(),
        };
        Box::new(index)
    }
}

struct TagIndex {
    keys: KeysIndex<String>,
}

impl Index<BooruPost> for TagIndex {
    // Db will call this method when calling Db.query
    // for every tag/metatag Db will use the index with the matching prefix(id:1) or the default index.
    fn query<'s>(
        &'s self,
        _ident: Option<&str>,
        text: &str,
        inverse: bool,
    ) -> Option<Query<Queryable<'s>>> {
        self.keys
            // returns a Queryable which contains the ids that have the tag (text)
            .get(text)
            // Indexes return a Query type allowing for more flexibility.
            // For example turning (maid*) into ((ids with maid tag) or (ids with maid_headdress tag) or ..)
            // In this case it just returns (ids with text tag) with the same inverse (-text) or (text)
            .map(|q| Query::new(Item::Single(q), inverse))
    }

    fn insert(&mut self, id: booru_db::ID, post: &BooruPost) {
        self.keys.insert(id, post.tags.iter());
    }

    fn remove(&mut self, id: booru_db::ID, post: &BooruPost) {
        self.keys.remove(id, post.tags.iter());
    }

    fn update(&mut self, id: booru_db::ID, old: &BooruPost, new: &BooruPost) {
        self.keys.update(id, &old.tags, &new.tags);
    }
}

#[derive(Default)]
struct IdIndex {
    id_to_post_id: HashMap<ID, u32>,
    post_id_to_id: HashMap<u32, ID>,
}

// IdIndex doesn't need a seperate loader struct since there are no optimizations it can do.
impl IndexLoader<BooruPost> for IdIndex {
    fn add(&mut self, id: ID, post: &BooruPost) {
        self.id_to_post_id.insert(id, post.id);
        self.post_id_to_id.insert(post.id, id);
    }

    fn load(self: Box<Self>) -> Box<dyn Index<BooruPost>> {
        self
    }
}

impl Index<BooruPost> for IdIndex {
    fn query<'s>(
        &'s self,
        _ident: Option<&str>,
        text: &str,
        inverse: bool,
    ) -> Option<Query<Queryable<'s>>> {
        let post_id = text.parse::<u32>().ok()?;
        let ids = self
            .post_id_to_id
            .get(&post_id)
            .map(|&id| vec![id])
            .unwrap_or_default();
        Some(Query::new(Item::Single(Queryable::IDsOwned(ids)), inverse))
    }

    fn insert(&mut self, id: ID, post: &BooruPost) {
        self.id_to_post_id.insert(id, post.id);
        self.post_id_to_id.insert(post.id, id);
    }

    fn remove(&mut self, id: ID, post: &BooruPost) {
        self.id_to_post_id.remove(&id);
        self.post_id_to_id.remove(&post.id);
    }

    fn update(&mut self, id: ID, old: &BooruPost, new: &BooruPost) {
        if old.id == new.id {
            return;
        }
        self.remove(id, old);
        self.insert(id, new);
    }
}
