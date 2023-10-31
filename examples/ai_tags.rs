use std::{collections::HashMap, time::Instant};

use booru_db::{
    db,
    index::{Index, IndexLoader, RangeIndex, RangeIndexLoader},
    query::Item,
    Query, Queryable, RangeQuery, ID,
};
use sqlx::postgres::PgPoolOptions;

pub struct BooruPost {
    id: u32,
    ai_tags: Vec<(u32, u16)>,
}

db!(BooruPost);

#[derive(sqlx::FromRow)]
struct AiTagsRow {
    post_id: i32,
    tag_id: i32,
    score: i16,
}

#[derive(sqlx::FromRow)]
struct TagsRow {
    id: i32,
    name: String,
}

#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect("postgres://postgres:password@localhost/postgres")
        .await
        .unwrap();

    let start_time = Instant::now();
    let posts = {
        let rows: Vec<AiTagsRow> = sqlx::query_as("SELECT * FROM ai_tags")
            .fetch_all(&pool)
            .await
            .unwrap();

        let mut posts = HashMap::new();
        for row in rows {
            let post = posts.entry(row.post_id).or_insert_with(|| BooruPost {
                id: row.post_id as u32,
                ai_tags: Vec::new(),
            });
            post.ai_tags.push((row.tag_id as u32, row.score as u16));
        }
        posts
    };
    let elapsed_ns = start_time.elapsed().as_nanos();
    println!(
        "FetchPosts: {:.3}s",
        elapsed_ns as f64 / 1000.0 / 1000.0 / 1000.0
    );

    let start_time = Instant::now();
    let name_to_id = {
        let rows: Vec<TagsRow> = sqlx::query_as("SELECT * FROM tags")
            .fetch_all(&pool)
            .await
            .unwrap();

        let mut name_to_id = HashMap::new();
        for row in rows {
            name_to_id.insert(row.name, row.id as u32);
        }
        name_to_id
    };
    let elapsed_ns = start_time.elapsed().as_nanos();
    println!(
        "FetchTags: {:.3}s",
        elapsed_ns as f64 / 1000.0 / 1000.0 / 1000.0
    );

    let start_time = Instant::now();
    let db = DbLoader::new()
        .with_loader("id", IdIndex::default())
        .with_loader(
            "ai",
            AiTagIndexLoader {
                tags: HashMap::new(),
                name_to_id,
            },
        )
        .load(posts.into_values());
    let elapsed_ns = start_time.elapsed().as_nanos();
    println!(
        "Index: {:.3}s",
        elapsed_ns as f64 / 1000.0 / 1000.0 / 1000.0
    );

    let query = Query::parse("ai:solo:>=90 ai:1girl:>=90").unwrap();
    let start_time = Instant::now();
    let result = db.query(&query).unwrap();
    let elapsed_ns = start_time.elapsed().as_nanos();
    println!("Query: {:.3}ms", elapsed_ns as f64 / 1000.0 / 1000.0);

    // let tag_index: &AiTagIndex = db.index().unwrap();
    // let tag_id = tag_index.name_to_id.get("solo").unwrap();
    // let sort = tag_index.tags.get(tag_id).unwrap().ids().iter().copied();
    // let page_1 = result.get_sorted(sort, 0, 20, false);

    let reverse = false;
    let page_1 = result.get(0, 20, reverse);
    let id_index: &IdIndex = db.index().unwrap();
    for id in page_1 {
        print!("ID: {id}, ");
        if let Some(post_id) = id_index.id_to_post_id.get(&id) {
            println!("PostID: {post_id}");
        }
    }
}

pub struct AiTagIndexLoader {
    tags: HashMap<u32, RangeIndexLoader<u16>>,
    name_to_id: HashMap<String, u32>,
}

impl IndexLoader<BooruPost> for AiTagIndexLoader {
    fn add(&mut self, id: ID, post: &BooruPost) {
        for &(tag, score) in &post.ai_tags {
            self.tags.entry(tag).or_default().add(id, score);
        }
    }

    fn load(self: Box<Self>) -> Box<(dyn Index<BooruPost>)> {
        let tags = self
            .tags
            .into_iter()
            .map(|(tag, index)| (tag, index.load()))
            .collect();
        let index = AiTagIndex {
            tags,
            name_to_id: self.name_to_id,
        };
        Box::new(index)
    }
}

pub struct AiTagIndex {
    tags: HashMap<u32, RangeIndex<u16>>,
    name_to_id: HashMap<String, u32>,
}

impl Index<BooruPost> for AiTagIndex {
    fn query<'s>(
        &'s self,
        _ident: Option<&str>,
        mut text: &str,
        inverse: bool,
    ) -> Option<Query<Queryable<'s>>> {
        let mut range_query = RangeQuery::All;
        if let Some((tag, q)) = text.split_once(':') {
            if let Ok(q) = q.parse::<RangeQuery<u16>>() {
                text = tag;
                range_query = q;
            }
        }
        let tag_id = text
            .parse::<u32>()
            .ok()
            .and_then(|tag_id| self.tags.contains_key(&tag_id).then_some(tag_id))
            .or_else(|| self.name_to_id.get(text).copied())?;
        let mut query = self.tags.get(&tag_id).map(|r| r.get(range_query))?;
        query.inverse = inverse;
        Some(query)
    }

    fn insert(&mut self, id: ID, post: &BooruPost) {
        for &(tag, score) in &post.ai_tags {
            self.tags.entry(tag).or_default().insert(id, score);
        }
    }

    fn remove(&mut self, id: ID, post: &BooruPost) {
        for (tag, score) in &post.ai_tags {
            if let Some(index) = self.tags.get_mut(tag) {
                index.remove(id, *score);
            }
        }
    }

    fn update(&mut self, id: ID, old: &BooruPost, new: &BooruPost) {
        if old.ai_tags == new.ai_tags {
            return;
        }
        self.remove(id, old);
        self.insert(id, new);
    }
}

#[derive(Default)]
struct IdIndex {
    id_to_post_id: HashMap<ID, u32>,
    post_id_to_id: HashMap<u32, ID>,
}

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
        let post_id: u32 = text.parse().ok()?;
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
