use super::{Item, Query};

impl<'s, S: From<&'s str>> Query<S> {
    // TODO: actual parser
    pub fn parse(query: &'s str) -> Result<Query<S>, ()> {
        let split: Vec<&str> = query.split_whitespace().collect();
        let (index, item) = parse_item(&split);
        if index != split.len() {
            return Err(());
        }
        Ok(Query::new(item, false))
    }
}

fn parse_item<'s, S: From<&'s str>>(input: &[&'s str]) -> (usize, Item<S>) {
    let mut index = 0;
    let mut and_chain = Vec::new();
    let mut or_chain = Vec::new();
    let mut was_or = false;

    while index < input.len() {
        let mut is_or = false;
        let item = &input[index];
        let item = match *item {
            "-" => None,
            "()" => None,
            "or" => {
                is_or = true;
                None
            }
            "-(" => {
                let (i, item) = parse_item(&input[index + 1..]);
                index += i;
                Some(Query::new(item, true))
            }
            "(" => {
                let (i, item) = parse_item(&input[index + 1..]);
                index += i;
                Some(Query::new(item, false))
            }
            ")" => {
                index += 1;
                break;
            }
            mut tag => {
                let inverse = tag.starts_with('-');
                if inverse {
                    tag = &tag[1..];
                }
                Some(Query::new(Item::Single(S::from(tag)), inverse))
            }
        };
        if let Some(item) = item {
            if !was_or && !or_chain.is_empty() {
                and_chain.push(Query::new(Item::OrChain(or_chain), false));
                or_chain = Vec::new();
            }
            if was_or
                || (or_chain.is_empty() && index + 1 < input.len() && input[index + 1] == "or")
            {
                or_chain.push(item);
            } else {
                if !or_chain.is_empty() {
                    and_chain.push(Query::new(Item::OrChain(or_chain), false));
                    or_chain = Vec::new();
                }
                and_chain.push(item);
            }
        } else if !is_or && !or_chain.is_empty() {
            and_chain.push(Query::new(Item::OrChain(or_chain), false));
            or_chain = Vec::new();
        }
        was_or = is_or;
        index += 1;
    }
    if !or_chain.is_empty() {
        and_chain.push(Query::new(Item::OrChain(or_chain), false));
    }
    (index, Item::AndChain(and_chain))
}
