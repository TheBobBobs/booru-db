use super::{Item, Query};

impl<T: Eq + Ord> Query<T> {
    pub fn simplify(&mut self) {
        self.remove_single_chains();
        self.remove_redundant_chains();
        self.remove_empty();
        self.sort();
        self.dedup();
    }

    pub fn sort(&mut self) {
        match &mut self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                for item in items.iter_mut() {
                    item.sort();
                }
                items.sort();
            }
            _ => {}
        }
    }

    pub fn dedup(&mut self) {
        match &mut self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                for item in items.iter_mut() {
                    item.dedup();
                }
                items.dedup();
            }
            _ => {}
        }
    }

    pub fn remove_empty(&mut self) {
        match &mut self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                items.retain_mut(|item| {
                    item.remove_empty();
                    !item.is_empty()
                });
            }
            _ => {}
        }
    }

    pub fn remove_redundant_chains(&mut self) {
        match &mut self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                for item in items.iter_mut() {
                    item.remove_redundant_chains();
                }
            }
            _ => return,
        }
        let mut redundant: Vec<Query<T>> = Vec::new();
        match &mut self.item {
            Item::AndChain(items) => {
                let mut index = 0;
                while index < items.len() {
                    let item = &items[index];
                    if matches!(item.item, Item::AndChain(_)) {
                        redundant.push(items.remove(index));
                    } else {
                        index += 1;
                    }
                }
            }
            Item::OrChain(items) => {
                let mut index = 0;
                while index < items.len() {
                    let item = &items[index];
                    if matches!(item.item, Item::OrChain(_)) {
                        redundant.push(items.remove(index));
                    } else {
                        index += 1;
                    }
                }
            }
            _ => return,
        };
        match &mut self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                for item in redundant {
                    match item.item {
                        Item::AndChain(inner_items) | Item::OrChain(inner_items) => {
                            for mut inner_item in inner_items {
                                inner_item.inverse ^= item.inverse;
                                items.push(inner_item);
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    pub fn remove_single_chains(&mut self) {
        match &mut self.item {
            Item::AndChain(items) | Item::OrChain(items) => {
                for item in items.iter_mut() {
                    item.remove_single_chains();
                    item.sort();
                    item.dedup();
                }
                items.sort();
                items.dedup();
                if items.len() == 1 {
                    let item = items.remove(0);
                    self.inverse ^= item.inverse;
                    self.item = item.item;
                }
            }
            _ => {}
        }
    }
}
