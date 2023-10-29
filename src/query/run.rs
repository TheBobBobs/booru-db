use crate::Packed;

use super::{queryable::Queryable, Item, Query};

fn bit_checks<F: FnMut((&mut Packed, &Packed))>(a: &mut [Packed], b: &[Packed], f: F) {
    a.iter_mut().zip(b.iter()).for_each(f);
}

fn and_checks(a: &mut [Packed], b: &[Packed]) {
    bit_checks(a, b, |(a, b)| *a &= b);
}

fn and_not_checks(a: &mut [Packed], b: &[Packed]) {
    bit_checks(a, b, |(a, b)| *a &= !b);
}

fn or_checks(a: &mut [Packed], b: &[Packed]) {
    bit_checks(a, b, |(a, b)| *a |= b);
}

// fn or_not_checks(a: &mut Vec<Packed>, b: &Vec<Packed>) {
//     bit_checks(a, b, |(a, b)| *a |= !b);
// }

impl<'i> Query<Queryable<'i>> {
    pub fn run(&self, base_checks: &[Packed]) -> Vec<Packed> {
        let mut checks = base_checks.to_vec();
        if let Item::Single(tag) = &self.item {
            tag.and(&mut checks, self.inverse);
        } else {
            self.inner_run(&mut checks, self.inverse);
            and_checks(&mut checks, base_checks);
        }
        checks
    }

    fn inner_run(&self, checks: &mut [Packed], inverse: bool) {
        match &self.item {
            Item::AndChain(query_items) => {
                for query_item in query_items {
                    query_item.inner_run(checks, query_item.inverse ^ inverse);
                }
            }
            Item::OrChain(query_items) => {
                let mut checks_2 = checks.to_vec();
                checks_2.fill(0);
                let mut checks_3 = None;
                for query_item in query_items {
                    if let Item::Single(tag) = &query_item.item {
                        tag.or(&mut checks_2, query_item.inverse);
                    } else {
                        let checks_3 = if let Some(c) = &mut checks_3 {
                            c
                        } else {
                            checks_3 = Some(checks.to_vec());
                            checks_3.as_mut().unwrap()
                        };
                        checks_3.fill(Packed::MAX);
                        query_item.inner_run(checks_3, query_item.inverse);
                        or_checks(&mut checks_2, checks_3);
                    }
                }

                if self.inverse {
                    and_not_checks(checks, &checks_2);
                } else {
                    and_checks(checks, &checks_2);
                }
            }
            Item::Single(tag) => {
                tag.and(checks, inverse);
            }
        }
    }
}
