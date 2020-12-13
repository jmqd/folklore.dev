use crate::index::Index;
use regex::Regex;
use std::collections::HashSet;
use std::iter::Iterator;

#[derive(Debug)]
pub struct Query {
    pub exact_ngram: Option<Vec<String>>,
    pub unigrams: Option<Vec<String>>,
}

pub fn query(query_str: String, index: &Index) -> Option<HashSet<String>> {
    lazy_static! {
        static ref QUERY_PARSER: Regex =
            Regex::new("(?:\"(?P<EXACT>.*)\"\\s*)?(?P<UNIGRAMS>.+)?").unwrap();
    }

    let captures = QUERY_PARSER.captures(&query_str).unwrap();
    let mut query = Query {
        exact_ngram: match captures.name("EXACT") {
            None => None,
            Some(exact) => Some(
                exact
                    .as_str()
                    .split_whitespace()
                    .into_iter()
                    .map(|s| s.to_lowercase().to_string())
                    .collect(),
            ),
        },
        unigrams: match captures.name("UNIGRAMS") {
            None => None,
            Some(unigrams) => Some(
                unigrams
                    .as_str()
                    .split_whitespace()
                    .into_iter()
                    .map(|s| s.to_lowercase().to_string())
                    .collect(),
            ),
        },
    };

    // If the user provided an exact match like `"football" manchester`, we want
    // to treat the quoted part just as if it's another unigram.
    if query.exact_ngram.is_some() && query.exact_ngram.as_ref().unwrap().len() == 1 {
        match query.unigrams {
            Some(ref mut unigrams) => {
                unigrams.push(query.exact_ngram.unwrap()[0].clone());
                query.exact_ngram = None;
            }
            None => {
                query.unigrams = Some(vec![query.exact_ngram.unwrap()[0].clone()]);
                query.exact_ngram = None;
            }
        }
    }
    println!("Parsed query: {:#?}", query);

    let mut unigram_result_set = HashSet::new();
    if query.unigrams.is_some() {
        let unigrams = query.unigrams.clone().unwrap();
        let mut iter = unigrams.into_iter();

        // We seed the result set with the first unigram result set.
        match index.unigram_match(iter.next().unwrap()) {
            None => return None,
            Some(results) => results.into_iter().for_each(|p| {
                unigram_result_set.insert(p);
            }),
        }

        // All other unigram result sets will iteratively perform set intersection
        // with the result set, to generate the final set of result candidates
        for unigram in iter {
            match index.unigram_match(unigram) {
                Some(results) => {
                    unigram_result_set = unigram_result_set
                        .intersection(&results)
                        .map(|s| s.to_string())
                        .collect();
                }
                None => unigram_result_set.clear(),
            }
        }
    }

    let exact_results = match query.exact_ngram.clone() {
        None => None,
        Some(ngram) => {
            let mut iter = ngram.as_slice().windows(2);
            let mut ngram_result_set: HashSet<String> =
                match index.ngram_match(iter.next().unwrap().to_vec()) {
                    Some(results) => results.into_iter().collect(),
                    None => HashSet::new(),
                };
            for bigram in iter {
                match index.ngram_match(bigram.to_vec()) {
                    Some(result) => {
                        ngram_result_set = ngram_result_set
                            .intersection(&result)
                            .map(|s| s.to_string())
                            .collect();
                    }
                    None => ngram_result_set.clear(),
                }
            }
            Some(ngram_result_set)
        }
    };

    match (query.exact_ngram.is_some(), query.unigrams.is_some()) {
        // The query is only asking for an exact string search.
        (true, false) => exact_results,

        // The query only wants to match N unigrams.
        (false, true) => Some(unigram_result_set),

        // The query is meaningless.
        (false, false) => None,

        // The query wants the intersection of an exact ngram search and N unigrams AND'd.
        (true, true) => Some(
            unigram_result_set
                .intersection(&exact_results.unwrap())
                .map(|s| s.to_string())
                .collect(),
        ),
    }
}
