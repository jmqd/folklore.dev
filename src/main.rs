#[macro_use]
extern crate lazy_static;

mod database;
mod document;
mod index;
mod net;

use gflags;
use index::Index;
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io;
use std::iter::Iterator;
use std::sync::Arc;

type ConnPool = r2d2::Pool<SqliteConnectionManager>;

#[derive(Debug)]
pub struct Query {
    pub exact_ngram: Option<Vec<String>>,
    pub unigrams: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub websites: Vec<Website>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Website {
    pub url: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = gflags::parse();
    println!("Binary arguments: {:#?}", args);
    let db = database::connect_to_db();
    let mut config: Config = toml::from_str(std::include_str!("../data.toml"))
        .expect("Failed to deserialized config file.");

    run(&mut config, db).await;
    Ok(())
}

async fn run<'i>(config: &mut Config, db: Arc<ConnPool>) {
    let index = index::build_index(&config.websites, db).await;
    println!("indexed sites length: {:#?}", index.document_codes.len());
    println!("unigram index length: {:#?}", index.unigrams.len());
    println!("ngram index length: {:#?}", index.ngrams.len());
    println!("word_codes length: {:#?}", index.word_codes.len());

    loop {
        cli_testing(&index);
    }
}

fn cli_testing(index: &Index) {
    let mut input = String::new();
    match io::stdin().read_line(&mut input) {
        Ok(n) => {
            println!("{} bytes read", n);
            println!("{}", input);
            println!("Query results: {:#?}", query(input, index));
        }
        Err(error) => println!("error: {}", error),
    }
}

fn query(query_str: String, index: &Index) -> Option<HashSet<String>> {
    lazy_static! {
        static ref QUERY_PARSER: Regex = Regex::new("(?:\"(.*)\")?\\s(.+)?").unwrap();
    }

    let captures = QUERY_PARSER.captures(&query_str).unwrap();
    let query = Query {
        exact_ngram: match captures.get(1) {
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
        unigrams: match captures.get(2) {
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
                None => (),
            }
        }
    }

    let exact_results = match query.exact_ngram.clone() {
        None => None,
        Some(ngram) => index.exact_ngram_match(ngram),
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
