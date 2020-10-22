#[macro_use]
extern crate lazy_static;

use bincode;
use itertools::Itertools;
use regex::Regex;
use reqwest;
use select::document::Document;
use select::predicate::Any;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::iter::Iterator;

#[derive(Debug)]
struct Query {
    pub exact_ngram: Option<Vec<String>>,
    pub unigrams: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    pub websites: Vec<Website>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Website {
    pub url: String,
}

/// An Index holds all state necessary to answer search queries.
///
/// The index normalizes all tokens to lowercase. Tokens are identified by
/// intervening whitespace. Different nodes in HTML documents isolate grams
/// from each other. For example, a gram cannot span from one paragraph or
/// div tag into another.
///
/// unigrams: A mapping from all words to all documents those words appear in.
///
/// ngrams: A mapping from all ngrams to all documents those ngrams appear in.
#[derive(Serialize, Deserialize)]
struct Index<'i> {
    #[serde(borrow)]
    pub unigrams: HashMap<String, HashSet<&'i str>>,

    #[serde(borrow)]
    pub ngrams: HashMap<Vec<String>, HashSet<&'i str>>,
}

impl<'i> Index<'i> {
    fn unigram_match(&self, unigram: &str) -> Option<HashSet<String>> {
        Index::pass_page_results(self.unigrams.get(unigram))
    }

    fn ngram_match(&self, ngram: &[String]) -> Option<HashSet<String>> {
        Index::pass_page_results(self.ngrams.get(ngram))
    }

    fn pass_page_results(page_results: Option<&HashSet<&str>>) -> Option<HashSet<String>> {
        match page_results {
            // If we found some pages that matches the search query:
            // We copy all the page URLs into a return value for the caller.
            Some(page_results) => Some(page_results.into_iter().map(|p| p.to_string()).collect()),

            // Otherwise, their search query had no results.
            None => None,
        }
    }

    fn exact_ngram_match(&self, ngram: &Vec<String>) -> Option<HashSet<String>> {
        match ngram.len() {
            1 => self.unigram_match(&ngram[0]),
            _ => self.ngram_match(&ngram),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config: Config = toml::from_str(std::include_str!("../data.toml"))
        .expect("Failed to deserialized config file.");

    run(&mut config).await;
    Ok(())
}

async fn run<'i>(config: &mut Config) {
    if std::fs::metadata(std::path::Path::new("/tmp/index.bin")).is_ok() {
        let cached_index = &std::fs::read("/tmp/index.bin").unwrap();
        let index: Index = bincode::deserialize(cached_index).unwrap();
        loop {
            cli_testing(&index);
        }
    }

    let mut index = Index {
        unigrams: HashMap::new(),
        ngrams: HashMap::new(),
    };

    for website in &config.websites {
        println!("Fetching {:#?}...", &website.url);
        match fetch(&website.url).await {
            Some(document) => index_texts(extract_texts(&document), &mut index, &website.url),
            None => (),
        };
    }
    let serialized = bincode::serialize(&index).expect("Could not encode bincode.");
    std::fs::write("/tmp/index.bin", serialized).expect("Could not write to file!");
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

async fn fetch(url: &str) -> Option<Document> {
    let cache_path: std::path::PathBuf = std::path::Path::new(&format!(
        "/tmp/{}",
        url.trim_start_matches("https://")
            .trim_start_matches("http://")
    ))
    .to_path_buf();

    if std::fs::metadata(std::path::Path::new(&cache_path)).is_ok() {
        return Some(Document::from(
            std::fs::read_to_string(&cache_path).unwrap().as_ref(),
        ));
    }

    match reqwest::get(url).await {
        Ok(resp) => {
            let text = resp.text().await.unwrap();

            println!("Got the site. Caching {:#?}", &cache_path);
            let parent = cache_path.parent();
            match parent {
                Some(path) => std::fs::create_dir_all(&path).expect("Failed to create dirs"),
                None => (),
            };

            std::fs::write(&cache_path, text.clone()).expect("Failed to write website contents.");
            Some(Document::from(text.as_ref()))
        }
        Err(e) => {
            println!("Error when getting site: {:#?}", e);
            None
        }
    }
}

fn extract_texts(document: &Document) -> HashSet<Vec<String>> {
    let mut texts = HashSet::new();
    for node in document.find(Any) {
        if has_search_terms(&node.text()) {
            let ngram = canonicalize_ngram(node.text().split_whitespace().collect::<Vec<&str>>());
            texts.insert(ngram);
        }
    }
    texts
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
        let mut iter = unigrams.iter();

        // We seed the result set with the first unigram result set.
        match index.unigram_match(&iter.next().unwrap()) {
            None => return None,
            Some(results) => results.into_iter().for_each(|p| {
                unigram_result_set.insert(p);
            }),
        }

        // All other unigram result sets will iteratively perform set intersection
        // with the result set, to generate the final set of result candidates
        for unigram in iter {
            match index.unigram_match(&unigram) {
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
        Some(ngram) => index.exact_ngram_match(&ngram),
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

fn index_texts<'i>(texts: HashSet<Vec<String>>, index: &mut Index<'i>, url: &'i str) {
    for ngram in texts.into_iter() {
        for unigram in ngram.clone().into_iter() {
            if index.unigrams.contains_key(&unigram) {
                index.unigrams.get_mut(&unigram).unwrap().insert(url);
            } else {
                let mut set = HashSet::with_capacity(1);
                set.insert(url);
                index.unigrams.insert(unigram, set);
            }
        }

        for bigram in ngram.clone().into_iter().tuple_windows::<(_, _)>() {
            let bigram_vec = vec![bigram.0, bigram.1];
            if index.ngrams.contains_key(&bigram_vec) {
                index.ngrams.get_mut(&bigram_vec).unwrap().insert(url);
            } else {
                let mut set = HashSet::with_capacity(1);
                set.insert(url);
                index.ngrams.insert(bigram_vec, set);
            }
        }

        for trigram in ngram.into_iter().tuple_windows::<(_, _, _)>() {
            let trigram_vec = vec![trigram.0, trigram.1, trigram.2];
            if index.ngrams.contains_key(&trigram_vec) {
                index.ngrams.get_mut(&trigram_vec).unwrap().insert(url);
            } else {
                let mut set = HashSet::with_capacity(1);
                set.insert(url);
                index.ngrams.insert(trigram_vec, set);
            }
        }
    }

    println!("unigram index length: {:#?}", index.unigrams.len());
    println!("ngram index length: {:#?}", index.ngrams.len());
}

fn has_search_terms(s: &str) -> bool {
    lazy_static! {
        static ref HAS_WORDS: Regex = Regex::new("\\S").unwrap();
    }
    return HAS_WORDS.is_match(s);
}

fn canonicalize_ngram(ngram: Vec<&str>) -> Vec<String> {
    ngram.into_iter().map(|s| s.to_lowercase()).collect()
}
