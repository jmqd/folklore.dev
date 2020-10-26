#[macro_use]
extern crate lazy_static;

use bimap::BiMap;
use gflags;
use itertools::Itertools;
use regex::Regex;
use reqwest;
use select::document::Document;
mod database;
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;
use select::predicate::Any;
use select::predicate::Name;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::iter::Iterator;
use std::sync::Arc;
use std::{io, thread, time};
use tokio::task;
use url::{ParseError, Url};

type ConnPool = r2d2::Pool<SqliteConnectionManager>;

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
/// In the unigram and ngram index data structures, we don't actually store the
/// words and URLs there. That would consume much more memory. Instead, we
/// assign each word and URL a unique u64 number, then map everything in the
/// indexes to these u64 integers. How to translate between the u64 integer and
/// the corresponding value is recorded in the `document_codes` and `word_codes`
/// members. At query time, we translate everything to numbers, perform the
/// search, then at the last moment, after finding all the matches, we translate
/// the results back to Strings for the user.
struct Index {
    /// A mapping from all words to all documents those words appear in.
    pub unigrams: HashMap<usize, HashSet<usize>>,

    /// A mapping from all ngrams to all documents those ngrams appear in.
    pub ngrams: HashMap<Vec<usize>, HashSet<usize>>,

    /// A bi-mapping from document_ids (e.g. URL strings) to its integer code.
    pub document_codes: BiMap<String, usize>,

    /// A bi-mapping from words to thier integer code.
    pub word_codes: BiMap<String, usize>,
}

impl Index {
    fn get_or_generate_word_code(&mut self, word: String) -> usize {
        match self.word_codes.get_by_left(&word) {
            Some(code) => *code,
            None => {
                self.word_codes.insert(word, self.word_codes.len());
                self.word_codes.len() - 1
            }
        }
    }

    fn get_or_generate_document_code(&mut self, document_id: String) -> usize {
        match self.document_codes.get_by_left(&document_id) {
            Some(code) => *code,
            None => {
                self.document_codes
                    .insert(document_id, self.document_codes.len());
                self.document_codes.len() - 1
            }
        }
    }

    fn index_texts(&mut self, document_id: String, texts: HashSet<Vec<String>>) {
        println!("Indexing document {}", document_id);
        let document_code = self.get_or_generate_document_code(document_id);

        for ngram in texts.into_iter() {
            for unigram in ngram.clone().into_iter() {
                self.insert_unigram(unigram, document_code);
            }

            for bigram in ngram.clone().into_iter().tuple_windows::<(_, _)>() {
                let bigram_vec = vec![bigram.0, bigram.1];
                self.insert_ngram(bigram_vec, document_code);
            }
        }

        println!("indexed sites length: {:#?}", self.document_codes.len());
        println!("unigram index length: {:#?}", self.unigrams.len());
        println!("ngram index length: {:#?}", self.ngrams.len());
        println!("word_codes length: {:#?}", self.word_codes.len());
    }

    fn insert_unigram(&mut self, unigram: String, document_code: usize) {
        let code = self.get_or_generate_word_code(unigram);

        if self.unigrams.contains_key(&code) {
            self.unigrams.get_mut(&code).unwrap().insert(document_code);
        } else {
            let mut set = HashSet::with_capacity(1);
            set.insert(document_code);
            self.unigrams.insert(code, set);
        }
    }

    fn insert_ngram(&mut self, ngram: Vec<String>, document_code: usize) {
        let ngram_codes = ngram
            .into_iter()
            .map(|w| self.get_or_generate_word_code(w))
            .collect::<Vec<usize>>();

        if self.ngrams.contains_key(&ngram_codes) {
            self.ngrams
                .get_mut(&ngram_codes)
                .unwrap()
                .insert(document_code);
        } else {
            let mut set = HashSet::with_capacity(1);
            set.insert(document_code);
            self.ngrams.insert(ngram_codes, set);
        }
    }

    fn unigram_match(&self, unigram: String) -> Option<HashSet<String>> {
        match self.word_codes.get_by_left(&unigram) {
            Some(code) => self.pass_page_results(self.unigrams.get(code)),
            None => None,
        }
    }

    fn ngram_match(&self, ngram: Vec<String>) -> Option<HashSet<String>> {
        let ngram_codes = ngram
            .into_iter()
            .map(|w| self.word_codes.get_by_left(&w))
            .collect::<Vec<Option<&usize>>>();

        if ngram_codes.iter().any(|c| c.is_none()) {
            return None;
        }

        let ngram_codes: Vec<usize> = ngram_codes.into_iter().map(|c| *c.unwrap()).collect();

        self.pass_page_results(self.ngrams.get(&ngram_codes))
    }

    fn pass_page_results(&self, page_results: Option<&HashSet<usize>>) -> Option<HashSet<String>> {
        match page_results {
            // If we found some pages that matches the search query:
            // We copy all the page URLs into a return value for the caller.
            Some(page_results) => Some(
                page_results
                    .into_iter()
                    .map(|p| self.document_codes.get_by_right(p).unwrap().to_string())
                    .collect(),
            ),

            // Otherwise, their search query had no results.
            None => None,
        }
    }

    fn exact_ngram_match(&self, ngram: Vec<String>) -> Option<HashSet<String>> {
        match ngram.len() {
            1 => self.unigram_match(ngram[0].clone()),
            _ => self.ngram_match(ngram),
        }
    }
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
    let index = build_index(&config.websites, db).await;

    loop {
        cli_testing(&index);
    }
}

async fn build_index<'i>(websites: &'i Vec<Website>, db: Arc<ConnPool>) -> Index {
    lazy_static! {
        static ref CLIENT: reqwest::Client = reqwest::Client::builder()
            .connect_timeout(time::Duration::from_millis(2048))
            .timeout(time::Duration::from_millis(2048))
            .user_agent("folklore.dev\tI'm human, if a bit Rusty.\tJordan McQueen <j@jm.dev>")
            .build()
            .unwrap();
    }

    let mut index = Index {
        unigrams: HashMap::new(),
        ngrams: HashMap::new(),
        document_codes: BiMap::new(),
        word_codes: BiMap::new(),
    };

    for website in websites {
        for (texts, id) in crawl(db.clone(), &CLIENT, &website.url).await {
            match texts {
                Some(texts) => {
                    database::save_texts(db.clone(), &id, &texts).unwrap();
                    index.index_texts(id, texts);
                }
                None => (),
            };
        }
    }
    index
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

async fn crawl(
    db: Arc<ConnPool>,
    client: &'static reqwest::Client,
    url: &str,
) -> Vec<(Option<HashSet<Vec<String>>>, String)> {
    let mut documents = Vec::new();
    let root = Url::parse(url).unwrap();
    let root_document = fetch(client, url, 0).await;
    documents.push((extract_texts(root_document.as_ref()), url.to_string()));
    let urls = root_document
        .map(|d| extract_links_same_host(root, &d))
        .unwrap();

    let mut handles: Vec<task::JoinHandle<(Option<HashSet<Vec<String>>>, String)>> = vec![];
    for url in urls.into_iter().filter(|l| link_looks_interesting(l)) {
        let conn = db.clone();
        handles.push(task::spawn(async move {
            match database::read_texts(conn, &url.to_string()) {
                Some(texts) => {
                    println!("Cache hit! {:#?}", url);
                    (Some(texts), url.to_string())
                }
                None => {
                    // Let's be nice to our friends' servers.
                    thread::sleep(time::Duration::from_millis(64));
                    (
                        extract_texts(fetch(client, &url.to_string(), 0).await.as_ref()),
                        url.to_string(),
                    )
                }
            }
        }));
    }

    for handle in handles {
        documents.push(handle.await.unwrap());
    }

    documents
}

fn link_looks_interesting(link: &reqwest::Url) -> bool {
    let s = link.to_string();
    lazy_static! {
        static ref DISALLOWED_ENDINGS: Vec<&'static str> =
            vec![".pdf", ".png", ".jpg", ".jpeg", ".gif", ".xml", ".rss", ".css", ".js", ".mov"];
    }

    DISALLOWED_ENDINGS.iter().all(|ending| !s.ends_with(ending))
}

fn extract_links_same_host(domain: Url, document: &Document) -> Vec<Url> {
    let mut urls: Vec<Url> = vec![];
    document.find(Name("a")).for_each(|node| {
        let link = match node.attr("href") {
            None => None,
            Some(link) => Some(Url::parse(link)),
        };

        let link = match link {
            Some(Ok(link)) => {
                if link.host() == domain.host() {
                    Some(link)
                } else {
                    None
                }
            }
            Some(Err(e)) => match e {
                ParseError::RelativeUrlWithoutBase => {
                    match domain.join(node.attr("href").unwrap()) {
                        Ok(link) => Some(link),
                        Err(e) => {
                            println!("Error when trying to fix link: {:#?}", e);
                            None
                        }
                    }
                }
                _ => {
                    println!("Error with link: {:#?}", e);
                    None
                }
            },
            _ => None,
        };

        if link.is_some() {
            urls.push(link.unwrap());
        }
    });
    urls
}

async fn fetch(client: &reqwest::Client, url: &str, attempt: u64) -> Option<Document> {
    match client.get(url).send().await {
        Ok(resp) => resp_to_document(resp).await,
        Err(e) => {
            println!("Error when getting site: {:#?}", e);
            while attempt < 4 {
                thread::sleep(time::Duration::from_millis(attempt * 512));
                let doc = match client.get(url).send().await {
                    Ok(resp) => resp_to_document(resp).await,
                    _ => None,
                };

                if doc.is_some() {
                    return doc;
                }
            }
            None
        }
    }
}

async fn resp_to_document(resp: reqwest::Response) -> Option<Document> {
    let text = resp.text().await.unwrap();
    Some(Document::from(text.as_ref()))
}

fn extract_texts(document: Option<&Document>) -> Option<HashSet<Vec<String>>> {
    if document.is_none() {
        return None;
    }

    let mut texts = HashSet::new();
    for node in document.unwrap().find(Any) {
        if has_search_terms(&node.text()) {
            let ngram = canonicalize_ngram(node.text().split_whitespace().collect::<Vec<&str>>());
            texts.insert(ngram);
        }
    }
    Some(texts)
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

fn has_search_terms(s: &str) -> bool {
    lazy_static! {
        static ref HAS_WORDS: Regex = Regex::new("\\S").unwrap();
    }
    return HAS_WORDS.is_match(s);
}

fn canonicalize_ngram(ngram: Vec<&str>) -> Vec<String> {
    ngram.into_iter().map(|s| s.to_lowercase()).collect()
}
