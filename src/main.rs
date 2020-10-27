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
    println!("indexed sites length: {:#?}", index.document_codes.len());
    println!("unigram index length: {:#?}", index.unigrams.len());
    println!("ngram index length: {:#?}", index.ngrams.len());
    println!("word_codes length: {:#?}", index.word_codes.len());

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

    let mut crawl_stack: Vec<reqwest::Url> = websites
        .iter()
        .map(|w| Url::parse(&w.url).unwrap())
        .collect();
    let mut visited: HashSet<reqwest::Url> = HashSet::new();

    while crawl_stack.len() > 0 {
        let url = crawl_stack.pop().unwrap();
        for (texts, id) in crawl(db.clone(), &CLIENT, url.clone(), &visited).await {
            match texts {
                Some(texts) => {
                    let mut visited_url = Url::parse(&id).unwrap();
                    visited_url.set_query(None);
                    visited_url.set_fragment(None);
                    if visited.insert(visited_url.clone()) {
                        database::save_texts(db.clone(), &id, &texts).unwrap();

                        // Guard against traversing to other origins.
                        if visited_url.origin() == url.origin() {
                            crawl_stack.push(visited_url);
                            index.index_texts(id, texts);
                        }
                    }
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
    root: reqwest::Url,
    visited: &HashSet<reqwest::Url>,
) -> Vec<(Option<HashSet<Vec<String>>>, String)> {
    let mut documents = Vec::new();
    let url = root.to_string();
    let root_document = fetch(db.clone(), client, &url, 0).await;
    documents.push((extract_texts(root_document.as_ref()), url.to_string()));
    let urls = root_document
        .map(|d| extract_links_same_domain(root, &d))
        .unwrap();

    let mut handles: Vec<task::JoinHandle<(Option<HashSet<Vec<String>>>, String)>> = vec![];
    for url in urls
        .into_iter()
        .filter(|l| link_looks_interesting(l) && !visited.contains(l))
    {
        let conn = db.clone();
        let cached_texts = database::read_texts(conn.clone(), &url.to_string());

        // Let's be nice to our friends' servers. If we need to go over the network
        // to get the document contents (i.e. cache miss), let's take a breather first.
        if cached_texts.is_none() {
            thread::sleep(time::Duration::from_millis(64));
        }

        handles.push(task::spawn(async move {
            match cached_texts {
                Some(texts) => {
                    println!("Cache hit! {:#?}", url);
                    (Some(texts), url.to_string())
                }
                None => (
                    extract_texts(fetch(conn, client, &url.to_string(), 0).await.as_ref()),
                    url.to_string(),
                ),
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

fn extract_links_same_domain(domain: Url, document: &Document) -> Vec<Url> {
    let mut urls: Vec<Url> = vec![];
    document.find(Name("a")).for_each(|node| {
        let link = match node.attr("href") {
            None => None,
            Some(link) => Some(Url::parse(link)),
        };

        let link = match link {
            Some(Ok(mut link)) => {
                if link.origin() == domain.origin() && link.path() != domain.path() {
                    link.set_query(None);
                    link.set_fragment(None);
                    Some(link)
                } else {
                    None
                }
            }
            Some(Err(e)) => match e {
                ParseError::RelativeUrlWithoutBase => {
                    match domain.join(node.attr("href").unwrap()) {
                        Ok(mut link) => {
                            link.set_query(None);
                            link.set_fragment(None);
                            Some(link)
                        }
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

async fn fetch(
    db: Arc<ConnPool>,
    client: &reqwest::Client,
    url: &str,
    mut attempt: u64,
) -> Option<Document> {
    if let Some(document) = database::read_document(db.clone(), url) {
        return Some(document);
    }

    match client.get(url).send().await {
        Ok(resp) => {
            let body = resp.text().await.unwrap();
            database::save_document(db, url, &body).expect("Failed to write document to db.");
            resp_to_document(body).await
        }
        Err(e) => {
            println!("Error when getting site: {:#?}", e);
            while attempt < 4 {
                attempt += 1;
                thread::sleep(time::Duration::from_millis(attempt * 512));
                let doc = match client.get(url).send().await {
                    Ok(resp) => {
                        let body = resp.text().await.unwrap();
                        database::save_document(db.clone(), url, &body)
                            .expect("Failed to write document to db.");
                        resp_to_document(body).await
                    }
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

async fn resp_to_document(resp_body: String) -> Option<Document> {
    Some(Document::from(resp_body.as_ref()))
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
