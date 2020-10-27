use crate::database;
use crate::net;
use crate::{ConnPool, Website};
use bimap::BiMap;
use itertools::Itertools;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time;
use url::Url;

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
pub struct Index {
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
    pub fn get_or_generate_word_code(&mut self, word: String) -> usize {
        match self.word_codes.get_by_left(&word) {
            Some(code) => *code,
            None => {
                self.word_codes.insert(word, self.word_codes.len());
                self.word_codes.len() - 1
            }
        }
    }

    pub fn get_or_generate_document_code(&mut self, document_id: String) -> usize {
        match self.document_codes.get_by_left(&document_id) {
            Some(code) => *code,
            None => {
                self.document_codes
                    .insert(document_id, self.document_codes.len());
                self.document_codes.len() - 1
            }
        }
    }

    pub fn index_texts(&mut self, document_id: String, texts: HashSet<Vec<String>>) {
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

    pub fn insert_unigram(&mut self, unigram: String, document_code: usize) {
        let code = self.get_or_generate_word_code(unigram);

        if self.unigrams.contains_key(&code) {
            self.unigrams.get_mut(&code).unwrap().insert(document_code);
        } else {
            let mut set = HashSet::with_capacity(1);
            set.insert(document_code);
            self.unigrams.insert(code, set);
        }
    }

    pub fn insert_ngram(&mut self, ngram: Vec<String>, document_code: usize) {
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

    pub fn unigram_match(&self, unigram: String) -> Option<HashSet<String>> {
        match self.word_codes.get_by_left(&unigram) {
            Some(code) => self.pass_page_results(self.unigrams.get(code)),
            None => None,
        }
    }

    pub fn ngram_match(&self, ngram: Vec<String>) -> Option<HashSet<String>> {
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

    pub fn pass_page_results(
        &self,
        page_results: Option<&HashSet<usize>>,
    ) -> Option<HashSet<String>> {
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

    pub fn exact_ngram_match(&self, ngram: Vec<String>) -> Option<HashSet<String>> {
        match ngram.len() {
            1 => self.unigram_match(ngram[0].clone()),
            _ => self.ngram_match(ngram),
        }
    }
}

pub async fn build_index<'i>(websites: &'i Vec<Website>, db: Arc<ConnPool>) -> Index {
    lazy_static! {
        static ref CLIENT: reqwest::Client = reqwest::Client::builder()
            .connect_timeout(time::Duration::from_millis(2048))
            .timeout(time::Duration::from_secs(64))
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
        for (texts, id) in net::crawl(db.clone(), &CLIENT, url.clone(), &visited).await {
            match texts {
                Some(texts) => {
                    let mut visited_url = Url::parse(&id).unwrap();
                    database::save_texts(db.clone(), &id, &texts).unwrap();
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
