use regex::Regex;
use select::document::Document;
use select::predicate::Any;
use std::collections::HashSet;

pub async fn resp_to_document(resp_body: String) -> Option<Document> {
    Some(Document::from(resp_body.as_ref()))
}

pub fn extract_texts(document: Option<&Document>) -> Option<HashSet<Vec<String>>> {
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

fn has_search_terms(s: &str) -> bool {
    lazy_static! {
        static ref HAS_WORDS: Regex = Regex::new("\\S").unwrap();
    }
    return HAS_WORDS.is_match(s);
}

fn canonicalize_ngram(ngram: Vec<&str>) -> Vec<String> {
    ngram.into_iter().map(|s| s.to_lowercase()).collect()
}
