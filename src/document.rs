use regex::Regex;
use select::document::Document;
use select::predicate::Any;

pub async fn resp_to_document(resp_body: String) -> Option<Document> {
    Some(Document::from(resp_body.as_ref()))
}

pub fn extract_texts(document: Option<&Document>) -> Option<Vec<String>> {
    document.map(|d| {
        d.find(Any)
            .into_iter()
            .filter_map(|n| find_searchable_text(n.text()))
            .collect()
    })
}

fn find_searchable_text(s: String) -> Option<String> {
    lazy_static! {
        static ref HAS_WORDS: Regex = Regex::new("\\S").unwrap();
    }

    if HAS_WORDS.is_match(&s) {
        Some(s)
    } else {
        None
    }
}
