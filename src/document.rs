use regex::Regex;
use select::document::Document;
use select::predicate::Text;

pub async fn resp_to_document(resp_body: String) -> Option<Document> {
    Some(Document::from(resp_body.as_ref()))
}

pub fn extract_texts(document: &Document) -> Vec<String> {
    document
        .find(Text)
        .into_iter()
        .filter_map(|n| find_searchable_text(n.text()))
        .collect()
}

fn find_searchable_text(s: String) -> Option<String> {
    let s = s.trim();

    lazy_static! {
        static ref HAS_WORDS: Regex = Regex::new("\\S").unwrap();
        static ref IGNORE_PREFIXES: Vec<&'static str> = vec![
            "body{",
            "/*",
            "if (",
            ".has-",
            ".widget",
            "img.",
            "{\"",
            "window.",
            "(function",
            "@font",
            "var ",
            "MathJax.",
            "<"
        ];
    }

    if HAS_WORDS.is_match(&s) && IGNORE_PREFIXES.iter().all(|prefix| !s.starts_with(prefix)) {
        Some(s.to_string())
    } else {
        None
    }
}
