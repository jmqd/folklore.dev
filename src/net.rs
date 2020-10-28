use crate::database;
use crate::document;
use crate::ConnPool;
use reqwest;
use select::document::Document;
use select::predicate::Name;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::task;
use tokio::time;
use url::{ParseError, Url};

pub async fn crawl(
    db: Arc<ConnPool>,
    client: &'static reqwest::Client,
    root: reqwest::Url,
    visited: Arc<Mutex<HashSet<reqwest::Url>>>,
) -> Vec<(Option<HashSet<Vec<String>>>, String)> {
    let mut documents = Vec::new();
    let url = root.to_string();
    let root_document = fetch(db.clone(), client, &url, 0).await;
    documents.push((
        document::extract_texts(root_document.as_ref()),
        url.to_string(),
    ));
    let urls = root_document
        .map(|d| extract_links_same_domain(root, &d))
        .unwrap();

    let mut handles: Vec<task::JoinHandle<(Option<HashSet<Vec<String>>>, String)>> = vec![];
    for url in urls
        .into_iter()
        .filter(|l| link_looks_interesting(l) && !visited.lock().unwrap().contains(l))
    {
        let conn = db.clone();
        let cached_texts =
            tokio::task::block_in_place(|| database::read_texts(conn.clone(), &url.to_string()));

        // Let's be nice to our friends' servers. If we need to go over the network
        // to get the document contents (i.e. cache miss), let's take a breather first.
        if cached_texts.is_none() {
            time::delay_for(time::Duration::from_millis(64)).await;
        }

        handles.push(task::spawn(async move {
            match cached_texts {
                Some(texts) => {
                    println!("Cache hit! {:#?}", url);
                    (Some(texts), url.to_string())
                }
                None => {
                    let extracted_text = document::extract_texts(
                        fetch(conn.clone(), client, &url.to_string(), 0).await.as_ref(),
                    );

                    if extracted_text.is_some() {
                        tokio::task::block_in_place(|| {
                            database::save_texts(conn, &url.to_string(), &extracted_text.clone().unwrap());
                        });
                    }

                    (extracted_text, url.to_string())
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
        static ref DISALLOWED_ENDINGS: Vec<&'static str> = vec![
            ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".xml", ".rss", ".css", ".js", ".mov", ".svg",
            ".PDF", ".PNG", ".JPG", ".JPEG", ".GIF", ".XML", ".RSS", ".CSS", ".JS", ".MOV", ".SVG",
        ];
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

pub async fn fetch(
    db: Arc<ConnPool>,
    client: &reqwest::Client,
    url: &str,
    mut attempt: u64,
) -> Option<Document> {
    if let Some(document) = tokio::task::block_in_place(|| database::read_document(db.clone(), url))
    {
        return Some(document);
    }

    match client.get(url).send().await {
        Ok(resp) => {
            if let Ok(body) = resp.text().await {
                tokio::task::block_in_place(|| {
                    database::save_document(db, url, &body);
                });
                document::resp_to_document(body).await
            } else {
                None
            }
        }
        Err(e) => {
            println!("Error when getting site: {:#?}", e);
            while attempt < 4 {
                attempt += 1;
                time::delay_for(time::Duration::from_millis(attempt * 512)).await;
                let doc = match client.get(url).send().await {
                    Ok(resp) => {
                        let body = resp.text().await.unwrap();
                        tokio::task::block_in_place(|| {
                            database::save_document(db.clone(), url, &body)
                                .expect("Failed to write document to db.");
                        });
                        document::resp_to_document(body).await
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
