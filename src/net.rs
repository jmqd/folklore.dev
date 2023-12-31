use crate::document;
use itertools::Itertools;
use reqwest;
use select::document::Document;
use select::predicate::Name;
use serde::{Deserialize, Serialize};
use serde_json;

use std::time::{SystemTime, UNIX_EPOCH};
use std::collections::HashSet;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use tokio::task;
use tokio::time;
use url::{ParseError, Url};

gflags::define! {
    /// The output directory for saving the crawled text files.
    --output_dir <OUTPUT_DIR> = "/home/jmq/src/folklore.dev/output/"
}

#[derive(Serialize, Deserialize)]
pub struct SearchableDocument {
    pub url: String,
    pub title: String,
    pub fetched_at_linux_epoch_secs: u64,
    pub searchable_texts: Vec<String>,
    pub links_same_domain: Vec<String>,
}

pub async fn crawl(
    client: &'static reqwest::Client,
    root: reqwest::Url,
    allowed_domains: &'static HashSet<String>
) -> Vec<SearchableDocument> {
    let mut documents = Vec::new();
    let url = root.to_string();
    let root_document = fetch(client, &root, &url, 0, allowed_domains).await;

    if root_document.is_none() {
        eprintln!("Failed to get root_document.");
        return vec![]
    }

    let urls: Vec<Url> = root_document
        .as_ref()
        .expect("Failed to unwrap root_document")
        .links_same_domain
        .iter()
        .map(|s| Url::parse(s).expect("Failed to parse URL"))
        .collect();

    // TODO: Remove this duplication for the root element.
    let local_fs_path = Path::new(OUTPUT_DIR.flag).join(url_to_filename(url.as_str()));
    if let Some(writeable_doc) = root_document.as_ref() {
        eprintln!("Creating file at {:?}", local_fs_path.as_os_str());
        let mut file = File::create(&local_fs_path).expect("creating file");
        file.write_all(&serde_json::to_vec(writeable_doc).expect("serializing searchabledoc"))
            .expect("writing searchable doc");

        eprintln!(
            "Wrote a SearchableDocument to {}",
            &local_fs_path.to_string_lossy()
        )
    }

    documents.push(root_document);

    let mut handles: Vec<task::JoinHandle<Option<SearchableDocument>>> = vec![];
    for url in urls.into_iter().filter(|l| link_looks_interesting(l)) {
        let root = root.clone();
        let local_fs_path = Path::new(OUTPUT_DIR.flag).join(url_to_filename(url.as_str()));

        if let Ok(_metadata) = std::fs::metadata(&local_fs_path) {
            match serde_json::from_str(&std::fs::read_to_string(&local_fs_path).unwrap()) {
                Ok(f) => {
                    print!("H");
                    handles.push(task::spawn(async move { return f }));
                    continue;
                }
                Err(err) => {
                    println!("Failed to demarshal {}", local_fs_path.display());
                    println!("{:?}", err);
                }
            }
        }

        // Let's be nice to our friends' servers. If we need to go over the network
        // to get the document contents (i.e. cache miss), let's take a breather first.
        time::sleep(time::Duration::from_millis(64)).await;

        handles.push(task::spawn(async move {
            let searchable_doc = fetch(client, &root, &url.to_string(), 0, allowed_domains).await;

            if let Some(writeable_doc) = searchable_doc.as_ref() {
                eprintln!("Creating file at {:?}", local_fs_path.as_os_str());
                let mut file = File::create(&local_fs_path).expect("creating file");
                file.write_all(
                    &serde_json::to_vec(writeable_doc).expect("serializing searchabledoc"),
                )
                .expect("writing searchable doc");

                eprintln!(
                    "Wrote a SearchableDocument to {}",
                    &local_fs_path.to_string_lossy()
                )
            }

            searchable_doc
        }));
    }

    for handle in handles {
        documents.push(handle.await.expect("awaiting handle"));
    }

    documents.into_iter().flatten().collect()
}

pub fn url_to_filename(url: &str) -> String {
    format!("{}.json", url.replace("://", "_").replace("/", "_").trim_end_matches("_"))
}

fn link_looks_interesting(link: &reqwest::Url) -> bool {
    lazy_static! {
        static ref DISALLOWED_ENDINGS: Vec<&'static str> = vec![
            ".pdf", ".png", ".jpg", ".jpeg", ".gif", ".xml", ".rss", ".css", ".js", ".mov", ".svg", ".ps", ".z",
            ".zip", ".gz", ".rar", ".json", ".webp", ".mp4", ".mp3", ".bz2", ".tar" , ".js", ".mod", ".webm", ".iso",
            ".dsk"
        ];
    }

    let s = link.to_string().to_ascii_lowercase();
    DISALLOWED_ENDINGS
        .iter()
        .all(|ending| !s.ends_with(ending))
}

fn extract_links_same_domain(domain: &Url, document: &Document, allowed_domains: &HashSet<String>) -> Vec<Url> {
    let mut urls: Vec<Url> = vec![];
    document.find(Name("a")).for_each(|node| {
        let link = match node.attr("href") {
            None => None,
            Some(link) => Some(Url::parse(link)),
        };

        let link = match link {
            Some(Ok(mut link)) => {
                if link.origin() == domain.origin() && link.path() != domain.path() && allowed_domains.contains(domain.domain().unwrap()) {
                    link.set_query(None);
                    link.set_fragment(None);
                    Some(link)
                } else {
                    None
                }
            }
            Some(Err(e)) => match e {
                ParseError::RelativeUrlWithoutBase => {
                    match domain.join(node.attr("href").expect("unwrapping href attr")) {
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
            urls.push(link.expect("unwrapping link"));
        }
    });

    urls
}

pub async fn parse_document(
    resp: reqwest::Response,
    root: &reqwest::Url,
    url: &str,
    allowed_domains: &HashSet<String>
) -> Option<SearchableDocument> {
    if let Ok(body) = resp.text().await {
        let doc = document::resp_to_document(body).await?;
        let texts = document::extract_texts(&doc);

        Some(SearchableDocument {
            url: url.to_string(),
            fetched_at_linux_epoch_secs: SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
            title: doc.find(Name("title")).next().map(|t| t.text()).unwrap_or("TODO".to_string()),
            searchable_texts: texts.into_iter().unique().collect(),
            links_same_domain: extract_links_same_domain(root, &doc, allowed_domains)
                .into_iter()
                .map(|u| u.to_string())
                .collect(),
        })
    } else {
        None
    }
}

pub async fn fetch(
    client: &reqwest::Client,
    root: &reqwest::Url,
    url: &str,
    mut attempt: u64,
    allowed_domains: &HashSet<String>
) -> Option<SearchableDocument> {
    if !allowed_domains.contains(root.domain().unwrap()) {
        println!("{}", root);
        return None;
    }

    match client.get(url).send().await {
        Ok(resp) => parse_document(resp, root, url, allowed_domains).await,
        Err(e) => {
            while attempt < 4 {
                println!("Error when getting site (attempt {}): {}", attempt, e);
                attempt += 1;
                time::sleep(time::Duration::from_millis(attempt * 512)).await;
                match client.get(url).send().await {
                    Ok(resp) => {
                        return parse_document(resp, root, url, allowed_domains).await;
                    }
                    Err(e) => {
                        eprintln!("Error getting site: {:#?}", e);
                    }
                }
            }

            // We tried 4 times, but couldn't get the document.
            None
        }
    }
}
