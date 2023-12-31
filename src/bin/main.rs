#[macro_use]
extern crate lazy_static;

use folklore::net::SearchableDocument;
use folklore::*;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use gflags;
use std::collections::HashSet;
use std::future::Future;
use std::io;
use std::sync::{Arc, Mutex};
use std::time;
use tokio::task;
use url::Url;

lazy_static! {
    static ref CLIENT: reqwest::Client = reqwest::Client::builder()
        .connect_timeout(time::Duration::from_millis(2048))
        .timeout(time::Duration::from_secs(64))
        .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
        .build()
        .unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = gflags::parse();
    println!("Binary arguments: {:#?}", args);
    let mut config: Config = toml::from_str(std::include_str!("../../data.toml"))
        .expect("Failed to deserialized config file.");

    run(&mut config).await;
    Ok(())
}

async fn run(config: &mut Config) {
    let visited = Arc::new(Mutex::new(HashSet::new()));
    let crawl_stack: Arc<Mutex<Vec<(reqwest::Url, bool, Arc<Mutex<HashSet<reqwest::Url>>>)>>> =
        Arc::new(Mutex::new(
            config
                .websites
                .iter()
                .map(|w| {
                    (
                        Url::parse(&w.url).unwrap(),
                        w.recursively_crawl,
                        visited.clone(),
                    )
                })
                .collect(),
        ));

    let mut handles = FuturesUnordered::<tokio::task::JoinHandle<()>>::new();

    loop {
        let mut crawl_envelope = crawl_stack.lock().unwrap().pop();
        let visited_ptr = visited.clone();

        if crawl_envelope.is_none() {
            while let Some(doc) = handles.next().await {
                println!("Future completed.")
            }

            if crawl_stack.lock().unwrap().is_empty() {
                break;
            } else {
                crawl_envelope = crawl_stack.lock().unwrap().pop();
            }
        }

        let crawl_envelope = crawl_envelope.unwrap();

        let crawl_stack_ptr = crawl_stack.clone();
        let visited_ref = visited.clone();
        handles.push(task::spawn(async move {
            for document in net::crawl(&CLIENT, crawl_envelope.0, crawl_envelope.1).await {
                let mut visited_url = Url::parse(&document.url).unwrap();
                visited_url.set_query(None);
                visited_url.set_fragment(None);
                if crawl_envelope.2.lock().unwrap().insert(visited_url.clone()) {
                    crawl_stack_ptr
                        .lock()
                        .unwrap()
                        .push((visited_url, true, visited_ref.clone()));
                }
            }
        }));

        println!("Finished all the crawling. Starting another generation.");
    }

    println!("Finished all the crawling.");
}

fn cli_testing() {
    let mut input = String::new();
    match io::stdin().read_line(&mut input) {
        Ok(n) => {
            println!("{} bytes read", n);
            println!("{}", input);
        }
        Err(error) => println!("error: {}", error),
    }
}
