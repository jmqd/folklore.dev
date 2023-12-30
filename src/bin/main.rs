#[macro_use]
extern crate lazy_static;

use folklore::*;

use gflags;
use std::collections::HashSet;
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
    let crawl_stack: Arc<Mutex<Vec<(reqwest::Url, Arc<Mutex<HashSet<reqwest::Url>>>)>>> =
        Arc::new(Mutex::new(
            config
                .websites
                .iter()
                .map(|w| (Url::parse(&w.url).unwrap(), visited.clone()))
                .collect(),
        ));
    let mut handles: Vec<task::JoinHandle<()>> = vec![];

    loop {
        let mut crawl_envelope = crawl_stack.lock().unwrap().pop();
        let visited_ptr = visited.clone();

        if crawl_envelope.is_none() {
            break;
        }

        let crawl_envelope = crawl_envelope.unwrap();

        handles.push(task::spawn(async move {
            for document in net::crawl(&CLIENT, crawl_envelope.0, visited_ptr.clone()).await {
                let mut visited_url = Url::parse(&document.url).unwrap();
                visited_url.set_query(None);
                visited_url.set_fragment(None);
                visited_ptr.lock().unwrap().insert(visited_url.clone());
            }
        }));
    }

    futures::future::join_all(handles).await;
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
