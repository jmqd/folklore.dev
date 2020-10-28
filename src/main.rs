#[macro_use]
extern crate lazy_static;

mod database;
mod document;
mod index;
mod net;
mod query;

use gflags;
use index::Index;
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;
use serde::{Deserialize, Serialize};
use std::io;
use std::sync::Arc;

type ConnPool = r2d2::Pool<SqliteConnectionManager>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub websites: Vec<Website>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Website {
    pub url: String,
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
    let index = index::build_index(&config.websites, db).await;
    println!(
        "indexed sites length: {:#?}",
        index.lock().unwrap().document_codes.len()
    );
    println!(
        "unigram index length: {:#?}",
        index.lock().unwrap().unigrams.len()
    );
    println!(
        "ngram index length: {:#?}",
        index.lock().unwrap().ngrams.len()
    );
    println!(
        "word_codes length: {:#?}",
        index.lock().unwrap().word_codes.len()
    );

    loop {
        cli_testing(&index.lock().unwrap());
    }
}

fn cli_testing(index: &Index) {
    let mut input = String::new();
    match io::stdin().read_line(&mut input) {
        Ok(n) => {
            println!("{} bytes read", n);
            println!("{}", input);
            println!("Query results: {:#?}", query::query(input, index));
        }
        Err(error) => println!("error: {}", error),
    }
}
