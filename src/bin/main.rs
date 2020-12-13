#[macro_use]
extern crate lazy_static;

use folklore::*;

use gflags;
use index::Index;
use r2d2;
use std::io;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = gflags::parse();
    println!("Binary arguments: {:#?}", args);
    let db = database::connect_to_db();
    let mut config: Config = toml::from_str(std::include_str!("../../data.toml"))
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
