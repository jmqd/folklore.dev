#[macro_use]
extern crate lazy_static;
use serde::{Deserialize, Serialize};

pub mod document;
pub mod net;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub websites: Vec<Website>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Website {
    pub url: String,
    #[serde(default = "default_recursively_crawl")]
    pub recursively_crawl: bool,
}

fn default_recursively_crawl() -> bool {
    true
}
