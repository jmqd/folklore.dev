#[macro_use]
extern crate lazy_static;
use serde::{Deserialize, Serialize};
use r2d2_sqlite::SqliteConnectionManager;

pub mod database;
pub mod index;
pub mod document;
pub mod net;
pub mod query;

pub type ConnPool = r2d2::Pool<SqliteConnectionManager>;

#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub websites: Vec<Website>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Website {
    pub url: String,
}
