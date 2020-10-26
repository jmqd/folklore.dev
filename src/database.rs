use bincode;
use gflags;
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::NO_PARAMS;
use rusqlite::{params, Connection, OptionalExtension, Result};
use select::document::Document;
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

type ConnPool = r2d2::Pool<SqliteConnectionManager>;

gflags::define! {
    /// The path to the sqlite database.
    --database-path <DATABASE_PATH> = "/tmp/folklore.sqlite3"
}

pub fn connect_to_db() -> Arc<r2d2::Pool<SqliteConnectionManager>> {
    // If the database doesn't already exist, create the tables and such.
    if fs::metadata(DATABASE_PATH.flag).is_err() {
        build_database(DATABASE_PATH.flag).expect("Failed to build database");
    }
    let connection_manager = SqliteConnectionManager::file(DATABASE_PATH.flag);
    let sqlite_pool =
        r2d2::Pool::new(connection_manager).expect("Failed to create connection pool");
    Arc::new(sqlite_pool)
}

/// Create the schemas, tables, etc. for a valid empty database.
fn build_database(path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open(path)?;
    conn.execute(
        "CREATE TABLE IF NOT EXISTS
           documents (
             id STRING PRIMARY KEY,
             body STRING,
             extracted_text BLOB
         )",
        NO_PARAMS,
    )?;
    Ok(())
}

/// Saves the extracted text values of a given document.
pub fn save_texts(
    db: Arc<ConnPool>,
    document_id: &str,
    texts: &HashSet<Vec<String>>,
) -> Result<(), rusqlite::Error> {
    db.get()
        .expect("Failed to create save-texts query.")
        .execute(
            "INSERT OR REPLACE INTO
           documents (id, extracted_text)
         VALUES
           (?1, ?2)",
            params![document_id, bincode::serialize(&texts).unwrap()],
        )?;
    Ok(())
}

/// Saves the extracted text values of a given document.
pub fn save_document(
    db: Arc<ConnPool>,
    document_id: &str,
    body: &str,
) -> Result<(), rusqlite::Error> {
    db.get()
        .expect("Failed to create save-texts query.")
        .execute(
            "INSERT OR REPLACE INTO
           documents (id, body)
         VALUES
           (?1, ?2)",
            params![document_id, body],
        )?;
    Ok(())
}

/// Reads the extracted text values for a given document, if cached.
pub fn read_texts(db: Arc<ConnPool>, document_id: &str) -> Option<HashSet<Vec<String>>> {
    let conn = db.get().expect("Failed to get connection.");
    let mut stmt = conn
        .prepare(
            "SELECT
               extracted_text
             FROM
               documents
             WHERE
               extracted_text IS NOT NULL
               AND id = ?1",
        )
        .unwrap();
    let row: Option<Vec<u8>> = stmt
        .query_row(params![document_id], |row| row.get(0))
        .optional()
        .expect("Failed to query for read_texts.");

    match row {
        Some(bytes) => Some(bincode::deserialize(&bytes).unwrap()),
        None => None,
    }
}

/// Reads a complete cached document, if cached.
pub fn read_document(db: Arc<ConnPool>, document_id: &str) -> Option<Document> {
    let conn = db.get().expect("Failed to get connection.");
    let mut stmt = conn
        .prepare(
            "SELECT
               body
             FROM
               documents
            WHERE
               id = ?1
               AND body IS NOT NULL",
        )
        .unwrap();
    let body: Option<String> = stmt
        .query_row(params![document_id], |row| row.get(0))
        .optional()
        .expect("Failed to query for read_document.");

    match body {
        Some(body) => Some(Document::from(body.as_str())),
        None => None,
    }
}
