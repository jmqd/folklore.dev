use bincode;
use gflags;
use r2d2;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::NO_PARAMS;
use rusqlite::{params, Connection, OptionalExtension, Result};
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs;
use std::sync::Arc;

type ConnPool = r2d2::Pool<SqliteConnectionManager>;

gflags::define! {
    /// The path to the sqlite database.
    --database-path <DATABASE_PATH> = "/tmp/folklore.sqlite3"
}

struct DocumentTextRow {
    document_id: String,
    body: Vec<u8>,
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
           texts (
             document_id STRING PRIMARY KEY,
             body BLOB NOT NULL
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
           texts (document_id, body)
         VALUES
           (?1, ?2)",
            params![document_id, bincode::serialize(&texts).unwrap()],
        )?;
    Ok(())
}

/// Reads the extracted text values for a given document, if cached.
pub fn read_texts(db: Arc<ConnPool>, document_id: &str) -> Option<HashSet<Vec<String>>> {
    let conn = db.get().expect("Failed to get connection.");
    let mut stmt = conn
        .prepare("SELECT body FROM texts WHERE document_id = ?1")
        .unwrap();
    let body: Option<Vec<u8>> = stmt
        .query_row(params![document_id], |row| row.get(0))
        .optional()
        .expect("Failed to query for read_texts.");

    match body {
        Some(bytes) => Some(bincode::deserialize(&bytes).unwrap()),
        None => None,
    }
}

/// Reads the extracted text values for a given document, if cached.
pub fn read_all_texts(db: Arc<ConnPool>) -> HashMap<String, HashSet<Vec<String>>> {
    let conn = db.get().expect("Failed to get connection.");
    let mut stmt = conn
        .prepare(
            "SELECT
               document_id, body
             FROM
               texts
             WHERE
               document_id = ?1",
        )
        .unwrap();
    let results = stmt
        .query_map(NO_PARAMS, |row| {
            Ok(DocumentTextRow {
                document_id: row.get(0).unwrap(),
                body: row.get(1).unwrap(),
            })
        })
        .expect("Failed to query for read_texts.");

    let mut all_texts = HashMap::new();

    for row in results {
        let row = row.unwrap();
        all_texts.insert(row.document_id, bincode::deserialize(&row.body).unwrap());
    }
    all_texts
}
