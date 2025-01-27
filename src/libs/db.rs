use anyhow::Context;
use rusqlite::{params, Connection, OptionalExtension};
use std::{path::PathBuf, u64};

use crate::Error;

pub fn create(db: &PathBuf) -> anyhow::Result<()> {
    // Create the table if it doesn't already exist
    let conn = Connection::open(db).with_context(|| "create db")?;
    conn.execute_batch("PRAGMA journal_mode = wal;")
        .expect("PRAGMA");
    conn.execute(
        "CREATE TABLE IF NOT EXISTS db_object (
                    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                    hashkey VARCHAR NOT NULL,
                    compressed BOOLEAN NOT NULL,
                    size INTEGER NOT NULL,
                    offset INTEGER NOT NULL,
                    length INTEGER NOT NULL,
                    pack_id INTEGER NOT NULL
                )",
        [],
    )?;

    conn.execute(
        "CREATE UNIQUE INDEX ix_db_object_hashkey ON db_object (hashkey)",
        [],
    )
    .with_context(|| "execute create SQL")?;

    Ok(())
}

pub fn print_table(db: &PathBuf) -> anyhow::Result<()> {
    // Open the database connection
    let conn = Connection::open(db)
        .with_context(|| format!("Open db {} for printing", db.to_string_lossy()))?;
    
    // Query to fetch all rows from the table
    let mut stmt = conn.prepare("SELECT id, hashkey, compressed, size, offset, length, pack_id FROM db_object")?;
    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,   // id
            row.get::<_, String>(1)?, // hashkey
            row.get::<_, bool>(2)?,   // compressed
            row.get::<_, i64>(3)?,   // size
            row.get::<_, i64>(4)?,   // offset
            row.get::<_, i64>(5)?,   // length
            row.get::<_, i64>(6)?,   // pack_id
        ))
    })?;

    // Print the rows
    println!("id | hashkey | compressed | size | offset | length | pack_id");
    println!("-----------------------------------------------------------");
    for row in rows {
        let (id, hashkey, compressed, size, offset, length, pack_id) = row?;
        println!(
            "{} | {} | {} | {} | {} | {} | {}",
            id, hashkey, compressed, size, offset, length, pack_id
        );
    }
    Ok(())
}

/// Counting number of packed objects and get ``total_size`` of their raw objects (size when not
/// compressed).
pub fn stat(db: &PathBuf) -> anyhow::Result<(u64, u64)> {
    let conn = Connection::open(db)
        .with_context(|| format!("Open db {} for auditing", db.to_string_lossy()))?;
    let mut stmt = conn.prepare("SELECT size FROM db_object")?;
    let rows = stmt
        .query([])
        .with_context(|| "query size of objects")?
        .mapped(|row| row.get::<_, u64>(0));

    let mut count = 0;
    let mut total_size = 0;
    for size in rows {
        total_size += size?;
        count += 1;
    }
    Ok((count, total_size))
}

pub fn insert_packin(conn: &Connection, packin: &PackEntry) -> anyhow::Result<()> {
    // NOTE: I use SQL: `INSERT OR IGNORE` to deal with duplicate keys
    let mut stmt = conn.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
    stmt.execute(params![
        packin.hashkey,
        packin.compressed,
        packin.size,
        packin.offset,
        packin.raw_size,
        packin.pack_id
    ])
    .with_context(|| format!("insert {packin:?} to db"))?;

    Ok(())
}

pub fn insert(
    conn: &Connection,
    hashkey: &str,
    compressed: bool,
    size: u64,
    offset: u64,
    length: u64,
    pack_id: u64,
) -> anyhow::Result<()> {
    // NOTE: I use SQL: `INSERT OR IGNORE` to deal with duplicate keys
    let mut stmt = conn.prepare_cached("INSERT OR IGNORE INTO db_object (hashkey, compressed, size, offset, length, pack_id) VALUES (?1, ?2, ?3, ?4, ?5, ?6)")?;
    stmt.execute(params![hashkey, compressed, size, offset, length, pack_id])
        .with_context(|| "insert to db")?;

    Ok(())
}

// XXX: this is almost duplicate as PObject, merge us
#[derive(Debug, Clone)]
pub struct PackEntry {
    pub hashkey: String,
    pub compressed: bool,
    pub raw_size: u64,
    pub size: u64,
    pub offset: u64,
    pub pack_id: u64,
}

// XXX: sub from select_multiple which only query once
pub fn select(conn: &Connection, hash_hex: &str) -> Result<Option<PackEntry>, Error> {
    let mut stmt = conn.
        prepare_cached("SELECT hashkey, compressed, size, offset, length, pack_id FROM db_object WHERE hashkey = ?1").
        map_err(|err| Error::SQLiteSelectError{source: err})?;
    let entry = stmt
        .query_row(params![hash_hex], |row| {
            Ok(PackEntry {
                hashkey: row.get(0)?,
                compressed: row.get(1)?,
                raw_size: row.get(2)?,
                offset: row.get(3)?,
                size: row.get(4)?,
                pack_id: row.get(5)?,
            })
        })
        .optional()
        .map_err(|err| Error::SQLiteSelectError { source: err })?;

    Ok(entry)
}
